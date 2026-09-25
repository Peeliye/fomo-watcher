"""Queue-to-Pons wiring and fail-closed boundaries; no network or send."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fomo.execution.journal import ExecutionJournal
from fomo.execution.pons_v4_once import TOKEN_OUT
from fomo.execution.pons_v4_signal_executor import PonsV4SignalExecutor
from fomo.execution.pons_v4_source import PonsV4SourceVerifier, TRANSFER_TOPIC
from fomo.execution.service import ExecutionService
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.fomo import FomoPushAdapter

ACTOR = "0x" + "22" * 20
TX_HASH = "0x" + "aa" * 32
BLOCK_HASH = "0x" + "bb" * 32


class FakeSourceRpc:
    def __init__(self):
        self.status = "0x1"
        self.chain_id = "0x1237"
        self.head = 102
        self.block_hash = BLOCK_HASH
        self.transfer_token = TOKEN_OUT
        self.transfer_to = ACTOR
        self.receipt_present = True
        self.calls: list[str] = []

    def call(self, method, params):
        self.calls.append(method)
        if method == "eth_chainId":
            return self.chain_id
        if method == "eth_getTransactionReceipt":
            if not self.receipt_present:
                return None
            return {"transactionHash": TX_HASH, "status": self.status,
                    "blockNumber": "0x64", "blockHash": BLOCK_HASH,
                    "logs": [{"address": self.transfer_token, "transactionHash": TX_HASH,
                              "blockHash": BLOCK_HASH, "topics": [TRANSFER_TOPIC,
                              "0x" + "0" * 64, "0x" + "0" * 24 + self.transfer_to[2:]],
                              "data": "0x" + f"{123:064x}"}]}
        if method == "eth_getBlockByNumber":
            return {"number": "0x64", "hash": self.block_hash}
        if method == "eth_blockNumber":
            return hex(self.head)
        raise AssertionError("unexpected_rpc_method")


def signal(**changes: object) -> TradeSignalEnvelope:
    now = datetime.now(timezone.utc).isoformat()
    data: dict[str, object] = {
        "source": "fomo_push", "source_event_id": "pons-test-1", "observed_at": now,
        "source_timestamp": now, "chain_id": "4663", "actor_wallet": ACTOR,
        "kol_id": "followed", "side": "buy", "token_in": "",
        "token_out": TOKEN_OUT, "source_amount": None, "estimated_usd": None,
        "tx_hash": TX_HASH, "signature": None, "log_index": None,
        "instruction_index": None, "confirmation_level": "pending",
        "reorg_key": "fomo:trade:" + "1" * 64, "decoder_version": "test-v1",
        "raw_payload_hash": "a" * 64,
    }
    data.update(changes)
    return TradeSignalEnvelope.create(**data)


def test_config_and_scope_reject():
    for amount, bps in [(0, 100), (True, 100), (1, -1), (1, 501)]:
        with pytest.raises(ValueError):
            PonsV4SignalExecutor(native_in_wei=amount, slippage_bps=bps)
    executor = PonsV4SignalExecutor(native_in_wei=10**12, slippage_bps=100,
                                    )
    for change, reason in [
        ({"chain_id": "1"}, "pons_signal_wrong_chain"),
        ({"side": "sell"}, "pons_signal_not_buy"),
        ({"token_out": "0x" + "11" * 20}, "pons_signal_wrong_token"),
        ({"source": "wallet_rpc_evm"}, "pons_signal_source_unsupported"),
    ]:
        with pytest.raises(ValueError, match=reason):
            executor.create_intent(signal(**change))
    valid = signal()
    with pytest.raises(ValueError, match="signalId must be deterministic"):
        executor.create_intent(TradeSignalEnvelope.from_dict({**valid.to_dict(), "signalId": "wrong"}))


@pytest.mark.parametrize("quote", [None, "WETH", "0x" + "11" * 20])
def test_real_fomo_payload_quote_is_untrusted_metadata(quote):
    payload = {"id": "fomo-1", "tradeId": "platform-trade-1", "type": "swap_buy",
               "createdAt": datetime.now(timezone.utc).isoformat(),
               "userId": "followed", "networkId": 4663, "tokenAddress": TOKEN_OUT}
    if quote is not None:
        payload["quoteToken"] = quote
    result = FomoPushAdapter({"followed"}).normalize(payload)
    assert result is not None
    assert result.confirmation_level == "pending"
    assert result.token_in == ""
    assert result.tx_hash is None and result.actor_wallet is None
    assert result.reorg_key.startswith("fomo:trade:")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        calls: list[dict] = []

        def fake_once(**kwargs):
            calls.append(kwargs)
            return {"simulation_success": True, "broadcast": False}

        executor = PonsV4SignalExecutor(native_in_wei=123, slippage_bps=100,

                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3", execute_once=fake_once)
        intent = executor.create_intent(result)
        assert intent.token_in == "native:4663" and intent.requested_asset_amount == "123"
        executed = executor.execute(intent, result, live_armed=False)
        assert executed["simulation_success"] is True
        assert executed["confirmationSource"] == "fomo_push_trusted"
        assert calls[0]["native_in_wei"] == 123 and calls[0]["token_out"] == TOKEN_OUT
        assert calls[0]["broadcast"] is False


@pytest.mark.parametrize("hash_value", [None, "trade-id-not-hash", "0x1234"])
def test_fomo_missing_hash_never_becomes_trade_id(hash_value):
    payload = {"id": "platform-event", "tradeId": "0x" + "ab" * 32,
               "type": "swap_buy", "createdAt": datetime.now(timezone.utc).isoformat(),
               "userId": "followed", "networkId": 4663, "wallet": ACTOR,
               "tokenAddress": TOKEN_OUT, "txHash": hash_value}
    result = FomoPushAdapter({"followed"}).normalize(payload)
    assert result is not None and result.tx_hash is None
    with pytest.raises(ValueError, match="pons_signal_source_unconfirmed"):
        PonsV4SourceVerifier(FakeSourceRpc(), minimum_confirmations=2).verify(result)


def test_fomo_missing_actor_wallet_blocks_receipt_evidence():
    item = signal(actor_wallet=None)
    with pytest.raises(ValueError, match="pons_signal_source_unconfirmed"):
        PonsV4SourceVerifier(FakeSourceRpc(), minimum_confirmations=2).verify(item)


@pytest.mark.parametrize("mutation,reason", [
    ({"receipt_present": False}, "pons_signal_source_unconfirmed"),
    ({"status": "0x0"}, "pons_signal_source_unconfirmed"),
    ({"block_hash": "0x" + "cc" * 32}, "pons_signal_source_reorged"),
    ({"head": 100}, "pons_signal_source_unconfirmed"),
    ({"transfer_token": "0x" + "11" * 20}, "pons_signal_target_transfer_missing"),
    ({"transfer_to": "0x" + "33" * 20}, "pons_signal_target_transfer_missing"),
    ({"chain_id": "0x1"}, "pons_signal_source_wrong_chain"),
])
def test_source_receipt_fail_closed(mutation, reason):
    rpc = FakeSourceRpc()
    for key, value in mutation.items():
        setattr(rpc, key, value)
    with pytest.raises(ValueError, match=reason):
        PonsV4SourceVerifier(rpc, minimum_confirmations=2).verify(signal())


def test_queue_consumes_once_simulates_and_never_sends():
    calls: list[dict] = []

    def fake_once(**kwargs):
        calls.append(kwargs)
        return {"simulation_success": True, "broadcast": False,
                "stagesMs": {"ethCall": 3}, "elapsedMs": 5}

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        executor = PonsV4SignalExecutor(native_in_wei=10**12, slippage_bps=100,

                                        allow_broadcast=True,
                                        ledger_path=root / "pons.sqlite3",
                                        economic_ledger_path=root / "economic.sqlite3",
                                        execute_once=fake_once)
        item = signal()
        assert queue.enqueue_envelope(item)
        assert not queue.enqueue_envelope(item)
        service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                   pons_v4_executor=executor)
        result = service.process_one()
        assert result is not None
        assert result["status"] == "processed"
        assert result["signalAccepted"] is True
        assert result["simulation_success"] is True
        assert result["broadcastRequested"] is True
        assert result["broadcastBlocked"] is True
        assert result["broadcastSent"] is False
        assert result["stagesMs"] == {"ethCall": 3}
        assert result["confirmationSource"] == "fomo_push_trusted"
        assert result["totalLatencyMs"] >= 0
        assert calls[0]["broadcast"] is False
        assert calls[0]["signal_id"] == item.signal_id
        assert calls[0]["native_in_wei"] == 10**12
        assert not (root / "pons.sqlite3").exists()
        assert service.process_one() is None
        assert len(calls) == 1
        journal.close()
        queue.close()


def test_shadow_platform_ids_do_not_claim_live_eligibility():
    calls: list[str] = []

    def fake_once(**kwargs):
        calls.append(kwargs["signal_id"])
        return {"simulation_success": True, "broadcast": False}

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3",
                                        execute_once=fake_once)
        first = signal(source_event_id="event-one")
        second = signal(source_event_id="event-two")
        assert executor.execute(executor.create_intent(first), first, live_armed=False)["simulation_success"]
        assert executor.execute(executor.create_intent(second), second, live_armed=False)["simulation_success"]
        assert calls == [first.signal_id, second.signal_id]
        assert not (root / "economic.sqlite3").exists()


def test_live_ca_claim_does_not_depend_on_trade_id_or_user():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        calls: list[str] = []

        def fake_once(**kwargs):
            calls.append(kwargs["signal_id"])
            kwargs["on_send_attempt"](7, "0x" + "ab" * 32)
            return {"simulation_success": True, "broadcast": True,
                    "receiptStatus": 1, "transactionHash": "0x" + "ab" * 32}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        allow_broadcast=True,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3",
                                        execute_once=fake_once)
        first = signal(source_event_id="one", reorg_key="fomo:event:one",
                       actor_wallet=None, tx_hash=None)
        second = signal(source_event_id="two", reorg_key="fomo:trade:" + "f" * 64,
                        kol_id="another-user")
        result = executor.execute(executor.create_intent(first), first, live_armed=True,
                                  source_reorged=lambda _: False)
        assert result["broadcastSent"] is True
        with pytest.raises(ValueError, match="duplicate_economic_event"):
            executor.execute(executor.create_intent(second), second, live_armed=True,
                             source_reorged=lambda _: False)
        assert calls == [first.signal_id]


def test_live_preflight_failure_releases_ca_for_new_signal():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        attempts: list[str] = []

        def fake_once(**kwargs):
            attempts.append(kwargs["signal_id"])
            if len(attempts) == 1:
                raise ValueError("pons_once_simulation_failed")
            kwargs["on_send_attempt"](7, "0x" + "ab" * 32)
            return {"simulation_success": True, "broadcast": True,
                    "receiptStatus": 1, "transactionHash": "0x" + "ab" * 32}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        allow_broadcast=True,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3",
                                        execute_once=fake_once)
        first = signal(source_event_id="first")
        second = signal(source_event_id="second")
        with pytest.raises(ValueError, match="simulation_failed"):
            executor.execute(executor.create_intent(first), first, live_armed=True,
                             source_reorged=lambda _: False)
        assert executor.execute(executor.create_intent(second), second, live_armed=True,
                                source_reorged=lambda _: False)["broadcastSent"] is True


def test_missing_wss_does_not_block_http_checked_mock_send(tmp_path):
    calls: list[str] = []

    def mock_send(**kwargs):
        calls.append("pre_sign")
        kwargs["pre_sign_guard"]()
        kwargs["on_send_attempt"](7, "0x" + "ab" * 32)
        calls.append("pre_send")
        kwargs["pre_send_guard"]()
        calls.append("send")
        return {"simulation_success": True, "broadcast": True,
                "transactionHash": "0x" + "ab" * 32, "receiptStatus": 1}

    executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                    allow_broadcast=True,
                                    economic_ledger_path=tmp_path / "economic.sqlite3",
                                    ledger_path=tmp_path / "signals.sqlite3",
                                    execute_once=mock_send)
    item = signal()
    result = executor.execute(executor.create_intent(item), item, live_armed=True,
                              source_reorged=lambda _: False)
    assert result["broadcastSent"] is True
    assert calls == ["pre_sign", "pre_send", "send"]


def test_queue_two_shadow_ids_do_not_consume_live_eligibility():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        first = signal(source_event_id="first")
        second = signal(source_event_id="second")
        assert queue.enqueue_envelope(first)
        assert queue.enqueue_envelope(second)
        calls: list[str] = []

        def fake_once(**kwargs):
            calls.append(kwargs["signal_id"])
            return {"simulation_success": True, "broadcast": False}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3", execute_once=fake_once)
        service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                   pons_v4_executor=executor)
        one = service.process_one()
        two = service.process_one()
        assert one is not None and one["status"] == "processed"
        assert two is not None and two["status"] == "processed"
        assert calls == [first.signal_id, second.signal_id]
        journal.close()
        queue.close()


def test_real_fomo_trade_id_does_not_suppress_shadow_runs():
    now = datetime.now(timezone.utc).isoformat()
    common = {"tradeId": "same-platform-trade", "type": "swap_buy", "createdAt": now,
              "userId": "followed", "networkId": 4663, "tokenAddress": TOKEN_OUT}
    first = FomoPushAdapter({"followed"}).normalize({**common, "id": "notification-1"})
    second = FomoPushAdapter({"followed"}).normalize({**common, "id": "notification-2"})
    assert first is not None and second is not None
    assert first.signal_id != second.signal_id and first.reorg_key == second.reorg_key
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        calls: list[str] = []

        def fake_once(**kwargs):
            calls.append(kwargs["signal_id"])
            return {"simulation_success": True, "broadcast": False}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3", execute_once=fake_once)
        assert executor.execute(executor.create_intent(first), first, live_armed=False)["simulation_success"]
        assert executor.execute(executor.create_intent(second), second, live_armed=False)["simulation_success"]
        assert calls == [first.signal_id, second.signal_id]


def test_source_cancel_during_quote_blocks_pre_sign_and_pre_send():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        reorged = False
        events: list[str] = []

        def fake_once(**kwargs):
            nonlocal reorged
            events.append("quote")
            reorged = True
            kwargs["pre_sign_guard"]()
            events.append("sign")
            kwargs["pre_send_guard"]()
            events.append("send")
            return {"simulation_success": True, "broadcast": True, "receiptStatus": 1}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        allow_broadcast=True,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3",
                                        execute_once=fake_once)
        item = signal()
        with pytest.raises(ValueError, match="pons_signal_source_reorged"):
            executor.execute(executor.create_intent(item), item, live_armed=True,
                             source_reorged=lambda _: reorged)
        assert events == ["quote"]


def test_missing_trade_id_is_not_a_broadcast_gate():
    item = signal(reorg_key="fomo:event:notification-1", actor_wallet=None, tx_hash=None)
    with tempfile.TemporaryDirectory() as directory:
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        allow_broadcast=True,
                                        economic_ledger_path=Path(directory) / "economic.sqlite3",
                                        ledger_path=Path(directory) / "signals.sqlite3",
                                        execute_once=lambda **kwargs: {
                                            "simulation_success": True, "broadcast": kwargs["broadcast"]})
        assert executor.execute(executor.create_intent(item), item, live_armed=False)["simulation_success"]
        with pytest.raises(ValueError, match="pons_once_broadcast_result_invalid"):
            executor.execute(executor.create_intent(item), item, live_armed=True,
                             source_reorged=lambda _: False)


@pytest.mark.parametrize("change,reason", [
    ({"chain_id": "1"}, "pons_signal_wrong_chain"),
    ({"side": "sell"}, "pons_signal_not_buy"),
    ({"token_out": "0x" + "11" * 20}, "pons_signal_wrong_token"),
    ({"source": "wallet_rpc_evm"}, "pons_signal_source_unsupported"),
])
def test_queue_rejects_without_execution(change, reason):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        item = signal(**change)
        assert queue.enqueue_envelope(item)
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        economic_ledger_path=root / "economic.sqlite3",
                                        execute_once=lambda **_: pytest.fail("must not execute"))
        service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                   pons_v4_executor=executor)
        result = service.process_one()
        assert result is not None
        assert result["status"] == "dropped"
        assert result["reason"] == reason
        journal.close()
        queue.close()


def test_queue_trusts_fomo_without_receipt_or_actor_wallet():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        item = signal(actor_wallet=None, tx_hash=None)
        assert queue.enqueue_envelope(item)
        simulated: list[str] = []
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        execute_once=lambda **kwargs: (
                                            simulated.append(kwargs["signal_id"])
                                            or {"simulation_success": True, "broadcast": False}))
        result = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                  pons_v4_executor=executor).process_one()
        assert result is not None
        assert result["status"] == "processed"
        assert result["confirmationSource"] == "fomo_push_trusted"
        assert result["broadcastSent"] is False
        assert simulated == [item.signal_id]
        journal.close()
        queue.close()


def test_raw_fomo_push_without_wallet_or_hash_reaches_simulation():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        now = datetime.now(timezone.utc).isoformat()
        payload = {"id": "platform-event", "tradeId": "platform-trade",
                   "type": "swap_buy", "createdAt": now, "userId": "followed",
                   "networkId": 4663, "tokenAddress": TOKEN_OUT, "quoteToken": "ETH"}
        item = FomoPushAdapter({"followed"}).normalize(payload, now)
        assert item is not None and item.actor_wallet is None and item.tx_hash is None
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        assert queue.enqueue(item.signal_id, "fomo_push", "raw_fomo", payload, now)
        calls: list[dict] = []

        def fake_once(**kwargs):
            calls.append(kwargs)
            return {"simulation_success": True, "broadcast": False}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,
                                        allow_broadcast=True,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        ledger_path=root / "signals.sqlite3", execute_once=fake_once)
        result = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                  pons_v4_executor=executor).process_one()
        assert result is not None and result["status"] == "processed"
        assert result["confirmationSource"] == "fomo_push_trusted"
        assert result["simulation_success"] is True
        assert result["broadcastBlocked"] is True and result["broadcastSent"] is False
        assert len(calls) == 1 and calls[0]["signal_id"] == item.signal_id
        assert calls[0]["broadcast"] is False
        journal.close()
        queue.close()


def test_simulation_failure_never_sends():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        item = signal()
        assert queue.enqueue_envelope(item)
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        economic_ledger_path=root / "economic.sqlite3",
                                        allow_broadcast=True,
                                        execute_once=lambda **_: (_ for _ in ()).throw(
                                            ValueError("pons_once_simulation_reverted")))
        service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                   pons_v4_executor=executor)
        result = service.process_one()
        assert result is not None
        assert result["status"] == "blocked"
        assert result["simulation_success"] is False
        assert result["broadcastSent"] is False
        journal.close()
        queue.close()


@pytest.mark.parametrize("recovery_state,expected_reason", [
    ("uncertain", "pons_once_send_result_uncertain"),
    ("failed", "pons_once_receipt_failed"),
])
def test_uncertain_retry_uses_persisted_hash_only(monkeypatch, recovery_state, expected_reason):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        ledger_path = root / "pons.sqlite3"
        from scripts import pons_v4_swap_once as pons
        ledger = pons.SignalLedger(ledger_path)
        ledger.claim(signal().signal_id, "0x" + "22" * 20, 1)
        ledger.mark_attempt(signal().signal_id, 7, "0x" + "ab" * 32, "0x" + "cd" * 32)
        ledger.mark(signal().signal_id, "uncertain")
        ledger.close()
        monkeypatch.setattr(pons, "_rpc_url", lambda: "http://unused.invalid")
        monkeypatch.setattr(pons, "PonsRpc", lambda _url: object())
        inspect_calls: list[str] = []

        def inspect(**kwargs):
            inspect_calls.append(kwargs["signal_id"])
            return {"state": recovery_state, "transactionHash": "0x" + "ab" * 32,
                    "receiptStatus": 0 if recovery_state == "failed" else None}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        allow_broadcast=True, ledger_path=ledger_path,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        execute_once=lambda **_: pytest.fail("must not re-send"),
                                        inspect_attempt=inspect)
        item = signal()
        result = executor.execute(executor.create_intent(item), item, live_armed=True,
                                  source_reorged=lambda _: False)
        assert result["recoveryState"] == recovery_state
        assert inspect_calls == [item.signal_id]
        queue = DurableSignalQueue(root / "queue.sqlite3")
        journal = ExecutionJournal(root / "execution.sqlite3")
        assert queue.enqueue_envelope(item)
        assert queue.acquire_service("crashed-owner")
        assert queue.claim("crashed-owner") is not None
        queue.db.execute("UPDATE signal_queue_service_lock SET lease_until_ms=0")
        queue.db.execute("UPDATE signal_queue SET lease_until_ms=0")
        service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                   pons_v4_executor=executor)
        recovered = service.process_one()
        assert recovered is not None
        assert recovered["attempts"] == 2
        assert recovered["status"] == "blocked"
        assert recovered["reason"] == expected_reason
        assert inspect_calls == [item.signal_id, item.signal_id]
        journal.close()
        queue.close()


def test_reorg_after_send_still_recovers_original_hash(monkeypatch):
    from scripts import pons_v4_swap_once as pons

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        item = signal()
        ledger_path = root / "signals.sqlite3"
        ledger = pons.SignalLedger(ledger_path)
        ledger.claim(item.signal_id, ACTOR, 1)
        ledger.mark_attempt(item.signal_id, 7, "0x" + "ab" * 32, "0x" + "cd" * 32)
        ledger.close()
        monkeypatch.setattr(pons, "_rpc_url", lambda: "http://unused.invalid")
        monkeypatch.setattr(pons, "PonsRpc", lambda _url: object())
        calls: list[str] = []

        def inspect(**kwargs):
            calls.append(kwargs["signal_id"])
            return {"state": "confirmed", "transactionHash": "0x" + "ab" * 32,
                    "receiptStatus": 1}

        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        ledger_path=ledger_path,
                                        economic_ledger_path=root / "economic.sqlite3",
                                        execute_once=lambda **_: pytest.fail("must not re-send"),
                                        inspect_attempt=inspect)
        result = executor.execute(executor.create_intent(item), item, live_armed=False,
                                  source_reorged=lambda _: True)
        assert result["sourceReorged"] is True
        assert result["transactionHash"] == "0x" + "ab" * 32
        assert calls == [item.signal_id]


def test_broadcast_requires_both_switches_and_success_receipt():
    for armed, allowed, expected in [(False, False, False), (False, True, False),
                                     (True, False, False), (True, True, True)]:
        calls: list[bool] = []

        def fake_once(**kwargs):
            calls.append(kwargs["broadcast"])
            return {"simulation_success": True, "broadcast": kwargs["broadcast"],
                    "receiptStatus": 1 if kwargs["broadcast"] else None,
                    "transactionHash": "0x" + "ab" * 32 if kwargs["broadcast"] else None}

        with tempfile.TemporaryDirectory() as directory:
            executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                            allow_broadcast=allowed,
                                            ledger_path=Path(directory) / "pons.sqlite3",
                                            economic_ledger_path=Path(directory) / "economic.sqlite3",
                                            execute_once=fake_once)
            item = signal()
            result = executor.execute(executor.create_intent(item), item, live_armed=armed,
                                      source_reorged=lambda _: False)
            assert calls == [expected]
            assert result["broadcastSent"] is expected
    with tempfile.TemporaryDirectory() as directory:
        executor = PonsV4SignalExecutor(native_in_wei=1, slippage_bps=100,

                                        allow_broadcast=True,
                                        ledger_path=Path(directory) / "pons.sqlite3",
                                        economic_ledger_path=Path(directory) / "economic.sqlite3",
                                        execute_once=lambda **_: {"simulation_success": True,
                                                                  "broadcast": True, "receiptStatus": 0})
        item = signal()
        with pytest.raises(ValueError, match="pons_once_broadcast_result_invalid"):
            executor.execute(executor.create_intent(item), item, live_armed=True,
                             source_reorged=lambda _: False)


def test_default_queue_path_is_cwd_independent(monkeypatch, capsys):
    from scripts import execution_service

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        expected = root / "repository" / "data" / "signal-queue.sqlite3"
        elsewhere = root / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.setattr(execution_service, "DEFAULT_QUEUE", expected)
        prior_cwd = Path.cwd()
        try:
            monkeypatch.chdir(elsewhere)
            monkeypatch.setattr(sys, "argv", ["execution_service", "--pons-v4", "--once",
                                              "--pons-native-in-wei", "1", "--pons-slippage-bps", "100",
                                              "--journal", str(root / "execution.sqlite3")])
            assert execution_service.main() == 0
        finally:
            monkeypatch.chdir(prior_cwd)
        output = capsys.readouterr().out.splitlines()
        assert json.loads(output[0])["queuePathVerified"] is True
        assert expected.is_file()
        assert not (elsewhere / "data" / "signal-queue.sqlite3").exists()


@pytest.mark.skipif(
    os.getenv("PONS_V4_REAL_RPC_TEST") != "1" or not os.getenv("PONS_REAL_FOMO_PAYLOAD_JSON"),
    reason="requires an actual Fomo event payload and explicit read-only RPC opt-in",
)
def test_real_queue_to_eth_call(monkeypatch, capsys):
    """Isolated local queue -> actual execution_service CLI -> real eth_call."""
    from scripts import execution_service

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        queue_path = root / "queue.sqlite3"
        journal_path = root / "execution.sqlite3"
        following_path = root / "following.json"
        payload = json.loads(os.environ["PONS_REAL_FOMO_PAYLOAD_JSON"])
        raw_event = {**(payload.get("body") or {}), **payload}
        user_id = str(raw_event["userId"])
        following_path.write_text(json.dumps({"updatedAt": int(time.time() * 1000),
                                              "followingIds": [user_id]}), encoding="utf-8")
        item = FomoPushAdapter({user_id}).normalize(payload)
        assert item is not None
        queue = DurableSignalQueue(queue_path)
        assert queue.enqueue(item.signal_id, "fomo_push", "raw_fomo", payload,
                             datetime.now(timezone.utc).isoformat())
        queue.close()
        monkeypatch.setattr(sys, "argv", ["execution_service", "--pons-v4",
                                          "--pons-native-in-wei", "1000000000000",
                                          "--pons-slippage-bps", "100",
                                          "--pons-economic-ledger", str(root / "economic.sqlite3"),
                                          "--queue", str(queue_path),
                                          "--journal", str(journal_path),
                                          "--following", str(following_path), "--once"])
        try:
            assert execution_service.main() == 0
        except Exception as error:
            raise AssertionError(f"real_rpc_e2e_failed:{type(error).__name__}") from None
        result = json.loads(capsys.readouterr().out.splitlines()[-1])
        assert result["status"] == "processed", result.get("reason")
        assert result["simulation_success"] is True
        assert result["broadcastSent"] is False
        assert result["broadcastBlocked"] is True
        assert "transactionHash" not in result
        journal = ExecutionJournal(journal_path)
        control = journal.db.execute("SELECT live_armed FROM execution_control WHERE singleton=1").fetchone()
        assert control is not None and control[0] == 0
        journal.close()
        print(json.dumps({"status": result["status"], "simulation_success": True,
                          "live_armed": False, "broadcastSent": False,
                          "totalLatencyMs": result["totalLatencyMs"],
                          "stagesMs": result["stagesMs"]}))
