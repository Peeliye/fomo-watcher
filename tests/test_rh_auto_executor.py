from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from fomo.execution.pons_v4_once import KEY, build_unsigned
from fomo.execution.rh_auto_executor import RhAutoExecutor
from fomo.execution.rh_auto_ledger import RhAutoLedger
from fomo.execution.journal import ExecutionJournal
from fomo.execution.service import ExecutionService
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import TradeSignalEnvelope

WALLET = "0x4ccb77f12801ee8853a9cc3782828678c8f5584b"
TOKEN = KEY.currency1
TX_HASH = "0x" + "1" * 64


def _signal(source_id: str = "event-1", *, token: str = TOKEN) -> TradeSignalEnvelope:
    now = datetime.now(timezone.utc).isoformat()
    return TradeSignalEnvelope.create(
        source="fomo_push", source_event_id=source_id, observed_at=now,
        source_timestamp=now, delivery_delay_ms=0, chain_id="4663",
        actor_wallet=None, kol_id="followed", side="buy", token_in="",
        token_out=token, confirmation_level="pending",
        reorg_key="test", decoder_version="test", raw_payload_hash="a" * 64,
    )


def _prepared() -> dict[str, object]:
    deadline = int(time.time()) + 120
    built = build_unsigned(amount_in=10**12, minimum_out=100,
                           deadline=deadline, nonce=2, gas_limit=300000,
                           priority_fee=1, maximum_fee=10, key=KEY)
    return {"chainId": 4663, "protocol": "pons_v4_reviewed_hook",
            "tokenOut": TOKEN, "block": 100, "blockHash": "0x" + "2" * 64,
            "deadline": deadline, "recipient": WALLET,
            "simulation_success": True, "unsignedTransaction": built}


class FakeRpc:
    def __init__(self, *, send_error: bool = False) -> None:
        self.methods: list[str] = []
        self.send_error = send_error
        self.wallet_profile = None

    def call(self, method: str, _params: object) -> object:
        self.methods.append(method)
        if method == "eth_estimateGas":
            return hex(100000)
        if method == "eth_call":
            return "0x"
        if method == "eth_sendRawTransaction":
            if self.send_error:
                raise ValueError("transport_failed")
            return TX_HASH
        raise AssertionError(method)

    def close(self) -> None:
        pass


def _executor(tmp_path: Path, rpc: FakeRpc, *, allow: bool) -> RhAutoExecutor:
    return RhAutoExecutor(native_in_wei=10**12, slippage_bps=100,
                          ledger_path=tmp_path / "auto.sqlite3",
                          execution_db_path=tmp_path / "execution.sqlite3",
                          allow_broadcast=allow, rpc_factory=lambda: rpc)


def test_wrong_chain_sell_or_token_rejected_before_rpc(tmp_path: Path) -> None:
    executor = _executor(tmp_path, FakeRpc(), allow=False)
    signal = _signal()
    for changes in ({"chain_id": "1"}, {"side": "sell"}, {"token_out": "bad"}):
        from dataclasses import replace
        with pytest.raises(ValueError, match="rh_auto_"):
            executor.create_intent(replace(signal, **changes))


def test_unarmed_simulates_without_signer_or_ledger(tmp_path: Path) -> None:
    rpc = FakeRpc()
    executor = _executor(tmp_path, rpc, allow=True)
    signal = _signal()
    with (patch("fomo.execution.rh_auto_executor.simulate_buy", return_value=_prepared()),
          patch("scripts.uniswap_v3_swap_once._wallet_profile", return_value=(WALLET, {})),
          patch.object(executor, "_operator_scope", return_value=False),
          patch("scripts.uniswap_v3_swap_once._signer") as signer):
        result = executor.execute(executor.create_intent(signal), signal,
                                  live_armed=False)
    assert result["simulation_success"] is True
    assert result["broadcastSent"] is False
    assert "eth_sendRawTransaction" not in rpc.methods
    assert not (tmp_path / "auto.sqlite3").exists()
    signer.assert_not_called()


@pytest.mark.parametrize("receipt_status,expected", [(1, "broadcast_confirmed"), (0, "broadcast_failed")])
def test_mock_send_receipt_and_per_ca_dedupe(tmp_path: Path, receipt_status: int, expected: str) -> None:
    rpc = FakeRpc()
    executor = _executor(tmp_path, rpc, allow=True)
    signal = _signal()
    class Signer:
        def sign(self, _built: object) -> bytes:
            return b"signed-test-bytes"
    with (patch("fomo.execution.rh_auto_executor.simulate_buy", return_value=_prepared()),
          patch("scripts.uniswap_v3_swap_once._wallet_profile", return_value=(WALLET, {})),
          patch.object(executor, "_operator_scope", return_value=True),
          patch.object(executor, "_fence"),
          patch("scripts.uniswap_v3_swap_once._signer", return_value=Signer()),
          patch("scripts.pons_v4_swap_once._verify_signed", return_value=TX_HASH),
          patch("scripts.pons_v4_swap_once._wait_receipt", return_value=receipt_status)):
        result = executor.execute(executor.create_intent(signal), signal,
                                  live_armed=True, source_reorged=lambda _: False)
        again = executor.execute(executor.create_intent(_signal("event-2")),
                                 _signal("event-2"), live_armed=True,
                                 source_reorged=lambda _: False)
    assert result["status"] == expected
    assert result["receiptStatus"] == receipt_status
    assert rpc.methods.count("eth_sendRawTransaction") == 1
    assert again["broadcastSent"] is False
    ledger = RhAutoLedger(tmp_path / "auto.sqlite3")
    try:
        item = ledger.lookup(TOKEN)
        assert item is not None
        assert item["state"] == ("confirmed" if receipt_status == 1 else "failed")
    finally:
        ledger.close()


def test_send_exception_persists_hash_and_never_retries(tmp_path: Path) -> None:
    rpc = FakeRpc(send_error=True)
    executor = _executor(tmp_path, rpc, allow=True)
    signal = _signal()
    class Signer:
        def sign(self, _built: object) -> bytes:
            return b"signed-test-bytes"
    with (patch("fomo.execution.rh_auto_executor.simulate_buy", return_value=_prepared()),
          patch("scripts.uniswap_v3_swap_once._wallet_profile", return_value=(WALLET, {})),
          patch.object(executor, "_operator_scope", return_value=True),
          patch.object(executor, "_fence"),
          patch("scripts.uniswap_v3_swap_once._signer", return_value=Signer()),
          patch("scripts.pons_v4_swap_once._verify_signed", return_value=TX_HASH)):
        result = executor.execute(executor.create_intent(signal), signal,
                                  live_armed=True, source_reorged=lambda _: False)
    assert result["status"] == "send_uncertain"
    ledger = RhAutoLedger(tmp_path / "auto.sqlite3")
    try:
        item = ledger.lookup(TOKEN)
        assert item is not None and item["state"] == "uncertain"
        assert item["txHash"] == TX_HASH
    finally:
        ledger.close()
    assert rpc.methods.count("eth_sendRawTransaction") == 1


def test_raw_fomo_queue_reaches_narrow_auto_executor_without_live(tmp_path: Path) -> None:
    queue = DurableSignalQueue(tmp_path / "queue.sqlite3")
    journal = ExecutionJournal(tmp_path / "execution.sqlite3")
    executor = _executor(tmp_path, FakeRpc(), allow=False)
    now = datetime.now(timezone.utc).isoformat()
    payload = {"id": "webhook-event", "tradeId": "trade-1", "userId": "followed",
               "type": "swap_buy", "networkId": 4663, "tokenAddress": TOKEN,
               "createdAt": now}
    signal = _signal("webhook-event")
    queue.enqueue(signal.signal_id, "fomo_push", "raw_fomo", payload, now)
    with patch.object(executor, "execute", return_value={
        "status": "simulated", "simulation_success": True,
        "broadcastSent": False, "protocol": "pons_v4_reviewed_hook",
    }) as execute:
        result = ExecutionService(queue, journal, lambda: {"followed"},
                                  lambda _: None, rh_auto_executor=executor).process_one()
    try:
        assert result is not None
        assert result["status"] == "processed"
        assert result["simulation_success"] is True
        assert result["broadcastSent"] is False
        assert result["selectedProtocol"] == "pons_v4_reviewed_hook"
        assert execute.call_args.kwargs["live_armed"] is False
    finally:
        journal.close()
        queue.close()
