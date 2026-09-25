"""Target-pool-only Pons fee, ABI, and fail-closed tests."""

from __future__ import annotations

import time
import sqlite3
import hashlib

import pytest
from coincurve import PrivateKey

from fomo.execution.direct_v4 import V4PoolKey, ZERO
from fomo.execution.evm_transaction import (decode_eip1559, keccak256,
                                            sign_eip1559)
from fomo.execution.pons_v4_hook import HOOK, PonsFeePolicy
from fomo.execution.pons_v4_once import (KEY, POOL_ID, TOKEN_OUT,
                                         build_unsigned, decode_calldata,
                                         decode_unsigned)
from fomo.execution.v4_math import quote_single_interval
from scripts import pons_v4_swap_once as once
from scripts.pons_v4_swap_once import _revert_reason


TRADE_KEY = "fomo:trade:" + "a" * 64


def _signal_id(label: str) -> str:
    return "sig:v1:fomo_push:" + hashlib.sha256(label.encode()).hexdigest()


def _broadcast_fields(label: str):
    return {"signal_id": _signal_id(label), "fomo_trade_key": TRADE_KEY,
            "pre_sign_guard": lambda: None, "pre_send_guard": lambda: None}


def _launches(*, registered: int = 1, memecoin: str = TOKEN_OUT,
              creator_tax: int = 200, hook_fee: int = 100) -> str:
    fields = [registered, 0, int(memecoin, 16), 0,
              1, 1, 2, creator_tax, 3000, 5000, hook_fee, 300, 0]
    return "0x" + "".join(f"{value:064x}" for value in fields)


def test_launches_complete_decode_and_fee_math():
    policy = PonsFeePolicy.from_launches_result(_launches(), key=KEY,
                                                 expected_pool_id=POOL_ID)
    assert policy.registered
    assert not policy.memecoin_is_currency0
    assert policy.memecoin == TOKEN_OUT
    assert policy.quote_token == ZERO
    assert policy.protocol_fee_share_bps == 3000
    assert policy.buyback_burn_bps == 5000
    assert policy.max_internal_price_impact_bps == 300
    assert policy.fee_components(10_000) == (100, 200, 9700)


@pytest.mark.parametrize("raw,key,pool", [
    (_launches(registered=0), KEY, POOL_ID),
    (_launches(memecoin="0x" + "11" * 20), KEY, POOL_ID),
    (_launches(creator_tax=2000), KEY, POOL_ID),
    (_launches(hook_fee=1100), KEY, POOL_ID),
    (_launches(), V4PoolKey(ZERO, TOKEN_OUT, 0, 200,
                           "0x" + "12" * 20), POOL_ID),
    (_launches(), KEY, "0x" + "ff" * 32),
])
def test_launches_reject_unregistered_wrong_key_or_fee(raw, key, pool):
    with pytest.raises(ValueError):
        PonsFeePolicy.from_launches_result(raw, key=key, expected_pool_id=pool)


def test_exact_target_unsigned_transaction_roundtrip_and_hook_data_empty():
    deadline = int(time.time()) + 120
    built = build_unsigned(amount_in=10**12, minimum_out=12345, deadline=deadline,
                           nonce=7, gas_limit=300_000, priority_fee=1,
                           maximum_fee=100)
    decoded = decode_unsigned(built)
    assert decoded["chainId"] == 4663
    assert decoded["poolId"] == POOL_ID
    assert decoded["tokenOut"] == TOKEN_OUT
    assert decoded["amountIn"] == decoded["msgValue"] == 10**12
    assert decoded["minOut"] == 12345
    assert decoded["nonce"] == 7
    assert decoded["recipientMode"] == "msg_sender"
    fields, _ = decode_eip1559(built.serialized, signed=False)
    assert fields.value == 10**12
    with pytest.raises(ValueError):
        decode_calldata(fields.data + b"\x00")
    assert _revert_reason("0x3b99b53d") == "SliceOutOfBounds()"


def test_other_nonzero_hook_is_not_encoded():
    assert KEY.hooks == HOOK
    with pytest.raises(ValueError):
        PonsFeePolicy.from_launches_result(
            _launches(), key=V4PoolKey(ZERO, TOKEN_OUT, 0, 200,
                                        "0x" + "12" * 20), expected_pool_id=POOL_ID)


def test_large_quote_crossing_tick_fails_closed():
    with pytest.raises(ValueError, match="v4_quote_tick_crossing_or_partial"):
        quote_single_interval(amount_in=10**18, zero_for_one=True,
                              sqrt_price_x96=1 << 96, tick=0, liquidity=10**18,
                              tick_spacing=200, lp_fee=0, protocol_fee=0,
                              nearest_initialized_tick=0)


class FakeRpc(once.PonsRpc):
    def __init__(self, *, status: str = "0x1", fail_send: bool = False,
                 head: int = 101, nonce: int = 3, balance: int = 10**18):
        self.status, self.fail_send = status, fail_send
        self.head, self.nonce, self.balance = head, nonce, balance
        self.send_count = 0
        self.last_hash = ""

    def call_batch(self, requests):
        return [self.call(method, params) for method, params in requests]

    def call(self, method, params):
        if method == "eth_blockNumber":
            return hex(self.head)
        if method == "eth_getBlockByNumber":
            return {"number": params[0], "hash": "0x" + "ab" * 32,
                    "timestamp": hex(int(time.time())), "baseFeePerGas": "0x1"}
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getTransactionCount":
            return hex(self.nonce)
        if method == "eth_getBalance":
            return hex(self.balance)
        if method == "eth_sendRawTransaction":
            self.send_count += 1
            if self.fail_send:
                raise ConnectionError("mock uncertain send")
            self.last_hash = "0x" + keccak256(bytes.fromhex(params[0][2:])).hex()
            return self.last_hash
        if method == "eth_getTransactionReceipt":
            if self.fail_send:
                return None
            return {"transactionHash": self.last_hash, "blockHash": "0x" + "ab" * 32,
                    "blockNumber": "0x64", "status": self.status}
        if method == "eth_getTransactionByHash":
            return None
        raise AssertionError(method)


def _mock_broadcast(monkeypatch, *, prepare_fails: bool = False):
    # Fixed public test-only key; never a wallet credential or funded key.
    key = bytes([7]) * 32
    public = PrivateKey(key).public_key.format(compressed=False)[1:]
    wallet = "0x" + keccak256(public)[-20:].hex()
    built = build_unsigned(amount_in=10**12, minimum_out=12345,
                           deadline=int(time.time()) + 120, nonce=3,
                           gas_limit=300_000, priority_fee=1, maximum_fee=100)
    summary = {"block": 100, "blockHash": "0x" + "ab" * 32,
               "deadline": int(time.time()) + 120,
               "simulation_success": True, "recipient": wallet}
    calls = {"signed": 0}

    def prepare(**kwargs):
        if prepare_fails:
            raise ValueError("pons_once_mock_simulation_failed")
        assert kwargs["confirmed"] is True
        return summary, built, wallet

    class Signer:
        def sign(self, transaction):
            calls["signed"] += 1
            return sign_eip1559(transaction.serialized, key)

    monkeypatch.setattr(once, "_prepare_once", prepare)
    monkeypatch.setattr(once, "_wallet_profile", lambda: (wallet, {"backend": "test"}))
    monkeypatch.setattr(once, "_signer", lambda profile, address: Signer())
    return wallet, calls


def _state(path, signal_id):
    with sqlite3.connect(path) as db:
        return db.execute("SELECT state,tx_hash,nonce FROM pons_signals WHERE signal_id=?",
                          (signal_id,)).fetchone()


def test_broadcast_gate_blocks_before_claim_or_sign(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: False)
    rpc = FakeRpc()
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s1"))
    assert not path.exists() and rpc.send_count == 0 and calls["signed"] == 0


def test_mock_sign_send_receipt_and_duplicate_rejected(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc()
    path = tmp_path / "signals.sqlite3"
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s1"))
    assert result["receiptStatus"] == 1 and result["broadcast"]
    assert rpc.send_count == 1 and calls["signed"] == 1
    assert _state(path, _signal_id("s1")) == ("confirmed", result["transactionHash"], 3)
    with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s1"))
    assert rpc.send_count == 1


def test_operator_once_requires_exact_confirmation_and_real_arm(monkeypatch, tmp_path):
    from fomo.execution.journal import ExecutionJournal
    from scripts import execution_control

    _, calls = _mock_broadcast(monkeypatch)
    rpc = FakeRpc()
    control_path = tmp_path / "execution.sqlite3"
    ledger_path = tmp_path / "signals.sqlite3"
    journal = ExecutionJournal(control_path)
    journal.close()
    signal_id = "operator:v1:one-small-buy"
    amount = 10**12
    def run(confirmation_text: str, *, name: str = signal_id):
        return once.execute_once(native_in_wei=amount, slippage_bps=100, signal_id=name,
                                 broadcast=True, rpc=rpc, ledger_path=ledger_path,
                                 execution_db_path=control_path,
                                 operator_confirmation=confirmation_text)

    confirmation = once.operator_confirmation_text(signal_id, amount)
    with pytest.raises(ValueError, match="pons_once_operator_confirmation_invalid"):
        run("wrong")
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        run(confirmation)
    assert calls["signed"] == 0 and rpc.send_count == 0 and not ledger_path.exists()
    execution_control.arm(control_path, chain_id=once.CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=once.ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    try:
        result = run(confirmation)
        assert result["receiptStatus"] == 1 and rpc.send_count == 1
        with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
            run(confirmation)
        assert rpc.send_count == 1
    finally:
        execution_control.disarm(control_path)
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        run(once.operator_confirmation_text("operator:v1:another-buy", amount),
            name="operator:v1:another-buy")
    assert rpc.send_count == 1


def test_operator_once_uncertain_send_cannot_repeat(monkeypatch, tmp_path):
    _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc(fail_send=True)
    signal_id = "operator:v1:uncertain-buy"
    confirmation = once.operator_confirmation_text(signal_id, 10**12)
    ledger_path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_send_result_uncertain"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100, signal_id=signal_id,
                          broadcast=True, rpc=rpc, ledger_path=ledger_path,
                          operator_confirmation=confirmation)
    with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100, signal_id=signal_id,
                          broadcast=True, rpc=rpc, ledger_path=ledger_path,
                          operator_confirmation=confirmation)
    assert rpc.send_count == 1


def test_submission_returns_before_receipt_and_recovers_by_hash(monkeypatch, tmp_path):
    _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    monkeypatch.setattr(once, "_wait_receipt", lambda *args, **kwargs: pytest.fail(
        "receipt must not block submission"))
    rpc = FakeRpc()
    path = tmp_path / "signals.sqlite3"
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               broadcast=True, rpc=rpc, ledger_path=path,
                               wait_for_receipt=False, **_broadcast_fields("async-receipt"))
    assert result["broadcast"] is True and result["receiptStatus"] is None
    assert result["submittedAtMs"] > 0 and result["signalToTxHashMs"] >= 0
    assert _state(path, _signal_id("async-receipt"))[0] == "submitted"
    assert once.inspect_attempt(signal_id=_signal_id("async-receipt"), rpc=rpc,
                                ledger_path=path)["receiptStatus"] == 1
    assert rpc.send_count == 1


def test_temp_sqlite_arm_and_explicit_service_flag_reach_mock_send_without_wss(monkeypatch, tmp_path):
    from datetime import datetime, timezone

    from fomo.execution.journal import ExecutionJournal
    from fomo.execution.pons_v4_signal_executor import PonsV4SignalExecutor
    from fomo.signals.envelope import TradeSignalEnvelope
    from scripts import execution_control

    wallet, calls = _mock_broadcast(monkeypatch)
    rpc = FakeRpc()
    monkeypatch.setattr(once, "_rpc_url", lambda: "https://unused.invalid")
    monkeypatch.setattr(once, "PonsRpc", lambda _url: rpc)
    control_path = tmp_path / "execution.sqlite3"
    journal = ExecutionJournal(control_path)
    journal.close()
    amount = 10**12
    execution_control.arm(control_path, chain_id=once.CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=once.ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    now = datetime.now(timezone.utc).isoformat()
    item = TradeSignalEnvelope.create(
        source="fomo_push", source_event_id="http-no-wss-mock", observed_at=now,
        source_timestamp=now, chain_id="4663", actor_wallet=None, kol_id="followed",
        side="buy", token_in="", token_out=TOKEN_OUT, confirmation_level="pending",
        reorg_key="fomo:event:http-no-wss-mock", decoder_version="test",
        raw_payload_hash="a" * 64,
    )
    executor = PonsV4SignalExecutor(native_in_wei=amount, slippage_bps=100,
                                    allow_broadcast=True, execution_db_path=control_path,
                                    ledger_path=tmp_path / "signals.sqlite3",
                                    economic_ledger_path=tmp_path / "economic.sqlite3")
    try:
        result = executor.execute(executor.create_intent(item), item, live_armed=True,
                                  source_reorged=lambda _: False)
        assert result["status"] == "broadcast_submitted"
        assert result["broadcastSent"] is True
        assert calls["signed"] == 1 and rpc.send_count == 1
        assert result["transactionHash"] == rpc.last_hash
    finally:
        execution_control.disarm(control_path)
    assert execution_control.status(control_path)["liveArmed"] is False


def test_fomo_source_guard_blocks_before_sign_and_before_send(monkeypatch, tmp_path):
    signal_id = "sig:v1:fomo_push:" + "a" * 64
    for stage in ("pre_sign", "pre_send"):
        _, calls = _mock_broadcast(monkeypatch)
        monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
        rpc = FakeRpc()
        path = tmp_path / f"{stage}.sqlite3"
        invoked: list[str] = []

        def before_sign():
            invoked.append("pre_sign")
            if stage == "pre_sign":
                raise ValueError("pons_signal_source_reorged")

        def before_send():
            invoked.append("pre_send")
            raise ValueError("pons_signal_source_reorged")

        with pytest.raises(ValueError, match="pons_signal_source_reorged"):
            once.execute_once(native_in_wei=10**12, slippage_bps=100,
                              signal_id=signal_id, broadcast=True, rpc=rpc,
                              ledger_path=path, fomo_trade_key=TRADE_KEY, pre_sign_guard=before_sign,
                              pre_send_guard=before_send)
        assert rpc.send_count == 0
        assert calls["signed"] == (0 if stage == "pre_sign" else 1)
        assert invoked == (["pre_sign"] if stage == "pre_sign" else ["pre_sign", "pre_send"])
        assert _state(path, signal_id)[0] == ("aborted" if stage == "pre_sign" else "send_attempted")


def test_fomo_broadcast_requires_source_guards(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc()
    with pytest.raises(ValueError, match="pons_once_source_guards_required"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          signal_id="sig:v1:fomo_push:" + "b" * 64,
                          fomo_trade_key=TRADE_KEY, broadcast=True, rpc=rpc,
                          ledger_path=tmp_path / "signals.sqlite3")
    assert calls["signed"] == 0 and rpc.send_count == 0


def test_uncertain_send_never_retried(monkeypatch, tmp_path):
    _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc(fail_send=True)
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_send_result_uncertain"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s2"))
    state, tx_hash, nonce = _state(path, _signal_id("s2"))
    assert state == "uncertain" and tx_hash.startswith("0x") and nonce == 3
    inspected = once.inspect_attempt(signal_id=_signal_id("s2"), rpc=rpc, ledger_path=path)
    assert inspected["transactionHash"] == tx_hash and inspected["state"] == "uncertain"
    with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s2"))
    with pytest.raises(ValueError, match="pons_once_nonce_conflict"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path,
                          **_broadcast_fields("s2-different-signal"))
    assert rpc.send_count == 1


def test_failed_receipt_not_success(monkeypatch, tmp_path):
    _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc(status="0x0")
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_receipt_failed"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s3"))
    assert _state(path, _signal_id("s3"))[0] == "failed"


@pytest.mark.parametrize("rpc", [FakeRpc(head=293), FakeRpc(nonce=4),
                                     FakeRpc(balance=1)])
def test_stale_nonce_or_balance_blocks_signing(monkeypatch, tmp_path, rpc):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s4"))
    assert calls["signed"] == 0 and rpc.send_count == 0
    assert _state(path, _signal_id("s4"))[0] == "aborted"


def test_failed_simulation_never_signs_or_sends(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch, prepare_fails=True)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    rpc = FakeRpc()
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_mock_simulation_failed"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s5"))
    assert calls["signed"] == 0 and rpc.send_count == 0
    assert _state(path, _signal_id("s5"))[0] == "aborted"


def test_receipt_timeout_is_uncertain_and_not_retried(monkeypatch, tmp_path):
    _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    monkeypatch.setattr(once, "_wait_receipt",
                        lambda *args, **kwargs: (_ for _ in ()).throw(
                            ValueError("pons_once_receipt_timeout")))
    rpc = FakeRpc()
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_receipt_timeout"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s6"))
    assert _state(path, _signal_id("s6"))[0] == "uncertain" and rpc.send_count == 1
    with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc, ledger_path=path, **_broadcast_fields("s6"))
    assert rpc.send_count == 1


def test_sqlite_claim_and_nonce_collision_are_atomic(tmp_path):
    path = tmp_path / "signals.sqlite3"
    first = once.SignalLedger(path)
    second = once.SignalLedger(path)
    try:
        first.claim("one", "0x" + "11" * 20, 10)
        with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
            second.claim("one", "0x" + "11" * 20, 10)
        second.claim("two", "0x" + "11" * 20, 10)
        first.mark_attempt("one", 3, "0x" + "aa" * 32, "0x" + "bb" * 32)
        with pytest.raises(ValueError, match="pons_once_nonce_conflict"):
            second.mark_attempt("two", 3, "0x" + "cc" * 32, "0x" + "dd" * 32)
        assert _state(path, "two") == ("claimed", None, None)
    finally:
        first.close()
        second.close()


def test_proven_pre_send_failure_preserves_hash_and_releases_only_local_nonce(tmp_path):
    path = tmp_path / "signals.sqlite3"
    wallet = "0x" + "11" * 20
    first_hash = "0x" + "aa" * 32
    ledger = once.SignalLedger(path)
    try:
        ledger.claim("first", wallet, 10)
        ledger.mark_attempt("first", 3, first_hash, "0x" + "bb" * 32)
        with pytest.raises(ValueError, match="pons_once_pre_send_release_conflict"):
            ledger.abort_before_send("first", "0x" + "cc" * 32,
                                     "operator_verified_pre_send_failure")
        ledger.abort_before_send("first", first_hash, "pre_send_check_failed")
        row = ledger.db.execute(
            "SELECT state,nonce,released_nonce,tx_hash,released_tx_hash,release_reason "
            "FROM pons_signals "
            "WHERE signal_id='first'").fetchone()
        assert row == ("not_submitted", None, 3, None, first_hash,
                       "pre_send_check_failed")
        ledger.claim("second", wallet, 10)
        ledger.mark_attempt("second", 3, first_hash, "0x" + "dd" * 32)
        assert ledger.lookup("second") == ("send_attempted", first_hash, 3)
        with pytest.raises(ValueError, match="pons_once_duplicate_signal"):
            ledger.claim("first", wallet, 10)
    finally:
        ledger.close()


@pytest.mark.parametrize("broadcast_started", [False, True])
def test_restart_with_unresolved_signed_attempt_keeps_nonce_locked(tmp_path,
                                                                     broadcast_started):
    path = tmp_path / "signals.sqlite3"
    wallet = "0x" + "11" * 20
    first = once.SignalLedger(path)
    first.claim("first", wallet, 10)
    first.mark_attempt("first", 3, "0x" + "aa" * 32, "0x" + "bb" * 32)
    if broadcast_started:
        first.mark_broadcast_started("first", "0x" + "aa" * 32)
    first.close()
    restarted = once.SignalLedger(path)
    try:
        restarted.claim("second", wallet, 10)
        with pytest.raises(ValueError, match="pons_once_nonce_conflict"):
            restarted.mark_attempt("second", 3, "0x" + "cc" * 32, "0x" + "dd" * 32)
    finally:
        restarted.close()


def test_signal_ledger_migration_backs_up_and_preserves_legacy_rows(tmp_path):
    path = tmp_path / "signals.sqlite3"
    legacy_schema = """CREATE TABLE pons_signals (
      signal_id TEXT PRIMARY KEY, wallet TEXT NOT NULL, chain_id INTEGER NOT NULL,
      token_out TEXT NOT NULL, amount_in_wei TEXT NOT NULL,
      state TEXT NOT NULL, nonce INTEGER, tx_hash TEXT UNIQUE,
      unsigned_hash TEXT, updated_at INTEGER NOT NULL,
      UNIQUE(wallet,chain_id,nonce)
    )"""
    with sqlite3.connect(path) as db:
        db.execute(legacy_schema)
        db.execute("INSERT INTO pons_signals VALUES(?,?,?,?,?,?,?,?,?,?)",
                   ("legacy", "0x" + "11" * 20, 4663, TOKEN_OUT, "10",
                    "send_attempted", 3, "0x" + "aa" * 32, "0x" + "bb" * 32, 1))
    with pytest.raises(ValueError, match="pons_once_signal_db_migration_required"):
        once.SignalLedger(path)
    backup = once.migrate_signal_ledger(path)
    assert backup is not None and backup.is_file()
    with sqlite3.connect(backup) as old, sqlite3.connect(path) as current:
        assert old.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert len(list(old.execute("PRAGMA table_info(pons_signals)"))) == 10
        assert len(list(current.execute("PRAGMA table_info(pons_signals)"))) == 13
        assert current.execute("SELECT nonce,tx_hash,released_nonce FROM pons_signals "
                               "WHERE signal_id='legacy'").fetchone() == (3, "0x" + "aa" * 32, None)
        restored = tmp_path / "restored.sqlite3"
        with sqlite3.connect(restored) as restore:
            old.backup(restore)
            assert restore.execute("SELECT * FROM pons_signals").fetchall() == (
                old.execute("SELECT * FROM pons_signals").fetchall())
    assert once.migrate_signal_ledger(path) is None


def test_signal_ledger_migration_from_intermediate_schema_preserves_release_fields(tmp_path):
    path = tmp_path / "intermediate.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE pons_signals (
          signal_id TEXT PRIMARY KEY, wallet TEXT NOT NULL, chain_id INTEGER NOT NULL,
          token_out TEXT NOT NULL, amount_in_wei TEXT NOT NULL,
          state TEXT NOT NULL, nonce INTEGER, tx_hash TEXT UNIQUE,
          unsigned_hash TEXT, updated_at INTEGER NOT NULL,
          released_nonce INTEGER, release_reason TEXT,
          UNIQUE(wallet,chain_id,nonce)
        )""")
        db.execute("INSERT INTO pons_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   ("old", "0x" + "11" * 20, 4663, TOKEN_OUT, "10",
                    "send_attempted", 0, "0x" + "aa" * 32, "0x" + "bb" * 32, 1,
                    None, None))
    backup = once.migrate_signal_ledger(path)
    assert backup is not None and backup.is_file()
    with sqlite3.connect(backup) as old, sqlite3.connect(path) as current:
        assert old.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert len(list(old.execute("PRAGMA table_info(pons_signals)"))) == 12
        assert current.execute("SELECT state,nonce,tx_hash,released_tx_hash "
                               "FROM pons_signals WHERE signal_id='old'").fetchone() == (
                                   "send_attempted", 0, "0x" + "aa" * 32, None)


def test_transient_freshness_batch_failure_rechecks_every_field(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)

    class FlakyBatchRpc(FakeRpc):
        checks = 0

        def call_batch(self, requests):
            if len(requests) == 6 and requests[0][0] == "eth_blockNumber":
                self.checks += 1
                if self.checks == 2:
                    raise ValueError("pons_once_batch_call_failed")
            return super().call_batch(requests)

    rpc = FlakyBatchRpc()
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               broadcast=True, rpc=rpc, ledger_path=tmp_path / "signals.sqlite3",
                               **_broadcast_fields("batch-fallback"))
    assert rpc.checks == 2 and rpc.send_count == 1 and calls["signed"] == 1
    assert result["receiptStatus"] == 1


def test_freshness_fallback_wrong_chain_stops_before_send(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)

    class WrongChainFallbackRpc(FakeRpc):
        checks = 0
        fallback = False

        def call_batch(self, requests):
            if len(requests) == 6 and requests[0][0] == "eth_blockNumber":
                self.checks += 1
                if self.checks == 2:
                    self.fallback = True
                    raise ValueError("pons_once_batch_call_failed")
            return super().call_batch(requests)

        def call(self, method, params):
            if self.fallback and method == "eth_chainId":
                return "0x1"
            return super().call(method, params)

    rpc = WrongChainFallbackRpc()
    signal_id = "operator:v1:bad-fallback"
    path = tmp_path / "signals.sqlite3"
    with pytest.raises(ValueError, match="pons_once_wrong_chain"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          signal_id=signal_id, broadcast=True, rpc=rpc,
                          ledger_path=path,
                          operator_confirmation=once.operator_confirmation_text(signal_id, 10**12))
    assert rpc.checks == 2 and rpc.send_count == 0 and calls["signed"] == 1
    assert _state(path, signal_id) == ("not_submitted", None, None)


def test_operator_pre_send_failure_releases_nonce_for_new_authorized_attempt(monkeypatch, tmp_path):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)

    class PreSendFaultRpc(FakeRpc):
        checks = 0

        def call_batch(self, requests):
            if len(requests) == 6 and requests[0][0] == "eth_blockNumber":
                self.checks += 1
                if self.checks == 2:
                    raise TimeoutError("test pre-send timeout")
            return super().call_batch(requests)

    path = tmp_path / "signals.sqlite3"
    first_id = "operator:v1:attempt-one"
    first_rpc = PreSendFaultRpc()
    with pytest.raises(TimeoutError, match="pre-send timeout"):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          signal_id=first_id, broadcast=True, rpc=first_rpc,
                          ledger_path=path,
                          operator_confirmation=once.operator_confirmation_text(first_id, 10**12))
    assert first_rpc.send_count == 0 and calls["signed"] == 1
    with sqlite3.connect(path) as db:
        row = db.execute("SELECT state,nonce,released_nonce,tx_hash,released_tx_hash "
                         "FROM pons_signals "
                         "WHERE signal_id=?", (first_id,)).fetchone()
    assert row[0] == "not_submitted" and row[1] is None and row[2] == 3
    assert row[3] is None and row[4].startswith("0x")
    second_id = "operator:v1:attempt-two"
    second_rpc = FakeRpc()
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               signal_id=second_id, broadcast=True, rpc=second_rpc,
                               ledger_path=path,
                               operator_confirmation=once.operator_confirmation_text(
                                   second_id, 10**12))
    assert result["receiptStatus"] == 1 and second_rpc.send_count == 1
    assert calls["signed"] == 2


def test_signed_wallet_scope_and_fee_cap(monkeypatch):
    built = build_unsigned(amount_in=10**12, minimum_out=12345,
                           deadline=int(time.time()) + 120, nonce=3,
                           gas_limit=300_000, priority_fee=1, maximum_fee=100)
    signed = sign_eip1559(built.serialized, bytes([7]) * 32)
    with pytest.raises(ValueError, match="pons_once_signer_wallet_mismatch"):
        once._verify_signed(signed, built.serialized, "0x" + "11" * 20)

    class BadFeeRpc(once.PonsRpc):
        def __init__(self):
            pass

        def call(self, method, params):
            if method == "eth_maxPriorityFeePerGas":
                return hex(once.MAX_PRIORITY_FEE_WEI + 1)
            if method == "eth_getBlockByNumber":
                return {"baseFeePerGas": "0x1"}
            raise AssertionError(method)

    with pytest.raises(ValueError, match="pons_once_gas_fee_cap_exceeded"):
        once._chain_fees(BadFeeRpc())


def test_default_mode_never_claims_or_sends(monkeypatch, tmp_path):
    rpc = FakeRpc()
    monkeypatch.setattr(once, "_prepare_once", lambda **kwargs: (
        {"simulation_success": True}, object(), "0x" + "11" * 20))
    path = tmp_path / "signals.sqlite3"
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               signal_id="read-only", rpc=rpc, ledger_path=path)
    assert result["simulation_success"] and rpc.send_count == 0 and not path.exists()
    assert not once._live_armed(tmp_path / "missing.sqlite3")


def test_warm_fixed_gas_simulates_final_transaction_without_estimate(monkeypatch):
    class ReadOnlyRpc(FakeRpc):
        _identity_verified = True

        def call(self, method, params):
            if method == "eth_estimateGas":
                pytest.fail("serial estimateGas is not in the warm signal path")
            if method == "eth_maxPriorityFeePerGas":
                return "0x1"
            if method == "eth_call":
                assert params[0]["gas"] == hex(once.FIXED_REVIEWED_GAS)
                return "0x"
            return super().call(method, params)

    class Policy:
        registered = True
        memecoin_is_currency0 = False
        memecoin = TOKEN_OUT
        quote_token = ZERO
        creator = "0x" + "11" * 20
        buyback_creator_recipient = "0x" + "11" * 20
        protocol_fee_recipient = "0x" + "11" * 20
        creator_tax_bps = 200
        protocol_fee_share_bps = 3000
        buyback_burn_bps = 5000
        hook_fee_bps = 100
        max_internal_price_impact_bps = 300
        buyback_enabled = False

        def fee_components(self, gross):
            return 100, 200, gross - 300

    monkeypatch.setattr(once, "quote_gross", lambda *args, **kwargs: (10_000, {}, Policy()))
    monkeypatch.setattr(once, "_wallet_profile", lambda: ("0x" + "11" * 20, {}))
    summary, built, _ = once._prepare_once(native_in_wei=10**12, slippage_bps=100,
                                            rpc=ReadOnlyRpc(head=once.INITIALIZE_BLOCK + 100), fast=True)
    assert summary["simulation_success"] is True
    assert summary["estimatedGas"] is None
    assert decode_unsigned(built)["gasLimit"] == once.FIXED_REVIEWED_GAS


def test_warm_quote_start_rejects_wrong_http_chain_before_pool_read(monkeypatch):
    class WrongChainRpc(FakeRpc):
        _identity_verified = True

        def call(self, method, params):
            if method == "eth_chainId":
                return "0x1"
            return super().call(method, params)

    monkeypatch.setattr(once, "quote_gross", lambda *args, **kwargs: pytest.fail(
        "wrong-chain quote must not read pool"))
    with pytest.raises(ValueError, match="pons_once_wrong_chain"):
        once._prepare_once(native_in_wei=10**12, slippage_bps=100,
                           rpc=WrongChainRpc(head=once.INITIALIZE_BLOCK + 100), fast=True)


@pytest.mark.parametrize("fault", ["wrong_chain", "reorg", "stale_block", "nonce", "balance", "base_fee", "timeout"])
def test_http_freshness_fault_before_sign_never_sends(monkeypatch, tmp_path, fault):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)

    class FaultRpc(FakeRpc):
        def call(self, method, params):
            if fault == "timeout" and method == "eth_blockNumber":
                raise TimeoutError("sanitized test timeout")
            if fault == "wrong_chain" and method == "eth_chainId":
                return "0x1"
            if fault == "reorg" and method == "eth_getBlockByNumber" and params[0] == "0x64":
                return {"number": "0x64", "hash": "0x" + "cd" * 32,
                        "baseFeePerGas": "0x1"}
            if fault == "base_fee" and method == "eth_getBlockByNumber" and params[0] == "pending":
                return {"baseFeePerGas": "0x100"}
            return super().call(method, params)

    rpc = FaultRpc(head=293 if fault == "stale_block" else 101,
                   nonce=4 if fault == "nonce" else 3,
                   balance=1 if fault == "balance" else 10**18)
    with pytest.raises((ValueError, TimeoutError)):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc,
                          ledger_path=tmp_path / f"{fault}.sqlite3",
                          **_broadcast_fields(f"fence-{fault}"))
    assert calls["signed"] == 0 and rpc.send_count == 0


@pytest.mark.parametrize("fault", ["deadline", "simulation"])
def test_http_fence_requires_deadline_and_successful_final_call(monkeypatch, tmp_path, fault):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    original_prepare = once._prepare_once

    def prepare(**kwargs):
        summary, built, wallet = original_prepare(**kwargs)
        summary = dict(summary)
        if fault == "deadline":
            summary["deadline"] = int(time.time()) + 1
        else:
            summary["simulation_success"] = False
        return summary, built, wallet

    monkeypatch.setattr(once, "_prepare_once", prepare)
    rpc = FakeRpc()
    with pytest.raises(ValueError):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          broadcast=True, rpc=rpc,
                          ledger_path=tmp_path / f"{fault}.sqlite3",
                          **_broadcast_fields(f"gate-{fault}"))
    assert calls["signed"] == 0 and rpc.send_count == 0


@pytest.mark.parametrize("fault", ["wrong_chain", "reorg", "timeout"])
def test_second_http_fence_fails_after_mock_sign_but_before_send(monkeypatch, tmp_path, fault):
    _, calls = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)

    class FaultRpc(FakeRpc):
        checks = 0

        def call_batch(self, requests):
            if len(requests) == 6 and requests[0][0] == "eth_blockNumber":
                self.checks += 1
                if self.checks == 2:
                    if fault == "timeout":
                        raise TimeoutError("sanitized test timeout")
                    values = super().call_batch(requests)
                    values[2 if fault == "wrong_chain" else 1] = (
                        "0x1" if fault == "wrong_chain" else
                        {"number": "0x64", "hash": "0x" + "cd" * 32})
                    return values
            return super().call_batch(requests)

    rpc = FaultRpc()
    path = tmp_path / f"{fault}.sqlite3"
    signal_id = _signal_id(f"second-{fault}")
    with pytest.raises((ValueError, TimeoutError)):
        once.execute_once(native_in_wei=10**12, slippage_bps=100,
                          signal_id=signal_id, broadcast=True, rpc=rpc,
                          ledger_path=path, fomo_trade_key=None,
                          pre_sign_guard=lambda: None, pre_send_guard=lambda: None)
    assert rpc.checks == 2 and calls["signed"] == 1 and rpc.send_count == 0
    assert _state(path, signal_id)[0] == "send_attempted"


def test_stale_block_rebuilds_exactly_once(monkeypatch, tmp_path):
    wallet, _ = _mock_broadcast(monkeypatch)
    monkeypatch.setattr(once, "_live_armed", lambda *args, **kwargs: True)
    calls = {"prepare": 0}
    built = build_unsigned(amount_in=10**12, minimum_out=12345,
                           deadline=int(time.time()) + 120, nonce=3,
                           gas_limit=300_000, priority_fee=1, maximum_fee=100)

    def prepare(**kwargs):
        calls["prepare"] += 1
        block = 100 if calls["prepare"] == 1 else 292
        return {"block": block, "blockHash": "0x" + "ab" * 32,
                "deadline": int(time.time()) + 120,
                "simulation_success": True}, built, wallet

    monkeypatch.setattr(once, "_prepare_once", prepare)
    rpc = FakeRpc(head=293)
    result = once.execute_once(native_in_wei=10**12, slippage_bps=100,
                               broadcast=True, rpc=rpc, ledger_path=tmp_path / "signals.sqlite3",
                               **_broadcast_fields("rebuild-once"))
    assert result["receiptStatus"] == 1
    assert calls["prepare"] == 2 and rpc.send_count == 1
