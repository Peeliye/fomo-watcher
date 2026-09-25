"""Real SQLite control migration and operator gating; never signs or sends."""

from __future__ import annotations

import sqlite3
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fomo.execution import journal
from fomo.execution.journal import ExecutionJournal, migrate_execution_database
from fomo.execution.pons_v4_once import CHAIN_ID, ROUTE_ID, TOKEN_OUT
from fomo.execution.pons_v4_signal_executor import PonsV4SignalExecutor
from fomo.execution.rh_auto_executor import ROUTE_SCOPE, RhAutoExecutor
from fomo.execution.service import ExecutionService
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.fomo import FomoPushAdapter
from scripts import execution_control
from scripts import pons_v4_swap_once as pons


def _v7(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.executescript("""
        CREATE TABLE execution_control (
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          live_armed INTEGER NOT NULL CHECK(live_armed=0),
          circuit_breaker_tripped INTEGER NOT NULL,
          breaker_reason TEXT NOT NULL,
          consecutive_failures INTEGER NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT INTO execution_control VALUES(1,0,1,'existing_breaker',7,'earlier');
        CREATE TABLE existing_execution_data (id INTEGER PRIMARY KEY, evidence TEXT NOT NULL);
        INSERT INTO existing_execution_data VALUES(1,'preserve-this-row');
        PRAGMA user_version=7;
        """)


def _other_data(path: Path) -> list[tuple[int, str]]:
    with sqlite3.connect(path) as db:
        return db.execute("SELECT * FROM existing_execution_data ORDER BY id").fetchall()


def _control_and_scope(path: Path) -> tuple[tuple, list[tuple]]:
    with sqlite3.connect(path) as db:
        control = db.execute("SELECT * FROM execution_control WHERE singleton=1").fetchone()
        scope = db.execute("SELECT * FROM execution_operator_scope ORDER BY singleton").fetchall()
    assert control is not None
    return control, scope


def test_operator_cli_status_arm_status_disarm_status(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12

    def cli(*args: str) -> dict:
        completed = subprocess.run(
            [sys.executable, "-m", "scripts.execution_control", "--database", str(path), *args],
            capture_output=True, text=True, check=False,
        )
        assert completed.returncode == 0, completed.stdout
        return json.loads(completed.stdout)

    before = cli("status")
    assert before["liveArmed"] is False
    assert before["circuitBreakerTripped"] is True
    assert before["breakerReason"] == "startup_read_only"
    assert before["consecutiveFailures"] == 0
    armed_output = cli("arm", "--chain-id", str(CHAIN_ID), "--token-out", TOKEN_OUT,
                       "--native-in-wei", str(amount), "--route", ROUTE_ID,
                       "--confirm", execution_control.confirmation_text(amount))
    armed = cli("status")
    assert armed_output["liveArmed"] is True
    assert armed["liveArmed"] is True and armed["circuitBreakerTripped"] is False
    assert armed["breakerReason"] == "operator_armed" and armed["consecutiveFailures"] == 0
    assert armed["broadcastParameters"]["nativeInWei"] == str(amount)
    assert armed["broadcastParameters"]["chainId"] == CHAIN_ID
    assert armed["broadcastParameters"]["tokenOut"] == TOKEN_OUT
    assert armed["broadcastParameters"]["route"] == ROUTE_ID
    assert pons._live_armed(path, native_in_wei=amount) is True
    cli("disarm")
    final = cli("status")
    assert final["liveArmed"] is False and final["circuitBreakerTripped"] is False
    assert pons._live_armed(path, native_in_wei=amount) is False


def test_operator_auto_scope_requires_distinct_confirmation_and_disarms(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12
    executor = RhAutoExecutor(native_in_wei=amount, slippage_bps=100,
                              ledger_path=tmp_path / "auto.sqlite3",
                              execution_db_path=path, allow_broadcast=True)
    assert not executor._operator_scope()
    with pytest.raises(ValueError, match="execution_auto_arm_confirmation_invalid"):
        execution_control.arm_auto(path, chain_id=4663, native_in_wei=amount,
                                   route=ROUTE_SCOPE, confirmation="wrong")
    assert execution_control.status(path)["liveArmed"] is False
    execution_control.arm_auto(path, chain_id=4663, native_in_wei=amount,
                               route=ROUTE_SCOPE,
                               confirmation=execution_control.auto_confirmation_text(amount))
    assert executor._operator_scope()
    assert execution_control.status(path)["broadcastParameters"]["tokenOut"] == "*"
    assert pons._live_armed(path, native_in_wei=amount) is False
    execution_control.disarm(path)
    assert not executor._operator_scope()


def test_operator_auto_scope_expires_without_fresh_arm(tmp_path):
    from datetime import datetime, timedelta, timezone

    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12
    executor = RhAutoExecutor(native_in_wei=amount, slippage_bps=100,
                              ledger_path=tmp_path / "auto.sqlite3",
                              execution_db_path=path, allow_broadcast=True)
    execution_control.arm_auto(path, chain_id=4663, native_in_wei=amount,
                               route=ROUTE_SCOPE,
                               confirmation=execution_control.auto_confirmation_text(amount))
    assert executor._operator_scope()
    with sqlite3.connect(path) as db:
        db.execute("UPDATE execution_control SET updated_at=? WHERE singleton=1", (
            (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat(),))
    assert not executor._operator_scope()


def test_operator_arm_refuses_non_startup_breaker_and_preserves_rows(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.record_source_reorg("test-reorg-signal")
    instance.close()
    before = _control_and_scope(path)
    amount = 10**12
    with pytest.raises(ValueError, match="execution_arm_active_circuit_breaker"):
        execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                              native_in_wei=amount, route=ROUTE_ID,
                              confirmation=execution_control.confirmation_text(amount))
    assert _control_and_scope(path) == before


def test_operator_arm_refuses_startup_breaker_with_failures(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    # Inject only the failure count; never bypass the breaker through SQL.
    with sqlite3.connect(path) as db:
        db.execute("UPDATE execution_control SET consecutive_failures=1 WHERE singleton=1")
    before = _control_and_scope(path)
    amount = 10**12
    with pytest.raises(ValueError, match="execution_arm_active_failure_history"):
        execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                              native_in_wei=amount, route=ROUTE_ID,
                              confirmation=execution_control.confirmation_text(amount))
    assert _control_and_scope(path) == before


@pytest.mark.parametrize("change", [
    {"chain_id": 1},
    {"native_in_wei": 0},
    {"token_out": "0x" + "11" * 20},
    {"route": "wrong-route"},
    {"confirmation": "wrong-confirmation"},
])
def test_operator_wrong_scope_or_confirmation_preserves_all_rows(tmp_path, change):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12
    before = _control_and_scope(path)
    args = {"chain_id": CHAIN_ID, "token_out": TOKEN_OUT, "native_in_wei": amount,
            "route": ROUTE_ID, "confirmation": execution_control.confirmation_text(amount)}
    args.update(change)
    with pytest.raises(ValueError, match="execution_arm_confirmation_invalid"):
        execution_control.arm(path, **args)
    assert _control_and_scope(path) == before


def test_operator_rearm_after_normal_disarm_does_not_need_startup_breaker(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12
    args = {"chain_id": CHAIN_ID, "token_out": TOKEN_OUT, "native_in_wei": amount,
            "route": ROUTE_ID, "confirmation": execution_control.confirmation_text(amount)}
    execution_control.arm(path, **args)
    execution_control.disarm(path)
    assert execution_control.status(path)["circuitBreakerTripped"] is False
    execution_control.arm(path, **args)
    assert execution_control.status(path)["liveArmed"] is True
    execution_control.disarm(path)
    assert execution_control.status(path)["liveArmed"] is False


def test_operator_arm_sqlite_failure_rolls_back_scope_and_control(tmp_path):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TRIGGER reject_arm BEFORE UPDATE ON execution_control
                      WHEN NEW.live_armed=1 BEGIN SELECT RAISE(ABORT,'injected_arm_failure'); END""")
    before = _control_and_scope(path)
    amount = 10**12
    with pytest.raises(sqlite3.DatabaseError, match="injected_arm_failure"):
        execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                              native_in_wei=amount, route=ROUTE_ID,
                              confirmation=execution_control.confirmation_text(amount))
    assert _control_and_scope(path) == before


def test_v7_backup_transaction_and_rollback_copy(tmp_path):
    path = tmp_path / "execution.sqlite3"
    _v7(path)
    backup = migrate_execution_database(path)
    assert backup is not None and backup.is_file()
    with sqlite3.connect(backup) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert "CHECK(live_armed=0)" in db.execute(
            "SELECT sql FROM sqlite_master WHERE name='execution_control'"
        ).fetchone()[0]
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 8
        assert "CHECK(live_armed IN (0,1))" in db.execute(
            "SELECT sql FROM sqlite_master WHERE name='execution_control'"
        ).fetchone()[0]
        assert db.execute("SELECT * FROM execution_control").fetchone() == (
            1, 0, 1, "existing_breaker", 7, "earlier"
        )
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert _other_data(path) == [(1, "preserve-this-row")]
    restored = tmp_path / "restored.sqlite3"
    with sqlite3.connect(backup) as source, sqlite3.connect(restored) as target:
        source.backup(target)
    assert _other_data(restored) == _other_data(path)
    assert migrate_execution_database(path) is None


def test_migration_failure_rolls_back_original_schema_and_data(tmp_path, monkeypatch):
    path = tmp_path / "execution.sqlite3"
    _v7(path)

    def fail_after_table_swap(_db):
        raise RuntimeError("injected_failure")

    monkeypatch.setattr(journal, "_apply_schema", fail_after_table_swap)
    with pytest.raises(RuntimeError, match="injected_failure"):
        migrate_execution_database(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert "CHECK(live_armed=0)" in db.execute(
            "SELECT sql FROM sqlite_master WHERE name='execution_control'"
        ).fetchone()[0]
        assert db.execute("SELECT live_armed FROM execution_control").fetchone()[0] == 0
    assert _other_data(path) == [(1, "preserve-this-row")]
    backups = list((tmp_path / "backups").glob("*.sqlite3"))
    assert len(backups) == 1
    with sqlite3.connect(backups[0]) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_operator_confirmation_scope_switch_and_other_data_unchanged(tmp_path):
    path = tmp_path / "execution.sqlite3"
    journal_instance = ExecutionJournal(path)
    journal_instance.set_capital_snapshot("paper-main", 123)
    journal_instance.close()
    with sqlite3.connect(path) as db:
        capital_before = db.execute("SELECT * FROM capital_accounts").fetchall()
        fingerprints_before = journal._table_fingerprints(db)
    initial = execution_control.status(path)
    assert initial["liveArmed"] is False
    assert initial["circuitBreakerTripped"] is True
    assert initial["broadcastParameters"]["nativeInWei"] is None
    amount = 10**12
    with pytest.raises(ValueError, match="execution_arm_confirmation_invalid"):
        execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                              native_in_wei=amount, route=ROUTE_ID, confirmation="wrong")
    assert execution_control.status(path)["liveArmed"] is False
    execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    armed = execution_control.status(path)
    assert armed["liveArmed"] is True
    assert armed["circuitBreakerTripped"] is False
    assert armed["breakerReason"] == "operator_armed"
    assert armed["consecutiveFailures"] == 0
    assert armed["broadcastParameters"]["nativeInWei"] == str(amount)
    assert pons._live_armed(path, native_in_wei=amount) is True
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT * FROM capital_accounts").fetchall() == capital_before
        fingerprints_after = journal._table_fingerprints(db)
        assert all(fingerprints_after[name] == value for name, value in fingerprints_before.items()
                   if name not in {"execution_control", "execution_operator_scope"})
        fingerprints_after = journal._table_fingerprints(db)
        assert all(fingerprints_after[name] == value for name, value in fingerprints_before.items()
                   if name not in {"execution_control", "execution_operator_scope"})
    assert pons._live_armed(path, native_in_wei=amount) is True
    assert pons._live_armed(path, native_in_wei=amount + 1) is False
    execution_control.disarm(path)
    assert execution_control.status(path)["liveArmed"] is False
    assert pons._live_armed(path, native_in_wei=amount) is False
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT * FROM capital_accounts").fetchall() == capital_before


def test_armed_db_without_service_flag_never_calls_broadcast(tmp_path):
    path = tmp_path / "execution.sqlite3"
    journal_instance = ExecutionJournal(path)
    journal_instance.close()
    amount = 10**12
    execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    calls: list[bool] = []

    def fake_once(**kwargs):
        calls.append(kwargs["broadcast"])
        return {"simulation_success": True, "broadcast": False}

    def signal(group: str) -> TradeSignalEnvelope:
        return TradeSignalEnvelope.create(
            source="fomo_push", source_event_id="event-1",
            observed_at=datetime.now(timezone.utc).isoformat(),
            source_timestamp=datetime.now(timezone.utc).isoformat(), chain_id=str(CHAIN_ID),
            actor_wallet=None, kol_id="followed", side="buy", token_in="",
            token_out=TOKEN_OUT, reorg_key=group, decoder_version="test",
            raw_payload_hash="a" * 64, confirmation_level="pending",
        )

    executor = PonsV4SignalExecutor(native_in_wei=amount, slippage_bps=100,
                                    allow_broadcast=False, execution_db_path=path,
                                    ledger_path=tmp_path / "signals.sqlite3",
                                    economic_ledger_path=tmp_path / "economic.sqlite3",
                                    execute_once=fake_once)
    item = signal("fomo:trade:" + "a" * 64)
    result = executor.execute(executor.create_intent(item), item, live_armed=True)
    assert result["simulation_success"] is True and calls == [False]
    executor.allow_broadcast = True
    missing_trade = signal("fomo:event:event-1")
    with pytest.raises(ValueError, match="pons_once_broadcast_result_invalid"):
        executor.execute(executor.create_intent(missing_trade), missing_trade,
                         live_armed=True, source_reorged=lambda _: False)
    assert calls == [False, True]
    execution_control.disarm(path)
    assert execution_control.status(path)["liveArmed"] is False


def test_real_signer_entry_is_unreachable_without_service_flag_and_after_disarm(tmp_path, monkeypatch):
    path = tmp_path / "execution.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    amount = 10**12
    execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    fake_rpc = pons.PonsRpc.__new__(pons.PonsRpc)
    monkeypatch.setattr(pons, "_rpc_url", lambda: "https://unused.invalid")
    monkeypatch.setattr(pons, "PonsRpc", lambda _url: object())
    monkeypatch.setattr(pons, "_prepare_once", lambda **kwargs: (
        {"simulation_success": True, "broadcast": False}, object(), "0x" + "11" * 20))
    monkeypatch.setattr(pons, "_signer", lambda *args: pytest.fail("must not sign"))
    now = datetime.now(timezone.utc).isoformat()
    item = TradeSignalEnvelope.create(
        source="fomo_push", source_event_id="no-service-flag", observed_at=now,
        source_timestamp=now, chain_id="4663", actor_wallet=None, kol_id="followed",
        side="buy", token_in="", token_out=TOKEN_OUT, confirmation_level="pending",
        reorg_key="fomo:event:no-service-flag", decoder_version="test",
        raw_payload_hash="a" * 64,
    )
    executor = PonsV4SignalExecutor(native_in_wei=amount, slippage_bps=100,
                                    allow_broadcast=False, execution_db_path=path,
                                    ledger_path=tmp_path / "signals.sqlite3",
                                    economic_ledger_path=tmp_path / "economic.sqlite3")
    result = executor.execute(executor.create_intent(item), item, live_armed=True)
    assert result["simulation_success"] is True and result["broadcastSent"] is False
    assert not (tmp_path / "signals.sqlite3").exists()
    execution_control.disarm(path)
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        pons.execute_once(token_out=TOKEN_OUT, native_in_wei=amount, slippage_bps=100,
                          signal_id=item.signal_id, broadcast=True,
                          rpc=fake_rpc,
                          execution_db_path=path,
                          pre_sign_guard=lambda: None, pre_send_guard=lambda: None)


def test_real_sqlite_service_armed_but_no_broadcast_flag_never_sends(tmp_path):
    path = tmp_path / "execution.sqlite3"
    queue = DurableSignalQueue(tmp_path / "queue.sqlite3")
    journal_instance = ExecutionJournal(path)
    amount = 10**12
    execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    now = datetime.now(timezone.utc).isoformat()
    payload = {"id": "event-1", "tradeId": "trade-1", "type": "swap_buy",
               "createdAt": now, "userId": "followed", "networkId": CHAIN_ID,
               "tokenAddress": TOKEN_OUT}
    normalized = FomoPushAdapter({"followed"}).normalize(payload, now)
    assert normalized is not None
    assert queue.enqueue(normalized.signal_id, "fomo_push", "raw_fomo", payload, now)
    calls: list[bool] = []

    def fake_once(**kwargs):
        calls.append(kwargs["broadcast"])
        return {"simulation_success": True, "broadcast": False}

    executor = PonsV4SignalExecutor(native_in_wei=amount, slippage_bps=100,
                                    allow_broadcast=False, execution_db_path=path,
                                    ledger_path=tmp_path / "signals.sqlite3",
                                    economic_ledger_path=tmp_path / "economic.sqlite3",
                                    execute_once=fake_once)
    service = ExecutionService(queue, journal_instance, lambda: {"followed"},
                               lambda _: None, pons_v4_executor=executor)
    result = service.process_one()
    assert result is not None and result["simulation_success"] is True
    assert result["broadcastBlocked"] is True and result["broadcastSent"] is False
    assert calls == [False]
    execution_control.disarm(path)
    assert execution_control.status(path)["liveArmed"] is False
    journal_instance.close()
    queue.close()


def test_direct_gate_blocks_unarmed_breaker_and_scope_not_trade_id(tmp_path, monkeypatch):
    path = tmp_path / "execution.sqlite3"
    signal_db = tmp_path / "signals.sqlite3"
    instance = ExecutionJournal(path)
    instance.close()
    monkeypatch.setattr(pons, "_signer", lambda *args: pytest.fail("must not sign"))
    amount = 10**12
    base = {"native_in_wei": amount, "slippage_bps": 100,
            "signal_id": "sig:v1:fomo_push:" + "a" * 64,
            "broadcast": True, "rpc": object(), "ledger_path": signal_db,
            "execution_db_path": path, "fomo_trade_key": "fomo:trade:" + "b" * 64,
            "pre_sign_guard": lambda: None, "pre_send_guard": lambda: None}
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        pons.execute_once(**base)
    execution_control.arm(path, chain_id=CHAIN_ID, token_out=TOKEN_OUT,
                          native_in_wei=amount, route=ROUTE_ID,
                          confirmation=execution_control.confirmation_text(amount))
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        pons.execute_once(**{**base, "native_in_wei": amount + 1})
    monkeypatch.setattr(pons, "_wallet_profile", lambda: ("0x" + "11" * 20, {}))
    monkeypatch.setattr(pons, "_prepare_once", lambda **kwargs: (_ for _ in ()).throw(
        ValueError("pons_once_test_preflight_stopped")))
    with pytest.raises(ValueError, match="pons_once_test_preflight_stopped"):
        pons.execute_once(**{**base, "fomo_trade_key": None})
    assert signal_db.exists()
    instance = ExecutionJournal(path)
    instance.record_source_reorg("fault-test")
    instance.close()
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        pons.execute_once(**base)
    execution_control.disarm(path)
    assert execution_control.status(path)["liveArmed"] is False
    with pytest.raises(ValueError, match="pons_once_live_not_armed"):
        pons.execute_once(**base)
