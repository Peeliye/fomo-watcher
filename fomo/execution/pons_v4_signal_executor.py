"""Narrow, opt-in queue consumer for the reviewed 4663 Pons V4 buy."""

from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Mapping

from fomo.signals.envelope import TradeSignalEnvelope, deterministic_signal_id
from fomo.signals.strategy import ExecutionIntent
from fomo.execution.pons_v4_once import KEY, POOL_ID, TOKEN_OUT, checked_key
from .pons_v4_economic_ledger import ECONOMIC_DB, PonsV4EconomicLedger
from scripts import pons_v4_swap_once as pons

MAX_LOCAL_SEND_MS = 15_000


class PonsV4SignalExecutor:
    def __init__(self, *, native_in_wei: int, slippage_bps: int,
                 allow_broadcast: bool = False, ledger_path: Path = pons.SIGNAL_DB,
                 economic_ledger_path: Path = ECONOMIC_DB,
                 execution_db_path: Path = pons.EXECUTION_DB,
                 execute_once: Callable[..., Mapping[str, Any]] = pons.execute_once,
                 inspect_attempt: Callable[..., Mapping[str, Any]] = pons.inspect_attempt) -> None:
        if type(native_in_wei) is not int or not 0 < native_in_wei < 1 << 128:
            raise ValueError("pons_signal_native_in_wei_invalid")
        if type(slippage_bps) is not int or not 0 <= slippage_bps <= 500:
            raise ValueError("pons_signal_slippage_bps_invalid")
        checked_key(KEY)
        if KEY.pool_id != POOL_ID:
            raise ValueError("pons_signal_pool_identity_invalid")
        self.native_in_wei = native_in_wei
        self.slippage_bps = slippage_bps
        self.allow_broadcast = allow_broadcast
        self.ledger_path = Path(ledger_path)
        self.economic_ledger_path = Path(economic_ledger_path)
        self.execution_db_path = Path(execution_db_path)
        self._execute_once = execute_once
        self._inspect_attempt = inspect_attempt
        self._rpc: pons.PonsRpc | None = None
        self._receipt_stop = threading.Event()
        self._receipt_thread: threading.Thread | None = None

    def prewarm(self) -> int:
        """Pin static identity and reuse the authenticated HTTP connection."""
        import time

        started = time.monotonic()
        rpc = pons.PonsRpc(pons._rpc_url())
        try:
            if pons._quantity(rpc.call("eth_chainId", [])) != 4663:
                raise ValueError("pons_once_wrong_chain")
            header = pons._pinned_header(rpc)
            pons._verify_static_identity(rpc, hex(pons._quantity(header["number"])))
            rpc.wallet_profile = pons._wallet_profile()
            self._rpc = rpc
            if self.allow_broadcast:
                self._receipt_thread = threading.Thread(target=self._receipt_loop,
                                                        name="pons-v4-receipts", daemon=True)
                self._receipt_thread.start()
            return int((time.monotonic() - started) * 1000)
        except BaseException:
            rpc.close()
            raise

    def close(self) -> None:
        self._receipt_stop.set()
        if self._receipt_thread is not None:
            self._receipt_thread.join(timeout=5)
            self._receipt_thread = None
        if self._rpc is not None:
            self._rpc.close()
            self._rpc = None

    def _receipt_loop(self) -> None:
        rpc: pons.PonsRpc | None = None
        try:
            while not self._receipt_stop.wait(2):
                if not self.ledger_path.is_file():
                    continue
                ledger = pons.SignalLedger(self.ledger_path)
                try:
                    pending = ledger.db.execute(
                        "SELECT signal_id FROM pons_signals "
                        "WHERE state IN ('broadcast_started','submitted','uncertain') "
                        "AND tx_hash IS NOT NULL LIMIT 100"
                    ).fetchall()
                finally:
                    ledger.close()
                if not pending:
                    continue
                if rpc is None:
                    rpc = pons.PonsRpc(pons._rpc_url())
                for (signal_id,) in pending:
                    if self._receipt_stop.is_set():
                        break
                    try:
                        recovery = self._inspect_attempt(signal_id=signal_id, rpc=rpc,
                                                         ledger_path=self.ledger_path)
                        if recovery.get("state") in {"confirmed", "failed", "uncertain"}:
                            economic = PonsV4EconomicLedger(self.economic_ledger_path)
                            try:
                                economic.transition(f"4663:{TOKEN_OUT}", signal_id,
                                                    {"failed": "receipt_failed"}.get(
                                                        str(recovery["state"]), str(recovery["state"])))
                            finally:
                                economic.close()
                    except Exception:
                        # Never retry a send, and never print transport errors or URLs.
                        continue
        except Exception:
            # The live path remains fail-closed if the receipt worker fails;
            # persistent signed hashes can be inspected after restart.
            pass
        finally:
            if rpc is not None:
                rpc.close()

    def create_intent(self, signal: TradeSignalEnvelope) -> ExecutionIntent:
        if signal.signal_id != deterministic_signal_id(signal.source, signal.source_event_id):
            raise ValueError("pons_signal_id_invalid")
        if signal.chain_id != "4663":
            raise ValueError("pons_signal_wrong_chain")
        if signal.side != "buy":
            raise ValueError("pons_signal_not_buy")
        if signal.source != "fomo_push":
            raise ValueError("pons_signal_source_unsupported")
        # The platform event is a trusted trigger, not a chain-finality claim.
        if signal.confirmation_level not in {"pending", "confirmed", "finalized"}:
            raise ValueError("pons_signal_unconfirmed")
        if signal.token_out.lower() != TOKEN_OUT:
            raise ValueError("pons_signal_wrong_token")
        return ExecutionIntent(
            intent_id=f"intent:{signal.signal_id}", signal_id=signal.signal_id,
            source=signal.source, strategy_id="pons-v4-native-fixed-v1",
            allocation_key=f"pons-v4:{signal.source}:{signal.signal_id}",
            chain_id="4663", side="buy", token_in="native:4663",
            token_out=TOKEN_OUT, requested_usd=Decimal(0),
            requested_asset_amount=str(self.native_in_wei),
        )

    def execute(self, intent: ExecutionIntent, signal: TradeSignalEnvelope,
                *, live_armed: bool,
                source_reorged: Callable[[str], bool] | None = None) -> Mapping[str, Any]:
        expected = self.create_intent(signal)
        if intent != expected:
            raise ValueError("pons_signal_intent_mismatch")
        broadcast = bool(live_armed and self.allow_broadcast)
        if broadcast and source_reorged is None:
            raise ValueError("pons_signal_source_guard_missing")
        # A persisted send attempt may only be recovered by its original hash.
        # Never re-sign or submit another transaction on a queue retry.
        if self.ledger_path.is_file():
            ledger = pons.SignalLedger(self.ledger_path)
            try:
                attempt = ledger.lookup(signal.signal_id)
                existing = ledger.db.execute(
                    "SELECT 1 FROM pons_signals WHERE signal_id=?", (signal.signal_id,)
                ).fetchone()
            finally:
                ledger.close()
            if attempt is not None:
                recovery = self._inspect_attempt(signal_id=signal.signal_id,
                                                 rpc=self._rpc or pons.PonsRpc(pons._rpc_url()),
                                                 ledger_path=self.ledger_path)
                return {"status": "recovered", "simulation_success": False,
                        "broadcastRequested": broadcast, "broadcastSent": False,
                        "confirmationSource": None,
                        "transactionHash": recovery.get("transactionHash"),
                        "receiptStatus": recovery.get("receiptStatus"),
                        "recoveryState": recovery.get("state"),
                        "sourceReorged": bool(source_reorged and source_reorged(signal.signal_id))}
            if existing is not None:
                raise ValueError("pons_once_duplicate_signal")
        if source_reorged is not None and source_reorged(signal.signal_id):
            raise ValueError("pons_signal_source_reorged")
        if (broadcast and self._rpc is not None
                and (self._receipt_thread is None or not self._receipt_thread.is_alive())):
            raise ValueError("pons_signal_receipt_tracker_unavailable")
        observed_ms = int(datetime.fromisoformat(signal.observed_at.replace("Z", "+00:00")).timestamp() * 1000)
        if broadcast and int(time.time() * 1000) - observed_ms > MAX_LOCAL_SEND_MS:
            raise ValueError("pons_signal_local_deadline_expired")
        # A platform event is a trusted trigger, never the economic identity.
        # Shadow runs do not consume the first future live buy for this CA.
        economic_key = f"4663:{TOKEN_OUT}"
        if broadcast:
            economic = PonsV4EconomicLedger(self.economic_ledger_path)
            try:
                economic.claim(economic_key, signal.signal_id)
            finally:
                economic.close()

        def source_guard() -> None:
            if source_reorged is None or source_reorged(signal.signal_id):
                raise ValueError("pons_signal_source_reorged")
            if int(time.time() * 1000) - observed_ms > MAX_LOCAL_SEND_MS:
                raise ValueError("pons_signal_local_deadline_expired")
            economic = PonsV4EconomicLedger(self.economic_ledger_path)
            try:
                if not economic.owned(economic_key, signal.signal_id):
                    raise ValueError("pons_signal_economic_claim_lost")
            finally:
                economic.close()

        def attempted(nonce: int, tx_hash: str) -> None:
            economic = PonsV4EconomicLedger(self.economic_ledger_path)
            try:
                economic.transition(economic_key, signal.signal_id, "send_attempted",
                                    nonce=nonce, tx_hash=tx_hash)
            finally:
                economic.close()

        try:
            result = self._execute_once(token_out=TOKEN_OUT,
                                        native_in_wei=self.native_in_wei,
                                        slippage_bps=self.slippage_bps,
                                        signal_id=signal.signal_id,
                                        broadcast=broadcast,
                                        fast=True,
                                        wait_for_receipt=not broadcast,
                                        **({"rpc": self._rpc} if self._rpc is not None else {}),
                                        ledger_path=self.ledger_path,
                                        execution_db_path=self.execution_db_path,
                                        **({"pre_sign_guard": source_guard,
                                            "pre_send_guard": source_guard,
                                            "on_send_attempt": attempted} if broadcast else {}))
        except Exception:
            if broadcast:
                # A persisted signed hash is irreversible. Never release it,
                # including after an uncertain send or process restart.
                attempted_hash = None
                if self.ledger_path.is_file():
                    pons_ledger = pons.SignalLedger(self.ledger_path)
                    try:
                        attempted_hash = pons_ledger.lookup(signal.signal_id)
                    finally:
                        pons_ledger.close()
                if attempted_hash is None:
                    economic = PonsV4EconomicLedger(self.economic_ledger_path)
                    try:
                        economic.transition(economic_key, signal.signal_id, "preflight_failed")
                    finally:
                        economic.close()
                else:
                    economic = PonsV4EconomicLedger(self.economic_ledger_path)
                    try:
                        terminal = {"failed": "receipt_failed", "uncertain": "uncertain"}.get(
                            attempted_hash[0], "send_attempted")
                        economic.transition(economic_key, signal.signal_id, terminal,
                                            nonce=attempted_hash[2], tx_hash=attempted_hash[1])
                    finally:
                        economic.close()
            raise
        sent = bool(result.get("broadcast"))
        tx_hash = result.get("transactionHash")
        if sent and (not isinstance(tx_hash, str)
                     or re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash) is None):
            raise ValueError("pons_once_broadcast_result_invalid")
        if broadcast and not sent:
            economic = PonsV4EconomicLedger(self.economic_ledger_path)
            try:
                economic.transition(economic_key, signal.signal_id, "preflight_failed")
            finally:
                economic.close()
            raise ValueError("pons_once_broadcast_result_invalid")
        if broadcast:
            economic = PonsV4EconomicLedger(self.economic_ledger_path)
            try:
                economic.transition(economic_key, signal.signal_id,
                                    "confirmed" if result.get("receiptStatus") == 1 else "submitted")
            finally:
                economic.close()
        if sent and (not broadcast or result.get("receiptStatus") not in {None, 1}):
            raise ValueError("pons_once_broadcast_result_invalid")
        return {"status": ("broadcast_confirmed" if result.get("receiptStatus") == 1
                           else "broadcast_submitted") if sent else "simulated",
                "simulation_success": result.get("simulation_success") is True,
                "confirmationSource": "fomo_push_trusted",
                "broadcastRequested": broadcast,
                "broadcastSent": sent,
                "headSource": result.get("headSource", "http"),
                "transactionHash": result.get("transactionHash") if sent else None,
                "receiptStatus": result.get("receiptStatus") if sent else None,
                "signalToTxHashMs": result.get("signalToTxHashMs") if sent else None,
                "submittedAtMs": result.get("submittedAtMs") if sent else None,
                "receiptLatencyMs": result.get("receiptLatencyMs") if sent else None,
                "signPersistMs": result.get("signPersistMs") if sent else None,
                "sendRawTransactionMs": result.get("sendRawTransactionMs") if sent else None,
                "stagesMs": ({"startupIdentity": result["startupIdentityMs"]}
                             if result.get("startupIdentityMs") is not None else {})
                | dict(result.get("stagesMs") or {}),
                "executionElapsedMs": result.get("elapsedMs")}
