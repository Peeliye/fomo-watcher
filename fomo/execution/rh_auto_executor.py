"""Narrow FOMO-triggered Robinhood auto-buy for four reviewed route classes.

Unknown routes fail closed. Live send additionally requires an operator scope,
the database live switch, and an explicit service flag. No route is inferred
from FOMO's quoteToken or claimed price.
"""

from __future__ import annotations

import re
import sqlite3
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

from fomo.signals.envelope import TradeSignalEnvelope, deterministic_signal_id
from fomo.signals.strategy import ExecutionIntent

from .evm_transaction import decode_eip1559, keccak256
from .pons_v4_dynamic import simulate_buy
from .rh_auto_ledger import RhAutoLedger
from .rh_auto_routes import simulate_zero_hook

CHAIN_ID = 4663
ROUTE_SCOPE = "rh-auto-v2-v3-v4-pons-s100"
MAX_SIGNAL_TO_SEND_MS = 15_000
MAX_AUTO_ARM_AGE_MS = 60_000
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")


class RhAutoExecutor:
    def __init__(self, *, native_in_wei: int, slippage_bps: int,
                 ledger_path: Path, execution_db_path: Path,
                 allow_broadcast: bool = False,
                 rpc_factory: Callable[[], Any] | None = None) -> None:
        if not 0 < native_in_wei < 1 << 128 or slippage_bps != 100:
            raise ValueError("rh_auto_config_invalid")
        self.native_in_wei = native_in_wei
        self.slippage_bps = slippage_bps
        self.ledger_path = Path(ledger_path)
        self.execution_db_path = Path(execution_db_path)
        self.allow_broadcast = allow_broadcast
        self.rpc_factory = rpc_factory

    def create_intent(self, signal: TradeSignalEnvelope) -> ExecutionIntent:
        if signal.signal_id != deterministic_signal_id(signal.source, signal.source_event_id):
            raise ValueError("rh_auto_signal_id_invalid")
        if (signal.source != "fomo_push" or signal.chain_id != "4663"
                or signal.side != "buy" or signal.confirmation_level != "pending"):
            raise ValueError("rh_auto_signal_scope_invalid")
        token = signal.token_out.lower()
        if not _ADDRESS.fullmatch(token):
            raise ValueError("rh_auto_token_invalid")
        return ExecutionIntent(
            intent_id=f"intent:{signal.signal_id}", signal_id=signal.signal_id,
            source=signal.source, strategy_id=ROUTE_SCOPE,
            allocation_key=f"rh-auto:{token}", chain_id="4663", side="buy",
            token_in="native:4663", token_out=token, requested_usd=Decimal(0),
            requested_asset_amount=str(self.native_in_wei),
        )

    def _operator_scope(self) -> bool:
        if not self.execution_db_path.is_file():
            return False
        try:
            uri = self.execution_db_path.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(uri, uri=True, timeout=2) as db:
                control = db.execute(
                    "SELECT live_armed,circuit_breaker_tripped,updated_at "
                    "FROM execution_control WHERE singleton=1"
                ).fetchone()
                scope = db.execute(
                    "SELECT chain_id,token_out,native_in_wei,route FROM execution_operator_scope "
                    "WHERE singleton=1"
                ).fetchone()
                if not control or control[:2] != (1, 0) or not scope:
                    return False
                armed_at = datetime.fromisoformat(str(control[2]).replace("Z", "+00:00"))
                if armed_at.tzinfo is None:
                    return False
                age_ms = int((datetime.now(timezone.utc) - armed_at).total_seconds() * 1000)
                return (0 <= age_ms <= MAX_AUTO_ARM_AGE_MS
                        and scope == (CHAIN_ID, "*", str(self.native_in_wei), ROUTE_SCOPE))
        except (OSError, sqlite3.Error, ValueError, TypeError):
            return False

    def _fence(self, *, rpc: Any, prepared: Mapping[str, Any],
               signal: TradeSignalEnvelope,
               source_reorged: Callable[[str], bool] | None) -> None:
        from scripts.pons_v4_swap_once import _ensure_broadcast_fresh

        if (source_reorged is None or source_reorged(signal.signal_id)
                or not self._operator_scope()):
            raise ValueError("rh_auto_source_or_live_scope_invalid")
        # Parse in one place to reject naive or malformed timestamps.
        from datetime import datetime
        observed = datetime.fromisoformat(signal.observed_at.replace("Z", "+00:00"))
        observed_ms = int(observed.timestamp() * 1000)
        if int(time.time() * 1000) - observed_ms > MAX_SIGNAL_TO_SEND_MS:
            raise ValueError("rh_auto_signal_send_deadline_expired")
        _ensure_broadcast_fresh(rpc, prepared, prepared["unsignedTransaction"],
                                str(prepared["recipient"]))

    def _rpc(self) -> Any:
        if self.rpc_factory is not None:
            return self.rpc_factory()
        from scripts.pons_v4_swap_once import PonsRpc
        from scripts.uniswap_v3_swap_once import _rpc_url
        return PonsRpc(_rpc_url())

    def _recover(self, *, token: str, existing: Mapping[str, object]) -> Mapping[str, Any]:
        tx_hash = existing.get("txHash")
        if not isinstance(tx_hash, str):
            return {"status": "manual_review_required", "simulation_success": False,
                    "broadcastSent": False, "recoveryState": existing["state"]}
        if existing["state"] in {"confirmed", "failed"}:
            return {"status": "already_final", "simulation_success": False,
                    "broadcastSent": False, "transactionHash": tx_hash,
                    "receiptStatus": existing.get("receiptStatus"),
                    "recoveryState": existing["state"]}
        from scripts.uniswap_v3_swap_once import _quantity
        rpc = self._rpc()
        try:
            receipt = rpc.call("eth_getTransactionReceipt", [tx_hash])
            if receipt is None:
                return {"status": "send_uncertain", "simulation_success": False,
                        "broadcastSent": False, "transactionHash": tx_hash,
                        "recoveryState": "uncertain"}
            if (not isinstance(receipt, Mapping)
                    or str(receipt.get("transactionHash") or "").lower() != tx_hash):
                raise ValueError("rh_auto_recovery_receipt_invalid")
            status = _quantity(receipt.get("status"))
            height = _quantity(receipt.get("blockNumber"))
            block = rpc.call("eth_getBlockByNumber", [hex(height), False])
            block_hash = str(receipt.get("blockHash") or "").lower()
            if (status not in (0, 1) or not isinstance(block, Mapping)
                    or not _HASH.fullmatch(block_hash)
                    or str(block.get("hash") or "").lower() != block_hash):
                raise ValueError("rh_auto_recovery_reorg_or_status_invalid")
            ledger = RhAutoLedger(self.ledger_path)
            try:
                ledger.receipt(token, str(existing["signalId"]), status)
            finally:
                ledger.close()
            return {"status": "already_confirmed" if status == 1 else "already_failed",
                    "simulation_success": False, "broadcastSent": False,
                    "transactionHash": tx_hash, "receiptStatus": status,
                    "recoveryState": "confirmed" if status == 1 else "failed"}
        finally:
            close = getattr(rpc, "close", None)
            if callable(close):
                close()

    def execute(self, intent: ExecutionIntent, signal: TradeSignalEnvelope,
                *, live_armed: bool,
                source_reorged: Callable[[str], bool] | None = None) -> Mapping[str, Any]:
        from scripts.pons_v4_swap_once import _verify_signed, _wait_receipt
        from scripts.uniswap_swap_once import execute_once as simulate_v2_v3
        from scripts.uniswap_v3_swap_once import _quantity, _signer, _wallet_profile

        if intent != self.create_intent(signal):
            raise ValueError("rh_auto_intent_mismatch")
        token = intent.token_out
        existing = None
        if self.ledger_path.is_file():
            ledger = RhAutoLedger(self.ledger_path)
            try:
                existing = ledger.lookup(token)
            finally:
                ledger.close()
        if existing is not None:
            return self._recover(token=token, existing=existing)
        rpc = self._rpc()
        try:
            wallet, signer_profile = _wallet_profile()
            rpc.wallet_profile = (wallet, signer_profile)
            prepared = simulate_buy(rpc, token_out=token,
                                    amount_in_wei=self.native_in_wei,
                                    slippage_bps=self.slippage_bps)
            if prepared is None:
                v2_v3 = simulate_v2_v3(chain_id=CHAIN_ID, token_out=token,
                                       native_in_wei=self.native_in_wei,
                                       slippage_bps=self.slippage_bps,
                                       include_prepared=True, rpc=rpc)
                if v2_v3.get("simulation_success") is True:
                    prepared = {**v2_v3, "protocol": v2_v3["selectedProtocol"]}
                elif v2_v3.get("reason") == "swap_once_no_supported_v2_v3_pool":
                    prepared = simulate_zero_hook(rpc, token_out=token,
                                                  amount_in_wei=self.native_in_wei,
                                                  slippage_bps=self.slippage_bps)
                else:
                    raise ValueError("rh_auto_v2_v3_preflight_invalid")
            if prepared is None:
                raise ValueError("rh_auto_no_supported_route")
            public = {key: value for key, value in prepared.items()
                      if key not in {"unsignedTransaction", "candidates", "poolState"}}
            if not (live_armed and self.allow_broadcast and self._operator_scope()):
                return {**public, "status": "simulated", "broadcastBlocked": True,
                        "broadcastSent": False}
            self._fence(rpc=rpc, prepared=prepared, signal=signal,
                        source_reorged=source_reorged)
            # The final bytes, not a caller envelope, are simulated again at
            # the current head before any signing.
            built = prepared["unsignedTransaction"]
            fields, _ = decode_eip1559(built.serialized, signed=False)
            call = {"from": wallet, "to": "0x" + fields.to.hex(),
                    "data": "0x" + fields.data.hex(), "value": hex(fields.value),
                    "gas": hex(fields.gas_limit),
                    "maxFeePerGas": hex(fields.maximum_fee),
                    "maxPriorityFeePerGas": hex(fields.priority_fee)}
            estimate_call = dict(call)
            estimate_call.pop("gas")
            latest_gas = _quantity(rpc.call("eth_estimateGas", [estimate_call, "latest"]))
            if not 21_000 <= latest_gas <= fields.gas_limit:
                raise ValueError("rh_auto_latest_gas_exceeds_limit")
            rpc.call("eth_call", [call, "latest"])
            ledger = RhAutoLedger(self.ledger_path)
            try:
                ledger.claim(token_out=token, signal_id=signal.signal_id,
                             wallet=wallet, route=str(prepared["protocol"]),
                             amount_in_wei=self.native_in_wei)
                self._fence(rpc=rpc, prepared=prepared, signal=signal,
                            source_reorged=source_reorged)
                signed = _signer(signer_profile, wallet).sign(built)
                tx_hash = _verify_signed(signed, built.serialized, wallet)
                ledger.signed(token_out=token, signal_id=signal.signal_id,
                              nonce=fields.nonce, tx_hash=tx_hash,
                              unsigned_hash="0x" + keccak256(built.serialized).hex())
                self._fence(rpc=rpc, prepared=prepared, signal=signal,
                            source_reorged=source_reorged)
                rpc.call("eth_call", [call, "latest"])
                ledger.broadcast_started(token, signal.signal_id)
                try:
                    returned = rpc.call("eth_sendRawTransaction", ["0x" + signed.hex()])
                    if not isinstance(returned, str) or returned.lower() != tx_hash:
                        raise ValueError("rh_auto_broadcast_hash_mismatch")
                    ledger.submitted(token, signal.signal_id)
                except Exception:
                    ledger.uncertain(token, signal.signal_id)
                    return {**public, "status": "send_uncertain", "broadcastSent": False,
                            "transactionHash": tx_hash, "recoveryState": "uncertain"}
                receipt_status = _wait_receipt(rpc, tx_hash)
                ledger.receipt(token, signal.signal_id, receipt_status)
                return {**public, "status": "broadcast_confirmed" if receipt_status == 1
                        else "broadcast_failed", "broadcastSent": True,
                        "transactionHash": tx_hash, "receiptStatus": receipt_status}
            finally:
                ledger.close()
        finally:
            close = getattr(rpc, "close", None)
            if callable(close):
                close()
