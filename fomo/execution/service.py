"""Single, durable source-neutral execution consumer; live remains hard-locked.

No code path here calls a broadcaster while execution_control.live_armed is 0.
The queue is shared by Node push ingress and Python wallet RPC ingress.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.fomo import FomoPushAdapter
from fomo.signals.freshness import decide_freshness
from fomo.signals.strategy import ExecutionIntent, FomoCopyStrategy, WalletCopyStrategy

from .journal import ExecutionJournal
from .pons_v4_signal_executor import PonsV4SignalExecutor
from .rh_auto_executor import RhAutoExecutor
from .signal_queue import DurableSignalQueue, QueuedSignal


class IntentExecutor(Protocol):
    def execute(self, intent: ExecutionIntent, signal: TradeSignalEnvelope) -> Mapping[str, Any]: ...


def _milliseconds(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("signal timestamp must have timezone")
    return int(parsed.timestamp() * 1000)


def load_followed_ids(path: str | Path, *, maximum_age_ms: int = 600_000) -> set[str]:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    updated_at = int(document.get("updatedAt") or 0)
    if not updated_at or int(time.time() * 1000) - updated_at > maximum_age_ms:
        raise ValueError("following_whitelist_stale")
    ids = document.get("followingIds")
    if not isinstance(ids, list):
        raise ValueError("following_whitelist_invalid")
    return {str(value) for value in ids}


class ExecutionService:
    def __init__(self, queue: DurableSignalQueue, journal: ExecutionJournal,
                 followed_ids: Callable[[], set[str]], wallet_config: Callable[[TradeSignalEnvelope], Mapping[str, Any] | None],
                 executor: IntentExecutor | None = None, *,
                 source_maximum_age_ms: Mapping[str, int] | None = None, fomo_fixed_usd: int = 10,
                 pons_v4_executor: PonsV4SignalExecutor | None = None,
                 rh_auto_executor: RhAutoExecutor | None = None,
                 after_queue_sequence: int = 0,
                 only_queue_sequence: int | None = None,
                 rh_auto_ignore_source_clock_once: bool = False) -> None:
        if after_queue_sequence < 0:
            raise ValueError("invalid_queue_sequence_watermark")
        if pons_v4_executor is not None and rh_auto_executor is not None:
            raise ValueError("execution_multiple_narrow_routes")
        if rh_auto_ignore_source_clock_once and rh_auto_executor is None:
            raise ValueError("source_clock_override_requires_rh_auto")
        self.queue = queue
        self.journal = journal
        self.followed_ids = followed_ids
        self.wallet_config = wallet_config
        self.executor = executor
        self.pons_v4_executor = pons_v4_executor
        self.rh_auto_executor = rh_auto_executor
        self.rh_auto_ignore_source_clock_once = rh_auto_ignore_source_clock_once
        self.after_queue_sequence = after_queue_sequence
        self.only_queue_sequence = only_queue_sequence
        self.source_maximum_age_ms = dict(source_maximum_age_ms or {
            "fomo_push": 5000, "wallet_rpc_evm": 5000, "wallet_rpc_solana": 5000,
        })
        self.fomo_strategy = FomoCopyStrategy(fomo_fixed_usd)
        self.owner = f"execution:{os.getpid()}:{uuid.uuid4().hex}"

    def _normalize(self, item: QueuedSignal) -> TradeSignalEnvelope | None:
        if item.payload_kind == "raw_fomo" and item.source == "fomo_push":
            return FomoPushAdapter(self.followed_ids()).normalize(item.payload, item.received_at)
        if item.payload_kind == "envelope":
            return TradeSignalEnvelope.from_dict(item.payload)
        return None

    def process_one(self) -> dict[str, Any] | None:
        lease_ms = 180_000 if (self.pons_v4_executor is not None
                               or self.rh_auto_executor is not None) else 30_000
        if not self.queue.acquire_service(self.owner, lease_ms):
            raise RuntimeError("another_execution_service_is_active")
        item = self.queue.claim(self.owner, lease_ms,
                                after_sequence=self.after_queue_sequence,
                                source="fomo_push" if self.rh_auto_executor is not None else None,
                                exact_sequence=self.only_queue_sequence)
        if item is None:
            return None
        started = time.perf_counter()
        now_ms = int(time.time() * 1000)
        telemetry: dict[str, Any] = {
            "signalId": item.signal_id, "source": item.source,
            "signalReceived": True,
            "localQueueDelayMs": max(0, now_ms - item.enqueued_at_ms),
            "attempts": item.attempts,
        }
        if self.pons_v4_executor is not None:
            telemetry.update({"signalAccepted": False, "simulation_success": False,
                              "broadcastRequested": self.pons_v4_executor.allow_broadcast,
                              "broadcastBlocked": True, "broadcastSent": False,
                              "stagesMs": {}})
        elif self.rh_auto_executor is not None:
            telemetry.update({"signalAccepted": False, "simulation_success": False,
                              "broadcastRequested": self.rh_auto_executor.allow_broadcast,
                              "broadcastBlocked": True, "broadcastSent": False})
        try:
            try:
                signal = self._normalize(item)
            except (ValueError, TypeError, KeyError):
                signal = None
            if signal is None or signal.signal_id != item.signal_id or signal.source != item.source:
                status, reason = "dropped", "source_signal_invalid_or_not_allowed"
            else:
                source_ms = _milliseconds(signal.source_timestamp)
                observed_ms = _milliseconds(signal.observed_at)
                max_age = int(self.source_maximum_age_ms.get(signal.source, 0))
                freshness = decide_freshness(source_ms, observed_ms, now_ms, max_age) if max_age > 0 else None
                telemetry.update({
                    "upstreamDelayMs": freshness.upstream_delay_ms if freshness else 0,
                    "upstreamDeliveryDelayMs": freshness.upstream_delay_ms if freshness else 0,
                    "localQueueDelayMs": freshness.local_queue_delay_ms if freshness else 0,
                    "clockSkewMs": freshness.clock_skew_ms if freshness else 0,
                })
                source_clock_override = self.rh_auto_ignore_source_clock_once
                if source_clock_override:
                    telemetry["sourceClockIgnoredForOneShot"] = True
                arrival_fresh = (
                    max_age > 0
                    and 0 <= now_ms - observed_ms <= max_age
                    and 0 <= now_ms - item.enqueued_at_ms <= max_age
                )
                if freshness is None or not (arrival_fresh if source_clock_override else freshness.accepted):
                    status, reason = "dropped", "dropped_late"
                elif self.pons_v4_executor is not None:
                    try:
                        intent = self.pons_v4_executor.create_intent(signal)
                        if signal.source == "fomo_push":
                            if not signal.kol_id or signal.kol_id not in self.followed_ids():
                                raise ValueError("pons_signal_source_not_followed")
                        elif signal.source == "wallet_rpc_evm":
                            if not signal.actor_wallet or self.wallet_config(signal) is None:
                                raise ValueError("pons_signal_source_not_watched")
                        else:
                            raise ValueError("pons_signal_source_unsupported")
                        telemetry["signalAccepted"] = True
                        control = self.journal.db.execute(
                            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
                        ).fetchone()
                        if control is None:
                            status, reason = "blocked", "execution_control_missing"
                        else:
                            live_armed = bool(control["live_armed"] and not control["circuit_breaker_tripped"])
                            telemetry["broadcastRequested"] = self.pons_v4_executor.allow_broadcast
                            telemetry["broadcastBlocked"] = not (live_armed and self.pons_v4_executor.allow_broadcast)
                            result = self.pons_v4_executor.execute(
                                intent, signal, live_armed=live_armed,
                                source_reorged=self.journal.source_reorged,
                            )
                            if result.get("sourceReorged"):
                                status, reason = "blocked", "source_reorged"
                            elif result.get("recoveryState") == "uncertain":
                                status, reason = "blocked", "pons_once_send_result_uncertain"
                            elif result.get("recoveryState") == "failed":
                                status, reason = "blocked", "pons_once_receipt_failed"
                            elif result.get("simulation_success") is not True and not result.get("recoveryState"):
                                status, reason = "blocked", "pons_once_simulation_failed"
                            else:
                                status, reason = "processed", str(result["status"])
                            telemetry.update({
                                "simulation_success": result.get("simulation_success") is True,
                                "confirmationSource": result.get("confirmationSource"),
                                "broadcastSent": bool(result.get("broadcastSent")),
                                "headSource": result.get("headSource"),
                                "stagesMs": result.get("stagesMs") or {},
                                "executionElapsedMs": result.get("executionElapsedMs"),
                                "executionLatencyMs": (
                                    max(0, int(result["submittedAtMs"]) - now_ms)
                                    if result.get("submittedAtMs") is not None else None),
                                "signalToTxHashMs": (
                                    max(0, int(result["submittedAtMs"]) - now_ms)
                                    if result.get("submittedAtMs") is not None else None),
                                "receiptLatencyMs": result.get("receiptLatencyMs"),
                                "signPersistMs": result.get("signPersistMs"),
                                "sendRawTransactionMs": result.get("sendRawTransactionMs"),
                            })
                            if result.get("broadcastSent"):
                                telemetry["transactionHash"] = result.get("transactionHash")
                                telemetry["receiptStatus"] = result.get("receiptStatus")
                            elif result.get("recoveryState"):
                                telemetry["recoveryState"] = result["recoveryState"]
                                telemetry["transactionHash"] = result.get("transactionHash")
                                telemetry["receiptStatus"] = result.get("receiptStatus")
                            if result.get("sourceReorged"):
                                telemetry["sourceReorged"] = True
                    except ValueError as error:
                        reason = str(error)
                        if not reason.startswith(("pons_signal_", "pons_once_", "source_reorged")):
                            reason = "pons_signal_execution_failed"
                        if reason == "pons_signal_source_reorged":
                            reason = "source_reorged"
                        elif reason in {"pons_signal_source_unconfirmed", "pons_signal_source_wrong_chain",
                                        "pons_signal_target_transfer_missing"}:
                            reason = "source_unconfirmed"
                        status = ("dropped" if reason.startswith("pons_signal_")
                                  or reason in {"source_reorged", "source_unconfirmed"} else "blocked")
                        telemetry["signalAccepted"] = False if status == "dropped" else telemetry.get("signalAccepted", False)
                        telemetry["simulation_success"] = False
                        telemetry["broadcastSent"] = False
                elif self.rh_auto_executor is not None:
                    try:
                        intent = self.rh_auto_executor.create_intent(signal)
                        telemetry["tokenOut"] = intent.token_out
                        if not signal.kol_id or signal.kol_id not in self.followed_ids():
                            raise ValueError("rh_auto_source_not_followed")
                        telemetry["signalAccepted"] = True
                        control = self.journal.db.execute(
                            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
                        ).fetchone()
                        live_armed = bool(control and control["live_armed"]
                                          and not control["circuit_breaker_tripped"])
                        result = self.rh_auto_executor.execute(
                            intent, signal, live_armed=live_armed,
                            source_reorged=self.journal.source_reorged,
                        )
                        telemetry.update({
                            "simulation_success": result.get("simulation_success") is True,
                            "broadcastBlocked": not (live_armed and self.rh_auto_executor.allow_broadcast),
                            "broadcastSent": result.get("broadcastSent") is True,
                            "selectedProtocol": result.get("protocol"),
                            "poolId": result.get("poolId"),
                            "pool": result.get("pool"),
                            "amountInWei": result.get("amountInWei"),
                            "quotedOut": result.get("quotedOut"),
                            "minOut": result.get("minOut"),
                            "estimatedGas": result.get("estimatedGas"),
                            "quoteBlock": result.get("block"),
                            "quoteBlockHash": result.get("blockHash"),
                            "transactionHash": result.get("transactionHash"),
                            "receiptStatus": result.get("receiptStatus"),
                            "recoveryState": result.get("recoveryState"),
                        })
                        reason = str(result.get("status") or "rh_auto_result_invalid")
                        status = ("processed" if reason in {"simulated", "broadcast_confirmed"}
                                  else "blocked" if reason in {"send_uncertain", "manual_review_required"}
                                  else "dropped")
                    except ValueError as error:
                        reason = str(error)
                        if (not reason.startswith(("rh_auto_", "pons_dynamic_", "swap_once_",
                                                   "v4_", "pons_"))
                                and reason not in {"v3_pool_state_invalid", "v3_pool_identity_mismatch",
                                                   "v3_slot0_response_invalid", "v3_block_stale_or_invalid"}):
                            reason = "rh_auto_preflight_failed"
                        status = "dropped" if reason in {
                            "rh_auto_no_supported_route", "rh_auto_duplicate_token_or_signal",
                            "rh_auto_signal_scope_invalid", "rh_auto_source_not_followed",
                            "rh_auto_token_invalid",
                        } else "blocked"
                        telemetry["simulation_success"] = False
                        telemetry["broadcastSent"] = False
                elif signal.source == "fomo_push" and signal.confirmation_level not in {"confirmed", "finalized"}:
                    status, reason = "dropped", "source_unconfirmed"
                else:
                    if signal.source == "fomo_push":
                        intent = self.fomo_strategy.create_intent(signal)
                    else:
                        config = self.wallet_config(signal)
                        intent = WalletCopyStrategy(config).create_intent(signal) if config else None
                    if intent is None:
                        status, reason = "dropped", "strategy_rejected"
                    elif self.journal.source_reorged(intent.signal_id):
                        status, reason = "dropped", "source_reorged"
                    else:
                        control = self.journal.db.execute(
                            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
                        ).fetchone()
                        if control is None or not control["live_armed"] or control["circuit_breaker_tripped"]:
                            status, reason = "blocked", "live_disabled_or_circuit_open"
                        elif self.executor is None:
                            status, reason = "blocked", "execution_adapter_unavailable"
                        else:
                            result = self.executor.execute(intent, signal)
                            status, reason = "processed", str(result.get("status") or "submitted")
            telemetry["decisionLatencyMs"] = round((time.perf_counter() - started) * 1000, 3)
            telemetry["totalLatencyMs"] = max(0, int(time.time() * 1000) - item.enqueued_at_ms)
            telemetry["reason"] = reason
            self.queue.finish(self.owner, item.signal_id, status, telemetry)
            return {"status": status, **telemetry}
        except Exception:
            # The claim expires for recovery; never turn an uncertain execution
            # exception into a successful/terminal queue acknowledgement.
            raise

    def run_forever(self, *, idle_wait_seconds: float = 0.02) -> None:
        while True:
            if self.process_one() is None:
                time.sleep(idle_wait_seconds)
