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
from fomo.signals.strategy import ExecutionIntent, FomoCopyStrategy, WalletCopyStrategy

from .journal import ExecutionJournal
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
                 source_maximum_age_ms: Mapping[str, int] | None = None, fomo_fixed_usd: int = 10) -> None:
        self.queue = queue
        self.journal = journal
        self.followed_ids = followed_ids
        self.wallet_config = wallet_config
        self.executor = executor
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
        if not self.queue.acquire_service(self.owner, 30_000):
            raise RuntimeError("another_execution_service_is_active")
        item = self.queue.claim(self.owner, 30_000)
        if item is None:
            return None
        started = time.perf_counter()
        now_ms = int(time.time() * 1000)
        telemetry: dict[str, Any] = {
            "signalId": item.signal_id, "source": item.source,
            "localQueueDelayMs": max(0, now_ms - item.enqueued_at_ms),
            "attempts": item.attempts,
        }
        try:
            signal = self._normalize(item)
            if signal is None or signal.signal_id != item.signal_id or signal.source != item.source:
                status, reason = "dropped", "source_signal_invalid_or_not_allowed"
            else:
                source_ms = _milliseconds(signal.source_timestamp)
                observed_ms = _milliseconds(signal.observed_at)
                raw_upstream = observed_ms - source_ms
                telemetry.update({
                    "upstreamDeliveryDelayMs": max(0, raw_upstream),
                    "clockSkewMs": max(0, -raw_upstream),
                })
                max_age = int(self.source_maximum_age_ms.get(signal.source, 0))
                if max_age <= 0 or now_ms - source_ms > max_age or source_ms - now_ms > max_age:
                    status, reason = "dropped", "dropped_late"
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
