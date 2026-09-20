"""Pure, local gates for the copy-trading hot path.

This module intentionally performs no HTTP/RPC calls. Expensive asset analysis
belongs after the order hand-off; only identity, event semantics, freshness and
hard sizing prerequisites are allowed here.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_ACTIVE_BUY_TYPES = frozenset({"swap_buy", "single_user_buy"})
PASSIVE_EVENT_MARKERS = (
    "transfer",
    "airdrop",
    "mint",
    "deposit",
    "receive",
    "token_deploy",
)


def _event_age_seconds(value: str, now: datetime | None = None) -> float:
    if not value:
        return float("inf")
    try:
        created = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (current - created.astimezone(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return float("inf")


@dataclass(frozen=True)
class FastPathGate:
    status: str
    signal_age_seconds: float
    deferred_checks: tuple[str, ...]
    decision_latency_ms: float

    @property
    def accepted(self) -> bool:
        return self.status == "accepted"


def evaluate_copy_buy(event: Any, settings: dict[str, Any], *, now: datetime | None = None) -> FastPathGate:
    """Evaluate only gates that must finish before a copy-buy can be handed off.

    Passive asset movements are intrinsically forbidden even if an operator
    accidentally adds such an event type to ``event_types``.
    """

    started = time.perf_counter()
    source_type = str(getattr(event, "source_type", "") or "").strip().lower()
    kind = str(getattr(event, "kind", "") or "").strip().lower()
    allowed_types = {str(value).strip().lower() for value in settings.get("event_types", DEFAULT_ACTIVE_BUY_TYPES)}
    active_types = {
        str(value).strip().lower()
        for value in settings.get("active_buy_event_types", DEFAULT_ACTIVE_BUY_TYPES)
    }
    allowed_networks = {
        int(value) for value in settings.get("network_ids", [1, 56, 4663, 5042, 8453, 1399811149])
    }
    age = _event_age_seconds(str(getattr(event, "created_at", "") or ""), now)
    deferred: list[str] = []

    if kind != "buy":
        status = "not_buy"
    elif any(marker in source_type for marker in PASSIVE_EVENT_MARKERS):
        status = "passive_asset_event"
    elif source_type not in allowed_types or source_type not in active_types:
        status = "unsupported_event_type"
    elif not str(getattr(event, "ca", "") or "").strip():
        status = "missing_ca"
    elif int(getattr(event, "network_id", 0) or 0) not in allowed_networks:
        status = "unsupported_network"
    elif age > float(settings.get("max_signal_age_seconds", 5)):
        status = "stale_signal"
    elif float(getattr(event, "amount_usd", 0) or 0) < float(settings.get("min_target_buy_usd", 100)):
        status = "target_trade_too_small"
    elif (
        str(settings.get("mode", "paper")) == "live"
        and bool(settings.get("require_trade_id_in_live", True))
        and not str(getattr(event, "trade_id", "") or "").strip()
    ):
        status = "missing_transaction_reference"
    else:
        market_cap = float(getattr(event, "market_cap", 0) or 0)
        minimum_market_cap = float(settings.get("min_market_cap_usd", 0) or 0)
        if not market_cap:
            if bool(settings.get("defer_asset_checks", True)):
                deferred.append("missing_market_cap")
            else:
                status = "missing_market_cap"
                return FastPathGate(status, age, (), round((time.perf_counter() - started) * 1000, 3))
        elif market_cap < minimum_market_cap:
            if bool(settings.get("defer_asset_checks", True)):
                deferred.append("market_cap_too_small")
            else:
                status = "market_cap_too_small"
                return FastPathGate(status, age, (), round((time.perf_counter() - started) * 1000, 3))
        status = "accepted"

    return FastPathGate(status, age, tuple(deferred), round((time.perf_counter() - started) * 1000, 3))
