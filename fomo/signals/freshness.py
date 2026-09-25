"""Version 1 push-arrival freshness rule shared with the Node shadow contract."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FreshnessDecision:
    accepted: bool
    upstream_delay_ms: int
    local_queue_delay_ms: int
    clock_skew_ms: int


def decide_freshness(source_ms: int, observed_ms: int, consumption_ms: int,
                     maximum_upstream_delay_ms: int) -> FreshnessDecision:
    if maximum_upstream_delay_ms <= 0:
        raise ValueError("maximum_upstream_delay_invalid")
    raw = observed_ms - source_ms
    queue = max(0, consumption_ms - observed_ms)
    return FreshnessDecision(
        accepted=0 <= raw <= maximum_upstream_delay_ms,
        upstream_delay_ms=max(0, raw),
        local_queue_delay_ms=queue,
        clock_skew_ms=max(0, -raw),
    )
