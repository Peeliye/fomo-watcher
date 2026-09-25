"""Background-only hand-off from verified V3 discovery to the buy cache.

Callers perform all chain reads before invoking this module. Nothing here is
called by the cached buy path or by the live execution service.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .cached_direct_buy import (QUOTE_MAX_AGE_MS, CaRouteCache,
                                CaRouteCacheEntry, CachedAmountQuote,
                                v3_evidence_hash)


@dataclass(frozen=True, slots=True)
class VerifiedV3Scan:
    chain_id: int
    ca: str
    quote_token: str
    token0: str
    token1: str
    fee: int
    tick_spacing: int
    pool: str
    factory_pool: str
    factory: str
    event_block: int | None
    block_height: int
    block_hash: str
    amount_in_units: int
    amount_out_units: int
    quoted_at_ms: int
    evidence_kind: str
    approved_fee_tiers: tuple[int, ...] | None = None
    block_timestamp_ms: int | None = None
    read_started_at_ms: int | None = None
    block_age_at_start_ms: int | None = None
    block_age_at_completion_ms: int | None = None
    snapshot_read_duration_ms: int | None = None


def publish_verified_v3_scan(cache: CaRouteCache, scan: VerifiedV3Scan,
                             *, now_ms: int | None = None) -> CaRouteCacheEntry:
    """Publish a complete, same-block scanner result; reject stale/partial data.

    The scanner must obtain factory_pool, pool state and quote using one pinned
    block hash. This hand-off checks the supplied evidence but does not read RPC.
    """
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if scan.pool.lower() != scan.factory_pool.lower():
        raise ValueError("direct_cache_factory_pool_mismatch")
    event_evidence = (
        scan.evidence_kind == "factory_event_and_pinned_v3_snapshot"
        and scan.event_block is not None and scan.event_block > 0
        and scan.block_height >= scan.event_block
        and scan.approved_fee_tiers is None
    )
    approved_tiers = scan.approved_fee_tiers
    direct_evidence = (
        scan.evidence_kind == "approved_fee_set_and_pinned_factory_snapshot"
        and scan.event_block is None and approved_tiers is not None and bool(approved_tiers)
        and approved_tiers == tuple(sorted(set(approved_tiers)))
        and all(0 < tier < 1_000_000 for tier in approved_tiers)
        and scan.fee in approved_tiers
    )
    if (scan.chain_id != 4663
            or scan.factory.lower() != "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
            or not (event_evidence or direct_evidence)
            or scan.token0.lower() >= scan.token1.lower()
            or {scan.ca.lower(), scan.quote_token.lower()}
            != {scan.token0.lower(), scan.token1.lower()}
            or scan.quote_token.lower() != "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
            or not 0 < scan.tick_spacing < 32768
            or scan.amount_in_units <= 0 or scan.amount_out_units <= 0):
        raise ValueError("direct_cache_scan_evidence_invalid")
    if not 0 <= current - scan.quoted_at_ms <= QUOTE_MAX_AGE_MS:
        raise ValueError("direct_cache_scan_quote_stale")
    timing_values = (
        scan.block_timestamp_ms, scan.read_started_at_ms,
        scan.block_age_at_start_ms, scan.block_age_at_completion_ms,
        scan.snapshot_read_duration_ms,
    )
    if any(value is None for value in timing_values):
        raise ValueError("direct_cache_scan_timing_missing")
    snapshot_to_commit = current - scan.quoted_at_ms
    chain_state_age = current - int(scan.block_timestamp_ms or 0)
    quote = CachedAmountQuote(
        scan.amount_in_units, scan.amount_out_units, scan.quoted_at_ms,
        scan.block_hash, scan.block_height,
        scan.block_timestamp_ms, scan.read_started_at_ms,
        scan.block_age_at_start_ms, scan.block_age_at_completion_ms,
        scan.snapshot_read_duration_ms, snapshot_to_commit, chain_state_age,
    )
    evidence_hash = v3_evidence_hash(
        chain_id=scan.chain_id, ca=scan.ca, quote_token=scan.quote_token,
        token0=scan.token0, token1=scan.token1, fee=scan.fee,
        tick_spacing=scan.tick_spacing, pool=scan.pool, factory=scan.factory,
        event_block=scan.event_block, evidence_kind=scan.evidence_kind,
        approved_fee_tiers=scan.approved_fee_tiers, quote=quote,
    )
    entry = CaRouteCacheEntry(
        chain_id=scan.chain_id, ca=scan.ca, protocol="V3", pool=scan.pool,
        pool_key=None, quote_token=scan.quote_token, hooks=None,
        written_at_ms=current, identity_verified=True,
        token0=scan.token0, token1=scan.token1, fee=scan.fee,
        tick_spacing=scan.tick_spacing, identity_event_block=scan.event_block,
        identity_factory=scan.factory,
        identity_evidence_kind=scan.evidence_kind,
        approved_fee_tiers=scan.approved_fee_tiers,
        identity_evidence_hash=evidence_hash, quote=quote,
    )
    entry.validate()
    cache.store_discovered(entry)
    return entry
