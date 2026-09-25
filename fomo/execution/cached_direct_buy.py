"""Cache-only, unsigned direct-buy preparation. Never reads RPC or broadcasts."""

from __future__ import annotations

import json
import hashlib
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.strategy import ExecutionIntent

from .direct_v2 import V2PoolSnapshot, build_unsigned_swap as build_v2
from .direct_v3 import CHAIN_CONFIGS as V3_CHAINS
from .direct_v4 import CHAIN_CONFIGS as V4_CHAINS, V4PoolKey, ZERO
from .interfaces import BuiltTransaction
from .v3_transaction import build_unsigned_swap as build_v3
from .v4_transaction import build_unsigned_swap as build_v4

_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
_HASH = re.compile(r"^0x[0-9a-f]{64}$")
# Robinhood's generic L0 V3 codec is the only 4663 direct CA builder admitted
# here. V2 signed scope and V4 simulation remain unverified on that chain.
ALLOWED_DIRECT_PROTOCOLS: Mapping[int, frozenset[str]] = {
    1: frozenset({"V2", "V3", "V4"}),
    8453: frozenset({"V2", "V3", "V4"}),
    4663: frozenset({"V3"}),
}
ROUTE_MAX_AGE_MS = 600_000
QUOTE_MAX_AGE_MS = 2_000


def v3_evidence_hash(*, chain_id: int, ca: str, quote_token: str,
                     token0: str, token1: str, fee: int, tick_spacing: int,
                     pool: str, factory: str, event_block: int | None,
                     evidence_kind: str, approved_fee_tiers: tuple[int, ...] | None,
                     quote: CachedAmountQuote) -> str:
    payload = {
        "chainId": chain_id, "ca": ca, "quoteToken": quote_token,
        "token0": token0, "token1": token1, "fee": fee,
        "tickSpacing": tick_spacing, "pool": pool, "factory": factory,
        "eventBlock": event_block, "evidenceKind": evidence_kind,
        "approvedFeeTiers": list(approved_fee_tiers) if approved_fee_tiers is not None else None,
        "blockHeight": quote.block_height, "blockHash": quote.block_hash,
        "amountInUnits": quote.amount_in_units, "amountOutUnits": quote.amount_out_units,
        "observedAtMs": quote.observed_at_ms,
        "blockTimestampMs": quote.block_timestamp_ms,
        "readStartedAtMs": quote.read_started_at_ms,
        "blockAgeAtStartMs": quote.block_age_at_start_ms,
        "blockAgeAtCompletionMs": quote.block_age_at_completion_ms,
        "snapshotReadDurationMs": quote.snapshot_read_duration_ms,
        "snapshotToCommitMs": quote.snapshot_to_commit_ms,
        "chainStateAgeAtPublishMs": quote.chain_state_age_at_publish_ms,
    }
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(canonical).hexdigest()


def _address(value: str, *, allow_zero: bool = False) -> str:
    if (not isinstance(value, str) or not _ADDRESS.fullmatch(value.lower())
            or (not allow_zero and not int(value, 16))):
        raise ValueError("direct_cache_address_invalid")
    return value.lower()


@dataclass(frozen=True, slots=True)
class CachedAmountQuote:
    amount_in_units: int
    amount_out_units: int
    observed_at_ms: int
    block_hash: str
    block_height: int | None = None
    block_timestamp_ms: int | None = None
    read_started_at_ms: int | None = None
    block_age_at_start_ms: int | None = None
    block_age_at_completion_ms: int | None = None
    snapshot_read_duration_ms: int | None = None
    snapshot_to_commit_ms: int | None = None
    chain_state_age_at_publish_ms: int | None = None

    def validate(self) -> None:
        if (not 0 < self.amount_in_units < 2**128
                or not 0 < self.amount_out_units < 2**128
                or self.observed_at_ms <= 0
                or not _HASH.fullmatch(self.block_hash)
                or self.block_height is not None and self.block_height <= 0
                or any(value is not None and value < 0 for value in (
                    self.block_timestamp_ms, self.read_started_at_ms,
                    self.block_age_at_completion_ms,
                    self.snapshot_read_duration_ms, self.snapshot_to_commit_ms,
                    self.chain_state_age_at_publish_ms))
                or (self.block_age_at_start_ms is not None
                    and not -60_000 <= self.block_age_at_start_ms <= 60_000)):
            raise ValueError("direct_cache_quote_invalid")


@dataclass(frozen=True, slots=True)
class CaRouteCacheEntry:
    chain_id: int
    ca: str
    protocol: str
    pool: str | None
    pool_key: V4PoolKey | None
    quote_token: str
    hooks: str | None
    written_at_ms: int
    identity_verified: bool
    token0: str | None = None
    token1: str | None = None
    fee: int | None = None
    tick_spacing: int | None = None
    identity_event_block: int | None = None
    identity_factory: str | None = None
    identity_evidence_kind: str | None = None
    approved_fee_tiers: tuple[int, ...] | None = None
    identity_evidence_hash: str | None = None
    quote: CachedAmountQuote | None = None
    v2_snapshot: V2PoolSnapshot | None = None

    def validate(self) -> None:
        ca = _address(self.ca)
        quote_token = _address(self.quote_token, allow_zero=self.protocol == "V4")
        if (ca == quote_token or self.protocol not in {"V2", "V3", "V4"}
                or self.chain_id not in ALLOWED_DIRECT_PROTOCOLS
                or self.written_at_ms <= 0 or self.identity_verified is not True):
            raise ValueError("direct_cache_identity_invalid")
        if self.quote is not None:
            self.quote.validate()
            if self.quote.observed_at_ms > self.written_at_ms:
                raise ValueError("direct_cache_quote_after_write")
        if self.protocol == "V4":
            if (self.pool_key is None or self.pool is not None
                    or self.hooks != self.pool_key.hooks
                    or {ca, quote_token} != {self.pool_key.currency0, self.pool_key.currency1}):
                raise ValueError("direct_cache_pool_key_invalid")
            if not all(_ADDRESS.fullmatch(value) for value in
                       (self.pool_key.currency0, self.pool_key.currency1, self.pool_key.hooks)):
                raise ValueError("direct_cache_pool_key_invalid")
        else:
            if (self.pool_key is not None or self.pool is None
                    or not _ADDRESS.fullmatch(self.pool) or not int(self.pool, 16)
                    or self.hooks is not None):
                raise ValueError("direct_cache_pool_invalid")
            if self.protocol == "V3" and (
                self.token0 is None or self.token1 is None
                or not all(_ADDRESS.fullmatch(value) for value in (self.token0, self.token1))
                or self.token0 >= self.token1
                or {ca, quote_token} != {self.token0, self.token1}
                or self.fee is None or not 0 < self.fee < 1_000_000
                or self.tick_spacing is None or not 0 < self.tick_spacing < 32768
                or self.chain_id == 4663 and not self._valid_robinhood_v3_evidence()
            ):
                raise ValueError("direct_cache_v3_identity_invalid")
            if self.protocol == "V2" and self.v2_snapshot is not None and (
                self.v2_snapshot.chain_id != str(self.chain_id)
                or self.v2_snapshot.pair != self.pool
                or self.v2_snapshot.base_token != ca
                or self.v2_snapshot.quote_token != quote_token
            ):
                raise ValueError("direct_cache_v2_snapshot_mismatch")

    def _valid_robinhood_v3_evidence(self) -> bool:
        if (self.identity_factory
                != "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
                or self.quote is None or self.quote.block_height is None
                or self.fee is None or self.token0 is None or self.token1 is None
                or self.pool is None or self.identity_evidence_kind is None
                or not isinstance(self.identity_evidence_hash, str)):
            return False
        expected_hash = v3_evidence_hash(
            chain_id=self.chain_id, ca=self.ca, quote_token=self.quote_token,
            token0=self.token0, token1=self.token1, fee=self.fee,
            tick_spacing=self.tick_spacing or 0, pool=self.pool,
            factory=self.identity_factory, event_block=self.identity_event_block,
            evidence_kind=self.identity_evidence_kind,
            approved_fee_tiers=self.approved_fee_tiers, quote=self.quote,
        )
        if self.identity_evidence_hash != expected_hash:
            return False
        timing = self.quote
        if any(value is None for value in (
                timing.block_timestamp_ms, timing.read_started_at_ms,
                timing.block_age_at_start_ms, timing.block_age_at_completion_ms,
                timing.snapshot_read_duration_ms, timing.snapshot_to_commit_ms,
                timing.chain_state_age_at_publish_ms)):
            return False
        if (timing.observed_at_ms - (timing.read_started_at_ms or 0)
                != timing.snapshot_read_duration_ms
                or (timing.read_started_at_ms or 0) - (timing.block_timestamp_ms or 0)
                != timing.block_age_at_start_ms
                or timing.observed_at_ms - (timing.block_timestamp_ms or 0)
                != timing.block_age_at_completion_ms
                or self.written_at_ms - timing.observed_at_ms
                != timing.snapshot_to_commit_ms
                or self.written_at_ms - (timing.block_timestamp_ms or 0)
                != timing.chain_state_age_at_publish_ms):
            return False
        if self.identity_evidence_kind == "factory_event_and_pinned_v3_snapshot":
            return (self.identity_event_block is not None
                    and self.identity_event_block > 0
                    and self.quote.block_height >= self.identity_event_block
                    and self.approved_fee_tiers is None)
        if self.identity_evidence_kind == "approved_fee_set_and_pinned_factory_snapshot":
            tiers = self.approved_fee_tiers
            return (self.identity_event_block is None and tiers is not None and bool(tiers)
                    and tiers == tuple(sorted(set(tiers)))
                    and all(0 < tier < 1_000_000 for tier in tiers)
                    and self.fee in tiers)
        return False


class CaRouteCache:
    """SQLite hand-off: only discovery workers write; buying opens mode=ro."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path).resolve()

    def initialize_for_discovery(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            with db:
                db.execute("""CREATE TABLE IF NOT EXISTS ca_direct_routes (
                    chain_id INTEGER NOT NULL, ca TEXT NOT NULL,
                    entry_json TEXT NOT NULL, written_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(chain_id, ca))""")

    def store_discovered(self, entry: CaRouteCacheEntry) -> None:
        """Explicit background/discovery write; never called from buy preparation."""
        entry.validate()
        payload = json.dumps(asdict(entry), separators=(",", ":"), sort_keys=True)
        with closing(sqlite3.connect(self.path, timeout=1.0)) as db:
            with db:
                db.execute("""INSERT INTO ca_direct_routes(chain_id,ca,entry_json,written_at_ms)
                    VALUES(?,?,?,?) ON CONFLICT(chain_id,ca) DO UPDATE SET
                    entry_json=excluded.entry_json,written_at_ms=excluded.written_at_ms""",
                           (entry.chain_id, entry.ca, payload, entry.written_at_ms))

    def invalidate_discovered(self, chain_id: int, ca: str) -> None:
        """Writer-side invalidation; buyers never call this mutation."""
        token = _address(ca)
        if not self.path.is_file():
            return
        with closing(sqlite3.connect(self.path, timeout=1.0)) as db:
            with db:
                db.execute("DELETE FROM ca_direct_routes WHERE chain_id=? AND ca=?",
                           (chain_id, token))

    def lookup(self, chain_id: int, ca: str) -> CaRouteCacheEntry | None:
        """A cache miss has no chain fallback and creates no database or schema."""
        token = _address(ca)
        if not self.path.is_file():
            return None
        try:
            with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True,
                                         timeout=0.05)) as db:
                row = db.execute(
                    "SELECT entry_json FROM ca_direct_routes WHERE chain_id=? AND ca=?",
                    (chain_id, token),
                ).fetchone()
        except sqlite3.Error as error:
            raise ValueError("direct_cache_unavailable") from error
        if row is None:
            return None
        try:
            raw: dict[str, Any] = json.loads(row[0])
            if raw.get("pool_key") is not None:
                raw["pool_key"] = V4PoolKey(**raw["pool_key"])
            if raw.get("quote") is not None:
                raw["quote"] = CachedAmountQuote(**raw["quote"])
            if raw.get("approved_fee_tiers") is not None:
                raw["approved_fee_tiers"] = tuple(raw["approved_fee_tiers"])
            if raw.get("v2_snapshot") is not None:
                raw["v2_snapshot"] = V2PoolSnapshot(**raw["v2_snapshot"])
            entry = CaRouteCacheEntry(**raw)
            entry.validate()
            if entry.chain_id != chain_id or entry.ca != token:
                raise ValueError("direct_cache_key_mismatch")
            return entry
        except (TypeError, KeyError, ValueError) as error:
            raise ValueError("direct_cache_record_invalid") from error


@dataclass(frozen=True, slots=True)
class CachedDirectBuild:
    status: str
    transaction: BuiltTransaction
    chain_id: int
    ca: str
    protocol: str


class IntentStrategy(Protocol):
    def create_intent(self, signal: TradeSignalEnvelope) -> ExecutionIntent | None: ...


def _cached_route(intent: ExecutionIntent, cache: CaRouteCache,
                  current: int) -> CaRouteCacheEntry:
    if intent.side != "buy" or not intent.chain_id.isdecimal():
        raise ValueError("direct_buy_intent_invalid")
    chain_id = int(intent.chain_id)
    ca, token_in = _address(intent.token_out), _address(intent.token_in, allow_zero=True)
    entry = cache.lookup(chain_id, ca)
    if entry is None:
        raise ValueError("direct_buy_cache_miss")
    if entry.protocol not in ALLOWED_DIRECT_PROTOCOLS.get(chain_id, frozenset()):
        raise ValueError("direct_buy_protocol_not_allowed")
    if entry.hooks not in (None, ZERO):
        raise ValueError("direct_buy_hook_rejected")
    if entry.quote_token != token_in or entry.ca != ca:
        raise ValueError("direct_buy_cache_route_mismatch")
    if not 0 <= current - entry.written_at_ms <= ROUTE_MAX_AGE_MS:
        raise ValueError("direct_buy_cache_stale")
    if entry.protocol == "V2":
        if (entry.v2_snapshot is None
                or not 0 <= current - entry.v2_snapshot.observed_at_ms <= 5_000):
            raise ValueError("direct_buy_cache_quote_miss")
    elif (entry.quote is None
          or not 0 <= current - entry.quote.observed_at_ms <= QUOTE_MAX_AGE_MS):
        raise ValueError("direct_buy_cache_quote_miss")
    return entry


def create_cached_direct_buy_intent(
    signal: TradeSignalEnvelope, *, strategy: IntentStrategy,
    cache: CaRouteCache, now_ms: int | None = None,
) -> ExecutionIntent:
    """The intent hand-off reads only cache identity, never discovers a route."""
    intent = strategy.create_intent(signal)
    if intent is None or intent.signal_id != signal.signal_id:
        raise ValueError("direct_buy_strategy_rejected")
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    _cached_route(intent, cache, current)
    return intent


def build_cached_direct_buy(
    intent: ExecutionIntent, *, cache: CaRouteCache, wallet: str,
    amount_in_units: int, nonce: int, gas_limit: int,
    priority_fee_wei: int, maximum_fee_wei: int, deadline: int,
    slippage_bps: int, now_ms: int | None = None,
) -> CachedDirectBuild:
    """Build only from cached identity and exact-size quote; no RPC object exists here."""
    if not 0 < amount_in_units < 2**128 or not 0 <= slippage_bps <= 500:
        raise ValueError("direct_buy_amount_or_slippage_invalid")
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    entry = _cached_route(intent, cache, current)
    chain_id = entry.chain_id
    ca, token_in = entry.ca, entry.quote_token
    common = dict(nonce=nonce, gas_limit=gas_limit,
                  priority_fee_wei=priority_fee_wei,
                  maximum_fee_wei=maximum_fee_wei, deadline=deadline)
    if entry.protocol == "V2":
        snapshot = entry.v2_snapshot
        assert snapshot is not None  # Enforced by _cached_route.
        transaction = build_v2(snapshot=snapshot, wallet=wallet, token_in=token_in,
                               amount_in_units=amount_in_units,
                               slippage_bps=slippage_bps, now_ms=current, **common)
    else:
        quote = entry.quote
        if quote is None or quote.amount_in_units != amount_in_units:
            raise ValueError("direct_buy_cache_quote_miss")
        if entry.protocol == "V3":
            config = V3_CHAINS.get(chain_id)
            if config is None or entry.token0 is None or entry.token1 is None or entry.fee is None:
                raise ValueError("direct_buy_adapter_unavailable")
            transaction = build_v3(chain_id=chain_id, router=config.router,
                                   token0=entry.token0, token1=entry.token1,
                                   fee=entry.fee, token_in=token_in, wallet=wallet,
                                   amount_in=amount_in_units,
                                   quoted_out=quote.amount_out_units,
                                   slippage_bps=slippage_bps, **common)
        else:
            key = entry.pool_key
            if key is None or chain_id not in V4_CHAINS:
                raise ValueError("direct_buy_adapter_unavailable")
            minimum_out = quote.amount_out_units * (10_000 - slippage_bps) // 10_000
            transaction = build_v4(chain_id=chain_id, key=key, token_in=token_in,
                                   amount_in=amount_in_units, minimum_out=minimum_out,
                                   **common)
    return CachedDirectBuild("built_not_sent", transaction, chain_id, ca, entry.protocol)
