"""Robinhood Chain V3 identity scanner and cache-only quote producer.

This process is the sole writer for its dedicated route cache.  The buy path
never imports it and never receives an RPC object.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from fomo.execution.cached_direct_buy import ROUTE_MAX_AGE_MS, CaRouteCache
from fomo.execution.direct_v3 import (CHAIN_CONFIGS, UniswapV3PoolReader,
                                      V3PinnedBlockContext, V3PoolTarget,
                                      pin_v3_block_context)
from fomo.execution.evm_transaction import keccak256
from fomo.execution.route_cache_writer import VerifiedV3Scan, publish_verified_v3_scan
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcUnavailable
from scripts._robinhood_probe_transport import RobinhoodDiscoveryRpc, proxy_configured

CHAIN_ID = 4663
TOKENS = ("0x39dbed3a2bd333467115de45665cc57f813c4571",
          "0x2e8c31162b855a2ffa90f6f8634643ad6f111e18")
FIRST_CODE_HINTS = {TOKENS[0]: 8963150, TOKENS[1]: 9721433}
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
POOL_CREATED = "0x" + keccak256(b"PoolCreated(address,address,uint24,int24,address)").hex()
MAX_LOG_CHUNK = 20_000
DEFAULT_CONFIRMATIONS = 2
DEFAULT_SCAN_REQUEST_BUDGET = 1_000
DEFAULT_SCAN_BLOCK_SPAN = 20_000
EVENT_EVIDENCE = "factory_event_and_pinned_v3_snapshot"
DIRECT_EVIDENCE = "approved_fee_set_and_pinned_factory_snapshot"
_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
WATCH_MAX_INTERVAL_MS = 1_000


def rpc_counters(rpc: Any) -> dict[str, int]:
    return {
        "httpRoundTrips": int(getattr(rpc, "http_round_trip_count",
                                      getattr(rpc, "request_count", 0))),
        "jsonRpcMethods": int(getattr(rpc, "json_rpc_method_count",
                                      getattr(rpc, "request_count", 0))),
        "multicallSubcalls": int(getattr(rpc, "multicall_subcall_count", 0)),
        "ethGetLogs": int(getattr(rpc, "eth_get_logs_count", 0)),
        "rateLimits": int(getattr(rpc, "rate_limit_count", 0)),
        "timeouts": int(getattr(rpc, "timeout_count", 0)),
    }


def counter_delta(after: Mapping[str, int], before: Mapping[str, int]) -> dict[str, int]:
    return {name: max(0, value - before.get(name, 0)) for name, value in after.items()}


def selector(signature: str) -> str:
    return "0x" + keccak256(signature.encode())[:4].hex()


def normalize_token(value: str) -> str:
    token = value.lower()
    if not _ADDRESS.fullmatch(token) or not int(token, 16) or token == WETH:
        raise ValueError("discovery_token_invalid")
    return token


def normalize_fee_tiers(values: list[int] | tuple[int, ...]) -> tuple[int, ...]:
    if not values or any(type(value) is not int or not 0 < value < 1_000_000
                         for value in values):
        raise ValueError("discovery_fee_tiers_invalid")
    return tuple(sorted(set(values)))


class ScanLimitReached(RuntimeError):
    """A configured history budget ended before the confirmed head."""


class CountingFailoverJsonRpc(FailoverJsonRpc):
    """Expose a diagnostic-only count without changing failover semantics."""

    def __init__(self, chain_id: str, endpoints: list[RpcEndpoint]) -> None:
        super().__init__(chain_id, endpoints)
        self.request_count = 0

    def _request(self, endpoint: RpcEndpoint, method: str, params: Any) -> Any:
        self.request_count += 1
        return super()._request(endpoint, method, params)


def topic_address(value: str) -> str:
    return "0x" + value[2:].rjust(64, "0")


def address_word(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 66:
        raise ValueError("discovery_address_word_invalid")
    number = int(value, 16)
    if number == 0 or number >= 1 << 160:
        raise ValueError("discovery_address_word_invalid")
    return f"0x{number:040x}"


def optional_address_word(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != 66:
        raise ValueError("discovery_address_word_invalid")
    if int(value, 16) == 0:
        return None
    return address_word(value)


def signed_word(value: int, bits: int) -> int:
    low = value & ((1 << bits) - 1)
    upper = value >> bits
    negative = bool(low & (1 << (bits - 1)))
    if upper != ((1 << (256 - bits)) - 1 if negative else 0):
        raise ValueError("discovery_signed_word_invalid")
    return low - (1 << bits) if negative else low


def call(rpc: Any, to: str, signature: str, tag: str, *args: int) -> str:
    data = selector(signature) + "".join(f"{arg % (1 << 256):064x}" for arg in args)
    result = rpc.call("eth_call", [{"to": to, "data": data}, tag])
    if not isinstance(result, str) or not result.startswith("0x"):
        raise ValueError("discovery_call_invalid")
    return result


def words(raw: str, count: int) -> list[int]:
    if not isinstance(raw, str) or len(raw) < 2 + 64 * count:
        raise ValueError("discovery_call_short")
    return [int(raw[2 + 64 * i:2 + 64 * (i + 1)], 16) for i in range(count)]


@dataclass(frozen=True, slots=True)
class V3Candidate:
    token0: str
    token1: str
    fee: int
    tick_spacing: int
    pool: str
    event_block: int
    transaction_hash: str
    log_index: int

    @property
    def ca(self) -> str:
        return self.token1 if self.token0 == WETH else self.token0


class ScanCursorStore:
    """Atomic JSON cursor; contains no endpoint or credential material."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path).resolve()

    def load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"version": 1, "chainId": CHAIN_ID, "tokens": {}}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("discovery_cursor_invalid") from error
        if (not isinstance(value, dict) or value.get("version") != 1
                or value.get("chainId") != CHAIN_ID or not isinstance(value.get("tokens"), dict)):
            raise ValueError("discovery_cursor_invalid")
        return value

    def save(self, value: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True),
                             encoding="utf-8")
        os.replace(temporary, self.path)


def _decode_pool_created(log: Mapping[str, Any], token: str,
                         *, factory: str) -> V3Candidate | None:
    if str(log.get("address") or "").lower() != factory or log.get("removed") is True:
        return None
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) != 4 or topics[0].lower() != POOL_CREATED:
        return None
    token0, token1 = address_word(topics[1]), address_word(topics[2])
    if token0 >= token1 or {token0, token1} != {token, WETH}:
        return None
    fee = int(topics[3], 16)
    decoded = words(str(log.get("data") or ""), 2)
    spacing, pool = signed_word(decoded[0], 24), f"0x{decoded[1]:040x}"
    block = int(str(log.get("blockNumber") or "0x0"), 16)
    tx_hash = str(log.get("transactionHash") or "").lower()
    log_index = int(str(log.get("logIndex") or "0x0"), 16)
    if (not 0 < fee < 1_000_000 or not 0 < spacing < 32768 or block <= 0
            or not _ADDRESS.fullmatch(pool) or not _HASH.fullmatch(tx_hash)):
        raise ValueError("discovery_pool_event_invalid")
    return V3Candidate(token0, token1, fee, spacing, pool, block, tx_hash, log_index)


def _query_logs(rpc: Any, *, token: str, start: int, end: int,
                consume_request: Callable[[], None] | None = None) -> list[Mapping[str, Any]]:
    config = CHAIN_CONFIGS[CHAIN_ID]
    found: dict[tuple[str, str], Mapping[str, Any]] = {}
    for position in (1, 2):
        topics: list[Any] = [POOL_CREATED] + [None] * (position - 1) + [topic_address(token)]
        if consume_request is not None:
            consume_request()
        result = rpc.call("eth_getLogs", [{"address": config.factory,
                                            "fromBlock": hex(start), "toBlock": hex(end),
                                            "topics": topics}])
        if not isinstance(result, list):
            raise ValueError("discovery_logs_invalid")
        for item in result:
            if not isinstance(item, Mapping):
                raise ValueError("discovery_logs_invalid")
            key = (str(item.get("transactionHash")), str(item.get("logIndex")))
            found[key] = item
    return list(found.values())


def first_code_block(rpc: Any, token: str, head: int) -> int:
    """Find token deployment; this is only the lower scan bound."""
    hinted = FIRST_CODE_HINTS.get(token)
    if hinted is not None and hinted <= head:
        prior = rpc.call("eth_getCode", [token, hex(hinted - 1)])
        at = rpc.call("eth_getCode", [token, hex(hinted)])
        if prior == "0x" and isinstance(at, str) and at != "0x":
            return hinted
    latest = rpc.call("eth_getCode", [token, hex(head)])
    if not isinstance(latest, str) or latest == "0x":
        raise ValueError("discovery_token_code_missing")
    low, high = 0, head
    while low < high:
        mid = (low + high) // 2
        code = rpc.call("eth_getCode", [token, hex(mid)])
        if not isinstance(code, str):
            raise ValueError("discovery_historical_code_invalid")
        if code == "0x":
            low = mid + 1
        else:
            high = mid
    return low


def scan_v3_candidates(rpc: Any, *, token: str, confirmed_head: int,
                       cursor_store: ScanCursorStore, maximum_chunk: int = MAX_LOG_CHUNK,
                       maximum_requests: int = DEFAULT_SCAN_REQUEST_BUDGET,
                       maximum_block_span: int = DEFAULT_SCAN_BLOCK_SPAN,
                       on_chunk: Callable[[int, int], None] | None = None) -> list[V3Candidate]:
    """Resume an inclusive, bounded Factory log scan without boundary gaps."""
    token = normalize_token(token)
    if (confirmed_head <= 0 or not 0 < maximum_chunk <= MAX_LOG_CHUNK
            or maximum_requests <= 0 or maximum_block_span <= 0):
        raise ValueError("discovery_scan_scope_invalid")
    state = cursor_store.load()
    tokens = state["tokens"]
    record = tokens.get(token)
    if record is None:
        start = first_code_block(rpc, token, confirmed_head)
        record = {"nextBlock": start, "candidates": []}
    next_block = int(record.get("nextBlock", 0))
    scan_end = min(confirmed_head, next_block + maximum_block_span - 1)
    request_count = 0

    def consume_request() -> None:
        nonlocal request_count
        if request_count >= maximum_requests:
            raise ScanLimitReached("discovery_scan_request_budget_exhausted")
        request_count += 1

    candidates = {item["pool"]: V3Candidate(**item)
                  for item in record.get("candidates", [])}
    chunk = maximum_chunk
    while next_block <= scan_end:
        end = min(scan_end, next_block + chunk - 1)
        try:
            logs = _query_logs(rpc, token=token, start=next_block, end=end,
                               consume_request=consume_request)
        except (RpcUnavailable, ValueError):
            if chunk == 1:
                raise
            chunk = max(1, chunk // 2)
            continue
        for log in logs:
            candidate = _decode_pool_created(log, token, factory=CHAIN_CONFIGS[CHAIN_ID].factory)
            if candidate is not None:
                candidates[candidate.pool] = candidate
        completed_start = next_block
        next_block = end + 1
        record = {"nextBlock": next_block,
                  "candidates": [asdict(item) for item in candidates.values()]}
        tokens[token] = record
        cursor_store.save(state)
        if on_chunk is not None:
            on_chunk(completed_start, end)
        # Keep the largest range this endpoint has actually accepted for the
        # rest of this run.  Resetting to ``maximum_chunk`` here makes a
        # range-limited provider reject every subsequent chunk and repeats
        # the entire backoff ladder for each cursor commit.
    if next_block <= confirmed_head:
        raise ScanLimitReached("discovery_scan_block_span_exhausted")
    return list(candidates.values())


def select_verified_candidate(rpc: Any, *, token: str, candidates: list[V3Candidate],
                              block_height: int) -> tuple[V3Candidate | None, str]:
    """Accept exactly one Factory-consistent WETH pool; otherwise fail closed."""
    config = CHAIN_CONFIGS[CHAIN_ID]
    valid: dict[str, V3Candidate] = {}
    tag = hex(block_height)
    for candidate in candidates:
        if candidate.ca != token or WETH not in {candidate.token0, candidate.token1}:
            continue
        result = optional_address_word(call(
            rpc, config.factory, "getPool(address,address,uint24)", tag,
            int(candidate.token0, 16), int(candidate.token1, 16), candidate.fee))
        if result == candidate.pool:
            valid[candidate.pool] = candidate
    if not valid:
        return None, "missing"
    if len(valid) != 1:
        return None, "ambiguous"
    return next(iter(valid.values())), "selected"


def probe_approved_fee_tiers(
    rpc: Any, *, cache: CaRouteCache, tokens: tuple[str, ...],
    fee_tiers: tuple[int, ...], amount_in_units: int, confirmations: int,
    now_ms: Callable[[], int] | None = None,
    invalidate_on_failure: bool = True,
) -> dict[str, Any]:
    """Find pools only inside an explicit fee set; never reads Factory logs."""
    started = time.monotonic()
    clock = now_ms or (lambda: int(time.time() * 1000))
    approved_tokens = tuple(sorted(set(normalize_token(token) for token in tokens)))
    approved_fees = normalize_fee_tiers(fee_tiers)
    counters_before = rpc_counters(rpc)
    context = pin_v3_block_context(rpc, chain_id=CHAIN_ID, confirmations=confirmations)
    fixed_height = context.block_height
    fixed_hash = context.block_hash
    tag = hex(fixed_height)
    result: dict[str, Any] = {
        "chainId": CHAIN_ID, "fixedBlock": fixed_height, "blockHash": fixed_hash,
        "approvedFeeTiers": list(approved_fees), "tokens": {},
        "scope": "unique_within_approved_fee_tiers_only", "tradingReady": False,
    }
    config = CHAIN_CONFIGS[CHAIN_ID]
    for token in approved_tokens:
        token0, token1 = sorted((token, WETH))
        tier_results: dict[str, Any] = {}
        verified: list[VerifiedV3Scan] = []
        incomplete = False
        for fee in approved_fees:
            try:
                pool = optional_address_word(call(
                    rpc, config.factory, "getPool(address,address,uint24)", tag,
                    int(token0, 16), int(token1, 16), fee))
                if pool is None:
                    tier_results[str(fee)] = {"status": "missing", "pool": None}
                    continue
                spacing = signed_word(words(call(
                    rpc, config.factory, "feeAmountTickSpacing(uint24)", tag, fee), 1)[0], 24)
                if not 0 < spacing < 32768:
                    raise ValueError("discovery_tick_spacing_invalid")
                candidate = V3Candidate(token0, token1, fee, spacing, pool, 0,
                                        "0x" + "00" * 31 + "01", 0)
                scan = _verified_scan_for_candidate(
                    rpc, candidate, amount_in_units=amount_in_units,
                    evidence_kind=DIRECT_EVIDENCE,
                    approved_fee_tiers=approved_fees, block_height=fixed_height,
                    context=context,
                )
                if scan.block_hash != fixed_hash:
                    raise ValueError("discovery_fixed_block_hash_mismatch")
                verified.append(scan)
                tier_results[str(fee)] = {
                    "status": "verified", "pool": pool,
                    "tickSpacing": scan.tick_spacing,
                    "amountOutUnits": str(scan.amount_out_units),
                }
            except Exception as error:
                incomplete = True
                reason = str(error)
                tier_results[str(fee)] = {
                    "status": "failed",
                    "reason": reason if reason.startswith(("discovery_", "v3_"))
                    else type(error).__name__,
                }
        item: dict[str, Any] = {"tiers": tier_results, "published": False}
        if incomplete:
            item["selection"] = "incomplete"
            if invalidate_on_failure:
                cache.invalidate_discovered(CHAIN_ID, token)
        elif not verified:
            item["selection"] = "missing"
            if invalidate_on_failure:
                cache.invalidate_discovered(CHAIN_ID, token)
        elif len(verified) > 1:
            item["selection"] = "ambiguous_within_approved_fee_tiers"
            if invalidate_on_failure:
                cache.invalidate_discovered(CHAIN_ID, token)
        else:
            scan = verified[0]
            published_at = clock()
            entry = publish_verified_v3_scan(cache, scan, now_ms=published_at)
            item.update({
                "selection": "unique_within_approved_fee_tiers",
                "published": True,
                "quote": {
                    "pool": scan.pool, "fee": scan.fee,
                    "block": scan.block_height, "blockHash": scan.block_hash,
                    "amountInUnits": str(scan.amount_in_units),
                    "amountOutUnits": str(scan.amount_out_units),
                    "publishedAtMs": entry.written_at_ms,
                    "observedAtMs": scan.quoted_at_ms,
                    "blockTimestampMs": entry.quote.block_timestamp_ms if entry.quote else None,
                    "blockAgeAtStartMs": entry.quote.block_age_at_start_ms if entry.quote else None,
                    "blockAgeAtCompletionMs": entry.quote.block_age_at_completion_ms if entry.quote else None,
                    "snapshotReadDurationMs": entry.quote.snapshot_read_duration_ms if entry.quote else None,
                    "snapshotToCommitMs": entry.quote.snapshot_to_commit_ms if entry.quote else None,
                    "chainStateAgeAtPublishMs": entry.quote.chain_state_age_at_publish_ms if entry.quote else None,
                },
            })
        result["tokens"][token] = item
    counters = counter_delta(rpc_counters(rpc), counters_before)
    result.update(counters)
    result["rpcCallCount"] = counters["jsonRpcMethods"]
    result["elapsedMs"] = int((time.monotonic() - started) * 1000)
    return result


def _verified_scan_for_candidate(
    rpc: Any, candidate: V3Candidate, *, amount_in_units: int,
    evidence_kind: str, approved_fee_tiers: tuple[int, ...] | None,
    block_height: int | None = None,
    context: V3PinnedBlockContext | None = None,
) -> VerifiedV3Scan:
    if not 0 < amount_in_units < 2**128:
        raise ValueError("discovery_amount_in_invalid")
    head = (int(str(rpc.call("eth_blockNumber", [])), 16)
            if block_height is None else block_height)
    # -1 means the strict reader must discover and validate both decimals in
    # its pinned identity Multicall; it is not a caller assertion.
    target = V3PoolTarget(candidate.pool, candidate.token0, candidate.token1,
                          candidate.fee, candidate.tick_spacing, -1, -1)
    snapshot = UniswapV3PoolReader(rpc=rpc, chain_id=CHAIN_ID, target=target).snapshot(
        context=context) if context is not None else UniswapV3PoolReader(
            rpc=rpc, chain_id=CHAIN_ID, target=target).snapshot(block_height=head)
    quote = snapshot.quote(token_in=WETH, amount_in=amount_in_units)
    return VerifiedV3Scan(
        chain_id=CHAIN_ID, ca=candidate.ca, quote_token=WETH,
        token0=candidate.token0, token1=candidate.token1, fee=candidate.fee,
        tick_spacing=candidate.tick_spacing,
        pool=candidate.pool, factory_pool=candidate.pool,
        factory=CHAIN_CONFIGS[CHAIN_ID].factory,
        event_block=candidate.event_block or None, block_height=snapshot.block_height,
        block_hash=snapshot.block_hash, amount_in_units=amount_in_units,
        amount_out_units=quote.amount_out, quoted_at_ms=snapshot.observed_at_ms,
        evidence_kind=evidence_kind, approved_fee_tiers=approved_fee_tiers,
        block_timestamp_ms=snapshot.block_timestamp_ms,
        read_started_at_ms=snapshot.read_started_at_ms,
        block_age_at_start_ms=snapshot.block_age_at_start_ms,
        block_age_at_completion_ms=snapshot.block_age_at_completion_ms,
        snapshot_read_duration_ms=snapshot.read_duration_ms,
    )


def _publish_candidate(
    rpc: Any, cache: CaRouteCache, candidate: V3Candidate, *, amount_in_units: int,
    now_ms: Callable[[], int], evidence_kind: str = EVENT_EVIDENCE,
    approved_fee_tiers: tuple[int, ...] | None = None,
    block_height: int | None = None,
    context: V3PinnedBlockContext | None = None,
) -> Mapping[str, Any]:
    scan = _verified_scan_for_candidate(
        rpc, candidate, amount_in_units=amount_in_units,
        evidence_kind=evidence_kind, approved_fee_tiers=approved_fee_tiers,
        block_height=block_height,
        context=context,
    )
    published_at = now_ms()
    entry = publish_verified_v3_scan(cache, scan, now_ms=published_at)
    return {"ca": candidate.ca, "pool": candidate.pool,
            "block": scan.block_height, "blockHash": scan.block_hash,
            "amountInUnits": str(amount_in_units),
            "amountOutUnits": str(scan.amount_out_units),
            "publishedAtMs": entry.written_at_ms,
            "observedAtMs": scan.quoted_at_ms,
            "blockTimestampMs": entry.quote.block_timestamp_ms if entry.quote else None,
            "blockAgeAtStartMs": entry.quote.block_age_at_start_ms if entry.quote else None,
            "blockAgeAtCompletionMs": entry.quote.block_age_at_completion_ms if entry.quote else None,
            "snapshotReadDurationMs": entry.quote.snapshot_read_duration_ms if entry.quote else None,
            "snapshotToCommitMs": entry.quote.snapshot_to_commit_ms if entry.quote else None,
            "chainStateAgeAtPublishMs": entry.quote.chain_state_age_at_publish_ms if entry.quote else None}


def run_once(rpc: Any, *, cache: CaRouteCache, cursor_store: ScanCursorStore,
             tokens: tuple[str, ...], amount_in_units: int, confirmations: int,
             maximum_chunk: int = MAX_LOG_CHUNK,
             maximum_requests: int = DEFAULT_SCAN_REQUEST_BUDGET,
             maximum_block_span: int = DEFAULT_SCAN_BLOCK_SPAN,
             now_ms: Callable[[], int] | None = None) -> dict[str, Any]:
    clock = now_ms or (lambda: int(time.time() * 1000))
    if int(str(rpc.call("eth_chainId", [])), 16) != CHAIN_ID:
        raise ValueError("discovery_wrong_chain")
    head = int(str(rpc.call("eth_blockNumber", [])), 16)
    confirmed_head = head - confirmations
    if confirmations < 0 or confirmed_head <= 0:
        raise ValueError("discovery_confirmed_head_invalid")
    output: dict[str, Any] = {"chainId": CHAIN_ID, "confirmedHead": confirmed_head,
                              "tokens": {}, "tradingReady": False}
    for token in tokens:
        try:
            candidates = scan_v3_candidates(rpc, token=token, confirmed_head=confirmed_head,
                                            cursor_store=cursor_store,
                                            maximum_chunk=maximum_chunk,
                                            maximum_requests=maximum_requests,
                                            maximum_block_span=maximum_block_span)
            selected, status = select_verified_candidate(rpc, token=token,
                                                         candidates=candidates,
                                                         block_height=confirmed_head)
            item = {"candidateCount": len(candidates), "selection": status,
                    "published": False}
            if selected is not None:
                item["quote"] = _publish_candidate(rpc, cache, selected,
                                                    amount_in_units=amount_in_units,
                                                    now_ms=clock)
                item["published"] = True
            else:
                cache.invalidate_discovered(CHAIN_ID, token)
        except Exception as error:
            reason = str(error)
            item = {"published": False,
                    "reason": reason if reason.startswith(("discovery_", "v3_", "direct_cache_"))
                    else type(error).__name__}
        output["tokens"][token] = item
    return output


def refresh_cached_identities(rpc: Any, *, cache: CaRouteCache, tokens: tuple[str, ...],
                              amount_in_units: int,
                              now_ms: Callable[[], int] | None = None) -> dict[str, Any]:
    """Refresh quotes only; this function never calls eth_getLogs."""
    clock = now_ms or (lambda: int(time.time() * 1000))
    output: dict[str, Any] = {"chainId": CHAIN_ID, "refreshed": {},
                              "tradingReady": False}
    for token in tokens:
        entry = cache.lookup(CHAIN_ID, token)
        if (entry is None or entry.protocol != "V3" or entry.pool is None
                or entry.token0 is None or entry.token1 is None or entry.fee is None
                or not entry.identity_verified
                or entry.identity_factory != CHAIN_CONFIGS[CHAIN_ID].factory
                or entry.identity_evidence_kind not in {EVENT_EVIDENCE, DIRECT_EVIDENCE}
                or not 0 <= clock() - entry.written_at_ms <= ROUTE_MAX_AGE_MS):
            output["refreshed"][token] = {"published": False,
                                           "reason": "identity_missing_or_stale"}
            if entry is not None:
                cache.invalidate_discovered(CHAIN_ID, token)
            continue
        if entry.identity_evidence_kind == DIRECT_EVIDENCE:
            try:
                tiers = entry.approved_fee_tiers or ()
                if len(tiers) > 1:
                    direct = probe_approved_fee_tiers(
                        rpc, cache=cache, tokens=(token,), fee_tiers=tiers,
                        amount_in_units=amount_in_units, confirmations=0,
                        now_ms=clock, invalidate_on_failure=False,
                    )
                    output["refreshed"][token] = direct["tokens"][token]
                    output["metrics"] = {
                        key: int(direct.get(key, 0)) for key in (
                            "elapsedMs", "httpRoundTrips", "jsonRpcMethods",
                            "multicallSubcalls", "ethGetLogs", "rateLimits", "timeouts")
                    }
                    continue
                if len(tiers) != 1 or entry.fee != tiers[0] or entry.tick_spacing is None:
                    raise ValueError("identity_approved_fee_scope_not_single")
                before = rpc_counters(rpc)
                started = time.monotonic()
                context = pin_v3_block_context(rpc, chain_id=CHAIN_ID, confirmations=0)
                candidate = V3Candidate(entry.token0, entry.token1, entry.fee,
                                        entry.tick_spacing, entry.pool, 0,
                                        "0x" + "00" * 31 + "01", 0)
                quote = _publish_candidate(
                    rpc, cache, candidate, amount_in_units=amount_in_units,
                    now_ms=clock, evidence_kind=DIRECT_EVIDENCE,
                    approved_fee_tiers=tiers, context=context,
                )
                output["refreshed"][token] = {"published": True, "quote": quote}
                metrics = counter_delta(rpc_counters(rpc), before)
                metrics["elapsedMs"] = int((time.monotonic() - started) * 1000)
                output["metrics"] = metrics
            except Exception as error:
                reason = str(error)
                output["refreshed"][token] = {
                    "published": False,
                    "reason": reason if reason.startswith(("discovery_", "v3_", "direct_cache_"))
                    else type(error).__name__,
                }
            continue
        if int(str(rpc.call("eth_chainId", [])), 16) != CHAIN_ID:
            raise ValueError("discovery_wrong_chain")
        try:
            factory_pool = optional_address_word(call(
                rpc, CHAIN_CONFIGS[CHAIN_ID].factory, "getPool(address,address,uint24)", "latest",
                int(entry.token0, 16), int(entry.token1, 16), entry.fee))
        except Exception as error:
            reason = str(error)
            output["refreshed"][token] = {
                "published": False,
                "reason": reason if reason.startswith(("discovery_", "v3_", "direct_cache_"))
                else type(error).__name__,
            }
            continue
        if factory_pool != entry.pool:
            output["refreshed"][token] = {"published": False,
                                           "reason": "factory_identity_changed"}
            cache.invalidate_discovered(CHAIN_ID, token)
            continue
        if (entry.tick_spacing is None
                or entry.identity_evidence_kind == EVENT_EVIDENCE
                and entry.identity_event_block is None
                or entry.identity_evidence_kind == DIRECT_EVIDENCE
                and not entry.approved_fee_tiers):
            output["refreshed"][token] = {"published": False,
                                           "reason": "identity_evidence_incomplete"}
            cache.invalidate_discovered(CHAIN_ID, token)
            continue
        candidate = V3Candidate(entry.token0, entry.token1, entry.fee,
                                entry.tick_spacing, entry.pool,
                                entry.identity_event_block or 0,
                                "0x" + "00" * 31 + "01", 0)
        try:
            output["refreshed"][token] = {
                "published": True,
                "quote": _publish_candidate(rpc, cache, candidate,
                                             amount_in_units=amount_in_units,
                                             now_ms=clock,
                                             evidence_kind=entry.identity_evidence_kind,
                                             approved_fee_tiers=entry.approved_fee_tiers),
            }
        except Exception as error:
            reason = str(error)
            output["refreshed"][token] = {
                "published": False,
                "reason": reason if reason.startswith(("v3_", "direct_cache_", "discovery_"))
                else type(error).__name__,
            }
    return output


def _percentile(values: list[int], percent: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = ((len(ordered) - 1) * percent + 99) // 100
    return ordered[index]


def _freshness_coverage(start_ms: int, end_ms: int,
                        published: list[int]) -> tuple[float, int]:
    if end_ms <= start_ms:
        return 0.0, 0
    intervals = sorted((max(start_ms, point), min(end_ms, point + 2_000))
                       for point in published if point < end_ms and point + 2_000 > start_ms)
    merged: list[tuple[int, int]] = []
    for left, right in intervals:
        if left >= right:
            continue
        if not merged or left > merged[-1][1]:
            merged.append((left, right))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], right))
    fresh = sum(right - left for left, right in merged)
    cursor = start_ms
    maximum_expired = 0
    for left, right in merged:
        maximum_expired = max(maximum_expired, left - cursor)
        cursor = max(cursor, right)
    maximum_expired = max(maximum_expired, end_ms - cursor)
    return round(fresh * 100 / (end_ms - start_ms), 3), maximum_expired


def run_watch(rpc: Any, *, cache: CaRouteCache, tokens: tuple[str, ...],
              amount_in_units: int, interval_ms: int,
              duration_seconds: int = 0,
              emit: Callable[[Mapping[str, Any]], None] | None = None,
              maximum_cycles: int | None = None) -> dict[str, Any]:
    """Serial refresh loop with bounded pacing and an auditable freshness summary."""
    if not 250 <= interval_ms <= WATCH_MAX_INTERVAL_MS or duration_seconds < 0:
        raise ValueError("watch_timing_invalid")
    output = emit or (lambda item: print(json.dumps(item, ensure_ascii=False), flush=True))
    watch_started_ms = int(time.time() * 1000)
    deadline = time.monotonic() + duration_seconds if duration_seconds else None
    durations: list[int] = []
    http_round_trips: list[int] = []
    json_methods: list[int] = []
    multicall_subcalls: list[int] = []
    success_times: list[int] = []
    chain_ages: list[int] = []
    failures: dict[str, int] = {}
    per_token = {token: {"successes": 0, "failures": 0} for token in tokens}
    rate_limits = timeouts = reorgs = sqlite_locks = logs = 0
    cycles = 0
    while ((deadline is None or time.monotonic() < deadline)
           and (maximum_cycles is None or cycles < maximum_cycles)):
        round_started = time.monotonic()
        counters_before = rpc_counters(rpc)
        try:
            result = refresh_cached_identities(
                rpc, cache=cache, tokens=tokens, amount_in_units=amount_in_units)
        except Exception as error:
            reason = str(error)
            safe = (reason if reason.startswith(("discovery_", "v3_", "direct_cache_"))
                    else type(error).__name__)
            result = {"chainId": CHAIN_ID,
                      "refreshed": {token: {"published": False, "reason": safe}
                                    for token in tokens},
                      "tradingReady": False}
        elapsed = int((time.monotonic() - round_started) * 1000)
        measured_metrics = counter_delta(rpc_counters(rpc), counters_before)
        cycles += 1
        durations.append(elapsed)
        metrics = measured_metrics
        http_round_trips.append(int(metrics.get("httpRoundTrips", 0)))
        json_methods.append(int(metrics.get("jsonRpcMethods", 0)))
        multicall_subcalls.append(int(metrics.get("multicallSubcalls", 0)))
        rate_limits += int(metrics.get("rateLimits", 0))
        timeouts += int(metrics.get("timeouts", 0))
        logs += int(metrics.get("ethGetLogs", 0))
        for token, item in result["refreshed"].items():
            quote = item.get("quote") if isinstance(item, Mapping) else None
            if item.get("published") and isinstance(quote, Mapping):
                per_token[token]["successes"] += 1
                success_times.append(int(quote["publishedAtMs"]))
                chain_ages.append(int(quote["chainStateAgeAtPublishMs"]))
            else:
                per_token[token]["failures"] += 1
                reason = str(item.get("reason") or item.get("selection") or "unknown")
                failures[reason] = failures.get(reason, 0) + 1
                reorgs += int("reorg" in reason)
                sqlite_locks += int("cache_unavailable" in reason or "locked" in reason)
        result["watchCycle"] = cycles
        result["cycleElapsedMs"] = elapsed
        result["intervalOverrun"] = elapsed > interval_ms
        output(result)
        remaining = interval_ms - elapsed
        if remaining > 0:
            if deadline is not None:
                remaining = min(remaining, max(0, int((deadline - time.monotonic()) * 1000)))
            if remaining:
                time.sleep(remaining / 1000)
    watch_ended_ms = int(time.time() * 1000)
    intervals = [right - left for left, right in zip(success_times, success_times[1:])]
    coverage, maximum_expired = _freshness_coverage(
        watch_started_ms, watch_ended_ms, success_times)
    summary = {
        "watchDurationMs": watch_ended_ms - watch_started_ms,
        "cycles": cycles, "successfulPublishes": len(success_times),
        "failures": sum(failures.values()), "failureReasons": failures,
        "refreshMs": {
            "average": round(statistics.fmean(durations), 3) if durations else None,
            "p50": _percentile(durations, 50), "p95": _percentile(durations, 95),
            "p99": _percentile(durations, 99), "maximum": max(durations, default=None),
        },
        "httpRoundTrips": {"average": round(statistics.fmean(http_round_trips), 3)
                           if http_round_trips else None,
                           "minimum": min(http_round_trips, default=None),
                           "maximum": max(http_round_trips, default=None)},
        "jsonRpcMethods": {"average": round(statistics.fmean(json_methods), 3)
                           if json_methods else None,
                           "minimum": min(json_methods, default=None),
                           "maximum": max(json_methods, default=None)},
        "multicallSubcalls": {
            "average": round(statistics.fmean(multicall_subcalls), 3)
            if multicall_subcalls else None,
            "minimum": min(multicall_subcalls, default=None),
            "maximum": max(multicall_subcalls, default=None),
        },
        "perToken": per_token,
        "successfulQuoteIntervalMs": {
            "average": round(statistics.fmean(intervals), 3) if intervals else None,
            "p95": _percentile(intervals, 95), "maximum": max(intervals, default=None),
        },
        "freshWithinTwoSecondsPercent": coverage,
        "maximumContinuousExpiredMs": maximum_expired,
        "chainStateAgeAtPublishMs": {
            "average": round(statistics.fmean(chain_ages), 3) if chain_ages else None,
            "p50": _percentile(chain_ages, 50), "p95": _percentile(chain_ages, 95),
            "p99": _percentile(chain_ages, 99), "maximum": max(chain_ages, default=None),
        },
        "providerChanges": 0, "rateLimits": rate_limits, "timeouts": timeouts,
        "reorgs": reorgs, "sqliteLockErrors": sqlite_locks,
        "ethGetLogsCalls": logs, "tradingReady": False,
    }
    output({"watchSummary": summary, "tradingReady": False})
    return summary


def _rpc() -> Any:
    endpoint = RpcEndpoint("4663", "v3-cache-producer", "primary",
                           http_env="RPC_ROBINHOOD_URL")
    return (RobinhoodDiscoveryRpc(endpoint, timeout_seconds=20.0) if proxy_configured()
            else CountingFailoverJsonRpc("4663", [endpoint]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--factory-probe", action="store_true")
    mode.add_argument("--scan-history", action="store_true")
    mode.add_argument("--watch", action="store_true")
    parser.add_argument("--cache-db", required=True)
    parser.add_argument("--cursor", help="Scan cursor JSON (defaults beside cache DB)")
    parser.add_argument("--token", action="append", default=[])
    parser.add_argument("--fee-tier", action="append", type=int, default=[])
    parser.add_argument("--amount-in-units", required=True, type=int)
    parser.add_argument("--confirmations", type=int, default=DEFAULT_CONFIRMATIONS)
    parser.add_argument("--chunk-size", type=int, default=MAX_LOG_CHUNK)
    parser.add_argument("--scan-request-budget", type=int,
                        default=DEFAULT_SCAN_REQUEST_BUDGET)
    parser.add_argument("--scan-block-span", type=int,
                        default=DEFAULT_SCAN_BLOCK_SPAN)
    parser.add_argument("--interval-ms", type=int, default=1_000)
    parser.add_argument("--watch-seconds", type=int, default=0)
    options = parser.parse_args(argv)
    if (not 0 < options.amount_in_units < 2**128
            or not 0 < options.chunk_size <= MAX_LOG_CHUNK
            or options.scan_request_budget <= 0 or options.scan_block_span <= 0
            or not 250 <= options.interval_ms <= WATCH_MAX_INTERVAL_MS
            or options.watch_seconds < 0):
        parser.error("amount/chunk/interval out of range")
    try:
        tokens = tuple(sorted(set(normalize_token(token) for token in options.token)))
        if not tokens:
            raise ValueError("discovery_token_invalid")
        fee_tiers = normalize_fee_tiers(options.fee_tier) if options.factory_probe else ()
    except ValueError as error:
        parser.error(str(error))
    if not options.factory_probe and options.fee_tier:
        parser.error("fee tiers are accepted only with --factory-probe")
    if not os.getenv("RPC_ROBINHOOD_URL", "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}))
        return 2
    cache = CaRouteCache(options.cache_db)
    cache.initialize_for_discovery()
    rpc = _rpc()
    try:
        if options.factory_probe:
            result = probe_approved_fee_tiers(
                rpc, cache=cache, tokens=tokens, fee_tiers=fee_tiers,
                amount_in_units=options.amount_in_units,
                confirmations=options.confirmations,
            )
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 0
        if options.scan_history:
            cursor_path = options.cursor or str(Path(options.cache_db).with_suffix(".cursor.json"))
            result = run_once(rpc, cache=cache, cursor_store=ScanCursorStore(cursor_path),
                              tokens=tokens, amount_in_units=options.amount_in_units,
                              confirmations=options.confirmations,
                              maximum_chunk=options.chunk_size,
                              maximum_requests=options.scan_request_budget,
                              maximum_block_span=options.scan_block_span)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 0
        run_watch(rpc, cache=cache, tokens=tokens,
                  amount_in_units=options.amount_in_units,
                  interval_ms=options.interval_ms,
                  duration_seconds=options.watch_seconds)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        reason = str(error)
        safe = reason if reason.startswith(("discovery_", "v3_", "direct_cache_")) else type(error).__name__
        print(json.dumps({"status": "failed", "reason": safe,
                          "tradingReady": False}), flush=True)
        return 1


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
