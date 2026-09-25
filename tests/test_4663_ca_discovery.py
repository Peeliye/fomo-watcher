from __future__ import annotations

import tempfile
import unittest
import io
import json
import time
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fomo.execution.cached_direct_buy import CaRouteCache
from fomo.execution.direct_v3 import CHAIN_CONFIGS
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.route_cache_writer import VerifiedV3Scan, publish_verified_v3_scan
from scripts._robinhood_probe_transport import RobinhoodDiscoveryRpc
from scripts.discover_two_ca_4663 import (CHAIN_ID, DIRECT_EVIDENCE, POOL_CREATED,
                                          TOKENS, WETH,
                                          ScanCursorStore, V3Candidate,
                                          _publish_candidate, refresh_cached_identities,
                                          main, normalize_fee_tiers,
                                          probe_approved_fee_tiers, run_once,
                                          run_watch, scan_v3_candidates, selector,
                                          select_verified_candidate)

POOL = "0x10cc6bd38112cac182db90b6a71d8bb5939526ba"
POOL2 = "0x" + "22" * 20
BLOCK_HASH = "0x" + "ab" * 32
FACTORY = CHAIN_CONFIGS[CHAIN_ID].factory


def word(value: int) -> str:
    return "0x" + f"{value:064x}"


def event(token: str, *, pool: str = POOL, block: int = 130, fee: int = 10_000,
          index: int = 0):
    token0, token1 = sorted((token, WETH))
    return {"address": FACTORY, "topics": [POOL_CREATED, word(int(token0, 16)),
                                             word(int(token1, 16)), word(fee)],
            "data": word(200) + f"{int(pool, 16):064x}",
            "blockNumber": hex(block), "blockHash": BLOCK_HASH,
            "transactionHash": "0x" + f"{block + index:064x}",
            "logIndex": hex(index), "removed": False}


class ScanRpc:
    last_provider = "fixture"

    def __init__(self, logs=(), *, chain_id=CHAIN_ID, factory_pool=POOL,
                 reject_width: int | None = None):
        self.logs = list(logs)
        self.chain_id = chain_id
        self.factory_pool = factory_pool
        self.reject_width = reject_width
        self.ranges: list[tuple[int, int]] = []
        self.methods: list[str] = []

    def call(self, method, params):
        self.methods.append(method)
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_blockNumber":
            return hex(200)
        if method == "eth_getCode":
            height = int(params[1], 16)
            return "0x" if height < 100 else "0x01"
        if method == "eth_getLogs":
            query = params[0]
            start, end = int(query["fromBlock"], 16), int(query["toBlock"], 16)
            self.ranges.append((start, end))
            if self.reject_width is not None and end - start + 1 > self.reject_width:
                raise ValueError("rpc_log_range_too_wide")
            token_topic = next((value for value in query["topics"][1:] if value), None)
            return [item for item in self.logs if start <= int(item["blockNumber"], 16) <= end
                    and token_topic in item["topics"][1:]]
        if method == "eth_call" and params[0]["to"] == FACTORY:
            return word(int(self.factory_pool, 16))
        raise AssertionError((method, params))


class FeeProbeRpc:
    last_provider = "fixture"

    def __init__(self, pools: dict[int, str | None | Exception]):
        self.pools = pools
        self.methods: list[str] = []
        self.request_count = 0

    def call(self, method, params):
        self.methods.append(method)
        self.request_count += 1
        if method == "eth_chainId":
            return hex(CHAIN_ID)
        if method == "eth_blockNumber":
            return hex(200)
        if method == "eth_getBlockByNumber":
            return {"number": hex(200), "hash": BLOCK_HASH,
                    "timestamp": hex(int(time.time()) - 1)}
        if method == "eth_call":
            data = params[0]["data"]
            fee = int(data[-64:], 16)
            if data.startswith(selector("getPool(address,address,uint24)")):
                pool = self.pools.get(fee)
                if isinstance(pool, Exception):
                    raise pool
                return word(0 if pool is None else int(pool, 16))
            if data.startswith(selector("feeAmountTickSpacing(uint24)")):
                return word(200)
        raise AssertionError((method, params))


def direct_scan(_rpc, candidate: V3Candidate, *, amount_in_units: int,
                evidence_kind: str, approved_fee_tiers: tuple[int, ...] | None,
                block_height: int | None = None, **_kwargs) -> VerifiedV3Scan:
    assert evidence_kind == DIRECT_EVIDENCE
    assert approved_fee_tiers is not None
    return VerifiedV3Scan(
        chain_id=CHAIN_ID, ca=candidate.ca, quote_token=WETH,
        token0=candidate.token0, token1=candidate.token1, fee=candidate.fee,
        tick_spacing=candidate.tick_spacing, pool=candidate.pool,
        factory_pool=candidate.pool, factory=FACTORY, event_block=None,
        block_height=block_height or 200, block_hash=BLOCK_HASH,
        amount_in_units=amount_in_units, amount_out_units=1234,
        quoted_at_ms=1_000, evidence_kind=evidence_kind,
        approved_fee_tiers=approved_fee_tiers,
        block_timestamp_ms=900, read_started_at_ms=950,
        block_age_at_start_ms=50, block_age_at_completion_ms=100,
        snapshot_read_duration_ms=50,
    )


class ProducerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.cursor = ScanCursorStore(root / "cursor.json")
        self.cache = CaRouteCache(root / "routes.sqlite3")
        self.cache.initialize_for_discovery()

    def test_late_pool_chunk_boundaries_and_provider_range_backoff(self):
        rpc = ScanRpc([event(TOKENS[0], block=130)], reject_width=10)
        found = scan_v3_candidates(rpc, token=TOKENS[0], confirmed_head=140,
                                   cursor_store=self.cursor, maximum_chunk=20)
        self.assertEqual([item.pool for item in found], [POOL])
        successful = [item for item in rpc.ranges if item[1] - item[0] + 1 <= 10]
        covered = set()
        for start, end in successful[::2]:  # two indexed token-position queries per chunk
            covered.update(range(start, end + 1))
        self.assertEqual(covered, set(range(100, 141)))
        rejected = [item for item in rpc.ranges if item[1] - item[0] + 1 > 10]
        self.assertEqual(rejected, [(100, 119)])
        self.assertEqual(self.cursor.load()["tokens"][TOKENS[0]]["nextBlock"], 141)

    def test_read_only_transport_records_provider_after_verified_response(self):
        endpoint = RpcEndpoint("4663", "fixture-provider", "primary",
                               public_http_url="https://rpc.example.com")
        rpc = RobinhoodDiscoveryRpc(endpoint)
        with patch("scripts._robinhood_probe_transport.read_only_proxy_request",
                   return_value=hex(CHAIN_ID)):
            self.assertEqual(rpc.call("eth_chainId", []), hex(CHAIN_ID))
        self.assertEqual(rpc.last_provider, "fixture-provider")

    def test_cursor_resumes_after_chunk_commit_without_duplicate_candidate(self):
        rpc = ScanRpc([event(TOKENS[0], block=105)])
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            scan_v3_candidates(rpc, token=TOKENS[0], confirmed_head=120,
                               cursor_store=self.cursor, maximum_chunk=10,
                               on_chunk=lambda _start, end: (_ for _ in ()).throw(
                                   RuntimeError("interrupted")) if end == 109 else None)
        resumed = ScanRpc([event(TOKENS[0], block=105)])
        found = scan_v3_candidates(resumed, token=TOKENS[0], confirmed_head=120,
                                   cursor_store=self.cursor, maximum_chunk=10)
        self.assertEqual(len(found), 1)
        self.assertTrue(all(start >= 110 for start, _ in resumed.ranges))

    def test_factory_mismatch_rejected_and_multiple_valid_is_ambiguous(self):
        one = V3Candidate(*sorted((TOKENS[0], WETH)), 10_000, 200, POOL,
                          130, "0x" + "11" * 32, 0)
        mismatch, status = select_verified_candidate(
            ScanRpc(factory_pool=POOL2), token=TOKENS[0], candidates=[one], block_height=200)
        self.assertIsNone(mismatch)
        self.assertEqual(status, "missing")
        two = V3Candidate(*sorted((TOKENS[0], WETH)), 3_000, 60, POOL2,
                          140, "0x" + "22" * 32, 0)
        rpc = ScanRpc()
        original = rpc.call
        pools = iter((POOL, POOL2))
        rpc.call = lambda method, params: (word(int(next(pools), 16))
                                           if method == "eth_call" else original(method, params))
        selected, status = select_verified_candidate(rpc, token=TOKENS[0],
                                                     candidates=[one, two], block_height=200)
        self.assertIsNone(selected)
        self.assertEqual(status, "ambiguous")

    def test_wrong_chain_rejected_before_scan(self):
        with self.assertRaisesRegex(ValueError, "discovery_wrong_chain"):
            run_once(ScanRpc(chain_id=1), cache=self.cache, cursor_store=self.cursor,
                     tokens=(TOKENS[0],), amount_in_units=10**12, confirmations=2)

    def test_missing_process_rpc_is_redacted_and_no_dotenv_loader_exists(self):
        output = io.StringIO()
        with patch.dict("os.environ", {"RPC_ROBINHOOD_URL": ""}), redirect_stdout(output):
            status = main(["--scan-history", "--cache-db", str(self.cache.path),
                           "--cursor", str(self.cursor.path),
                           "--token", TOKENS[0],
                           "--amount-in-units", "1000000000000"])
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(output.getvalue()),
                         {"status": "未注入", "tradingReady": False})

    def test_factory_probe_deduplicates_fee_set_and_never_reads_logs(self):
        rpc = FeeProbeRpc({500: None, 10_000: POOL})
        with patch("scripts.discover_two_ca_4663._verified_scan_for_candidate",
                   side_effect=direct_scan):
            result = probe_approved_fee_tiers(
                rpc, cache=self.cache, tokens=(TOKENS[0],),
                fee_tiers=(10_000, 500, 10_000), amount_in_units=10**12,
                confirmations=0, now_ms=lambda: 1_000)
        item = result["tokens"][TOKENS[0]]
        self.assertEqual(result["approvedFeeTiers"], [500, 10_000])
        self.assertEqual(item["selection"], "unique_within_approved_fee_tiers")
        self.assertTrue(item["published"])
        self.assertNotIn("eth_getLogs", rpc.methods)
        self.assertLessEqual(rpc.methods.count("eth_call"), 2 * 2)
        entry = self.cache.lookup(CHAIN_ID, TOKENS[0])
        self.assertEqual(entry.approved_fee_tiers, (500, 10_000))
        self.assertEqual(entry.identity_evidence_kind, DIRECT_EVIDENCE)

    def test_factory_probe_zero_multiple_and_failed_tier_fail_closed(self):
        cases = (
            ({500: None, 10_000: None}, "missing"),
            ({500: POOL, 10_000: POOL2}, "ambiguous_within_approved_fee_tiers"),
            ({500: POOL, 10_000: ValueError("rpc_failed")}, "incomplete"),
        )
        for pools, expected in cases:
            with self.subTest(expected=expected):
                rpc = FeeProbeRpc(pools)
                with patch("scripts.discover_two_ca_4663._verified_scan_for_candidate",
                           side_effect=direct_scan):
                    result = probe_approved_fee_tiers(
                        rpc, cache=self.cache, tokens=(TOKENS[0],),
                        fee_tiers=(500, 10_000), amount_in_units=10**12,
                        confirmations=0, now_ms=lambda: 1_000)
                self.assertEqual(result["tokens"][TOKENS[0]]["selection"], expected)
                self.assertFalse(result["tokens"][TOKENS[0]]["published"])
                self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))
                self.assertNotIn("eth_getLogs", rpc.methods)

    def test_factory_probe_input_validation_is_fail_closed(self):
        self.assertEqual(normalize_fee_tiers([10_000, 500, 10_000]), (500, 10_000))
        for arguments in (
            ["--factory-probe", "--cache-db", str(self.cache.path),
             "--amount-in-units", "1", "--fee-tier", "500"],
            ["--factory-probe", "--cache-db", str(self.cache.path),
             "--amount-in-units", "1", "--token", "0x0", "--fee-tier", "500"],
            ["--factory-probe", "--cache-db", str(self.cache.path),
             "--amount-in-units", "1", "--token", TOKENS[0]],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                main(arguments)

    def test_history_scan_request_budget_stops_without_publishing(self):
        rpc = ScanRpc([event(TOKENS[0], block=105)])
        with self.assertRaisesRegex(RuntimeError, "request_budget_exhausted"):
            scan_v3_candidates(
                rpc, token=TOKENS[0], confirmed_head=140,
                cursor_store=self.cursor, maximum_chunk=10,
                maximum_requests=1, maximum_block_span=1_000)
        self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))

    def test_each_token_publishes_before_next_token_scan(self):
        order = []
        candidate0 = V3Candidate(*sorted((TOKENS[0], WETH)), 10_000, 200, POOL,
                                 130, "0x" + "11" * 32, 0)
        candidate1 = V3Candidate(*sorted((TOKENS[1], WETH)), 10_000, 200, POOL2,
                                 140, "0x" + "22" * 32, 0)

        def scan(_rpc, *, token, **_kwargs):
            order.append("scan:" + token)
            return [candidate0 if token == TOKENS[0] else candidate1]

        def publish(_rpc, _cache, candidate, **_kwargs):
            order.append("publish:" + candidate.ca)
            return {"amountOutUnits": "1"}

        with (patch("scripts.discover_two_ca_4663.scan_v3_candidates", side_effect=scan),
              patch("scripts.discover_two_ca_4663.select_verified_candidate",
                    side_effect=lambda _rpc, token, candidates, block_height: (candidates[0], "selected")),
              patch("scripts.discover_two_ca_4663._publish_candidate", side_effect=publish)):
            result = run_once(ScanRpc(), cache=self.cache, cursor_store=self.cursor,
                              tokens=TOKENS, amount_in_units=10**12, confirmations=2)
        self.assertEqual(order, ["scan:" + TOKENS[0], "publish:" + TOKENS[0],
                                 "scan:" + TOKENS[1], "publish:" + TOKENS[1]])
        self.assertTrue(all(item["published"] for item in result["tokens"].values()))

    def test_ambiguous_selection_never_publishes(self):
        candidates = [
            V3Candidate(*sorted((TOKENS[0], WETH)), 10_000, 200, POOL,
                        130, "0x" + "11" * 32, 0),
            V3Candidate(*sorted((TOKENS[0], WETH)), 3_000, 60, POOL2,
                        140, "0x" + "22" * 32, 0),
        ]
        with (patch("scripts.discover_two_ca_4663.scan_v3_candidates",
                    return_value=candidates),
              patch("scripts.discover_two_ca_4663.select_verified_candidate",
                    return_value=(None, "ambiguous")),
              patch("scripts.discover_two_ca_4663._publish_candidate") as publish):
            result = run_once(ScanRpc(), cache=self.cache, cursor_store=self.cursor,
                              tokens=(TOKENS[0],), amount_in_units=10**12,
                              confirmations=2)
        publish.assert_not_called()
        self.assertFalse(result["tokens"][TOKENS[0]]["published"])
        self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))

    def test_non_v3_factory_events_cannot_become_candidates(self):
        foreign = event(TOKENS[0])
        foreign["address"] = "0x" + "55" * 20
        rpc = ScanRpc([foreign])
        found = scan_v3_candidates(rpc, token=TOKENS[0], confirmed_head=140,
                                   cursor_store=self.cursor, maximum_chunk=20)
        self.assertEqual(found, [])
        self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))

    def test_quote_older_than_two_seconds_and_reorg_do_not_write(self):
        candidate = V3Candidate(*sorted((TOKENS[0], WETH)), 10_000, 200, POOL,
                                130, "0x" + "11" * 32, 0)
        snapshot = SimpleNamespace(block_height=200, block_hash=BLOCK_HASH,
                                   observed_at_ms=1_000,
                                   block_timestamp_ms=0, read_started_at_ms=900,
                                   block_age_at_start_ms=900,
                                   block_age_at_completion_ms=1_000,
                                   read_duration_ms=100,
                                   quote=lambda **_kwargs: SimpleNamespace(amount_out=1234))
        reader = SimpleNamespace(snapshot=lambda **_kwargs: snapshot)
        rpc = ScanRpc()
        rpc.call = lambda method, params: (hex(200) if method == "eth_blockNumber" else
                                            {"number": hex(200), "hash": BLOCK_HASH}
                                            if method == "eth_getBlockByNumber" else
                                            word(18) if method == "eth_call" else None)
        with patch("scripts.discover_two_ca_4663.UniswapV3PoolReader", return_value=reader):
            with self.assertRaisesRegex(ValueError, "direct_cache_scan_quote_stale"):
                _publish_candidate(rpc, self.cache, candidate, amount_in_units=10**12,
                                   now_ms=lambda: 3_001)
        self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))
        with patch("scripts.discover_two_ca_4663.UniswapV3PoolReader") as factory:
            factory.return_value.snapshot.side_effect = ValueError("v3_block_reorged_or_provider_unknown")
            with self.assertRaisesRegex(ValueError, "v3_block_reorged"):
                _publish_candidate(rpc, self.cache, candidate, amount_in_units=10**12,
                                   now_ms=lambda: 1_001)
        self.assertIsNone(self.cache.lookup(CHAIN_ID, TOKENS[0]))

    def test_watch_refresh_never_scans_and_failure_preserves_timestamp(self):
        publish_verified_v3_scan(self.cache, VerifiedV3Scan(
            CHAIN_ID, TOKENS[0], WETH, min(TOKENS[0], WETH), max(TOKENS[0], WETH),
            10_000, 200, POOL, POOL, FACTORY, 130, 200, BLOCK_HASH,
            10**12, 1234, 1_000, "factory_event_and_pinned_v3_snapshot", None,
            0, 900, 900, 1_000, 100),
            now_ms=1_000)
        rpc = ScanRpc(factory_pool=POOL)
        with patch("scripts.discover_two_ca_4663._publish_candidate",
                   side_effect=ValueError("v3_refresh_failed")):
            result = refresh_cached_identities(rpc, cache=self.cache, tokens=(TOKENS[0],),
                                               amount_in_units=10**12,
                                               now_ms=lambda: 1_500)
        self.assertFalse(result["refreshed"][TOKENS[0]]["published"])
        self.assertNotIn("eth_getLogs", rpc.methods)
        self.assertEqual(self.cache.lookup(CHAIN_ID, TOKENS[0]).written_at_ms, 1_000)

    def test_watch_preserves_direct_probe_approved_fee_scope(self):
        candidate = V3Candidate(*sorted((TOKENS[0], WETH)), 10_000, 200, POOL,
                                0, "0x" + "11" * 32, 0)
        scan = direct_scan(None, candidate, amount_in_units=10**12,
                           evidence_kind=DIRECT_EVIDENCE,
                           approved_fee_tiers=(10_000,), block_height=200)
        publish_verified_v3_scan(self.cache, scan, now_ms=1_000)
        rpc = ScanRpc(factory_pool=POOL)
        context = SimpleNamespace(block_height=200)
        quote = {"publishedAtMs": 1_500, "chainStateAgeAtPublishMs": 500}
        with (patch("scripts.discover_two_ca_4663.pin_v3_block_context",
                    return_value=context),
              patch("scripts.discover_two_ca_4663._publish_candidate",
                    return_value=quote) as publish):
            result = refresh_cached_identities(
                rpc, cache=self.cache, tokens=(TOKENS[0],),
                amount_in_units=10**12, now_ms=lambda: 1_500)
        self.assertTrue(result["refreshed"][TOKENS[0]]["published"])
        self.assertEqual(publish.call_args.kwargs["approved_fee_tiers"], (10_000,))
        self.assertNotIn("eth_getLogs", rpc.methods)

    def test_watch_contains_transport_failure_and_continues_serially(self):
        emitted = []
        success = {
            "chainId": CHAIN_ID, "tradingReady": False,
            "refreshed": {TOKENS[0]: {"published": True, "quote": {
                "publishedAtMs": 2_000, "chainStateAgeAtPublishMs": 500,
            }}},
            "metrics": {"httpRoundTrips": 7, "jsonRpcMethods": 7,
                        "ethGetLogs": 0, "rateLimits": 0, "timeouts": 0},
        }
        with (patch("scripts.discover_two_ca_4663.refresh_cached_identities",
                    side_effect=[OSError("secret endpoint text"), success]) as refresh,
              patch("scripts.discover_two_ca_4663.time.sleep")):
            summary = run_watch(
                object(), cache=self.cache, tokens=(TOKENS[0],),
                amount_in_units=10**12, interval_ms=250,
                emit=emitted.append, maximum_cycles=2,
            )
        self.assertEqual(refresh.call_count, 2)
        self.assertEqual(summary["cycles"], 2)
        self.assertEqual(summary["failureReasons"], {"OSError": 1})
        self.assertEqual(summary["successfulPublishes"], 1)
        self.assertEqual(summary["ethGetLogsCalls"], 0)


if __name__ == "__main__":
    unittest.main()
