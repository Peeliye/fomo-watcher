"""The unsigned buy branch may read SQLite, never a chain or aggregator."""

from __future__ import annotations

import tempfile
import time
import unittest
import sqlite3
import json
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from fomo.execution.assembly import ExecutionAssembly
from fomo.execution.cached_direct_buy import (CaRouteCache, CaRouteCacheEntry,
                                              CachedAmountQuote,
                                              create_cached_direct_buy_intent,
                                              v3_evidence_hash)
from fomo.execution.direct_v4 import V4PoolKey, ZERO
from fomo.execution.direct_v2 import V2PoolSnapshot
from fomo.execution.journal import ExecutionJournal
from fomo.execution.route_cache_writer import VerifiedV3Scan, publish_verified_v3_scan
from fomo.execution.v3_transaction import decode_exact_input_single
from fomo.execution.v4_transaction import decode_unsigned_swap as decode_v4_unsigned
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.strategy import ExecutionIntent, FomoCopyStrategy

CA = "0x39dbed3a2bd333467115de45665cc57f813c4571"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
POOL = "0x10cc6bd38112cac182db90b6a71d8bb5939526ba"
WALLET = "0x" + "11" * 20
HOOK = "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"


def timed_quote(now: int) -> CachedAmountQuote:
    return CachedAmountQuote(
        10**12, 4019172325541344, now, "0x" + "ab" * 32, 9_100_000,
        now - 1_000, now - 100, 900, 1_000, 100, 0, 1_000,
    )


def intent(ca: str = CA, chain: str = "4663") -> ExecutionIntent:
    return ExecutionIntent("intent:test", "signal:test", "fomo_push", "test",
                           "test", chain, "buy", WETH, ca, Decimal("10"))


def v3_entry(now: int) -> CaRouteCacheEntry:
    quote = timed_quote(now)
    evidence_hash = v3_evidence_hash(
        chain_id=4663, ca=CA, quote_token=WETH, token0=WETH, token1=CA,
        fee=10000, tick_spacing=200, pool=POOL,
        factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        event_block=9_000_000,
        evidence_kind="factory_event_and_pinned_v3_snapshot",
        approved_fee_tiers=None, quote=quote)
    return CaRouteCacheEntry(
        chain_id=4663, ca=CA, protocol="V3", pool=POOL, pool_key=None,
        quote_token=WETH, hooks=None, written_at_ms=now, identity_verified=True,
        token0=WETH, token1=CA, fee=10000, tick_spacing=200,
        identity_event_block=9_000_000,
        identity_factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        identity_evidence_kind="factory_event_and_pinned_v3_snapshot",
        identity_evidence_hash=evidence_hash, quote=quote,
    )


def direct_v3_entry(now: int) -> CaRouteCacheEntry:
    quote = timed_quote(now)
    tiers = (500, 10_000)
    evidence_hash = v3_evidence_hash(
        chain_id=4663, ca=CA, quote_token=WETH, token0=WETH, token1=CA,
        fee=10_000, tick_spacing=200, pool=POOL,
        factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        event_block=None,
        evidence_kind="approved_fee_set_and_pinned_factory_snapshot",
        approved_fee_tiers=tiers, quote=quote)
    return CaRouteCacheEntry(
        chain_id=4663, ca=CA, protocol="V3", pool=POOL, pool_key=None,
        quote_token=WETH, hooks=None, written_at_ms=now, identity_verified=True,
        token0=WETH, token1=CA, fee=10_000, tick_spacing=200,
        identity_event_block=None,
        identity_factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
        identity_evidence_kind="approved_fee_set_and_pinned_factory_snapshot",
        approved_fee_tiers=tiers, identity_evidence_hash=evidence_hash, quote=quote,
    )


class CachedDirectBuyTests(unittest.TestCase):
    @staticmethod
    def signal() -> TradeSignalEnvelope:
        timestamp = datetime.now(timezone.utc).isoformat()
        return TradeSignalEnvelope.create(
            source="fomo_push", source_event_id="cached-buy", observed_at=timestamp,
            source_timestamp=timestamp, chain_id="4663", side="buy",
            token_in=WETH, token_out=CA, kol_id="followed", reorg_key="test",
            decoder_version="fixture", raw_payload_hash="0" * 64,
        )

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.cache = CaRouteCache(root / "routes.sqlite3")
        self.cache.initialize_for_discovery()
        self.journal = ExecutionJournal(root / "execution.sqlite3")
        self.addCleanup(self.journal.close)
        self.now = int(time.time() * 1000)

    def prepare(self, buy_intent=None, *, amount=10**12):
        return ExecutionAssembly(self.journal, direct_cache=self.cache).prepare_cached_direct_buy(
            buy_intent or intent(), wallet=WALLET, amount_in_units=amount,
            nonce=7, gas_limit=250_000, priority_fee_wei=1,
            maximum_fee_wei=2, deadline=int(time.time()) + 100,
            slippage_bps=100, now_ms=self.now,
        )

    def test_cache_miss_fails_without_chain_discovery(self):
        forbidden = AssertionError("network fallback forbidden")
        with (patch("fomo.watching.rpc_transport.FailoverJsonRpc.call", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.ZeroXQuoteAdapter.quote", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.JupiterQuoteAdapter.quote", side_effect=forbidden)):
            with self.assertRaisesRegex(ValueError, "direct_buy_cache_miss"):
                self.prepare()
        with self.assertRaisesRegex(ValueError, "direct_buy_cache_miss"):
            create_cached_direct_buy_intent(self.signal(), strategy=FomoCopyStrategy(10),
                                            cache=self.cache, now_ms=self.now)

    def test_missing_cache_file_is_not_created_by_buy(self):
        missing = CaRouteCache(self.cache.path.with_name("never-created.sqlite3"))
        assembly = ExecutionAssembly(self.journal, direct_cache=missing)
        with self.assertRaisesRegex(ValueError, "direct_buy_cache_miss"):
            assembly.prepare_cached_direct_buy(
                intent(), wallet=WALLET, amount_in_units=10**12,
                nonce=7, gas_limit=250_000, priority_fee_wei=1,
                maximum_fee_wei=2, deadline=int(time.time()) + 100,
                slippage_bps=100, now_ms=self.now,
            )
        self.assertFalse(missing.path.exists())

    def test_intent_creation_uses_only_cached_route(self):
        self.cache.store_discovered(v3_entry(self.now))
        signal = self.signal()
        with patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                   side_effect=AssertionError("RPC forbidden")):
            created = create_cached_direct_buy_intent(
                signal, strategy=FomoCopyStrategy(10), cache=self.cache, now_ms=self.now)
        self.assertEqual(created.signal_id, signal.signal_id)
        self.assertEqual(created.token_out, CA)

    def test_identity_without_fresh_quote_does_not_create_buy_intent(self):
        with self.assertRaisesRegex(ValueError, "direct_cache_v3_identity_invalid"):
            self.cache.store_discovered(replace(v3_entry(self.now), quote=None))

    def test_4663_v3_boolean_identity_without_provenance_is_rejected(self):
        unproven = replace(v3_entry(self.now), identity_factory=None,
                           identity_evidence_kind=None)
        with self.assertRaisesRegex(ValueError, "direct_cache_v3_identity_invalid"):
            self.cache.store_discovered(unproven)

    def test_event_and_approved_fee_evidence_are_independently_strict(self):
        self.cache.store_discovered(v3_entry(self.now))
        self.cache.store_discovered(direct_v3_entry(self.now))
        direct = self.cache.lookup(4663, CA)
        self.assertIsNotNone(direct)
        assert direct is not None
        self.assertEqual(direct.approved_fee_tiers, (500, 10_000))
        with self.assertRaisesRegex(ValueError, "direct_cache_v3_identity_invalid"):
            self.cache.store_discovered(replace(v3_entry(self.now),
                                                 approved_fee_tiers=(10_000,)))
        with self.assertRaisesRegex(ValueError, "direct_cache_v3_identity_invalid"):
            self.cache.store_discovered(replace(direct_v3_entry(self.now),
                                                 identity_event_block=1))

    def test_direct_evidence_json_tampering_is_rejected_on_read(self):
        mutations = (
            lambda raw: raw.update(approved_fee_tiers=[500]),
            lambda raw: raw.update(identity_factory="0x" + "22" * 20),
            lambda raw: raw.update(pool="0x" + "33" * 20),
            lambda raw: raw["quote"].update(block_hash="0x" + "cd" * 32),
            lambda raw: raw["quote"].update(amount_in_units=10**11),
            lambda raw: raw["quote"].update(block_age_at_completion_ms=999),
            lambda raw: raw["quote"].update(chain_state_age_at_publish_ms=999),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                self.cache.store_discovered(direct_v3_entry(self.now))
                with closing(sqlite3.connect(self.cache.path)) as db:
                    raw = json.loads(db.execute(
                        "SELECT entry_json FROM ca_direct_routes WHERE chain_id=? AND ca=?",
                        (4663, CA)).fetchone()[0])
                    mutate(raw)
                    with db:
                        db.execute(
                            "UPDATE ca_direct_routes SET entry_json=? WHERE chain_id=? AND ca=?",
                            (json.dumps(raw), 4663, CA))
                with self.assertRaisesRegex(ValueError, "direct_cache_record_invalid"):
                    self.cache.lookup(4663, CA)

    def test_unknown_hook_and_4663_unapproved_protocol_fail(self):
        hooked = CaRouteCacheEntry(
            chain_id=4663, ca=CA, protocol="V4", pool=None,
            pool_key=V4PoolKey(WETH, CA, 8388608, 8, HOOK),
            quote_token=WETH, hooks=HOOK, written_at_ms=self.now,
            identity_verified=True,
            quote=CachedAmountQuote(10**12, 1000, self.now, "0x" + "ab" * 32),
        )
        self.cache.store_discovered(hooked)
        with self.assertRaisesRegex(ValueError, "direct_buy_protocol_not_allowed"):
            self.prepare()
        # Even on a chain where V4 is admitted, a nonzero hook rejects before
        # any pool-state read or transaction construction.
        base_ca = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        base_weth = "0x4200000000000000000000000000000000000006"
        self.cache.store_discovered(CaRouteCacheEntry(
            chain_id=8453, ca=base_ca, protocol="V4", pool=None,
            pool_key=V4PoolKey(base_weth, base_ca, 3000, 60, HOOK),
            quote_token=base_weth, hooks=HOOK, written_at_ms=self.now,
            identity_verified=True,
            quote=CachedAmountQuote(10**12, 1000, self.now, "0x" + "ab" * 32),
        ))
        base_intent = ExecutionIntent("intent:base", "signal:base", "fomo_push", "test",
                                      "test", "8453", "buy", base_weth, base_ca,
                                      Decimal("10"))
        with self.assertRaisesRegex(ValueError, "direct_buy_hook_rejected"):
            self.prepare(base_intent)

    def test_v3_cache_hit_builds_unsigned_with_zero_rpc(self):
        self.cache.store_discovered(v3_entry(self.now))
        forbidden = AssertionError("hot path discovery/RPC forbidden")
        with (patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                    side_effect=forbidden),
              patch("fomo.execution.direct_v2.UniswapV2PoolReader.snapshot",
                    side_effect=forbidden),
              patch("fomo.execution.direct_v3.UniswapV3PoolReader.snapshot",
                    side_effect=forbidden),
              patch("fomo.execution.direct_v4.UniswapV4PoolReader.snapshot",
                    side_effect=forbidden),
              patch("fomo.execution.v4_discovery.discover_pool",
                    side_effect=forbidden),
              patch("fomo.execution.quote_adapters.ZeroXQuoteAdapter.quote",
                    side_effect=forbidden),
              patch("fomo.execution.quote_adapters.JupiterQuoteAdapter.quote",
                    side_effect=forbidden)):
            result = self.prepare()
        self.assertEqual(result.status, "built_not_sent")
        self.assertEqual(result.protocol, "V3")
        self.assertEqual(result.transaction.provider, "uniswap_v3_l0")
        self.assertFalse(ExecutionAssembly(self.journal, direct_cache=self.cache)
                         .chain_status("4663")["ready"])
        status = ExecutionAssembly(self.journal, direct_cache=self.cache).chain_status("4663")
        self.assertFalse(status["liveArmed"])
        from fomo.execution.evm_transaction import decode_eip1559
        fields, _ = decode_eip1559(result.transaction.serialized, signed=False)
        decoded = decode_exact_input_single(fields.data, chain_id=4663)
        self.assertEqual(decoded["tokenOut"], CA)
        self.assertEqual(decoded["amountIn"], 10**12)

    def test_direct_fee_probe_cache_builds_only_built_not_sent(self):
        self.cache.store_discovered(direct_v3_entry(self.now))
        forbidden = AssertionError("network or aggregator fallback forbidden")
        with (patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                    side_effect=forbidden),
              patch("fomo.execution.quote_adapters.ZeroXQuoteAdapter.quote",
                    side_effect=forbidden),
              patch("fomo.execution.quote_adapters.JupiterQuoteAdapter.quote",
                    side_effect=forbidden)):
            result = self.prepare()
        self.assertEqual(result.status, "built_not_sent")
        status = ExecutionAssembly(self.journal, direct_cache=self.cache).chain_status("4663")
        self.assertFalse(status["ready"])
        self.assertFalse(status["liveArmed"])

    def test_background_v3_scan_write_then_cache_only_build(self):
        scan = VerifiedV3Scan(
            chain_id=4663, ca=CA, quote_token=WETH, token0=WETH, token1=CA,
            fee=10000, tick_spacing=200, pool=POOL, factory_pool=POOL,
            factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
            event_block=9_000_000, block_height=9_100_000,
            block_hash="0x" + "ab" * 32, amount_in_units=10**12,
            amount_out_units=4019172325541344, quoted_at_ms=self.now,
            evidence_kind="factory_event_and_pinned_v3_snapshot",
            block_timestamp_ms=self.now - 1_000,
            read_started_at_ms=self.now - 100,
            block_age_at_start_ms=900, block_age_at_completion_ms=1_000,
            snapshot_read_duration_ms=100)
        publish_verified_v3_scan(self.cache, scan, now_ms=self.now)
        publish_verified_v3_scan(self.cache, scan, now_ms=self.now)
        with closing(sqlite3.connect(self.cache.path)) as cache_db:
            self.assertEqual(cache_db.execute(
                "SELECT COUNT(*) FROM ca_direct_routes WHERE chain_id=? AND ca=?",
                (4663, CA)).fetchone()[0], 1)
        with patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                   side_effect=AssertionError("buy path RPC forbidden")):
            result = self.prepare()
        self.assertEqual(result.status, "built_not_sent")
        self.assertEqual(result.protocol, "V3")

    def test_scan_writer_rejects_mismatch_or_expired_quote(self):
        scan = VerifiedV3Scan(
            chain_id=4663, ca=CA, quote_token=WETH, token0=WETH, token1=CA,
            fee=10000, tick_spacing=200, pool=POOL,
            factory_pool="0x" + "22" * 20,
            factory="0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
            event_block=9_000_000, block_height=9_100_000,
            block_hash="0x" + "ab" * 32, amount_in_units=10**12,
            amount_out_units=1234, quoted_at_ms=self.now,
            evidence_kind="factory_event_and_pinned_v3_snapshot",
            block_timestamp_ms=self.now - 1_000,
            read_started_at_ms=self.now - 100,
            block_age_at_start_ms=900, block_age_at_completion_ms=1_000,
            snapshot_read_duration_ms=100)
        with self.assertRaisesRegex(ValueError, "direct_cache_factory_pool_mismatch"):
            publish_verified_v3_scan(self.cache, scan, now_ms=self.now)
        with self.assertRaisesRegex(ValueError, "direct_cache_scan_quote_stale"):
            publish_verified_v3_scan(self.cache, replace(scan, factory_pool=POOL),
                                     now_ms=self.now + 2_001)
        self.assertIsNone(self.cache.lookup(4663, CA))

    def test_scan_writer_does_not_trust_an_identity_boolean_substitute(self):
        scan = VerifiedV3Scan(
            chain_id=4663, ca=CA, quote_token=WETH, token0=WETH, token1=CA,
            fee=10000, tick_spacing=200, pool=POOL, factory_pool=POOL,
            factory="0x" + "22" * 20, event_block=9_000_000,
            block_height=9_100_000, block_hash="0x" + "ab" * 32,
            amount_in_units=10**12, amount_out_units=1234,
            quoted_at_ms=self.now, evidence_kind="caller_claimed_verified",
            block_timestamp_ms=self.now - 1_000,
            read_started_at_ms=self.now - 100,
            block_age_at_start_ms=900, block_age_at_completion_ms=1_000,
            snapshot_read_duration_ms=100)
        with self.assertRaisesRegex(ValueError, "direct_cache_scan_evidence_invalid"):
            publish_verified_v3_scan(self.cache, scan, now_ms=self.now)
        self.assertIsNone(self.cache.lookup(4663, CA))

    def test_stale_or_wrong_sized_quote_never_refreshes(self):
        self.cache.store_discovered(v3_entry(self.now))
        forbidden = AssertionError("network fallback forbidden")
        with (patch("fomo.watching.rpc_transport.FailoverJsonRpc.call", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.ZeroXQuoteAdapter.quote", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.JupiterQuoteAdapter.quote", side_effect=forbidden)):
            with self.assertRaisesRegex(ValueError, "direct_buy_cache_quote_miss"):
                self.prepare(amount=10**11)
            self.now += 2_001
            with self.assertRaisesRegex(ValueError, "direct_buy_cache_quote_miss"):
                self.prepare()

    def test_sqlite_lock_contention_fails_closed_without_rpc_or_aggregator(self):
        self.cache.store_discovered(v3_entry(self.now))
        lock = sqlite3.connect(self.cache.path, timeout=0)
        self.addCleanup(lock.close)
        lock.execute("PRAGMA locking_mode=EXCLUSIVE")
        lock.execute("BEGIN EXCLUSIVE")
        forbidden = AssertionError("network fallback forbidden")
        with (patch("fomo.watching.rpc_transport.FailoverJsonRpc.call", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.ZeroXQuoteAdapter.quote", side_effect=forbidden),
              patch("fomo.execution.quote_adapters.JupiterQuoteAdapter.quote", side_effect=forbidden)):
            with self.assertRaisesRegex(ValueError, "direct_cache_unavailable"):
                self.prepare()

    def test_v2_cached_snapshot_builds_without_reserve_read(self):
        quote_token = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        ca = "0x" + "aa" * 20
        pair = "0x" + "bb" * 20
        snapshot = V2PoolSnapshot("1", pair, ca, quote_token,
                                  10**24, 10**22, 18, 18, 100,
                                  "0x" + "ab" * 32, self.now, "fixture")
        self.cache.store_discovered(CaRouteCacheEntry(
            chain_id=1, ca=ca, protocol="V2", pool=pair, pool_key=None,
            quote_token=quote_token, hooks=None, written_at_ms=self.now,
            identity_verified=True, v2_snapshot=snapshot,
        ))
        v2_intent = ExecutionIntent("intent:v2", "signal:v2", "fomo_push", "test",
                                    "test", "1", "buy", quote_token, ca, Decimal("10"))
        with patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                   side_effect=AssertionError("RPC forbidden")):
            result = self.prepare(v2_intent)
        self.assertEqual(result.status, "built_not_sent")
        self.assertEqual(result.transaction.provider, "uniswap_v2_direct")
        self.now += 5_001
        with self.assertRaisesRegex(ValueError, "direct_buy_cache_quote_miss"):
            self.prepare(v2_intent)

    def test_v4_zero_hook_cached_quote_builds_only_unsigned(self):
        usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        key = V4PoolKey(ZERO, usdc, 500, 10)
        self.cache.store_discovered(CaRouteCacheEntry(
            chain_id=1, ca=usdc, protocol="V4", pool=None, pool_key=key,
            quote_token=ZERO, hooks=ZERO, written_at_ms=self.now,
            identity_verified=True,
            quote=CachedAmountQuote(10**12, 1000, self.now, "0x" + "ab" * 32),
        ))
        v4_intent = ExecutionIntent("intent:v4", "signal:v4", "fomo_push", "test",
                                    "test", "1", "buy", ZERO, usdc, Decimal("10"))
        with patch("fomo.watching.rpc_transport.FailoverJsonRpc.call",
                   side_effect=AssertionError("RPC forbidden")):
            result = self.prepare(v4_intent)
        self.assertEqual(result.status, "built_not_sent")
        self.assertEqual(result.transaction.provider, "uniswap_v4_l0")
        decoded = decode_v4_unsigned(result.transaction, chain_id=1)
        self.assertEqual(decoded["msgValue"], 10**12)
        self.assertEqual(decoded["amountOutMinimum"], 990)

    def test_zero_hook_is_not_a_hook_exception(self):
        self.assertEqual(ZERO, "0x" + "00" * 20)


if __name__ == "__main__":
    unittest.main()
