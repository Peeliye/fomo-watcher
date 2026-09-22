from __future__ import annotations

import time
import unittest
from dataclasses import replace
from decimal import Decimal

from fomo.execution.evm_transaction import keccak256
from fomo.execution.pool_prices import EvmV2PoolBinding, EvmV2PoolPriceCache


WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
POOL = "0x" + "12" * 20
BLOCK = "0x" + "ab" * 32
TOPIC = "0x" + keccak256(b"Sync(uint112,uint112)").hex()


class PoolRpc:
    def __init__(self, log, *, wrong_block=False, wrong_token=False):
        self.log = log
        self.wrong_block = wrong_block
        self.wrong_token = wrong_token

    def call(self, method, params):
        if method == "eth_getBlockByNumber":
            return {"hash": "0x" + "cd" * 32 if self.wrong_block else BLOCK,
                    "timestamp": hex(int(time.time()) - 1)}
        if method == "eth_blockNumber":
            return "0x67"
        if method == "eth_getLogs":
            return [self.log]
        if method == "eth_call":
            if params[0]["data"].startswith("0xe6a43905"):
                return "0x" + POOL[2:].rjust(64, "0")
            if params[0]["data"] == "0x0902f1ac":
                return self.log["data"] + "0" * 64
            address = WETH if params[0]["data"] == "0x0dfe1681" else USDC
            return "0x" + ((USDC if self.wrong_token else address)[2:]).rjust(64, "0")
        raise AssertionError(method)


class PoolPriceTests(unittest.TestCase):
    def setUp(self):
        self.binding = EvmV2PoolBinding("1", POOL, WETH, USDC, True, 18, 6)
        self.log = {
            "address": POOL, "topics": [TOPIC], "data": "0x" + f"{10**20:064x}{250_000_000_000:064x}",
            "blockNumber": "0x64", "blockHash": BLOCK,
            "transactionHash": "0x" + "ef" * 32, "logIndex": "0x0", "removed": False,
        }

    def test_verified_pool_price_in_memory_and_route_independence(self):
        cache = EvmV2PoolPriceCache(binding=self.binding, rpc=PoolRpc(self.log))
        with self.assertRaisesRegex(ValueError, "pool_price_stream_empty"):
            cache.latest()
        observed = cache.ingest_sync_log(self.log)
        self.assertEqual(observed.price_usd, Decimal("2500"))
        self.assertEqual(observed.liquidity_usd, Decimal("500000"))
        self.assertEqual(cache.latest(), observed)
        self.assertFalse(observed.independent_of("0x_allowance_holder", ("0x" + "34" * 20,)))
        self.assertFalse(observed.independent_of("0x_allowance_holder", (POOL,)))
        with self.assertRaisesRegex(ValueError, "independent_price_stale"):
            cache.latest(now_ms=observed.fetched_at_ms + 5001)

    def test_spoof_reorg_and_wrong_binding_rejected(self):
        for rpc in (PoolRpc(self.log, wrong_block=True), PoolRpc(self.log, wrong_token=True)):
            with self.assertRaises(ValueError):
                EvmV2PoolPriceCache(binding=self.binding, rpc=rpc).ingest_sync_log(self.log)
        with self.assertRaisesRegex(ValueError, "pool_sync_reorged"):
            EvmV2PoolPriceCache(binding=self.binding, rpc=PoolRpc(self.log)).ingest_sync_log(
                replace_log(self.log, removed=True))
        with self.assertRaisesRegex(ValueError, "pool_binding_not_audited"):
            replace(self.binding, stable_token="0x" + "ef" * 20)

    def test_fixture_never_reports_production_ready(self):
        cache = EvmV2PoolPriceCache(binding=self.binding, rpc=PoolRpc(self.log))
        cache.ingest_sync_log(self.log)
        self.assertFalse(cache.self_check().ready)
        with self.assertRaisesRegex(ValueError, "pool_sync_reorged"):
            cache.ingest_sync_log(replace_log(self.log, removed=True))
        with self.assertRaisesRegex(ValueError, "pool_price_stream_empty"):
            cache.latest()


def replace_log(log, **changes):
    return {**log, **changes}
