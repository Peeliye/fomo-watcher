from __future__ import annotations

import unittest
from unittest.mock import patch

from fomo.execution.direct_v4 import CHAIN_CONFIGS, V4PoolKey, ZERO
from fomo.execution.evm_transaction import keccak256
from fomo.execution.v4_discovery import INITIALIZE_TOPIC
from scripts.uniswap_v3_swap_once import SwapOnceRpc
from scripts.uniswap_v4_verify_once import TARGET, _logs, discover, execute_once

HASH = "0x" + "aa" * 32
HOOK = "0x" + "44" * 20
OTHER = "0x" + "55" * 20


def event(key: V4PoolKey):
    words = (key.fee, key.tick_spacing, int(key.hooks, 16), 1 << 96, 0)
    return {"address": CHAIN_CONFIGS[4663].pool_manager,
            "topics": [INITIALIZE_TOPIC, key.pool_id,
                       "0x" + key.currency0[2:].rjust(64, "0"),
                       "0x" + key.currency1[2:].rjust(64, "0")],
            "data": "0x" + "".join(f"{word:064x}" for word in words),
            "blockNumber": "0x66", "blockHash": HASH,
            "transactionHash": "0x" + "12" * 32, "logIndex": "0x0",
            "removed": False}


class FixtureRpc(SwapOnceRpc):
    last_provider = "fixture"

    def __init__(self, logs=()):
        self.logs = logs
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, list(params)))
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getBlockByNumber":
            return {"number": "0x6e", "hash": HASH}
        if method == "eth_getLogs":
            query = params[0]
            low, high = int(query["fromBlock"], 16), int(query["toBlock"], 16)
            assert high - low <= 3
            return [log for log in self.logs if low <= int(log["blockNumber"], 16) <= high
                    and all(value is None or log["topics"][index] == value
                            for index, value in enumerate(query["topics"]))]
        if method == "eth_getCode":
            return "0x6000"
        raise AssertionError(method)


class V4OnceTests(unittest.TestCase):
    def test_bounded_scan_checks_both_currency_positions_and_boundary(self):
        first = event(V4PoolKey(ZERO, TARGET, 3000, 60, ZERO))
        second = event(V4PoolKey(TARGET, OTHER, 0, 200, HOOK))
        second["transactionHash"] = "0x" + "34" * 32
        rpc = FixtureRpc((first, second))
        fixture_code_hash = "0x" + keccak256(bytes.fromhex("6000")).hex()
        with (patch("scripts.uniswap_v4_verify_once.first_code_block", return_value=100),
              patch("scripts.uniswap_v4_verify_once._pool_state",
                    return_value={"sqrtPriceX96": 1 << 96, "tick": 0,
                                  "protocolFee": 0, "lpFee": 3000, "liquidity": 100}),
              patch("scripts.uniswap_v4_verify_once.REVIEWED_RUNTIME_CODE_HASH",
                    fixture_code_hash),
              patch("scripts.uniswap_v4_verify_once.REVIEWED_HOOK", HOOK)):
            pools, window = discover(rpc, window_blocks=4)
        self.assertEqual((window["scanFrom"], window["scanTo"]), (100, 103))
        self.assertEqual({item["poolId"] for item in pools},
                         {first["topics"][1], second["topics"][1]})
        self.assertEqual({item["inputPath"] for item in pools},
                         {"native_eth", "unsupported_input_asset"})
        self.assertIsNotNone(next(item for item in pools if item["hookCodeHash"])["hookCodeHash"])
        self.assertEqual(next(item for item in pools if item["hookCodeHash"])["hookReview"],
                         "reviewed_runtime_other_pool_key")
        self.assertNotIn("eth_sendRawTransaction", [method for method, _ in rpc.calls])

    def test_empty_window_reports_limited_absence_without_simulation(self):
        rpc = FixtureRpc()
        with patch("scripts.uniswap_v4_verify_once.first_code_block", return_value=100):
            result = execute_once(native_in_wei=10**12, slippage_bps=100,
                                  rpc=rpc, window_blocks=5)
        self.assertEqual(result["result"], "bounded_window_no_initialize")
        self.assertEqual((result["scan"]["scanFrom"], result["scan"]["scanTo"]), (100, 104))
        self.assertFalse(result["simulation_success"])
        self.assertNotIn("eth_call", [method for method, _ in rpc.calls])

    def test_nonzero_hook_never_uses_generic_quote(self):
        rpc = FixtureRpc()
        pool = {"poolKey": {"hooks": HOOK}, "hookCodeHash": "0x" + "12" * 32}
        with patch("scripts.uniswap_v4_verify_once.discover",
                   return_value=([pool], {"scanFrom": 100, "scanTo": 103})):
            result = execute_once(native_in_wei=10**12, slippage_bps=100, rpc=rpc)
        self.assertEqual(result["result"], "unsupported_v4_hook")
        self.assertFalse(result["simulation_success"])
        self.assertEqual(rpc.calls, [])

    def test_log_chunk_boundaries_do_not_skip_blocks(self):
        rpc = FixtureRpc()
        self.assertEqual(_logs(rpc, start=100, end=108, topic_index=2), [])
        ranges = [(int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16))
                  for method, params in rpc.calls if method == "eth_getLogs"]
        self.assertEqual(ranges, [(100, 103), (104, 107), (108, 108)])


if __name__ == "__main__":
    unittest.main()
