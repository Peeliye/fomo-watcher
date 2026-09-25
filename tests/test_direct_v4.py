import time
import unittest

from fomo.execution.direct_v4 import (CHAIN_CONFIGS, MAINNET_PROBE_KEY, BASE_PROBE_KEY,
                                      ROBINHOOD_PROBE_KEY, ZERO,
                                      UniswapV4PoolReader, V4PoolKey, _selector,
                                      attempt_same_block_simulation)
from fomo.execution.v3_math import sqrt_ratio_at_tick
from fomo.execution.v4_transaction import build_unsigned_swap
from fomo.execution.evm_transaction import keccak256
from fomo.watching.rpc_transport import RpcUnavailable


BLOCK_HASH = "0x" + "ab" * 32


def word(value):
    return "0x" + f"{value % (1 << 256):064x}"


class V4RpcFixture:
    def __init__(self, *, liquidity=10**24, reorg=False, bitmap=3,
                 chain_id=1, unavailable_head=False):
        self.liquidity = liquidity
        self.reorg = reorg
        self.bitmap = bitmap
        self.chain_id = chain_id
        self.unavailable_head = unavailable_head
        self.selected_height = 100
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_blockNumber":
            return "0x64"
        if method == "eth_getBlockByNumber":
            height = 100 if params[0] == "latest" else int(params[0], 16)
            if self.unavailable_head and height == 100:
                assert params[1] is False
                raise RpcUnavailable("header_unavailable")
            self.selected_height = height
            return {"number": hex(height), "timestamp": hex(int(time.time()) - 1),
                    "hash": "0x" + "cd" * 32 if self.reorg and params[0] != "latest" else BLOCK_HASH}
        if method == "eth_getCode":
            assert params[1] == hex(self.selected_height)
            return "0x6000"
        if method == "eth_call":
            assert params[1] == hex(self.selected_height)
            call = params[0]
            data = call["data"]
            config = CHAIN_CONFIGS[self.chain_id]
            key = {1: MAINNET_PROBE_KEY, 8453: BASE_PROBE_KEY,
                   4663: ROBINHOOD_PROBE_KEY}[self.chain_id]
            packed = (sqrt_ratio_at_tick(5) + (5 << 160) + (key.fee << 208))
            if call["to"] == config.universal_router:
                return "0x"
            if data.startswith(_selector("getSlot0(bytes32)")):
                return "0x" + "".join(f"{value:064x}" for value in
                                      (sqrt_ratio_at_tick(5), 5, 0, key.fee))
            if data.startswith(_selector("getLiquidity(bytes32)")):
                return word(self.liquidity)
            if data.startswith(_selector("extsload(bytes32)")):
                slot = int(data[-64:], 16)
                base = int.from_bytes(keccak256(bytes.fromhex(key.pool_id[2:])
                                                + (6).to_bytes(32, "big")), "big")
                return word(self.liquidity if slot == base + 3 else packed)
            if data.startswith(_selector("getTickBitmap(bytes32,int16)")):
                return word(self.bitmap)
            if data.startswith(_selector("getTickInfo(bytes32,int24)")):
                return "0x" + "".join(f"{value:064x}" for value in (100, 0, 0, 0))
        raise AssertionError((method, params))


class DirectV4Tests(unittest.TestCase):
    def test_robinhood_zero_hook_reader_has_no_hook_policy_reads(self):
        rpc = V4RpcFixture(chain_id=4663)
        snapshot = UniswapV4PoolReader(rpc=rpc, chain_id=4663).snapshot()
        self.assertEqual(snapshot.key, ROBINHOOD_PROBE_KEY)
        self.assertGreater(snapshot.quote(token_in=ZERO, amount_in=10**12).amount_out, 0)
        self.assertFalse(any(method == "eth_getLogs" for method, _ in rpc.calls))

    def test_base_reader_rejects_when_both_headers_unavailable(self):
        class MissingHeaders(V4RpcFixture):
            def call(self, method, params):
                if method == "eth_getBlockByNumber":
                    assert params[1] is False
                    raise RpcUnavailable("header_unavailable")
                return super().call(method, params)

        reader = UniswapV4PoolReader(rpc=MissingHeaders(chain_id=8453), chain_id=8453)
        with self.assertRaises(RpcUnavailable):
            reader.snapshot()
        self.assertEqual(reader.last_header_diagnostic["attempted"], [100, 99])

    def test_base_reader_binds_previous_header_when_tip_unavailable(self):
        rpc = V4RpcFixture(chain_id=8453, unavailable_head=True)
        reader = UniswapV4PoolReader(rpc=rpc, chain_id=8453)
        snapshot = reader.snapshot()
        self.assertEqual(snapshot.block_height, 99)
        self.assertEqual(snapshot.pool_id, BASE_PROBE_KEY.pool_id)
        self.assertEqual(reader.last_header_diagnostic,
                         {"head": 100, "selected": 99, "fallback": True,
                          "attempted": [100, 99]})
        self.assertTrue(all(params[1] == "0x63" for method, params in rpc.calls
                            if method == "eth_call"))

    def test_key_pool_id_and_unapproved_hooks(self):
        self.assertEqual(len(MAINNET_PROBE_KEY.pool_id), 66)
        with self.assertRaisesRegex(ValueError, "v4_pool_key_unapproved"):
            V4PoolKey(ZERO, MAINNET_PROBE_KEY.currency1, 500, 10,
                      "0x" + "01" * 20).validate(CHAIN_CONFIGS[1])

    def test_reader_quotes_only_same_block_fully_covered_interval(self):
        rpc = V4RpcFixture()
        snapshot = UniswapV4PoolReader(rpc=rpc).snapshot()
        self.assertEqual(snapshot.block_hash, BLOCK_HASH)
        self.assertGreater(snapshot.quote(token_in=ZERO, amount_in=10**12).amount_out, 0)
        self.assertGreater(snapshot.quote(token_in=MAINNET_PROBE_KEY.currency1,
                                          amount_in=10**12).amount_out, 0)
        self.assertTrue(all(params[1] == "0x64" for method, params in rpc.calls
                            if method == "eth_call"))
        with self.assertRaisesRegex(ValueError, "v4_quote_tick_crossing_or_partial"):
            snapshot.quote(token_in=ZERO, amount_in=10**22)

    def test_uninitialized_missing_tick_and_reorg_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "v4_pool_uninitialized"):
            UniswapV4PoolReader(rpc=V4RpcFixture(liquidity=0)).snapshot()
        snapshot = UniswapV4PoolReader(rpc=V4RpcFixture(bitmap=0)).snapshot()
        with self.assertRaisesRegex(ValueError, "v4_tick_coverage_missing"):
            snapshot.quote(token_in=ZERO, amount_in=10**12)
        with self.assertRaisesRegex(ValueError, "v4_block_reorged"):
            UniswapV4PoolReader(rpc=V4RpcFixture(reorg=True)).snapshot()

    def test_void_router_return_never_claims_simulation(self):
        rpc = V4RpcFixture()
        snapshot = UniswapV4PoolReader(rpc=rpc).snapshot()
        quote = snapshot.quote(token_in=ZERO, amount_in=10**12)
        tx = build_unsigned_swap(chain_id=1, key=snapshot.key, token_in=ZERO,
                                 amount_in=quote.amount_in, minimum_out=1,
                                 deadline=int(time.time()) + 100)
        self.assertFalse(attempt_same_block_simulation(
            rpc, transaction=tx, snapshot=snapshot, quote=quote,
            wallet="0x" + "44" * 20,
        ))


if __name__ == "__main__":
    unittest.main()
