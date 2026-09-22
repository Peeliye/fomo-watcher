import time
import unittest
from dataclasses import replace

from unittest.mock import patch

from fomo.execution.direct_v3 import (CHAIN_CONFIGS, MAINNET_PROBE_POOL, BASE_PROBE_POOL, V3ChainConfig,
                                      UniswapV3PoolReader, V3PoolTarget,
                                      _selector, _signed, simulate_same_block)
from fomo.execution.evm_transaction import Eip1559Fields, decode_eip1559
from fomo.execution.v3_math import sqrt_ratio_at_tick
from fomo.execution.v3_transaction import build_unsigned_swap
from fomo.watching.rpc_transport import RpcUnavailable


BLOCK_HASH = "0x" + "ab" * 32


def word(value):
    return "0x" + f"{value % (1 << 256):064x}"


class V3RpcFixture:
    last_provider = "fixture-mainnet"

    def __init__(self, *, wrong_fee=False, reorg=False, bitmap=0, missing_tick=False,
                 simulation_out=None, target=None, config=None, heads=None,
                 future_first=False, unavailable_head=False):
        self.wrong_fee = wrong_fee
        self.reorg = reorg
        self.bitmap = bitmap
        self.missing_tick = missing_tick
        self.simulation_out = simulation_out
        self.target = target or MAINNET_PROBE_POOL
        self.config = config or CHAIN_CONFIGS[1]
        self.heads = iter(heads) if heads is not None else None
        self.current_head = 100
        self.future_first = future_first
        self.unavailable_head = unavailable_head
        self.selected_height = 100
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        target, config = self.target, self.config
        if method == "eth_blockNumber":
            if self.heads is not None:
                self.current_head = next(self.heads)
            return hex(self.current_head)
        if method == "eth_getBlockByNumber":
            height = self.current_head if params[0] == "latest" else int(params[0], 16)
            if self.unavailable_head and height == 100:
                assert params[1] is False
                raise RpcUnavailable("header_unavailable")
            self.selected_height = height
            return {"number": hex(height), "hash": ("0x" + "cd" * 32 if self.reorg and params[0] != "latest"
                                                   else ("0x" + "bc" * 32 if height == 101 else BLOCK_HASH)),
                    "timestamp": hex(int(time.time()) + 2 if self.future_first and height == 100
                                     else int(time.time()) - 1)}
        if method == "eth_getCode":
            self.assert_pinned(params[1])
            return "0x6000"
        if method == "eth_call":
            call, tag = params[:2]
            self.assert_pinned(tag)
            data = call["data"]
            selectors = {name: _selector(name) for name in (
                "getPool(address,address,uint24)", "factory()", "token0()", "token1()",
                "fee()", "tickSpacing()", "feeAmountTickSpacing(uint24)",
                "decimals()", "slot0()", "liquidity()", "tickBitmap(int16)", "ticks(int24)",
            )}
            if data.startswith(selectors["getPool(address,address,uint24)"]):
                return word(int(target.pool, 16))
            if data == selectors["factory()"]:
                return word(int(config.factory, 16))
            if data == selectors["token0()"]:
                return word(int(target.token0, 16))
            if data == selectors["token1()"]:
                return word(int(target.token1, 16))
            if data == selectors["fee()"]:
                return word(3000 if self.wrong_fee else target.fee)
            if data == selectors["tickSpacing()"] or data.startswith(selectors["feeAmountTickSpacing(uint24)"]):
                return word(target.tick_spacing)
            if data == selectors["decimals()"]:
                return word(target.decimals0 if call["to"] == target.token0 else target.decimals1)
            if data == selectors["slot0()"]:
                return "0x" + "".join(f"{value:064x}" for value in
                                      (sqrt_ratio_at_tick(200000), 200000, 0, 0, 0, 0, 1))
            if data == selectors["liquidity()"]:
                return word(10**18)
            if data.startswith(selectors["tickBitmap(int16)"]):
                return word(self.bitmap)
            if data.startswith(selectors["ticks(int24)"]):
                return "0x" + "".join(f"{value:064x}" for value in
                                      (1000, 0, 0, 0, 0, 0, 0, 0 if self.missing_tick else 1))
            if call["to"] == config.router:
                return word(self.simulation_out if self.simulation_out is not None else 0)
        raise AssertionError((method, params))

    def assert_pinned(self, tag):
        assert tag == hex(self.selected_height), tag


class DirectV3Tests(unittest.TestCase):
    def test_base_reader_rejects_when_both_headers_unavailable(self):
        class MissingHeaders(V3RpcFixture):
            def call(self, method, params):
                if method == "eth_getBlockByNumber":
                    assert params[1] is False
                    raise RpcUnavailable("header_unavailable")
                return super().call(method, params)

        rpc = MissingHeaders(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        with self.assertRaisesRegex(ValueError, "v3_rpc_failure:call=2:method=eth_getBlockByNumber:block_tag=true"):
            reader.snapshot()
        self.assertEqual(reader.last_block_diagnostic["headerAttempts"], [100, 99])

    def test_base_reader_binds_previous_header_when_tip_unavailable(self):
        rpc = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                           unavailable_head=True)
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        snapshot = reader.snapshot()
        self.assertEqual(snapshot.block_height, 99)
        self.assertTrue(reader.last_block_diagnostic["headerFallback"])
        self.assertTrue(all(params[1] == "0x63" for method, params in rpc.calls
                            if method == "eth_call"))

    def test_base_new_block_timestamp_restarts_instead_of_marking_pool_invalid(self):
        rpc = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                           heads=[100, 101, 101, 101], future_first=True)
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        self.assertEqual(reader.snapshot().block_height, 101)
        self.assertEqual(reader.last_block_diagnostic["checks"]["ageWithin60Seconds"], True)

    def test_base_head_advance_discards_round_and_retries_whole_block(self):
        rpc = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                           heads=[100, 101, 101, 101])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        snapshot = reader.snapshot()
        self.assertEqual(snapshot.block_height, 101)
        self.assertEqual(snapshot.block_hash, "0x" + "bc" * 32)
        self.assertEqual(reader.last_block_diagnostic["headBefore"], 101)
        self.assertEqual(reader.last_block_diagnostic["headAfter"], 101)
        tags = [params[1] for method, params in rpc.calls if method == "eth_call"]
        self.assertIn("0x64", tags)
        self.assertIn("0x65", tags)
        self.assertTrue(all(tag in {"0x64", "0x65"} for tag in tags))

    def test_base_three_head_advances_fail_closed(self):
        rpc = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                           heads=[100, 101, 101, 102, 102, 103])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        with self.assertRaisesRegex(ValueError, "v3_head_advanced_retry"):
            reader.snapshot()
        self.assertEqual(reader.last_block_diagnostic["headAfter"], 103)

    def test_read_failure_reports_call_method_and_tag(self):
        class FailingRpc(V3RpcFixture):
            def call(self, method, params):
                if method == "eth_call":
                    raise RpcUnavailable("redacted")
                return super().call(method, params)

        with self.assertRaisesRegex(ValueError,
                                    "v3_rpc_failure:call=2:method=eth_call:block_tag=true"):
            UniswapV3PoolReader(rpc=FailingRpc()).snapshot()

    def test_v3_identity_read_never_calls_get_code(self):
        rpc = V3RpcFixture()
        UniswapV3PoolReader(rpc=rpc).snapshot()
        self.assertFalse(any(method == "eth_getCode" for method, _ in rpc.calls))

    def test_only_nearest_initialized_ticks_are_read(self):
        rpc = V3RpcFixture(bitmap=(1 << 1) | (1 << 2) | (1 << 200) | (1 << 201))
        snapshot = UniswapV3PoolReader(rpc=rpc).snapshot()
        tick_calls = [params for method, params in rpc.calls
                      if method == "eth_call" and params[0]["data"].startswith(_selector("ticks(int24)"))]
        self.assertLessEqual(len(tick_calls), 2)
        self.assertEqual(len(snapshot.bitmaps), 1)

    def test_same_block_pool_identity_state_and_quote(self):
        rpc = V3RpcFixture()
        snapshot = UniswapV3PoolReader(rpc=rpc).snapshot()
        self.assertEqual(snapshot.block_hash, BLOCK_HASH)
        self.assertEqual(snapshot.pool, MAINNET_PROBE_POOL.pool)
        self.assertEqual(snapshot.fee, 500)
        self.assertEqual(len(snapshot.bitmaps), 1)
        self.assertGreater(snapshot.spot_quote_per_base, 0)
        self.assertGreater(snapshot.quote(token_in=MAINNET_PROBE_POOL.token0,
                                          amount_in=10**6).amount_out, 0)
        self.assertGreater(snapshot.quote(token_in=MAINNET_PROBE_POOL.token1,
                                          amount_in=10**15).amount_out, 0)
        self.assertTrue(all(params[1] == "0x64" for method, params in rpc.calls if method == "eth_call"))

    def test_wrong_immutables_reorg_or_missing_tick_reject(self):
        for kwargs, error in (({"wrong_fee": True}, "v3_pool_identity_mismatch"),
                              ({"reorg": True}, "v3_block_reorged"),
                              ({"bitmap": 1, "missing_tick": True}, "v3_tick_uninitialized")):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, error):
                UniswapV3PoolReader(rpc=V3RpcFixture(**kwargs)).snapshot()
        with self.assertRaisesRegex(ValueError, "v3_chain_unapproved"):
            UniswapV3PoolReader(rpc=V3RpcFixture(), chain_id=10)
        with self.assertRaisesRegex(ValueError, "v3_chain_unapproved"):
            UniswapV3PoolReader(rpc=V3RpcFixture(), target=V3PoolTarget(
                "0x" + "11" * 20, "0x" + "22" * 20, "0x" + "33" * 20, 500, 10, 18, 6,
            ))

    def test_signed_int24_canonical_sign_extension(self):
        self.assertEqual(_signed((1 << 256) - 10, 24), -10)
        with self.assertRaisesRegex(ValueError, "v3_signed_word_invalid"):
            _signed(1 << 24, 24)

    def test_additional_chain_uses_binding_and_target_not_copied_math(self):
        token0, token1 = "0x" + "11" * 20, "0x" + "22" * 20
        config = V3ChainConfig(8453, "0x" + "33" * 20, "0x" + "44" * 20,
                               frozenset({token0, token1}))
        target = V3PoolTarget("0x" + "55" * 20, token0, token1, 3000, 60, 6, 18)
        with patch.dict(CHAIN_CONFIGS, {8453: config}):
            reader = UniswapV3PoolReader(rpc=V3RpcFixture(), chain_id=8453, target=target)
            self.assertEqual(reader.target.pool, target.pool)
            self.assertEqual(reader.config.router, config.router)
            with self.assertRaisesRegex(ValueError, "v3_chain_unapproved"):
                UniswapV3PoolReader(rpc=V3RpcFixture(), chain_id=8453)

    def test_simulation_same_block_output_and_scope(self):
        rpc = V3RpcFixture()
        snapshot = UniswapV3PoolReader(rpc=rpc).snapshot()
        quote = snapshot.quote(token_in=MAINNET_PROBE_POOL.token0, amount_in=10**6)
        wallet = "0x" + "44" * 20
        built = build_unsigned_swap(
            chain_id=1, router=CHAIN_CONFIGS[1].router,
            token0=snapshot.token0, token1=snapshot.token1, fee=snapshot.fee,
            token_in=snapshot.token0, wallet=wallet, amount_in=quote.amount_in,
            quoted_out=quote.amount_out, slippage_bps=100, nonce=0,
            gas_limit=300000, priority_fee_wei=1, maximum_fee_wei=2,
            deadline=int(time.time()) + 60,
        )
        rpc.simulation_out = quote.amount_out
        self.assertEqual(simulate_same_block(rpc, wallet=wallet, transaction=built,
                                             snapshot=snapshot, quote=quote), quote.amount_out)
        self.assertFalse(any(method.startswith("eth_send") for method, _ in rpc.calls))
        rpc.simulation_out = quote.amount_out - 1
        with self.assertRaisesRegex(ValueError, "v3_simulation_quote_mismatch_or_partial"):
            simulate_same_block(rpc, wallet=wallet, transaction=built,
                                snapshot=snapshot, quote=quote)
        rpc.simulation_out = quote.amount_out
        with self.assertRaisesRegex(ValueError, "v3_simulation_scope_invalid"):
            simulate_same_block(rpc, wallet=wallet, transaction=built,
                                snapshot=snapshot,
                                quote=replace(quote, block_hash="0x" + "cd" * 32))
        rpc.reorg = True
        with self.assertRaisesRegex(ValueError, "v3_simulation_block_reorged"):
            simulate_same_block(rpc, wallet=wallet, transaction=built,
                                snapshot=snapshot, quote=quote)
        rpc.reorg = False
        fields, _ = decode_eip1559(built.serialized, signed=False)
        wrong = Eip1559Fields(1, fields.nonce, fields.priority_fee, fields.maximum_fee,
                              fields.gas_limit, bytes.fromhex(("0x" + "11" * 20)[2:]),
                              fields.value, fields.data)
        from fomo.execution.interfaces import BuiltTransaction
        with self.assertRaisesRegex(ValueError, "v3_simulation_scope_invalid"):
            simulate_same_block(rpc, wallet=wallet,
                                transaction=BuiltTransaction(wrong.unsigned_bytes(), built.provider, "0"),
                                snapshot=snapshot, quote=quote)


if __name__ == "__main__":
    unittest.main()
