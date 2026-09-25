import time
import unittest
from dataclasses import replace

from unittest.mock import patch

from fomo.execution.direct_v3 import (CHAIN_CONFIGS, MAINNET_PROBE_POOL, BASE_PROBE_POOL, V3ChainConfig,
                                      UniswapV3PoolReader, V3PoolTarget,
                                      _base_multicall,
                                      _selector, _signed, _encode_multicall3,
                                      _decode_multicall3, simulate_same_block)
from fomo.execution.evm_transaction import Eip1559Fields, decode_eip1559
from fomo.execution.v3_math import sqrt_ratio_at_tick
from fomo.execution.v3_transaction import build_unsigned_swap
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcEndpoint, RpcUnavailable


BLOCK_HASH = "0x" + "ab" * 32


def word(value):
    return "0x" + f"{value % (1 << 256):064x}"


def aggregate3_result(values):
    elements = []
    for value in values:
        data = bytes.fromhex(value[2:])
        elements.append((1).to_bytes(32, "big") + (64).to_bytes(32, "big")
                        + len(data).to_bytes(32, "big") + data
                        + bytes((-len(data)) % 32))
    offset = len(elements) * 32
    offsets = []
    for element in elements:
        offsets.append(offset.to_bytes(32, "big"))
        offset += len(element)
    return "0x" + ((32).to_bytes(32, "big") + len(elements).to_bytes(32, "big")
                  + b"".join(offsets) + b"".join(elements)).hex()


def aggregate3_calls(calldata, tag):
    raw = bytes.fromhex(calldata[2:])
    assert raw[:4] == bytes.fromhex(_selector("aggregate3((address,bool,bytes)[])")[2:])
    assert int.from_bytes(raw[4:36], "big") == 32
    count = int.from_bytes(raw[36:68], "big")
    calls = []
    for index in range(count):
        offset = int.from_bytes(raw[68 + index * 32:100 + index * 32], "big")
        start = 68 + offset
        target = "0x" + raw[start + 12:start + 32].hex()
        assert int.from_bytes(raw[start + 32:start + 64], "big") == 0
        assert int.from_bytes(raw[start + 64:start + 96], "big") == 96
        length = int.from_bytes(raw[start + 96:start + 128], "big")
        data = "0x" + raw[start + 128:start + 128 + length].hex()
        calls.append(("eth_call", [{"to": target, "data": data}, tag]))
    return calls


class V3RpcFixture:
    last_provider = "fixture-mainnet"

    def __init__(self, *, wrong_fee=False, wrong_identity: str | None = None,
                 reorg=False, bitmap=0, missing_tick=False,
                 simulation_out=None, target=None, config=None, heads=None,
                 future_first=False, unavailable_head=False):
        self.wrong_fee = wrong_fee
        self.wrong_identity = wrong_identity
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
        if method == "eth_chainId":
            return hex(config.chain_id)
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
                return word(int("0x" + "44" * 20, 16)
                            if self.wrong_identity == "factory_pool" else int(target.pool, 16))
            if data == selectors["factory()"]:
                return word(int("0x" + "44" * 20, 16)
                            if self.wrong_identity == "pool_factory" else int(config.factory, 16))
            if data == selectors["token0()"]:
                return word(int("0x" + "44" * 20, 16)
                            if self.wrong_identity == "token0" else int(target.token0, 16))
            if data == selectors["token1()"]:
                return word(int("0x" + "44" * 20, 16)
                            if self.wrong_identity == "token1" else int(target.token1, 16))
            if data == selectors["fee()"]:
                return word(3000 if self.wrong_fee or self.wrong_identity == "fee" else target.fee)
            if data == selectors["tickSpacing()"]:
                return word(target.tick_spacing + 1
                            if self.wrong_identity == "tick_spacing" else target.tick_spacing)
            if data.startswith(selectors["feeAmountTickSpacing(uint24)"]):
                return word(target.tick_spacing + 1
                            if self.wrong_identity == "enabled_spacing" else target.tick_spacing)
            if data == selectors["decimals()"]:
                return word(target.decimals0 if call["to"] == target.token0 else target.decimals1)
            if data == selectors["slot0()"]:
                tick = -197000 if config.chain_id == 8453 else 200000
                return "0x" + "".join(f"{value:064x}" for value in
                                      (sqrt_ratio_at_tick(tick), tick % (1 << 256), 0, 0, 0, 0, 1))
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


class BatchedV3RpcFixture(V3RpcFixture):
    def __init__(self):
        super().__init__()
        self.batch_sizes: list[int] = []

    def call_batch(self, requests):
        self.batch_sizes.append(len(requests))
        return [self.call(method, params) for method, params in requests]


class MulticallTransportFixture:
    supports_multicall3 = True

    def __init__(self):
        self.calls = 0

    def call(self, method, params):
        self.calls += 1
        assert method == "eth_call"
        nested = aggregate3_calls(params[0]["data"], params[1])
        return aggregate3_result([word(index + 1) for index in range(len(nested))])


class DirectV3Tests(unittest.TestCase):
    def test_base_multicall_abi_rejects_failed_or_truncated_results(self):
        requests = [("eth_call", [{"to": BASE_PROBE_POOL.pool,
                                  "data": _selector("liquidity()")}, "0x64"])]
        calldata = _encode_multicall3(requests, "0x64")
        self.assertEqual(aggregate3_calls(calldata, "0x64"), requests)
        result = aggregate3_result([word(10**18)])
        self.assertEqual(_decode_multicall3(result, 1), [word(10**18)])
        with self.assertRaisesRegex(ValueError, "v3_multicall_response_invalid"):
            _decode_multicall3(result[:-2], 1)
        failed = bytearray.fromhex(result[2:])
        failed[127] = 0
        with self.assertRaisesRegex(ValueError, "v3_multicall_response_invalid"):
            _decode_multicall3("0x" + failed.hex(), 1)

    def test_base_network_path_uses_at_most_seven_round_trips_and_one_block(self):
        for bitmap, expected_round_trips in ((0, 6), (1 << 1, 7)):
            with self.subTest(bitmap=bitmap):
                fixture = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                                       bitmap=bitmap)
                rpc = FailoverJsonRpc("8453", [RpcEndpoint("8453", "base-test", "primary",
                                                          http_env="RPC_BASE_URL")])
                observed = []

                def read(_, method, params):
                    observed.append((method, params))
                    rpc.last_provider = "fixture-base"
                    rpc.last_http_status = 200
                    if method == "eth_call":
                        self.assertEqual(params[1], "0x64")
                        values = [fixture.call(name, args) for name, args in
                                  aggregate3_calls(params[0]["data"], params[1])]
                        return aggregate3_result(values)
                    return fixture.call(method, params)

                with patch("fomo.execution.direct_v3._base_read", side_effect=read):
                    reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
                    snapshot = reader.snapshot()
                self.assertEqual(snapshot.block_hash, BLOCK_HASH)
                self.assertEqual(reader.last_block_diagnostic["roundTrips"], expected_round_trips)
                self.assertEqual(len(observed), expected_round_trips)
                self.assertEqual([method for method, _ in observed[:5]],
                                 ["eth_chainId", "eth_blockNumber", "eth_getBlockByNumber",
                                  "eth_call", "eth_call"])
                self.assertEqual(observed[-1][0], "eth_getBlockByNumber")
                self.assertEqual(len(aggregate3_calls(observed[3][1][0]["data"], "0x64")), 11)

    def test_base_http_429_retries_whole_round_only_twice(self):
        fixture = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453])
        rpc = FailoverJsonRpc("8453", [RpcEndpoint("8453", "base-test", "primary",
                                                  http_env="RPC_BASE_URL")])
        heads = 0
        identity_attempts = 0

        def read(_, method, params):
            nonlocal heads, identity_attempts
            rpc.last_provider = "fixture-base"
            rpc.last_http_status = 200
            if method == "eth_blockNumber":
                heads += 1
            if method == "eth_call":
                if len(aggregate3_calls(params[0]["data"], params[1])) == 11:
                    identity_attempts += 1
                    if identity_attempts <= 2:
                        rpc.last_http_status = 429
                        raise RpcUnavailable("redacted_429")
                values = [fixture.call(name, args) for name, args in
                          aggregate3_calls(params[0]["data"], params[1])]
                return aggregate3_result(values)
            return fixture.call(method, params)

        with (patch("fomo.execution.direct_v3._base_read", side_effect=read),
              patch("fomo.execution.direct_v3.time.sleep") as sleep):
            reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
            snapshot = reader.snapshot()
        self.assertEqual(snapshot.block_hash, BLOCK_HASH)
        self.assertEqual(heads, 3)
        self.assertEqual(identity_attempts, 3)
        self.assertEqual(reader.last_block_diagnostic["round"], 3)
        self.assertEqual([item["reason"] for item in reader.last_block_diagnostic["retryHistory"]],
                         ["http_429", "http_429"])
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.25, 0.5])

    def test_base_http_429_exhausts_after_three_rounds_without_header_fallback(self):
        class RateLimitedHeader(V3RpcFixture):
            last_http_status = None

            def call(self, method, params):
                if method == "eth_getBlockByNumber":
                    self.calls.append((method, params))
                    self.last_http_status = 429
                    raise RpcUnavailable("redacted_429")
                self.last_http_status = None
                return super().call(method, params)

        rpc = RateLimitedHeader(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        with patch("fomo.execution.direct_v3.time.sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "v3_rpc_failure:call=3:method=eth_getBlockByNumber:block_tag=true"):
                reader.snapshot()
        self.assertEqual([params[0] for method, params in rpc.calls
                          if method == "eth_getBlockByNumber"], ["0x64"] * 3)
        self.assertEqual(len(reader.last_block_diagnostic["retryHistory"]), 3)
        self.assertEqual(sleep.call_count, 2)

    def test_base_reader_rejects_when_both_headers_unavailable(self):
        class MissingHeaders(V3RpcFixture):
            def call(self, method, params):
                if method == "eth_getBlockByNumber":
                    assert params[1] is False
                    raise RpcUnavailable("header_unavailable")
                return super().call(method, params)

        rpc = MissingHeaders(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        with self.assertRaisesRegex(ValueError, "v3_rpc_failure:call=3:method=eth_getBlockByNumber:block_tag=true"):
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

    def test_base_tip_advance_does_not_invalidate_pinned_block(self):
        rpc = V3RpcFixture(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453],
                           heads=[100, 101])
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        snapshot = reader.snapshot()
        self.assertEqual(snapshot.block_height, 100)
        self.assertEqual(snapshot.block_hash, BLOCK_HASH)
        self.assertEqual(reader.last_block_diagnostic["headBefore"], 100)
        self.assertEqual(reader.last_block_diagnostic["round"], 1)
        self.assertEqual(reader.last_block_diagnostic["roundTrips"], 6)
        tags = [params[1] for method, params in rpc.calls if method == "eth_call"]
        self.assertTrue(tags and all(tag == "0x64" for tag in tags))

    def test_base_three_reorged_rounds_fail_closed(self):
        class Reorging(V3RpcFixture):
            def __init__(self):
                super().__init__(target=BASE_PROBE_POOL, config=CHAIN_CONFIGS[8453])
                self.header_reads = 0

            def call(self, method, params):
                if method == "eth_getBlockByNumber":
                    self.header_reads += 1
                    if self.header_reads % 2 == 0:
                        result = super().call(method, params)
                        return {**result, "hash": "0x" + "cd" * 32}
                return super().call(method, params)

        rpc = Reorging()
        reader = UniswapV3PoolReader(rpc=rpc, chain_id=8453)
        with self.assertRaisesRegex(ValueError, "v3_block_reorged_or_provider_unknown"):
            reader.snapshot()
        self.assertEqual(reader.last_block_diagnostic["round"], 3)
        self.assertEqual(rpc.header_reads, 6)

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
        cases = [({"wrong_identity": value}, "v3_pool_identity_mismatch") for value in (
            "factory_pool", "pool_factory", "token0", "token1", "fee",
            "tick_spacing", "enabled_spacing")]
        cases.extend((
            ({"reorg": True}, "v3_block_reorged"),
            ({"bitmap": 1, "missing_tick": True}, "v3_tick_uninitialized"),
        ))
        for kwargs, error in cases:
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, error):
                UniswapV3PoolReader(rpc=V3RpcFixture(**kwargs)).snapshot()
        with self.assertRaisesRegex(ValueError, "v3_chain_unapproved"):
            UniswapV3PoolReader(rpc=V3RpcFixture(), chain_id=10)
        with self.assertRaisesRegex(ValueError, "v3_chain_unapproved"):
            UniswapV3PoolReader(rpc=V3RpcFixture(), target=V3PoolTarget(
                "0x" + "11" * 20, "0x" + "22" * 20, "0x" + "33" * 20, 500, 10, 18, 6,
            ))

    def test_explicit_snapshot_height_is_used_for_every_state_read(self):
        rpc = V3RpcFixture()
        snapshot = UniswapV3PoolReader(rpc=rpc).snapshot(block_height=99)
        self.assertEqual(snapshot.block_height, 99)
        self.assertTrue(all(params[1] == hex(99)
                            for method, params in rpc.calls if method == "eth_call"))

    def test_bounded_batch_transport_reduces_round_trips_without_skipping_reads(self):
        rpc = BatchedV3RpcFixture()
        snapshot = UniswapV3PoolReader(rpc=rpc).snapshot(block_height=99)
        self.assertGreater(snapshot.liquidity, 0)
        self.assertTrue(rpc.batch_sizes)
        self.assertTrue(all(0 < size <= 3 for size in rpc.batch_sizes))

    def test_explicit_multicall_transport_uses_one_pinned_rpc(self):
        rpc = MulticallTransportFixture()
        requests = [
            ("eth_call", [{"to": MAINNET_PROBE_POOL.pool,
                            "data": _selector("liquidity()")}, "0x63"]),
            ("eth_call", [{"to": MAINNET_PROBE_POOL.pool,
                            "data": _selector("fee()")}, "0x63"]),
        ]
        self.assertEqual(_base_multicall(rpc, requests, tag="0x63", call_offset=3),
                         [word(1), word(2)])
        self.assertEqual(rpc.calls, 1)

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
