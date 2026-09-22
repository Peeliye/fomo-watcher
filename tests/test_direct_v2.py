from __future__ import annotations

import time
import unittest
from decimal import Decimal

from coincurve import PrivateKey

from fomo.execution.direct_v2 import (FACTORY, ROUTER02, UniswapV2PoolReader, amount_in,
                                      amount_out, build_approve_calldata, build_swap_calldata,
                                      build_unsigned_swap, minimum_out, parse_signed_direct_swap,
                                      simulate_unsigned_swap, usdc_state_override,
                                      verify_token_override, verify_weth9_override,
                                      weth9_state_override)
from fomo.execution.evm_transaction import (Eip1559Fields, decode_eip1559, decode_v2_swap,
                                            keccak256, sign_eip1559)


BASE = "0x" + "11" * 20
QUOTE = "0x" + "22" * 20
PAIR = "0x" + "33" * 20
BLOCK = "0x" + "ab" * 32


def word(value: int) -> str:
    return "0x" + f"{value:064x}"


class V2RpcFixture:
    last_provider = "verified-mainnet"

    def __init__(self, *, reorg: bool = False, wrong_token: bool = False,
                 no_pair: bool = False, stale: bool = False):
        self.reorg = reorg
        self.wrong_token = wrong_token
        self.no_pair = no_pair
        self.stale = stale
        self.calls: list[tuple[str, list]] = []

    def call(self, method, params):
        self.calls.append((method, list(params)))
        if method == "eth_getBlockByNumber":
            if params[0] == "0x64" and self.reorg:
                return {"number": "0x64", "hash": "0x" + "cd" * 32,
                        "timestamp": hex(int(time.time()))}
            return {"number": "0x64", "hash": BLOCK,
                    "timestamp": hex(int(time.time()) - (120 if self.stale else 1))}
        if method == "eth_call":
            call = params[0]
            assert params[1] == "0x64"
            if call["to"] == FACTORY:
                return word(0 if self.no_pair else int(PAIR, 16))
            if call["data"] == "0x0dfe1681":
                return word(int(BASE if not self.wrong_token else QUOTE, 16))
            if call["data"] == "0xd21220a7":
                return word(int(QUOTE, 16))
            if call["data"] == "0x0902f1ac":
                return "0x" + f"{100 * 10**18:064x}{250_000 * 10**6:064x}{1:064x}"
            if call["data"] == "0x313ce567":
                return word(18 if call["to"] == BASE else 6)
            if call["data"].startswith("0xdd62ed3e"):
                return word(10**25)
            if call["data"].startswith("0x70a08231"):
                return word(10**25)
        raise AssertionError(method)


class SimulationRpc:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.methods: list[str] = []

    def call(self, method, params):
        self.methods.append(method)
        if method == "eth_call":
            if self.fail:
                raise ValueError("simulated_revert")
            return "0x"
        if method == "eth_estimateGas":
            return "0x2bf20"
        raise AssertionError(method)


class DirectV2Tests(unittest.TestCase):
    def test_base_config_is_distinct_and_does_not_change_v2_math(self):
        from fomo.execution.direct_v2 import CHAIN_CONFIGS
        self.assertNotEqual(CHAIN_CONFIGS["1"].factory, CHAIN_CONFIGS["8453"].factory)
        self.assertNotEqual(CHAIN_CONFIGS["1"].router02, CHAIN_CONFIGS["8453"].router02)
        self.assertEqual(amount_out(1000, 10000, 10000), 906)

    def test_weth9_state_override_is_ephemeral_and_must_be_verified(self):
        from fomo.execution.direct_v2 import MAINNET_USDC, MAINNET_WETH9

        wallet = "0x" + "44" * 20
        override = weth9_state_override(wallet=wallet, balance_units=10**18,
                                        allowance_units=10**18)
        state = override[MAINNET_WETH9]["stateDiff"]
        self.assertEqual(len(state), 2)
        self.assertEqual(override[wallet]["balance"], hex(10**18))
        self.assertEqual(set(state.values()), {word(10**18)})
        usdc_override = usdc_state_override(wallet=wallet, balance_units=10**7,
                                            allowance_units=10**7)
        self.assertEqual(len(usdc_override[MAINNET_USDC]["stateDiff"]), 2)
        self.assertEqual(set(usdc_override[MAINNET_USDC]["stateDiff"].values()), {word(10**7)})
        self.assertNotEqual(set(usdc_override[MAINNET_USDC]["stateDiff"]), set(state))

        class Rpc:
            def __init__(self, *, ignored=False, units=10**18):
                self.ignored = ignored
                self.units = units
                self.calls = []

            def call(self, method, params):
                self.calls.append((method, params))
                return word(0 if self.ignored else self.units)

        rpc = Rpc()
        verify_weth9_override(rpc, wallet=wallet, block_height=100,
                              override=override, expected_units=10**18)
        self.assertEqual(len(rpc.calls), 2)
        self.assertTrue(all(method == "eth_call" and params[1] == "0x64"
                            and params[2] is override for method, params in rpc.calls))
        with self.assertRaisesRegex(ValueError, "v2_override_not_applied"):
            verify_weth9_override(Rpc(ignored=True), wallet=wallet, block_height=100,
                                  override=override, expected_units=10**18)
        usdc_rpc = Rpc(units=10**7)
        verify_token_override(usdc_rpc, token=MAINNET_USDC, wallet=wallet, block_height=100,
                              override=usdc_override, expected_units=10**7)
        self.assertTrue(all(params[0]["to"] == MAINNET_USDC for _, params in usdc_rpc.calls))

    def test_canonical_integer_swap_math_and_slippage(self):
        self.assertEqual(amount_out(1000, 10000, 10000), 906)
        self.assertEqual(amount_in(906, 10000, 10000), 1000)
        self.assertEqual(minimum_out(906, 100), 896)
        for fn, args in ((amount_out, (0, 1, 1)), (amount_out, (1, 0, 1)),
                         (amount_in, (10, 10, 10)), (minimum_out, (1, 501))):
            with self.assertRaises(ValueError):
                fn(*args)

    def test_single_block_pool_snapshot_and_quote_token_spot(self):
        rpc = V2RpcFixture()
        reader = UniswapV2PoolReader(rpc=rpc, base_token=BASE, quote_token=QUOTE)
        snapshot = reader.snapshot()
        self.assertEqual(snapshot.pair, PAIR)
        self.assertEqual(snapshot.spot_quote_per_base, Decimal("2500"))
        self.assertEqual(snapshot.reserve_base, 100 * 10**18)
        self.assertEqual(snapshot.reserve_quote, 250_000 * 10**6)
        output, floor = snapshot.quote_buy(1000 * 10**6, 100)
        self.assertGreater(output, floor)
        self.assertGreater(floor, 0)
        self.assertGreater(snapshot.quote_sell(10**18, 100)[0], 0)
        count = len(rpc.calls)
        self.assertIs(reader.snapshot(), snapshot)
        self.assertEqual(len(rpc.calls), count)
        reader.snapshot(now_ms=snapshot.observed_at_ms + 401)
        self.assertEqual(sum(method == "eth_call" and params[0]["to"] == FACTORY
                             for method, params in rpc.calls), 1)

    def test_wrong_pair_token_reorg_stale_and_missing_pair_fail_closed(self):
        for fixture, error in ((V2RpcFixture(reorg=True), "v2_block_reorged"),
                               (V2RpcFixture(wrong_token=True), "v2_pair_token_mismatch"),
                               (V2RpcFixture(no_pair=True), "v2_address_invalid"),
                               (V2RpcFixture(stale=True), "v2_block_stale_or_invalid")):
            with self.assertRaisesRegex(ValueError, error):
                UniswapV2PoolReader(rpc=fixture, base_token=BASE, quote_token=QUOTE).snapshot()

    def test_local_swap_and_separate_approval_encoding(self):
        swap = build_swap_calldata(amount_in_units=12345, minimum_out_units=9000,
                                   token_in=QUOTE, token_out=BASE,
                                   recipient=BASE, deadline=int(time.time()) + 60)
        parsed = decode_v2_swap(swap)
        self.assertEqual(parsed["path"], [QUOTE, BASE])
        self.assertEqual(parsed["minimumOut"], 9000)
        self.assertEqual(len(swap), 260)
        approval = build_approve_calldata(amount_units=12345)
        self.assertEqual(approval[:4].hex(), "095ea7b3")
        self.assertEqual(approval[16:36].hex(), ROUTER02[2:])
        with self.assertRaisesRegex(ValueError, "v2_approval_scope_invalid"):
            build_approve_calldata(spender=PAIR, amount_units=12345)
        reader = UniswapV2PoolReader(rpc=V2RpcFixture(), base_token=BASE, quote_token=QUOTE)
        snapshot = reader.snapshot()
        self.assertEqual(reader.allowance(owner=BASE, token_in=QUOTE, snapshot=snapshot), 10**25)
        self.assertEqual(reader.token_balance(owner=BASE, token=QUOTE, snapshot=snapshot), 10**25)

    def test_unsigned_build_and_final_signed_bytes_scope(self):
        snapshot = UniswapV2PoolReader(rpc=V2RpcFixture(), base_token=BASE,
                                       quote_token=QUOTE).snapshot()
        test_key = b"\x01" * 32  # deterministic fixture, never a production credential
        wallet = "0x" + keccak256(PrivateKey(test_key).public_key.format(compressed=False)[1:])[-20:].hex()
        built = build_unsigned_swap(
            snapshot=snapshot, wallet=wallet, token_in=QUOTE,
            amount_in_units=1000 * 10**6, slippage_bps=100, nonce=5,
            gas_limit=200_000, priority_fee_wei=10**9, maximum_fee_wei=2 * 10**9,
            deadline=int(time.time()) + 60,
        )
        parsed = parse_signed_direct_swap(sign_eip1559(built.serialized, test_key))
        self.assertEqual(parsed["wallet"], wallet)
        self.assertEqual(parsed["tokenIn"], QUOTE)
        self.assertEqual(parsed["tokenOut"], BASE)
        self.assertEqual(parsed["nonce"], 5)
        simulation_rpc = SimulationRpc()
        self.assertTrue(simulate_unsigned_swap(simulation_rpc, wallet=wallet,
                                               transaction=built)["passed"])
        self.assertEqual(simulation_rpc.methods, ["eth_call", "eth_estimateGas"])
        with self.assertRaisesRegex(ValueError, "simulated_revert"):
            simulate_unsigned_swap(SimulationRpc(fail=True), wallet=wallet, transaction=built)
        fields, _ = decode_eip1559(built.serialized, signed=False)
        wrong_target = Eip1559Fields(fields.chain_id, fields.nonce, fields.priority_fee,
                                     fields.maximum_fee, fields.gas_limit, bytes.fromhex(PAIR[2:]),
                                     fields.value, fields.data)
        with self.assertRaises(ValueError):
            parse_signed_direct_swap(sign_eip1559(wrong_target.unsigned_bytes(), test_key))
        wrong_chain = Eip1559Fields(56, fields.nonce, fields.priority_fee, fields.maximum_fee,
                                    fields.gas_limit, fields.to, fields.value, fields.data)
        with self.assertRaisesRegex(ValueError, "v2_signed_direct_scope_invalid"):
            parse_signed_direct_swap(sign_eip1559(wrong_chain.unsigned_bytes(), test_key))
        with self.assertRaisesRegex(ValueError, "v2_unsigned_swap_scope_invalid"):
            build_unsigned_swap(snapshot=snapshot, wallet=wallet, token_in=QUOTE,
                                amount_in_units=1000, slippage_bps=100, nonce=0,
                                gas_limit=200_000, priority_fee_wei=1, maximum_fee_wei=2,
                                deadline=int(time.time()) + 60,
                                now_ms=snapshot.observed_at_ms + 5001)


if __name__ == "__main__":
    unittest.main()
