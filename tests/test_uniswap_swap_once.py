from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fomo.execution.direct_v2 import CHAIN_CONFIGS as V2_CONFIGS
from fomo.execution.direct_v3 import CHAIN_CONFIGS as V3_CONFIGS
from fomo.execution.evm_transaction import decode_eip1559
from fomo.execution.v3_transaction import decode_exact_input_single
from scripts.uniswap_swap_once import (Candidate, FEE_TIERS, WETH,
                                       _transaction, discover, execute_once)
from scripts.uniswap_v3_swap_once import SwapOnceRpc

TOKEN = "0x314ad0f11422842d28b4f950a64cd40fafb029fd"
WALLET = "0x4ccb77f12801ee8853a9cc3782828678c8f5584b"
POOL2 = "0x1111111111111111111111111111111111111111"
POOL3 = "0x2222222222222222222222222222222222222222"


def word(value: int) -> str:
    return f"0x{value:064x}"


class EmptyFactoryRpc(SwapOnceRpc):
    last_provider = "fixture"

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []

    def call(self, method, params):
        self.calls.append((method, list(params)))
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getBlockByNumber":
            return {"hash": "0x" + "ab" * 32}
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith("0x22afcccb"):
                return word(200)
            return word(0)
        raise AssertionError(method)


class SwapOnceTests(unittest.TestCase):
    def test_finite_factories_prove_no_supported_pool_without_logs(self):
        rpc = EmptyFactoryRpc()
        context = SimpleNamespace(block_height=100, block_hash="0x" + "ab" * 32)
        with patch("scripts.uniswap_swap_once.pin_v3_block_context", return_value=context):
            selected, records, tag = discover(rpc, token=TOKEN, amount=10**12)
        self.assertIsNone(selected)
        self.assertEqual(tag, "0x64")
        self.assertEqual(len(records), 1 + len(FEE_TIERS))
        self.assertTrue(all(item["status"] == "missing" for item in records))
        self.assertEqual([item["fee"] for item in records[1:]], list(FEE_TIERS))
        methods = [method for method, _ in rpc.calls]
        self.assertNotIn("eth_getLogs", methods)
        self.assertNotIn("eth_sendRawTransaction", methods)

    def test_discovery_selects_highest_output_from_valid_single_pools(self):
        rpc = EmptyFactoryRpc()
        context = SimpleNamespace(block_height=100, block_hash="0x" + "ab" * 32)
        token0, token1 = sorted((WETH, TOKEN))
        v2 = Candidate("v2", POOL2, None, 3000, token0, token1)
        snapshot = SimpleNamespace(quote=lambda **kwargs: SimpleNamespace(amount_out=5000))
        reader = SimpleNamespace(snapshot=lambda **kwargs: snapshot)
        with (patch("scripts.uniswap_swap_once.pin_v3_block_context", return_value=context),
              patch("scripts.uniswap_swap_once._v2_pair", return_value=POOL2),
              patch("scripts.uniswap_swap_once._v2_candidate", return_value=v2),
              patch("scripts.uniswap_swap_once._v3_spacing", side_effect=lambda rpc, fee, tag:
                    10 if fee == 500 else 0),
              patch("scripts.uniswap_swap_once._factory_pool", return_value=POOL3),
              patch("scripts.uniswap_swap_once.UniswapV3PoolReader", return_value=reader)):
            selected, records, _ = discover(rpc, token=TOKEN, amount=10**12)
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual((selected.protocol, selected.pool, selected.fee), ("v3", POOL3, 500))
        self.assertEqual(len(records), 5)

    def test_empty_v3_fee_tier_does_not_hide_valid_later_tier(self):
        rpc = EmptyFactoryRpc()
        context = SimpleNamespace(block_height=100, block_hash="0x" + "ab" * 32)
        snapshot = SimpleNamespace(quote=lambda **kwargs: SimpleNamespace(amount_out=5000))

        def reader(*, target, **kwargs):
            if target.fee == 500:
                return SimpleNamespace(snapshot=lambda **kwargs: (_ for _ in ()).throw(
                    ValueError("v3_pool_state_invalid")))
            return SimpleNamespace(snapshot=lambda **kwargs: snapshot)

        with (patch("scripts.uniswap_swap_once.pin_v3_block_context", return_value=context),
              patch("scripts.uniswap_swap_once._v3_spacing", side_effect=lambda rpc, fee, tag:
                    10 if fee in (500, 3000) else 0),
              patch("scripts.uniswap_swap_once._factory_pool", return_value=POOL3),
              patch("scripts.uniswap_swap_once.UniswapV3PoolReader", side_effect=reader)):
            selected, records, _ = discover(rpc, token=TOKEN, amount=10**12)

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected.fee, 3000)
        self.assertEqual(records[2]["status"], "inactive_zero_liquidity")

    def test_invalid_v3_state_with_liquidity_remains_fatal(self):
        rpc = EmptyFactoryRpc()
        context = SimpleNamespace(block_height=100, block_hash="0x" + "ab" * 32)
        reader = SimpleNamespace(snapshot=lambda **kwargs: (_ for _ in ()).throw(
            ValueError("v3_pool_state_invalid")))
        with (patch("scripts.uniswap_swap_once.pin_v3_block_context", return_value=context),
              patch("scripts.uniswap_swap_once._v2_pair", return_value=None),
              patch("scripts.uniswap_swap_once._v3_spacing", side_effect=lambda rpc, fee, tag:
                    10 if fee == 500 else 0),
              patch("scripts.uniswap_swap_once._factory_pool", return_value=POOL3),
              patch("scripts.uniswap_swap_once._eth_call_word", return_value=1),
              patch("scripts.uniswap_swap_once.UniswapV3PoolReader", return_value=reader)):
            with self.assertRaisesRegex(ValueError, "v3_pool_state_invalid"):
                discover(rpc, token=TOKEN, amount=10**12)

    def test_v2_native_transaction_has_exact_router_path_value_and_deadline(self):
        candidate = Candidate("v2", POOL2, None, 5000, WETH, TOKEN)
        transaction = _transaction(
            selected=candidate, token=TOKEN, wallet=WALLET, amount=10**12,
            slippage_bps=100, nonce=7, gas=200_000, priority=1,
            maximum=10, deadline=2_000_000_000,
        )
        fields, _ = decode_eip1559(transaction.serialized, signed=False)
        self.assertEqual(fields.chain_id, 4663)
        self.assertEqual(fields.nonce, 7)
        self.assertEqual(fields.value, 10**12)
        self.assertEqual("0x" + fields.to.hex(), V2_CONFIGS["4663"].router02)
        self.assertEqual(fields.data[:4].hex(), "7ff36ab5")
        self.assertEqual(int.from_bytes(fields.data[-32:], "big"), int(TOKEN, 16))

    def test_v3_native_transaction_preserves_fee_recipient_value_and_amount(self):
        token0, token1 = sorted((WETH, TOKEN))
        candidate = Candidate("v3", POOL3, 500, 8000, token0, token1)
        transaction = _transaction(
            selected=candidate, token=TOKEN, wallet=WALLET, amount=10**12,
            slippage_bps=100, nonce=8, gas=200_000, priority=1,
            maximum=10, deadline=2_000_000_000,
        )
        fields, _ = decode_eip1559(transaction.serialized, signed=False)
        decoded = decode_exact_input_single(fields.data, chain_id=4663)
        self.assertEqual(fields.value, 10**12)
        self.assertEqual("0x" + fields.to.hex(), V3_CONFIGS[4663].router)
        self.assertEqual(decoded["fee"], 500)
        self.assertEqual(decoded["recipient"], WALLET)
        self.assertEqual(decoded["amountIn"], 10**12)
        self.assertEqual(decoded["amountOutMinimum"], 7920)

    def test_no_pool_stops_before_wallet_signing_or_simulation(self):
        rpc = EmptyFactoryRpc()
        with patch("scripts.uniswap_swap_once.discover", return_value=(None, [], "0x64")):
            result = execute_once(chain_id=4663, token_out=TOKEN,
                                  native_in_wei=10**12, slippage_bps=100, rpc=rpc)
        self.assertEqual(result["reason"], "swap_once_no_supported_v2_v3_pool")
        self.assertFalse(result["simulation_success"])
        self.assertEqual(rpc.calls, [])

    def test_native_swap_is_simulated_without_send(self):
        class SimRpc(EmptyFactoryRpc):
            def call(self, method, params):
                self.calls.append((method, list(params)))
                if method == "eth_maxPriorityFeePerGas":
                    return "0x1"
                if method == "eth_getBlockByNumber":
                    return ({"baseFeePerGas": "0xa"} if params[0] == "pending"
                            else {"hash": "0x" + "ab" * 32})
                if method == "eth_getTransactionCount":
                    return "0x7"
                if method == "eth_getBalance":
                    return hex(10**18)
                if method == "eth_estimateGas":
                    return hex(150_000)
                if method == "eth_call":
                    return "0x"
                raise AssertionError(method)

        rpc = SimRpc()
        token0, token1 = sorted((WETH, TOKEN))
        v3 = Candidate("v3", POOL3, 500, 2000, token0, token1,
                       "0x" + "ab" * 32)
        records = [{"protocol": "v2", "status": "valid"},
                   {"protocol": "v3", "fee": 500, "status": "valid"}]
        with (patch("scripts.uniswap_swap_once.discover",
                    return_value=(v3, records, "0x64")),
              patch("scripts.uniswap_swap_once._wallet_profile",
                    return_value=(WALLET, {"backend": "disabled"}))):
            result = execute_once(chain_id=4663, token_out=TOKEN,
                                  native_in_wei=10**12, slippage_bps=100, rpc=rpc)
        self.assertTrue(result["simulation_success"])
        self.assertEqual(result["selectedProtocol"], "v3")
        self.assertEqual(result["fee"], 500)
        self.assertEqual([method for method, _ in rpc.calls].count("eth_estimateGas"), 2)
        self.assertEqual([method for method, _ in rpc.calls].count("eth_call"), 2)
        self.assertNotIn("eth_sendRawTransaction", [method for method, _ in rpc.calls])


if __name__ == "__main__":
    unittest.main()
