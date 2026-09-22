import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from fomo.execution.direct_v3 import MAINNET_PROBE_POOL
from fomo.execution.v3_math import quote_exact_input, sqrt_ratio_at_tick
from fomo.execution.v3_transaction import decode_exact_input_single
from fomo.watching.rpc_transport import RpcUnavailable
from scripts.direct_v3_probe import main
from tests.test_direct_v3 import V3RpcFixture, word


class ProbeRpcFixture(V3RpcFixture):
    def __init__(self, *, wrong_simulation=False, ignored_override=False, **kwargs):
        super().__init__(**kwargs)
        self.wrong_simulation = wrong_simulation
        self.ignored_override = ignored_override

    def call(self, method, params):
        if method == "eth_call" and len(params) == 3:
            self.calls.append((method, params))
            call = params[0]
            if call["to"] in {MAINNET_PROBE_POOL.token0, MAINNET_PROBE_POOL.token1}:
                return word(0 if self.ignored_override else
                            (10 * 10**6 if call["to"] == MAINNET_PROBE_POOL.token0 else 10**18))
            if call["to"] == "0xe592427a0aece92de3edee1f18e0157c05861564":
                swap = decode_exact_input_single(bytes.fromhex(call["data"][2:]))
                output = quote_exact_input(
                    amount_in=swap["amountIn"],
                    zero_for_one=swap["tokenIn"] == MAINNET_PROBE_POOL.token0,
                    sqrt_price_x96=sqrt_ratio_at_tick(200000), tick=200000,
                    liquidity=10**18, fee_pips=500, tick_spacing=10,
                    bitmaps={77: 0, 78: 0, 79: 0}, liquidity_nets={},
                ).amount_out
                if swap["amountOutMinimum"] > output:
                    self.last_diagnostic = "rpc_method_unavailable"
                    raise RpcUnavailable("rpc_method_unavailable")
                return word(output - 1 if self.wrong_simulation else output)
        return super().call(method, params)


class V3ProbeTests(unittest.TestCase):
    def _run(self, rpc):
        output = io.StringIO()
        with (patch.dict("os.environ", {"RPC_ETHEREUM_URL": "https://secret.invalid/private-key"}),
              patch("scripts.direct_v3_probe.FailoverJsonRpc", return_value=rpc),
              redirect_stdout(output)):
            status = main()
        self.assertNotIn("secret.invalid", output.getvalue())
        return status, json.loads(output.getvalue())

    def test_missing_rpc_does_not_run_probe(self):
        output = io.StringIO()
        with patch.dict("os.environ", {"RPC_ETHEREUM_URL": ""}), redirect_stdout(output):
            status = main()
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(output.getvalue()),
                         {"status": "未注入", "tradingReady": False})

    def test_two_direction_same_block_probe_is_functional_only(self):
        rpc = ProbeRpcFixture()
        status, result = self._run(rpc)
        self.assertEqual(status, 0, result)
        self.assertTrue(result["readOnlyOk"])
        self.assertTrue(result["simulationOk"])
        self.assertFalse(result["tradingReady"])
        self.assertEqual([row["direction"] for row in result["simulation"]["directions"]],
                         ["WETH->USDC", "USDC->WETH"])
        self.assertTrue(all(row["blockHash"] == result["evidence"]["blockHash"]
                            for row in result["simulation"]["directions"]))
        self.assertTrue(all(row["excessiveMinOutRejected"]
                            for row in result["simulation"]["directions"]))
        self.assertFalse(any(method.startswith("eth_send") for method, _ in rpc.calls))

    def test_ignored_override_or_mismatched_simulation_fails_closed(self):
        for rpc in (ProbeRpcFixture(ignored_override=True),
                    ProbeRpcFixture(wrong_simulation=True),
                    ProbeRpcFixture(wrong_fee=True)):
            with self.subTest(rpc=rpc):
                status, result = self._run(rpc)
                self.assertEqual(status, 1)
                self.assertFalse(result["tradingReady"])
                self.assertFalse(result["simulationOk"])
                self.assertEqual(result["readOnlyOk"], not rpc.wrong_fee)


if __name__ == "__main__":
    unittest.main()
