from __future__ import annotations

import io
import json
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from fomo.execution.direct_v2 import V2PoolSnapshot, amount_out
from fomo.execution.evm_transaction import decode_v2_swap
from fomo.watching.rpc_transport import RpcUnavailable
from scripts.direct_v2_probe import PAIR, PUBLIC_WALLET, USDC, WETH, main


BLOCK = "0x" + "ab" * 32


def word(value: int) -> str:
    return "0x" + f"{value:064x}"


class OverrideRpcFixture:
    last_provider = "fixture-mainnet"
    last_diagnostic = "ok"

    def __init__(self, *, negative_transport_failure: bool = False,
                 override_ignored: bool = False):
        self.negative_transport_failure = negative_transport_failure
        self.override_ignored = override_ignored
        self.calls: list[tuple[str, list]] = []

    def call(self, method, params):
        self.calls.append((method, list(params)))
        if method == "eth_getBlockByNumber":
            return {"number": "0x64", "hash": BLOCK,
                    "timestamp": hex(int(time.time()) - 1)}
        if method != "eth_call" or len(params) != 3 or params[1] != "0x64":
            raise AssertionError(method)
        call = params[0]
        if call["to"] in {WETH, USDC}:
            assert call["to"] in params[2]
            if self.override_ignored:
                return word(0)
            if call["data"].startswith(("0x70a08231", "0xdd62ed3e")):
                return word(10**18 if call["to"] == WETH else 10 * 10**6)
        if call["to"] == "0x7a250d5630b4cf539739df2c5dacb4c659f2488d":
            assert call["from"] == PUBLIC_WALLET
            swap = decode_v2_swap(bytes.fromhex(call["data"][2:]))
            excessive = (250_000 * 10**6 if swap["path"][0] == WETH
                         else 100 * 10**18)
            if swap["minimumOut"] >= excessive:
                self.last_diagnostic = ("rpc_transport_failure" if self.negative_transport_failure
                                        else "rpc_method_unavailable")
                raise RpcUnavailable(self.last_diagnostic)
            self.last_diagnostic = "ok"
            output = (amount_out(swap["amountIn"], 100 * 10**18, 250_000 * 10**6)
                      if swap["path"][0] == WETH else
                      amount_out(swap["amountIn"], 250_000 * 10**6, 100 * 10**18))
            return "0x" + "".join(f"{n:064x}" for n in
                                  (32, 2, swap["amountIn"], output))
        raise AssertionError(call)


class ReaderFixture:
    def __init__(self, rpc):
        self.rpc = rpc

    def snapshot(self):
        return V2PoolSnapshot(
            "1", PAIR, WETH, USDC, 100 * 10**18, 250_000 * 10**6,
            18, 6, 100, BLOCK, int(time.time() * 1000), "fixture-mainnet",
        )

    def allowance(self, *, owner, token_in, snapshot):
        assert owner == PUBLIC_WALLET and token_in in {WETH, USDC} and snapshot.pair == PAIR
        return 0


class DirectV2ProbeTests(unittest.TestCase):
    def _run(self, rpc):
        output = io.StringIO()
        with (patch.dict("os.environ", {"RPC_ETHEREUM_URL": "https://example.invalid/test-only"}),
              patch("scripts.direct_v2_probe.FailoverJsonRpc", return_value=rpc),
              patch("scripts.direct_v2_probe.UniswapV2PoolReader",
                    return_value=ReaderFixture(rpc)),
              redirect_stdout(output)):
            status = main()
        return status, json.loads(output.getvalue())

    def test_missing_rpc_reports_only_not_injected(self):
        output = io.StringIO()
        with patch.dict("os.environ", {"RPC_ETHEREUM_URL": ""}), redirect_stdout(output):
            status = main()
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(output.getvalue()),
                         {"status": "未注入", "tradingReady": False})

    def test_rpc_failure_is_redacted(self):
        output = io.StringIO()
        with (patch.dict("os.environ", {"RPC_ETHEREUM_URL": "https://secret.invalid/key-value"}),
              patch("scripts.direct_v2_probe.FailoverJsonRpc",
                    side_effect=RuntimeError("secret-url")), redirect_stdout(output)):
            status = main()
        self.assertEqual(status, 1)
        self.assertNotIn("secret", output.getvalue())
        self.assertFalse(json.loads(output.getvalue())["tradingReady"])

    def test_zero_actual_allowance_does_not_block_override_simulation(self):
        rpc = OverrideRpcFixture()
        status, result = self._run(rpc)
        self.assertEqual(status, 0)
        self.assertTrue(result["readOnlyOk"])
        self.assertTrue(result["simulationOk"])
        self.assertFalse(result["tradingReady"])
        self.assertEqual(result["evidence"]["actualWethAllowanceUnits"], "0")
        self.assertEqual(result["evidence"]["actualUsdcAllowanceUnits"], "0")
        self.assertEqual(result["evidence"]["blockHeight"], 100)
        directions = result["simulation"]["directions"]
        self.assertEqual([item["direction"] for item in directions],
                         ["WETH->USDC", "USDC->WETH"])
        self.assertTrue(all(item["successSimulated"] and item["excessiveMinOutRejected"]
                            and item["blockHeight"] == result["evidence"]["blockHeight"]
                            and item["blockHash"] == result["evidence"]["blockHash"]
                            for item in directions))
        self.assertEqual(sum(method == "eth_call" and params[0]["to"] not in {WETH, USDC}
                             for method, params in rpc.calls), 6)
        self.assertFalse(any(method.startswith("eth_send") for method, _ in rpc.calls))

    def test_transport_failure_cannot_pass_excessive_minimum_check(self):
        status, result = self._run(OverrideRpcFixture(negative_transport_failure=True))
        self.assertEqual(status, 1)
        self.assertTrue(result["readOnlyOk"])
        self.assertFalse(result["simulationOk"])

    def test_ignored_state_override_fails_closed(self):
        status, result = self._run(OverrideRpcFixture(override_ignored=True))
        self.assertEqual(status, 1)
        self.assertTrue(result["readOnlyOk"])
        self.assertFalse(result["simulationOk"])


if __name__ == "__main__":
    unittest.main()
