from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fomo.execution.readiness import wallet_balance_snapshot
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.watching.rpc_transport import RpcUnavailable


class Probe:
    def __init__(self, _chain: str, _endpoints: object) -> None:
        self.calls: list[str] = []

    def call(self, method: str, _params: object) -> str:
        self.calls.append(method)
        if method == "eth_getBalance":
            return "0xde0b6b3a7640000"
        raise RpcUnavailable("rpc_wrong_chain")


class WrongChainProbe(Probe):
    def call(self, method: str, _params: object) -> str:
        raise RpcUnavailable("rpc_wrong_chain")


class ReadinessRpcSafetyTests(unittest.TestCase):
    def test_native_balance_uses_verified_transport_and_never_claims_execution_ready(self) -> None:
        readiness = {"walletId": "test", "accounts": [], "chains": [{
            "chainId": 1, "address": "0x1111111111111111111111111111111111111111", "rpcEnv": None,
        }]}
        endpoint = RpcEndpoint("1", "primary", "primary", public_http_url="https://rpc.example.com")
        with tempfile.TemporaryDirectory() as directory, \
                patch("fomo.execution.readiness.execution_readiness", return_value=readiness), \
                patch("fomo.execution.readiness.load_rpc_endpoints", return_value=[endpoint]), \
                patch("fomo.execution.readiness.FailoverJsonRpc", Probe):
            result = wallet_balance_snapshot(Path(directory), {})
        self.assertEqual(result["balances"][0]["balance"], "1")
        self.assertTrue(result["balances"][0]["rpcIdentityVerified"])
        self.assertFalse(result["executionReady"])
        with tempfile.TemporaryDirectory() as directory, \
                patch("fomo.execution.readiness.execution_readiness", return_value=readiness), \
                patch("fomo.execution.readiness.load_rpc_endpoints", return_value=[endpoint]), \
                patch("fomo.execution.readiness.FailoverJsonRpc", WrongChainProbe):
            bad = wallet_balance_snapshot(Path(directory), {})
        self.assertFalse(bad["balances"][0]["available"])
        self.assertFalse(bad["balances"][0]["rpcIdentityVerified"])


if __name__ == "__main__":
    unittest.main()
