from __future__ import annotations

import unittest
import time
from typing import Any, Sequence

from fomo.execution.rpc_pool import RpcEndpoint
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcUnavailable, SOLANA_MAINNET_GENESIS


class RpcFixture:
    def __init__(self) -> None:
        self.chains = {"primary": 1, "backup": 1}
        self.heights = {"primary": 100, "backup": 101}
        self.hashes = {"primary": "h100", "backup": "h100"}
        self.nonces = {"primary": 7, "backup": 7}
        self.fail_primary_method = False
        self.calls: list[tuple[str, str]] = []

    def request(self, endpoint: RpcEndpoint, method: str, params: Sequence[Any]) -> Any:
        name = endpoint.provider
        self.calls.append((name, method))
        if method == "eth_chainId":
            return hex(self.chains[name])
        if method == "eth_blockNumber":
            return hex(self.heights[name])
        if method == "eth_getBlockByNumber":
            height = int(params[0], 16)
            return {"hash": self.hashes[name] if height == 100 else f"h{height}"}
        if name == "primary" and self.fail_primary_method:
            raise OSError("credential-containing URL must not escape")
        if method == "eth_getTransactionCount":
            return hex(self.nonces[name])
        if method == "eth_getTransactionReceipt":
            return {"transactionHash": "0xtest"}
        if method == "eth_getLogs":
            return []
        if method == "eth_sendRawTransaction":
            return "0xtest"
        raise AssertionError(method)


def endpoints(chain: str = "1") -> list[RpcEndpoint]:
    return [
        RpcEndpoint(chain, "primary", "primary", priority=1, public_http_url="https://primary.example.com"),
        RpcEndpoint(chain, "backup", "backup", priority=2, public_http_url="https://backup.example.com"),
    ]


class VerifiedRpcTransportTests(unittest.TestCase):
    def test_wrong_chain_backup_never_serves_nonce_or_receipt(self) -> None:
        for method, params in (("eth_getTransactionCount", ["0xwallet", "pending"]),
                               ("eth_getTransactionReceipt", ["0xtest"]),
                               ("eth_getLogs", [{"fromBlock": "0x64", "toBlock": "0x64"}])):
            fixture = RpcFixture()
            fixture.chains["backup"] = 56
            fixture.fail_primary_method = True
            rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request)
            with self.assertRaisesRegex(RpcUnavailable, "rpc_wrong_chain") as error:
                rpc.call(method, params)
            self.assertNotIn("example.com", str(error.exception))
            self.assertNotIn(("backup", method), fixture.calls)

    def test_stale_or_conflicting_backup_is_rejected(self) -> None:
        for stale, conflict in ((True, False), (False, True)):
            fixture = RpcFixture()
            rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request, maximum_lag_blocks=2)
            self.assertEqual(rpc.call("eth_getTransactionCount", ["0xwallet", "pending"]), "0x7")
            fixture.fail_primary_method = True
            if stale:
                fixture.heights["backup"] = 90
            if conflict:
                fixture.hashes["backup"] = "fork-h100"
            with self.assertRaises(RpcUnavailable):
                rpc.call("eth_getTransactionCount", ["0xwallet", "pending"])
            self.assertNotIn(("backup", "eth_getTransactionCount"), fixture.calls)

    def test_matching_backup_can_serve_read_but_not_duplicate_ambiguous_send(self) -> None:
        fixture = RpcFixture()
        fixture.fail_primary_method = True
        rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request)
        self.assertEqual(rpc.call("eth_getTransactionReceipt", ["0xtest"]), {"transactionHash": "0xtest"})
        self.assertEqual(rpc.last_provider, "backup")
        fixture.calls.clear()
        with self.assertRaises(RpcUnavailable):
            rpc.call("eth_sendRawTransaction", ["0xserialized"])
        self.assertNotIn(("backup", "eth_sendRawTransaction"), fixture.calls)

    def test_pending_nonce_requires_a_prior_anchor_before_failover(self) -> None:
        fixture = RpcFixture()
        fixture.fail_primary_method = True
        rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request)
        with self.assertRaisesRegex(RpcUnavailable, "rpc_pending_nonce_unanchored_failover"):
            rpc.call("eth_getTransactionCount", ["0xwallet", "pending"])
        self.assertNotIn(("backup", "eth_getTransactionCount"), fixture.calls)
        fixture.fail_primary_method = False
        self.assertEqual(rpc.call("eth_getTransactionCount", ["0xwallet", "pending"]), "0x7")
        fixture.fail_primary_method = True
        self.assertEqual(rpc.call("eth_getTransactionCount", ["0xwallet", "pending"]), "0x7")
        self.assertEqual(rpc.last_provider, "backup")

    def test_regressed_pending_nonce_cannot_override_prior_observation(self) -> None:
        fixture = RpcFixture()
        rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request)
        fixture.nonces["primary"] = 9
        self.assertEqual(rpc.call("eth_getTransactionCount", ["0xwallet", "pending"]), "0x9")
        fixture.fail_primary_method = True
        with self.assertRaisesRegex(RpcUnavailable, "rpc_pending_nonce_regressed"):
            rpc.call("eth_getTransactionCount", ["0xwallet", "pending"])

    def test_consistent_view_never_switches_mid_receipt(self) -> None:
        fixture = RpcFixture()
        rpc = FailoverJsonRpc("1", endpoints(), requester=fixture.request)
        with rpc.consistent_view():
            rpc.call("eth_getTransactionReceipt", ["0xtest"])
            fixture.fail_primary_method = True
            with self.assertRaises(RpcUnavailable):
                rpc.call("eth_getTransactionReceipt", ["0xtest"])
        self.assertNotIn(("backup", "eth_getTransactionReceipt"), fixture.calls)

    def test_solana_wrong_genesis_is_rejected(self) -> None:
        def request(endpoint: RpcEndpoint, method: str, _params: Sequence[Any]) -> Any:
            if method == "getGenesisHash":
                return SOLANA_MAINNET_GENESIS if endpoint.provider == "primary" else "wrong-genesis"
            if method == "getSlot":
                return 100
            if method == "getBlock":
                return {"blockhash": "h100"}
            if endpoint.provider == "primary":
                raise OSError("offline")
            return "unexpected"

        rpc = FailoverJsonRpc("1399811149", endpoints("1399811149"), requester=request)
        with self.assertRaisesRegex(RpcUnavailable, "rpc_wrong_chain"):
            rpc.call("getSignatureStatuses", [["sig"]])

    def test_identity_cache_expiry_revalidates_endpoint(self) -> None:
        fixture = RpcFixture()
        rpc = FailoverJsonRpc("1", endpoints()[:1], requester=fixture.request, identity_ttl_seconds=0.1)
        self.assertEqual(rpc.call("eth_getTransactionReceipt", ["0xtest"]), {"transactionHash": "0xtest"})
        fixture.chains["primary"] = 56
        time.sleep(0.12)
        with self.assertRaisesRegex(RpcUnavailable, "rpc_wrong_chain"):
            rpc.call("eth_getTransactionReceipt", ["0xtest"])


if __name__ == "__main__":
    unittest.main()
