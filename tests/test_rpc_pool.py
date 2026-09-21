import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from fomo.execution.rpc_pool import (
    RpcEndpoint,
    RpcHealthStore,
    load_rpc_endpoints,
    probe_rpc_endpoint,
    rpc_pool_readiness,
)


CFG = {
    "rpc_pool": {
        "sample_window": 20,
        "maximum_p95_latency_ms": 500,
        "maximum_block_lag": 2,
        "endpoints": {
            "1": [
                {"provider": "fast", "role": "primary", "http_env": "RPC_FAST", "priority": 10},
                {"provider": "slow", "role": "backup", "http_env": "RPC_SLOW", "priority": 20},
            ]
        },
    }
}


class RpcPoolTests(unittest.TestCase):
    def test_public_readiness_never_exposes_url(self):
        secret_url = "https://secret.example/key-value"
        with patch.dict(os.environ, {"RPC_FAST": secret_url}, clear=False):
            result = rpc_pool_readiness(CFG)
        self.assertEqual(result["configured"], 1)
        self.assertNotIn(secret_url, repr(result))

    def test_public_rpc_url_is_configured_but_not_exposed(self):
        url = "https://rpc.mainnet.arc.io"
        cfg = {"rpc_pool": {"endpoints": {"5042": [{
            "provider": "arc_official", "role": "primary", "public_http_url": url,
        }]}}}
        endpoints = load_rpc_endpoints(cfg)
        result = rpc_pool_readiness(cfg)
        self.assertEqual(endpoints[0].resolved_http_url, url)
        self.assertEqual(result["configured"], 1)
        self.assertNotIn(url, repr(result))

    def test_health_selects_fast_healthy_endpoint(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"RPC_FAST": "https://fast.invalid", "RPC_SLOW": "https://slow.invalid"}, clear=False
        ):
            endpoints = load_rpc_endpoints(CFG)
            store = RpcHealthStore(Path(directory) / "health.sqlite3")
            try:
                for latency in (40, 45, 50):
                    store.record(endpoints[0], method="eth_blockNumber", latency_ms=latency, success=True, block_height=100)
                for latency in (180, 190, 210):
                    store.record(endpoints[1], method="eth_blockNumber", latency_ms=latency, success=True, block_height=100)
                result = store.snapshot(CFG)
            finally:
                store.close()
        self.assertEqual(result["healthy"], 2)
        self.assertEqual(result["reachable"], 2)
        self.assertEqual(result["selected"]["1"], "1:fast:primary")

    def test_reachable_slow_endpoint_is_not_marked_healthy(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"RPC_FAST": "https://slow-but-live.invalid", "RPC_SLOW": ""}, clear=False
        ):
            endpoint = load_rpc_endpoints(CFG)[0]
            store = RpcHealthStore(Path(directory) / "health.sqlite3")
            try:
                for latency in (700, 800, 900):
                    store.record(endpoint, method="eth_blockNumber", latency_ms=latency, success=True, block_height=100)
                result = store.snapshot(CFG)
            finally:
                store.close()
        self.assertEqual(result["reachable"], 1)
        self.assertEqual(result["healthy"], 0)
        self.assertEqual(result["endpoints"][0]["status"], "slow")

    def test_historical_samples_never_make_unconfigured_endpoint_reachable(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"RPC_FAST": "", "RPC_SLOW": ""}, clear=False):
            endpoint = load_rpc_endpoints(CFG)[0]
            store = RpcHealthStore(Path(directory) / "health.sqlite3")
            try:
                store.record(endpoint, method="eth_blockNumber", latency_ms=40, success=True, block_height=100)
                result = store.snapshot(CFG)
            finally:
                store.close()
        self.assertEqual(result["reachable"], 0)
        self.assertEqual(result["endpoints"][0]["status"], "unconfigured")

    def test_stale_telemetry_fails_closed(self):
        cfg = {"rpc_pool": {**CFG["rpc_pool"], "maximum_sample_age_seconds": 60}}
        old = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"RPC_FAST": "https://fast.invalid"}, clear=False):
            endpoint = load_rpc_endpoints(cfg)[0]
            store = RpcHealthStore(Path(directory) / "health.sqlite3")
            try:
                for _ in range(3):
                    store.record(endpoint, method="eth_blockNumber", latency_ms=40, success=True, block_height=100, sampled_at=old)
                result = store.snapshot(cfg)
            finally:
                store.close()
        self.assertEqual(result["reachable"], 0)
        self.assertEqual(result["endpoints"][0]["status"], "stale")

    def test_retention_limit_applies_across_short_lived_store_instances(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"RPC_FAST": "https://fast.invalid"}, clear=False
        ):
            path = Path(directory) / "health.sqlite3"
            endpoint = load_rpc_endpoints(CFG)[0]
            for cycle in range(3):
                store = RpcHealthStore(path, max_samples_per_endpoint=100)
                try:
                    store.record_many([
                        (endpoint, {
                            "method": "eth_blockNumber", "latency_ms": 10 + cycle,
                            "success": True, "block_height": cycle * 100 + index,
                        })
                        for index in range(60)
                    ])
                finally:
                    store.close()
            check = RpcHealthStore(path, max_samples_per_endpoint=100)
            try:
                count = check.connection.execute(
                    "SELECT COUNT(*) FROM rpc_samples WHERE endpoint_id=?", (endpoint.endpoint_id,)
                ).fetchone()[0]
            finally:
                check.close()
            self.assertEqual(count, 100)

    def test_probe_rejects_dns_rebinding_before_network_request(self):
        endpoint = RpcEndpoint("1", "test", "primary", public_http_url="https://rpc.example/key")
        with (
            patch(
                "fomo.watching.rpc_transport.validate_endpoint_url",
                side_effect=[
                    (endpoint.resolved_http_url, frozenset({"93.184.216.34"})),
                    (endpoint.resolved_http_url, frozenset({"93.184.216.35"})),
                ],
            ),
            patch("fomo.watching.rpc_transport.cf.post") as post,
        ):
            result = probe_rpc_endpoint(endpoint)
        self.assertEqual(result["error_code"], "rpc_dns_rebinding_rejected")
        post.assert_not_called()

    def test_probe_rejects_redirect_without_following_it(self):
        endpoint = RpcEndpoint("1", "test", "primary", public_http_url="https://rpc.example/key")
        response = Mock(status_code=302, primary_ip="93.184.216.34")
        with (
            patch(
                "fomo.watching.rpc_transport.validate_endpoint_url",
                return_value=(endpoint.resolved_http_url, frozenset({"93.184.216.34"})),
            ),
            patch("fomo.watching.rpc_transport.cf.post", return_value=response) as post,
        ):
            result = probe_rpc_endpoint(endpoint)
        self.assertEqual(result["error_code"], "rpc_redirect_rejected")
        self.assertFalse(post.call_args.kwargs["allow_redirects"])
        self.assertEqual(post.call_args.kwargs["proxy"], "")

    def test_probe_prefers_ipv4_when_dns_returns_ipv4_and_ipv6(self):
        endpoint = RpcEndpoint("1", "test", "primary", public_http_url="https://rpc.example/key")
        addresses = frozenset({"2606:4700:10::1", "93.184.216.34"})
        def response_for_method(_url, **kwargs):
            method = kwargs["json"]["method"]
            value = {"eth_chainId": "0x1", "eth_blockNumber": "0x10",
                     "eth_getBlockByNumber": {"hash": "h16"}}[method]
            response = Mock(status_code=200, primary_ip="93.184.216.34")
            response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": value}
            return response
        with (
            patch(
                "fomo.watching.rpc_transport.validate_endpoint_url",
                return_value=(endpoint.resolved_http_url, addresses),
            ),
            patch("fomo.watching.rpc_transport.cf.post", side_effect=response_for_method) as post,
        ):
            result = probe_rpc_endpoint(endpoint)
        self.assertTrue(result["success"])
        resolve_entries = next(
            value for key, value in post.call_args.kwargs["curl_options"].items()
            if getattr(key, "name", "") == "RESOLVE" or int(key) == 10203
        )
        self.assertEqual(resolve_entries, ["rpc.example:443:93.184.216.34"])


if __name__ == "__main__":
    unittest.main()
