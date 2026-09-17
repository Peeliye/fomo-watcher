import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fomo.execution.rpc_pool import RpcHealthStore, load_rpc_endpoints, rpc_pool_readiness


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


if __name__ == "__main__":
    unittest.main()
