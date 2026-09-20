from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fomo.web.rpc_management import RpcManagementError, RpcManagementStore, _safe_url


class RpcManagementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.env_path = self.root / ".env"
        self.env_path.write_text("UNCHANGED=value\nRPC_TEST_URL=https://old.example/key\n", encoding="utf-8")
        self.cfg = {
            "rpc_pool": {
                "database": "rpc.sqlite3",
                "endpoints": {
                    "1": [
                        {
                            "provider": "test",
                            "role": "primary",
                            "http_env": "RPC_TEST_URL",
                            "ws_env": "RPC_TEST_WSS",
                            "priority": 10,
                        }
                    ]
                },
            }
        }
        self.store = RpcManagementStore(self.root, self.cfg, self.root / "audit.ndjson")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_snapshot_masks_url_and_never_exposes_key(self) -> None:
        secret = "https://rpc.example/v2/very-secret-key"
        with patch.dict(os.environ, {"RPC_TEST_URL": secret}, clear=False):
            snapshot = self.store.snapshot()
        self.assertNotIn("very-secret-key", repr(snapshot))
        self.assertEqual(snapshot["endpoints"][0]["maskedHttpUrl"], "https://rpc.example/…")
        self.assertFalse(snapshot["secretsExposed"])

    def test_save_updates_only_predeclared_environment_slot(self) -> None:
        public_dns = [(2, 1, 6, "", ("93.184.216.34", 443))]
        with (
            patch.dict(os.environ, {"RPC_TEST_URL": "https://old.example/key"}, clear=False),
            patch("fomo.execution.url_safety.socket.getaddrinfo", return_value=public_dns),
        ):
            result = self.store.mutate(
                {
                    "action": "save_endpoint",
                    "endpointId": "1:test:primary",
                    "httpUrl": "https://new.example/rpc-key",
                }
            )
            self.assertEqual(os.environ["RPC_TEST_URL"], "https://new.example/rpc-key")
        content = self.env_path.read_text(encoding="utf-8")
        self.assertIn("UNCHANGED=value", content)
        self.assertIn("RPC_TEST_URL=https://new.example/rpc-key", content)
        self.assertNotIn("rpc-key", repr(result))
        self.assertTrue((self.root / "audit.ndjson").exists())

    def test_invalid_or_credentialed_urls_are_rejected(self) -> None:
        for url in ("ftp://rpc.example", "https://user:password@rpc.example"):
            with self.subTest(url=url), self.assertRaises(RpcManagementError):
                self.store.mutate(
                    {
                        "action": "save_endpoint",
                        "endpointId": "1:test:primary",
                        "httpUrl": url,
                    }
                )

    def test_ssrf_and_env_injection_urls_are_rejected(self) -> None:
        for url in (
            "http://127.0.0.1:8545",
            "http://169.254.169.254/latest/meta-data",
            "https://rpc.example\nINJECTED=x",
        ):
            with self.subTest(url=url), self.assertRaises(RpcManagementError):
                self.store.mutate(
                    {
                        "action": "save_endpoint",
                        "endpointId": "1:test:primary",
                        "httpUrl": url,
                    }
                )

    def test_dns_failure_private_ipv6_and_mixed_resolution_are_rejected(self) -> None:
        with self.assertRaises(RpcManagementError):
            _safe_url("https://does-not-resolve.invalid/rpc")
        for value in ("http://[::1]:8545", "http://[fc00::1]:8545", "http://0.0.0.0:8545"):
            with self.subTest(value=value), self.assertRaises(RpcManagementError):
                _safe_url(value)
        mixed = [
            (2, 1, 6, "", ("93.184.216.34", 443)),
            (2, 1, 6, "", ("10.0.0.2", 443)),
        ]
        with (
            patch("fomo.execution.url_safety.socket.getaddrinfo", return_value=mixed),
            self.assertRaises(RpcManagementError),
        ):
            _safe_url("https://mixed.example/rpc")

    def test_public_endpoint_cannot_be_edited(self) -> None:
        cfg = {
            "rpc_pool": {
                "database": "public.sqlite3",
                "endpoints": {
                    "5042": [
                        {
                            "provider": "arc",
                            "role": "primary",
                            "public_http_url": "https://rpc.arc.example",
                        }
                    ]
                },
            }
        }
        store = RpcManagementStore(self.root, cfg, self.root / "audit.ndjson")
        with self.assertRaisesRegex(RpcManagementError, "不可修改"):
            store.mutate(
                {
                    "action": "save_endpoint",
                    "endpointId": "5042:arc:primary",
                    "httpUrl": "https://replacement.example",
                }
            )

    def test_single_probe_delegates_to_selected_endpoint(self) -> None:
        with (
            patch.dict(os.environ, {"RPC_TEST_URL": "https://rpc.example"}, clear=False),
            patch(
                "fomo.web.rpc_management.run_rpc_probe_endpoints",
                return_value={"reachable": 1, "healthy": 1, "endpoints": []},
            ) as probe,
        ):
            self.store.mutate({"action": "test_endpoint", "endpointId": "1:test:primary", "samples": 2})
        self.assertEqual(probe.call_args.args[2], {"1:test:primary"})
        self.assertEqual(probe.call_args.args[3], 2)

    def test_network_can_be_disabled_and_persists_runtime_setting(self) -> None:
        callback_calls: list[bool] = []
        store = RpcManagementStore(
            self.root, self.cfg, self.root / "audit.ndjson",
            on_network_change=lambda: callback_calls.append(True),
        )
        result = store.mutate({"action": "set_chain_enabled", "chainId": 1, "enabled": False})
        self.assertEqual(result["snapshot"]["enabledNetworks"], 0)
        self.assertFalse(result["snapshot"]["networks"][0]["enabled"])
        self.assertEqual(self.cfg["copy_trading"]["network_ids"], [])
        self.assertEqual(self.cfg["execution"]["enabled_chain_ids"], [])
        self.assertEqual(callback_calls, [True])
        settings = (self.root / "data" / "network-settings.json").read_text(encoding="utf-8")
        self.assertIn('"enabledChainIds": []', settings)

    def test_disabled_network_cannot_be_probed_manually(self) -> None:
        with patch.dict(os.environ, {"RPC_TEST_URL": "https://rpc.example"}, clear=False):
            self.store.mutate({"action": "set_chain_enabled", "chainId": 1, "enabled": False})
            with self.assertRaisesRegex(RpcManagementError, "已禁用"):
                self.store.mutate({"action": "test_endpoint", "endpointId": "1:test:primary"})

    def test_network_toggle_requires_boolean_value(self) -> None:
        with self.assertRaisesRegex(RpcManagementError, "开关值无效"):
            self.store.mutate({"action": "set_chain_enabled", "chainId": 1, "enabled": "false"})


if __name__ == "__main__":
    unittest.main()
