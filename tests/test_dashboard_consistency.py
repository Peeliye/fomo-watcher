from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import urllib.request
import urllib.error
from unittest.mock import patch
from pathlib import Path

from fomo.web.server import build_identity_payload, dashboard_requires_auth, start_dashboard


class DashboardConsistencyTests(unittest.TestCase):
    def test_reverse_proxy_configuration_never_inherits_loopback_auth_bypass(self):
        self.assertTrue(dashboard_requires_auth("127.0.0.1", {"trusted_proxies": ["127.0.0.1"]}))
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ", {"TEST_ADMIN": "admin", "TEST_CSRF": "csrf"}, clear=False
        ):
            server = start_dashboard(Path(directory), {"dashboard": {
                "enabled": True, "host": "127.0.0.1", "port": 0,
                "trusted_proxies": ["127.0.0.1"], "admin_token_env": "TEST_ADMIN", "csrf_token_env": "TEST_CSRF",
            }})
            self.assertIsNotNone(server)
            assert server is not None
            try:
                port = int(server.server_address[1])
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=5)
                self.assertEqual(caught.exception.code, 401)
            finally:
                server.shutdown()
                server.server_close()

    def test_every_object_api_response_has_comparable_instance_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = start_dashboard(root, {"dashboard": {"enabled": True, "host": "127.0.0.1", "port": 0}})
            self.assertIsNotNone(server)
            assert server is not None
            try:
                port = int(server.server_address[1])
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=5) as response:
                    payload = json.load(response)
                    instance_header = response.headers["X-Fomo-Service-Instance"]
                self.assertTrue(payload["serviceInstanceId"])
                self.assertEqual(payload["serviceInstanceId"], payload["startupId"])
                self.assertEqual(instance_header, payload["serviceInstanceId"])
                self.assertIn("generatedAt", payload)
                self.assertIsInstance(payload["dataRevision"], int)
            finally:
                server.shutdown()
                server.server_close()

    def test_identity_units_are_separate_and_sourced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "wallet-registry.json"
            registry.write_text('{"version":3,"wallets":[]}', encoding="utf-8")
            risk = root / "risk.ndjson"
            risk.write_text(
                '{"recordedAt":"2026-09-21T00:00:00Z","outcome":"needs_identity","kolId":"k1","networkId":1}\n'
                '{"recordedAt":"2026-09-21T00:01:00Z","outcome":"needs_identity","kolId":"k1","networkId":56}\n'
                '{"recordedAt":"2026-09-21T00:02:00Z","outcome":"needs_identity","kolId":"k2","networkId":1}\n',
                encoding="utf-8",
            )
            following = root / "following.json"
            following.write_text('{"followingIds":["k1","k2","k3"]}', encoding="utf-8")
            intelligence = root / "intelligence.sqlite3"
            db = sqlite3.connect(intelligence)
            try:
                db.execute("CREATE TABLE intelligence_events(kol_id TEXT)")
                db.executemany("INSERT INTO intelligence_events VALUES(?)", [("k1",), ("k1",), ("k2",)])
                db.commit()
            finally:
                db.close()
            payload = build_identity_payload(registry, risk, following_path=following, intelligence_database=intelligence)
        self.assertEqual(payload["currentFollowedUniqueKols"], 3)
        self.assertEqual(payload["observedUniqueKols"], 2)
        self.assertEqual(payload["materializedProfiles"], 2)
        self.assertEqual(payload["pendingUniqueKols"], 2)
        self.assertEqual(payload["pendingKolChainPairs"], 3)

    def test_31_followed_kols_are_not_relabelled_as_observed_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "wallet-registry.json"
            registry.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
            risk = root / "risk.ndjson"
            risk.write_text("", encoding="utf-8")
            following = root / "following.json"
            following.write_text(json.dumps({"followingIds": [f"follow-{i}" for i in range(31)]}), encoding="utf-8")
            intelligence = root / "intelligence.sqlite3"
            db = sqlite3.connect(intelligence)
            try:
                db.execute("CREATE TABLE intelligence_events(kol_id TEXT,side TEXT,token_address TEXT,event_time TEXT)")
                db.executemany(
                    "INSERT INTO intelligence_events VALUES(?,?,?,?)",
                    [(f"observed-{i}", "buy", f"token-{i}", "2026-09-21T00:00:00Z") for i in range(110)],
                )
                db.commit()
            finally:
                db.close()
            payload = build_identity_payload(registry, risk, following_path=following,
                                             intelligence_database=intelligence)
        self.assertEqual(payload["currentFollowedUniqueKols"], 31)
        self.assertEqual(payload["observedUniqueKols"], 110)
        self.assertEqual(payload["materializedProfiles"], 110)


if __name__ == "__main__":
    unittest.main()
