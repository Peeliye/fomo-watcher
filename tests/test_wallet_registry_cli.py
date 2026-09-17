import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fomo.app import Event
from fomo.risk.engine import WalletRegistry
from fomo.risk.pipeline import RiskPipeline
from fomo.web.server import build_identity_payload
from scripts.wallet_registry_cli import register_wallet, revoke_wallet


class WalletRegistryCliTests(unittest.TestCase):
    def _entry(self, kol_id: str = "kol-1", address: str = "0x1111111111111111111111111111111111111111") -> dict:
        now = datetime.now(timezone.utc)
        return {
            "kolId": kol_id,
            "handle": "alice",
            "chainIds": ["1", "8453"],
            "address": address,
            "confidence": 0.95,
            "evidence": [{"type": "signed-message", "reference": "proof:example", "recordedAt": now.isoformat()}],
            "verifiedAt": now.isoformat(),
            "expiresAt": (now + timedelta(days=30)).isoformat(),
            "status": "shadow-only",
        }

    def test_register_is_validated_versioned_and_backed_up(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wallets.json"
            path.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
            register_wallet(path, self._entry())
            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(document["version"], 2)
            self.assertEqual(document["wallets"][0]["address"], "0x1111111111111111111111111111111111111111")
            self.assertTrue(path.with_suffix(".json.bak").exists())
            WalletRegistry.from_dict(document)

    def test_revoke_removes_wallet_from_fomo_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wallets.json"
            path.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
            entry = self._entry()
            register_wallet(path, entry)
            revoke_wallet(path, "kol-1", "1", entry["address"])
            registry = WalletRegistry.from_file(path)
            self.assertEqual(registry.wallets_for_kol("1", "kol-1"), ())
            self.assertEqual(len(registry.wallets_for_kol("1", "kol-1", include_revoked=True)), 1)

    def test_identity_payload_builds_pending_backlog(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "wallets.json"
            registry.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
            log = root / "risk.ndjson"
            log.write_text(
                '{"recordedAt":"2026-09-10T01:00:00Z","outcome":"needs_identity","kolId":"kol-2","networkId":1399811149,"handle":"bob","symbol":"ABC","blockers":["wallet_not_registered"]}\n'
                '{"recordedAt":"2026-09-10T01:01:00Z","outcome":"needs_identity","kolId":"kol-2","networkId":1399811149,"handle":"bob","symbol":"XYZ","blockers":["wallet_not_registered"]}\n',
                encoding="utf-8",
            )
            payload = build_identity_payload(registry, log)
            self.assertEqual(payload["pending"], 1)
            self.assertEqual(payload["backlog"][0]["events"], 2)
            self.assertEqual(payload["backlog"][0]["symbols"], ["ABC", "XYZ"])

    def test_running_pipeline_hot_reloads_a_valid_registry_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "wallets.json"
            registry.write_text('{"version":1,"wallets":[]}', encoding="utf-8")
            policy = Path(__file__).resolve().parents[1] / "risk-policy.example.json"
            pipeline = RiskPipeline(root, {
                "policy_path": str(policy), "registry_path": str(registry), "log_path": "risk.ndjson",
            })
            now = datetime.now(timezone.utc)
            first = Event(id="one", kind="buy", handle="alice", user_id="kol-1", created_at=now.isoformat(), network_id=1, ca="0x3333333333333333333333333333333333333333")
            self.assertEqual(pipeline.evaluate_event(first)["outcome"], "needs_identity")
            register_wallet(registry, self._entry())
            second = Event(id="two", kind="buy", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x3333333333333333333333333333333333333333")
            self.assertEqual(pipeline.evaluate_event(second)["outcome"], "needs_data")


if __name__ == "__main__":
    unittest.main()
