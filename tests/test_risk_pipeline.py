import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fomo.app import Event
from fomo.risk.pipeline import RiskPipeline
from fomo.web.server import build_risk_payload


POLICY_PATH = Path(__file__).resolve().parents[1] / "risk-policy.example.json"


class RiskPipelineTests(unittest.TestCase):
    def _pipeline(self, directory: str, wallets: list[dict]) -> RiskPipeline:
        root = Path(directory)
        registry = root / "wallets.json"
        registry.write_text(json.dumps({"version": 3, "wallets": wallets}), encoding="utf-8")
        return RiskPipeline(root, {
            "policy_path": str(POLICY_PATH),
            "registry_path": str(registry),
            "log_path": "decisions.ndjson",
        })

    def test_unregistered_wallet_is_audited_as_needs_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = self._pipeline(directory, [])
            event = Event(
                id="feed:1", kind="buy", handle="alice", user_id="fomo-user-1",
                created_at=datetime.now(timezone.utc).isoformat(), network_id=1,
                symbol="MEME", ca="0x1111111111111111111111111111111111111111",
                amount_usd=250, source_type="swap_buy",
            )
            record = pipeline.evaluate_event(event)
            self.assertEqual(record["outcome"], "needs_identity")
            self.assertEqual(record["blockers"], ["wallet_not_registered"])
            self.assertIsNone(record["wallet"])
            self.assertTrue(record["readOnly"])

    def test_registered_wallet_produces_unified_signal_and_needs_data(self):
        with tempfile.TemporaryDirectory() as directory:
            now = datetime.now(timezone.utc)
            wallet = "0x2222222222222222222222222222222222222222"
            pipeline = self._pipeline(directory, [{
                "kolId": "fomo-user-2", "handle": "bob", "chainIds": ["1"],
                "address": wallet, "confidence": 0.95, "evidence": [],
                "verifiedAt": (now - timedelta(days=1)).isoformat(),
                "expiresAt": (now + timedelta(days=30)).isoformat(),
                "status": "shadow-only",
            }])
            event = Event(
                id="feed:2", kind="buy", handle="bob", user_id="fomo-user-2",
                created_at=now.isoformat(), network_id=1, symbol="MEME",
                ca="0x3333333333333333333333333333333333333333",
                amount_usd=500, source_type="swap_buy",
            )
            record = pipeline.evaluate_event(event)
            self.assertEqual(record["outcome"], "needs_data")
            self.assertEqual(record["signal"]["wallet"], wallet)
            self.assertEqual(record["signal"]["kolId"], "fomo-user-2")
            self.assertIn("asset_snapshot_required", record["blockers"])
            self.assertTrue(record["readOnly"])

    def test_dashboard_risk_payload_aggregates_outcomes_and_blockers(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "risk.ndjson"
            log_path.write_text(
                '{"recordedAt":"2026-09-10T01:00:00Z","outcome":"needs_identity","blockers":["wallet_not_registered"]}\n'
                '{"recordedAt":"2026-09-10T01:01:00Z","outcome":"needs_data","blockers":["asset_snapshot_required"]}\n',
                encoding="utf-8",
            )
            payload = build_risk_payload(log_path)
            self.assertEqual(payload["total"], 2)
            self.assertEqual(payload["outcomes"]["needs_identity"], 1)
            self.assertEqual(payload["blockers"]["asset_snapshot_required"], 1)
            self.assertEqual(payload["decisions"][0]["outcome"], "needs_data")


if __name__ == "__main__":
    unittest.main()
