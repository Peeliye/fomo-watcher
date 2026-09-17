import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from fomo.intelligence.profile import WalletIntelligenceStore


def event(index: int, *, amount: float = 100, cap: float = 500_000, day: int = 1):
    return SimpleNamespace(
        id=f"event-{index}", kind="buy", user_id="kol-1", handle="alpha",
        created_at=f"2026-09-{day:02d}T00:00:00+00:00", network_id=1399811149,
        ca=f"token-{index}", symbol=f"T{index}", amount_usd=amount,
        market_cap=cap, price=0.1, source_type="single_user_buy", trade_id=f"trade-{index}",
    )


class WalletIntelligenceTests(unittest.TestCase):
    def test_deduplicates_trade_and_never_claims_verified_performance(self):
        with tempfile.TemporaryDirectory() as directory:
            store = WalletIntelligenceStore(Path(directory) / "intel.sqlite3")
            try:
                item = event(1)
                self.assertTrue(store.record_event(item))
                item.id = "different-feed-id"
                self.assertFalse(store.record_event(item))
                result = store.snapshot()
            finally:
                store.close()
        self.assertEqual(result["totalEvents"], 1)
        self.assertFalse(result["performanceVerified"])
        self.assertIn("pnl_unverified", result["profiles"][0]["riskFlags"])

    def test_behavior_sample_and_whale_style_are_separate_from_pnl(self):
        settings = {"minimum_buy_events": 3, "minimum_distinct_tokens": 3, "minimum_active_days": 3}
        with tempfile.TemporaryDirectory() as directory:
            store = WalletIntelligenceStore(Path(directory) / "intel.sqlite3", settings)
            try:
                for index in range(3):
                    store.record_event(event(index, amount=25_000, cap=5_000_000, day=index + 1))
                profile = store.snapshot()["profiles"][0]
            finally:
                store.close()
        self.assertEqual(profile["evidenceStatus"], "sufficient_behavior_sample")
        self.assertEqual(profile["primaryStyle"], "whale_momentum")
        self.assertEqual(profile["recommendedMode"], "observe_only")
        self.assertFalse(profile["performanceVerified"])

    def test_synthetic_handle_history_merges_into_unique_real_kol(self):
        with tempfile.TemporaryDirectory() as directory:
            store = WalletIntelligenceStore(Path(directory) / "intel.sqlite3")
            try:
                store.ingest_order_rows([{
                    "eventId": "old", "recordedAt": "2026-09-01T00:00:00Z", "handle": "Alpha",
                    "networkId": 1, "ca": "old-token", "symbol": "OLD", "sourceType": "buy",
                    "targetBuyUsd": 10,
                }])
                store.record_event(event(99))
                result = store.snapshot()
            finally:
                store.close()
        self.assertEqual(result["profiled"], 1)
        self.assertEqual(result["profiles"][0]["kolId"], "kol-1")
        self.assertEqual(result["profiles"][0]["kolAliases"], ["handle:Alpha", "kol-1"])


if __name__ == "__main__":
    unittest.main()
