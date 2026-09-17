import tempfile
import unittest
from pathlib import Path

from fomo.intelligence.performance import VerifiedPerformanceStore
from fomo.intelligence.strategy import strategy_for_profile


class VerifiedPerformanceTests(unittest.TestCase):
    def test_fifo_pnl_lifecycle_and_verified_win_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3", {"maximum_data_age_seconds": 999999999})
            try:
                fills = []
                marks = []
                for i, pnl in enumerate((50, 20, -10), 1):
                    token = f"token-{i}"
                    fills.extend([
                        {"txHash": f"b{i}", "kolId": "k1", "handle": "alpha", "wallet": "w", "chainId": 1,
                         "tokenAddress": token, "symbol": "T", "side": "buy", "tokenQuantity": 100,
                         "grossUsd": 100, "priceUsd": 1, "marketCapUsd": 100_000,
                         "executedAt": f"2026-01-0{i}T00:00:00Z", "sourceConfidence": 1},
                        {"txHash": f"s{i}", "kolId": "k1", "handle": "alpha", "wallet": "w", "chainId": 1,
                         "tokenAddress": token, "symbol": "T", "side": "sell", "tokenQuantity": 100,
                         "grossUsd": 100 + pnl, "priceUsd": (100 + pnl) / 100,
                         "executedAt": f"2026-01-0{i}T01:00:00Z", "sourceConfidence": 1},
                    ])
                    marks.extend([
                        {"chainId": 1, "tokenAddress": token, "observedAt": f"2026-01-0{i}T00:00:00Z", "priceUsd": 1, "marketCapUsd": 100_000},
                        {"chainId": 1, "tokenAddress": token, "observedAt": f"2026-01-0{i}T00:30:00Z", "priceUsd": 5, "marketCapUsd": 500_000},
                    ])
                self.assertEqual(store.ingest_fills(fills), 6)
                self.assertEqual(store.ingest_market_history(marks), 6)
                result = store.snapshot()["profiles"][0]
            finally:
                store.close()
        self.assertTrue(result["performanceVerified"])
        self.assertAlmostEqual(result["winRate"], 2 / 3)
        self.assertAlmostEqual(result["realizedPnlUsd"], 60)
        self.assertAlmostEqual(result["roi"], 0.2)
        self.assertAlmostEqual(result["maxDrawdownUsd"], 10)
        self.assertEqual(len(result["winRateConfidence95"]), 2)
        self.assertTrue(result["dataFresh"])
        self.assertTrue(all(row["earlyEntry"] for row in result["tokensDetail"]))

    def test_unproven_rows_are_rejected_and_strategy_stays_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                inserted = store.ingest_fills([{"txHash": "x", "side": "buy", "tokenQuantity": 1, "grossUsd": 10, "sourceConfidence": 0.5}])
            finally:
                store.close()
        self.assertEqual(inserted, 0)
        decision = strategy_for_profile({"primaryStyle": "early_alpha", "performanceVerified": False}, {"marketCapUsd": 500_000, "kolBuyUsd": 10_000})
        self.assertEqual(decision["action"], "observe")
        self.assertTrue(decision["readOnly"])

    def test_historically_verified_data_is_not_live_verified_when_stale(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3", {"maximum_data_age_seconds": 1})
            try:
                rows = []
                for index in range(3):
                    rows.extend([
                        {"txHash": f"old-b{index}", "kolId": "k", "handle": "old", "wallet": "w", "chainId": 1,
                         "tokenAddress": f"t{index}", "side": "buy", "tokenQuantity": 1, "grossUsd": 1,
                         "executedAt": "2020-01-01T00:00:00Z", "sourceConfidence": 1},
                        {"txHash": f"old-s{index}", "kolId": "k", "handle": "old", "wallet": "w", "chainId": 1,
                         "tokenAddress": f"t{index}", "side": "sell", "tokenQuantity": 1, "grossUsd": 2,
                         "executedAt": "2020-01-01T01:00:00Z", "sourceConfidence": 1},
                    ])
                store.ingest_fills(rows)
                result = store.snapshot()["profiles"][0]
            finally:
                store.close()
        self.assertTrue(result["historicallyVerified"])
        self.assertFalse(result["performanceVerified"])
        self.assertEqual(result["performanceStatus"], "stale_verified_history")


if __name__ == "__main__":
    unittest.main()
