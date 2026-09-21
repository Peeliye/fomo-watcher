import tempfile
import unittest
import sqlite3
from decimal import Decimal
from pathlib import Path

from fomo.intelligence.performance import VerifiedPerformanceStore
from fomo.intelligence.strategy import strategy_for_profile


class VerifiedPerformanceTests(unittest.TestCase):
    @staticmethod
    def fill(tx: str, side: str, quantity: float, gross: float, **overrides):
        row = {
            "txHash": tx, "kolId": "k1", "handle": "alpha", "wallet": "0xABC",
            "chainId": 1, "tokenAddress": "0xTOKEN", "symbol": "T", "side": side,
            "tokenQuantity": quantity, "grossUsd": gross,
            "executedAt": "2026-09-20T00:00:00Z", "sourceConfidence": 1,
            "historyComplete": True, "receiptVerified": True,
            "finality": "finalized", "indexerCheckpoint": "test-checkpoint",
        }
        row.update(overrides)
        return row

    def test_real_fifo_not_moving_average(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                rows = [
                    self.fill("b1", "buy", 1, 10, executedAt="2026-09-20T00:00:00Z"),
                    self.fill("b2", "buy", 1, 20, executedAt="2026-09-20T00:01:00Z"),
                    self.fill("s1", "sell", 1, 15, executedAt="2026-09-20T00:02:00Z"),
                ]
                self.assertEqual(store.ingest_fills(rows), 3)
                token = store.snapshot()["profiles"][0]["tokensDetail"][0]
            finally:
                store.close()
        self.assertEqual(token["realizedPnlUsd"], 5)
        self.assertEqual(token["remainingCostUsd"], 20)
        self.assertEqual(Decimal(token["remainingQuantity"]), Decimal("1"))

    def test_orphan_and_oversold_sells_never_become_verified_profit(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                store.ingest_fills([
                    self.fill("orphan", "sell", 1, 100, wallet="0x111", tokenAddress="0xA"),
                    self.fill("buy", "buy", 1, 10, wallet="0x222", tokenAddress="0xB"),
                    self.fill("oversold", "sell", 2, 30, wallet="0x222", tokenAddress="0xB",
                              executedAt="2026-09-20T00:01:00Z"),
                ])
                profile = store.snapshot()["profiles"][0]
                tokens = {item["tokenAddress"]: item for item in profile["tokensDetail"]}
            finally:
                store.close()
        self.assertEqual(tokens["0xa"]["realizedPnlUsd"], 0)
        self.assertEqual(tokens["0xa"]["inventoryStatus"], "orphan_sell")
        self.assertEqual(tokens["0xb"]["realizedPnlUsd"], 5)
        self.assertEqual(tokens["0xb"]["inventoryStatus"], "oversold")
        self.assertEqual(profile["closedTokens"], 0)
        self.assertIsNone(profile["roi"])
        self.assertFalse(profile["performanceVerified"])

    def test_wallets_are_independent_and_fees_flow_through_fifo(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                store.ingest_fills([
                    self.fill("w1b", "buy", 2, 20, feeUsd=2, wallet="0xAAA"),
                    self.fill("w2b", "buy", 1, 100, wallet="0xBBB"),
                    self.fill("w1s1", "sell", 1, 20, feeUsd=1, wallet="0xaaa",
                              executedAt="2026-09-20T00:01:00Z"),
                    self.fill("w1s2", "sell", 1, 30, feeUsd=1, wallet="0xaaa",
                              executedAt="2026-09-20T00:02:00Z"),
                ])
                profile = store.snapshot()["profiles"][0]
                by_wallet = {item["wallet"]: item for item in profile["tokensDetail"]}
            finally:
                store.close()
        self.assertEqual(set(by_wallet), {"0xaaa", "0xbbb"})
        self.assertEqual(by_wallet["0xaaa"]["realizedPnlUsd"], 26)
        self.assertEqual(Decimal(by_wallet["0xbbb"]["remainingQuantity"]), Decimal("1"))

    def test_case_normalization_and_duplicate_import_are_chain_aware(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                evm = self.fill("evm", "buy", 1, 10, wallet="0xAbC", tokenAddress="0xDeF")
                sol = self.fill("sol", "buy", 1, 10, wallet="SoLCase", tokenAddress="MintCase",
                                chainId=1399811149)
                self.assertEqual(store.ingest_fills([evm, sol]), 2)
                self.assertEqual(store.ingest_fills([evm, sol]), 0)
                details = store.snapshot()["profiles"][0]["tokensDetail"]
            finally:
                store.close()
        keys = {(item["chainId"], item["wallet"], item["tokenAddress"]) for item in details}
        self.assertIn((1, "0xabc", "0xdef"), keys)
        self.assertIn((1399811149, "SoLCase", "MintCase"), keys)

    def test_market_mark_is_independent_and_controls_freshness(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(
                Path(directory) / "verified.sqlite3", {"maximum_data_age_seconds": 999999999}
            )
            try:
                store.ingest_fills([self.fill("b", "buy", 1, 10, priceUsd=10)])
                missing = store.snapshot()["profiles"][0]
                store.ingest_market_history([{
                    "chainId": 1, "tokenAddress": "0xtoken", "priceUsd": 12,
                    "observedAt": "2026-09-20T01:00:00Z", "source": "independent-test",
                }])
                marked = store.snapshot()["profiles"][0]
            finally:
                store.close()
        self.assertIsNone(missing["unrealizedPnlUsd"])
        self.assertEqual(missing["markStatus"], "missing")
        self.assertEqual(marked["unrealizedPnlUsd"], 2)
        self.assertEqual(marked["lastMarketObservedAt"], "2026-09-20T01:00:00+00:00")
        self.assertEqual(marked["metrics"]["unrealizedPnl"]["source"], "independent_market_observations")

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
                         "executedAt": f"2026-01-0{i}T00:00:00Z", "sourceConfidence": 1,
                         "historyComplete": True, "receiptVerified": True, "finality": "finalized", "indexerCheckpoint": "test"},
                        {"txHash": f"s{i}", "kolId": "k1", "handle": "alpha", "wallet": "w", "chainId": 1,
                         "tokenAddress": token, "symbol": "T", "side": "sell", "tokenQuantity": 100,
                         "grossUsd": 100 + pnl, "priceUsd": (100 + pnl) / 100,
                         "executedAt": f"2026-01-0{i}T01:00:00Z", "sourceConfidence": 1,
                         "historyComplete": True, "receiptVerified": True, "finality": "finalized", "indexerCheckpoint": "test"},
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
                         "executedAt": "2020-01-01T00:00:00Z", "sourceConfidence": 1,
                         "historyComplete": True, "receiptVerified": True, "finality": "finalized", "indexerCheckpoint": "test"},
                        {"txHash": f"old-s{index}", "kolId": "k", "handle": "old", "wallet": "w", "chainId": 1,
                         "tokenAddress": f"t{index}", "side": "sell", "tokenQuantity": 1, "grossUsd": 2,
                         "executedAt": "2020-01-01T01:00:00Z", "sourceConfidence": 1,
                         "historyComplete": True, "receiptVerified": True, "finality": "finalized", "indexerCheckpoint": "test"},
                    ])
                store.ingest_fills(rows)
                result = store.snapshot()["profiles"][0]
            finally:
                store.close()
        self.assertTrue(result["historicallyVerified"])
        self.assertFalse(result["performanceVerified"])
        self.assertEqual(result["performanceStatus"], "stale_verified_history")

    def test_v2_migration_backs_up_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "verified.sqlite3"
            db = sqlite3.connect(path)
            db.executescript(
                """
                CREATE TABLE verified_fills (
                  fill_id TEXT PRIMARY KEY, tx_hash TEXT NOT NULL,
                  instruction_index INTEGER NOT NULL DEFAULT 0,
                  kol_id TEXT NOT NULL, handle TEXT NOT NULL, wallet TEXT NOT NULL,
                  chain_id INTEGER NOT NULL, token_address TEXT NOT NULL,
                  symbol TEXT NOT NULL, side TEXT NOT NULL,
                  token_quantity TEXT NOT NULL, gross_usd_micros INTEGER NOT NULL,
                  fee_usd_micros INTEGER NOT NULL DEFAULT 0, price_usd TEXT NOT NULL,
                  market_cap_usd_micros INTEGER, executed_at TEXT NOT NULL,
                  source TEXT NOT NULL, source_confidence TEXT NOT NULL,
                  raw_payload_hash TEXT NOT NULL
                );
                CREATE TABLE token_market_history (
                  chain_id INTEGER NOT NULL, token_address TEXT NOT NULL,
                  observed_at TEXT NOT NULL, price_usd TEXT NOT NULL,
                  market_cap_usd_micros INTEGER, liquidity_usd_micros INTEGER,
                  source TEXT NOT NULL,
                  PRIMARY KEY(chain_id, token_address, observed_at, source)
                );
                PRAGMA user_version=2;
                """
            )
            db.execute(
                "INSERT INTO verified_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "legacy-fill", "0xTX", 0, "legacy-kol", "legacy", "0xAbC", 1,
                    "0xDeF", "LEG", "buy", "1", 10_000_000, 0, "10", None,
                    "2026-09-20T00:00:00+00:00", "legacy", "1", "payload",
                ),
            )
            db.commit()
            db.close()

            store = VerifiedPerformanceStore(path)
            try:
                row = store.db.execute(
                    "SELECT wallet_key,token_key,history_complete FROM verified_fills"
                ).fetchone()
                self.assertEqual(tuple(row), ("0xabc", "0xdef", 0))
                self.assertEqual(store.db.execute("PRAGMA user_version").fetchone()[0], 5)
                self.assertEqual(store.db.execute("SELECT COUNT(*) FROM performance_profiles").fetchone()[0], 1)
                self.assertEqual(tuple(store.db.execute(
                    "SELECT fill_count,market_count,social_count FROM performance_stats"
                ).fetchone()), (1, 0, 0))
                market = {
                    "chainId": 1, "tokenAddress": "0xDeF", "observedAt": "2026-09-20T01:00:00Z",
                    "priceUsd": 11, "source": "migration-test",
                }
                social = {
                    "kolId": "legacy-kol", "platform": "fomo", "accountHandle": "legacy",
                    "evidenceReference": "audit:1", "confidence": 1,
                    "verifiedAt": "2026-09-20T01:00:00Z",
                }
                store.ingest_market_history([market, market])
                store.ingest_social_identities([social, social])
                counts = store.snapshot()
                self.assertEqual(
                    (counts["verifiedFills"], counts["marketObservations"], counts["socialIdentities"]),
                    (1, 1, 1),
                )
            finally:
                store.close()
            pattern = "verified.pre-v5.from-v2.*.sqlite3"
            self.assertEqual(len(list((Path(directory) / "backups").glob(pattern))), 1)

            reopened = VerifiedPerformanceStore(path)
            reopened.close()
            self.assertEqual(len(list((Path(directory) / "backups").glob("*.sqlite3"))), 1)

    def test_snapshot_and_latest_market_query_plans_use_materialized_indexes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = VerifiedPerformanceStore(Path(directory) / "verified.sqlite3")
            try:
                profile_plan = " ".join(
                    str(row[3]) for row in store.db.execute(
                        """EXPLAIN QUERY PLAN SELECT payload_json FROM performance_profiles
                        ORDER BY performance_verified DESC,total_pnl_usd_micros DESC,kol_id LIMIT 200"""
                    )
                )
                market_plan = " ".join(
                    str(row[3]) for row in store.db.execute(
                        """EXPLAIN QUERY PLAN SELECT * FROM token_market_history
                        WHERE chain_id=1 AND token_key='0xtoken'
                        ORDER BY observed_at DESC LIMIT 1"""
                    )
                )
                count_plan = " ".join(
                    str(row[3]) for row in store.db.execute(
                        "EXPLAIN QUERY PLAN SELECT fill_count,market_count,social_count "
                        "FROM performance_stats WHERE singleton=1"
                    )
                )
            finally:
                store.close()
        self.assertIn("idx_performance_profiles_rank", profile_plan)
        self.assertIn("idx_market_token_latest", market_plan)
        self.assertIn("INTEGER PRIMARY KEY", count_plan)
        self.assertNotIn("verified_fills", count_plan)


if __name__ == "__main__":
    unittest.main()
