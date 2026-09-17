import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fomo.app import Event
from fomo.execution.readiness import execution_readiness
from fomo.portfolio.ledger import PortfolioLedger, portfolio_snapshot


class PortfolioLedgerTests(unittest.TestCase):
    def test_independent_market_marks_refresh_open_position(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC")
            try:
                item = Event(id="mark-buy", kind="buy", handle="alice", user_id="kol-1",
                             created_at=datetime.now(timezone.utc).isoformat(), network_id=1,
                             ca="0x1111111111111111111111111111111111111111", symbol="MEME", price=1)
                ledger.apply_event(item, {"status": "accepted", "paperBuyUsd": 10})
                self.assertEqual(ledger.update_market_marks([{"chainId": 1, "tokenAddress": item.ca,
                                                               "priceUsd": 2, "capturedAt": datetime.now(timezone.utc).isoformat()}]), 1)
                row = ledger.db.execute("SELECT last_price_usd FROM portfolio_positions").fetchone()
            finally:
                ledger.close()
        self.assertEqual(row[0], "2")

    def test_buy_sell_persists_purchase_time_and_exact_pnl(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio.sqlite3"
            ledger = PortfolioLedger(path, "Asia/Shanghai")
            bought_at = datetime.now(timezone.utc).isoformat()
            buy = Event(id="buy-1", kind="buy", handle="alice", user_id="kol-1", created_at=bought_at, network_id=1, ca="0x1111111111111111111111111111111111111111", symbol="MEME", price=2)
            result = ledger.apply_event(buy, {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(result["quantity"], "5")
            sell = Event(id="sell-1", kind="sell", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca=buy.ca, symbol="MEME", price=3)
            result = ledger.apply_event(sell, None)
            self.assertEqual(result["realizedPnlUsd"], 5)
            ledger.close()

            snapshot = portfolio_snapshot(path)
            self.assertEqual(snapshot["openPositions"], 0)
            self.assertEqual(snapshot["realizedPnlUsd"], 5)
            self.assertEqual(snapshot["positions"][0]["firstBoughtAt"], bought_at)
            self.assertEqual(snapshot["fills"][0]["side"], "sell")
            self.assertEqual(snapshot["daily"][0]["buyUsd"], 10)
            self.assertEqual(snapshot["daily"][0]["sellUsd"], 15)

    def test_duplicate_event_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC")
            event = Event(id="same", kind="buy", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x1111111111111111111111111111111111111111", price=2)
            ledger.apply_event(event, {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(ledger.apply_event(event, {"status": "accepted", "paperBuyUsd": 10})["status"], "duplicate")
            ledger.close()
            self.assertEqual(portfolio_snapshot(Path(directory) / "portfolio.sqlite3")["costBasisUsd"], 10)

    def test_summary_is_not_truncated_by_detail_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio.sqlite3"
            ledger = PortfolioLedger(path, "UTC")
            for index in range(3):
                event = Event(id=f"buy-{index}", kind="buy", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca=f"0x{index + 1:040x}", price=2)
                ledger.apply_event(event, {"status": "accepted", "paperBuyUsd": 10})
            ledger.close()
            snapshot = portfolio_snapshot(path, limit=1)
            self.assertEqual(snapshot["openPositions"], 3)
            self.assertEqual(snapshot["costBasisUsd"], 30)
            self.assertEqual(len(snapshot["positions"]), 1)

    def test_durable_pretrade_guard_enforces_kol_daily_and_open_position_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "Asia/Shanghai")
            first = Event(id="first", kind="buy", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x1111111111111111111111111111111111111111", price=1)
            second = Event(id="second", kind="buy", handle="bob", user_id="kol-2", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x2222222222222222222222222222222222222222", price=1)
            third = Event(id="third", kind="buy", handle="carol", user_id="kol-3", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x3333333333333333333333333333333333333333", price=1)
            ledger.apply_event(first, {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(ledger.pretrade_guard(first, 10, {"per_kol_daily_limit_usd": 15}), "per_kol_daily_limit_reached")
            ledger.apply_event(second, {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(ledger.pretrade_guard(third, 10, {"max_open_positions": 2}), "maximum_open_positions_reached")
            exposure = ledger.exposure_snapshot(first)
            self.assertEqual(exposure.open_positions, 2)
            self.assertTrue(exposure.has_open_position)
            ledger.close()

    def test_daily_backup_is_consistent_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = PortfolioLedger(root / "portfolio.sqlite3", "Asia/Shanghai")
            event = Event(id="backup-buy", kind="buy", handle="alice", user_id="kol-1", created_at=datetime.now(timezone.utc).isoformat(), network_id=1, ca="0x1111111111111111111111111111111111111111", price=2)
            ledger.apply_event(event, {"status": "accepted", "paperBuyUsd": 10})
            backup = ledger.maybe_daily_backup(root / "backups")
            self.assertIsNotNone(backup)
            self.assertTrue(backup.exists())
            self.assertIsNone(ledger.maybe_daily_backup(root / "backups"))
            self.assertEqual(portfolio_snapshot(backup)["costBasisUsd"], 10)
            ledger.close()

    def test_one_logical_wallet_reuses_one_evm_account(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "wallet.json"
            profile.write_text(json.dumps({
                "walletId": "primary", "mode": "disabled", "signer": {"backend": "disabled", "reference": None},
                "accounts": [
                    {"accountId": "evm", "family": "evm", "address": "0x1111111111111111111111111111111111111111", "chainIds": ["1", "8453"]},
                    {"accountId": "sol", "family": "solana", "address": "11111111111111111111111111111111", "chainIds": ["1399811149"]},
                ],
            }), encoding="utf-8")
            cfg = {"execution": {
                "wallet_profile": str(profile), "enabled_chain_ids": [1, 8453, 1399811149],
                "rpc": {"1": {"env": "TEST_RPC_ETH"}, "8453": {"env": "TEST_RPC_BASE"}, "1399811149": {"env": "TEST_RPC_SOL"}},
            }}
            with patch.dict("os.environ", {"TEST_RPC_ETH": "http://rpc", "TEST_RPC_BASE": "http://rpc", "TEST_RPC_SOL": "http://rpc"}, clear=False):
                result = execution_readiness(root, cfg)
            self.assertEqual(result["stage"], "routing")
            self.assertTrue(any(item.startswith("independent_route_adapters_required") for item in result["blockers"]))
            self.assertEqual(result["chains"][0]["address"], result["chains"][1]["address"])
            self.assertFalse(result["signerConfigured"])
            self.assertTrue(result["readOnly"])


if __name__ == "__main__":
    unittest.main()
