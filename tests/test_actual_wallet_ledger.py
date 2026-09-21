from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path

from fomo.portfolio.ledger import PortfolioLedger


class ActualWalletLedgerTests(unittest.TestCase):
    def test_native_gas_reserve_requires_fresh_sourced_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC", "live")
            event = SimpleNamespace(user_id="k", handle="k", network_id=1, ca="TOKEN")
            self.assertEqual(str(ledger.exposure_snapshot(event).native_gas_reserve_usd), "-1")
            ledger.record_native_balance_snapshot(1, "42.5", datetime.now(timezone.utc).isoformat(), "rpc-primary")
            self.assertEqual(str(ledger.exposure_snapshot(event).native_gas_reserve_usd), "42.5")
            ledger.record_native_balance_snapshot(
                1, "99", (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(), "rpc-stale"
            )
            self.assertEqual(str(ledger.exposure_snapshot(event).native_gas_reserve_usd), "42.5")
            ledger.close()

    def test_partial_sell_and_reorg_restore_fact_and_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC", "live")
            buy = ledger.apply_chain_receipt({
                "txHash": "0xbuy", "chainId": 1, "accountId": "live", "tokenAddress": "TOKEN",
                "tokenDelta": "10", "nativeDelta": "-0.01", "gasFeeUsd": "1", "dexFeeUsd": "0.2",
                "grossUsd": "100", "finality": "finalized", "status": "confirmed",
                "source": "wallet_rpc_evm", "allocationKey": "wallet:1:actor",
            })
            sell = ledger.apply_chain_receipt({
                "txHash": "0xsell", "chainId": 1, "accountId": "live", "tokenAddress": "TOKEN",
                "tokenDelta": "-4", "nativeDelta": "0.02", "gasFeeUsd": "1", "dexFeeUsd": "0.2",
                "grossUsd": "60", "finality": "finalized", "status": "confirmed",
                "source": "wallet_rpc_evm", "allocationKey": "wallet:1:actor",
            })
            position = ledger.db.execute("SELECT quantity FROM actual_wallet_positions").fetchone()[0]
            lot = ledger.db.execute("SELECT remaining_quantity FROM strategy_allocation_lots").fetchone()[0]
            self.assertEqual((position, lot), ("6", "6"))
            ledger.reverse_chain_receipt(sell["receiptId"])
            position = ledger.db.execute("SELECT quantity FROM actual_wallet_positions").fetchone()[0]
            lot = ledger.db.execute("SELECT remaining_quantity FROM strategy_allocation_lots").fetchone()[0]
            self.assertEqual((position, lot), ("10", "10"))
            self.assertFalse(buy["duplicate"])
            ledger.close()

    def test_failed_receipt_never_creates_position_or_fill(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC", "live")
            result = ledger.apply_chain_receipt({
                "txHash": "0xfail", "chainId": 1, "accountId": "live", "tokenAddress": "TOKEN",
                "tokenDelta": "10", "finality": "confirmed", "status": "failed",
            })
            self.assertEqual(result["status"], "failed")
            self.assertEqual(ledger.db.execute("SELECT COUNT(*) FROM actual_wallet_positions").fetchone()[0], 0)
            ledger.close()


if __name__ == "__main__":
    unittest.main()
