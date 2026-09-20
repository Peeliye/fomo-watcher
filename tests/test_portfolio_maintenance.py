from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from fomo.portfolio.ledger import PortfolioLedger
from scripts.portfolio_maintenance import reconcile_legacy_events, reconciliation_report


class PortfolioMaintenanceTests(unittest.TestCase):
    def test_reconcile_is_dry_run_by_default_and_apply_is_backed_up_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio.sqlite3"
            ledger = PortfolioLedger(path, "Asia/Shanghai")
            try:
                with ledger._transaction():
                    ledger.db.execute(
                        """INSERT INTO portfolio_events
                        (event_id,received_at,event_time,kol_id,handle,chain_id,token_address,symbol,kind,status,reason)
                        VALUES('legacy','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',
                        'k','alpha',1,'0xabc','T','buy','filled','buy')"""
                    )
            finally:
                ledger.close()

            dry_run = reconcile_legacy_events(path, "paper-main")
            db = sqlite3.connect(path)
            try:
                self.assertEqual(db.execute("SELECT status FROM portfolio_events").fetchone()[0], "filled")
            finally:
                db.close()
            self.assertEqual(dry_run["mode"], "dry-run")
            self.assertEqual(dry_run["planned"], 1)

            applied = reconcile_legacy_events(path, "paper-main", apply=True)
            self.assertEqual(applied["changed"], 1)
            self.assertTrue(Path(str(applied["backup"])).exists())
            db = sqlite3.connect(path)
            try:
                self.assertEqual(
                    db.execute("SELECT status,reason FROM portfolio_events").fetchone(),
                    ("legacy_observed", "missing_legacy_fill"),
                )
            finally:
                db.close()
            self.assertEqual(reconcile_legacy_events(path, "paper-main", apply=True)["changed"], 0)

    def test_reconciliation_detects_orphan_fill_and_position_daily_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio.sqlite3"
            ledger = PortfolioLedger(path, "Asia/Shanghai")
            ledger.close()
            db = sqlite3.connect(path)
            try:
                db.execute("PRAGMA foreign_keys=OFF")
                db.execute(
                    """INSERT INTO portfolio_fills VALUES(
                    'orphan-fill','missing-event','paper-main','k','alpha',1,'0xabc','T','buy',
                    '1','10',10000000,10000000,0,0,'2026-01-01T00:00:00+00:00','paper')"""
                )
                db.commit()
            finally:
                db.close()
            report = reconciliation_report(path, "paper-main")
        self.assertEqual(report["fillsWithoutEvent"], 1)
        self.assertGreaterEqual(report["positionMismatches"], 1)
        self.assertFalse(report["ok"])

    def test_accepted_paper_buy_requires_fill_or_explicit_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "portfolio.sqlite3"
            orders = root / "paper-orders.ndjson"
            ledger = PortfolioLedger(path, "Asia/Shanghai")
            ledger.close()
            orders.write_text(
                '{"eventId":"missing","status":"accepted"}\n'
                '{"eventId":"failed","status":"accepted"}\n',
                encoding="utf-8",
            )
            db = sqlite3.connect(path)
            try:
                db.execute(
                    """INSERT INTO portfolio_events
                    (event_id,received_at,event_time,kol_id,handle,chain_id,token_address,symbol,kind,status,reason)
                    VALUES('failed','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',
                    'k','alpha',1,'0xabc','T','buy','needs_price','missing_execution_price')"""
                )
                db.commit()
            finally:
                db.close()

            report = reconciliation_report(path, "paper-main", paper_orders_path=orders)
        self.assertEqual(report["acceptedPaperBuysChecked"], 2)
        self.assertEqual(
            report["acceptedPaperBuysWithoutFillOrFailure"],
            [{"eventId": "missing", "reason": "missing_portfolio_event"}],
        )
        self.assertFalse(report["ok"])


if __name__ == "__main__":
    unittest.main()
