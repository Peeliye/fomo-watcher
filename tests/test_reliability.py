from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fomo.app import (
    Event,
    NotificationWorker,
    State,
    process_pending_events,
    sidecar_events,
)
from fomo.audit import append_ndjson
from fomo.portfolio.ledger import PortfolioLedger, portfolio_snapshot
from fomo.web.server import DashboardChangeBus, build_dashboard_payload


def event(event_id: str = "evt-1", price: float = 1) -> Event:
    return Event(
        id=event_id,
        kind="buy",
        handle="alice",
        user_id="kol-1",
        created_at=datetime.now(timezone.utc).isoformat(),
        symbol="MEME",
        ca="0x1111111111111111111111111111111111111111",
        network_id=1,
        amount_usd=500,
        market_cap=1_000_000,
        price=price,
        source_type="swap_buy",
    )


def config(root: Path) -> dict:
    return {
        "timezone": "UTC",
        "notifications": {},
        "copy_trading": {
            "enabled": True,
            "mode": "paper",
            "network_ids": [1],
            "event_types": ["swap_buy"],
            "fixed_usd": 10,
            "min_target_buy_usd": 100,
            "max_signal_age_seconds": 30,
            "min_market_cap_usd": 100_000,
            "per_token_limit_usd": 100,
            "per_kol_daily_limit_usd": 100,
            "daily_limit_usd": 100,
            "max_open_positions": 10,
            "log_path": str(root / "orders.ndjson"),
        },
    }


class ReliabilityTests(unittest.TestCase):
    def test_incomplete_sidecar_tail_does_not_advance_and_later_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stream = root / "events.ndjson"
            state = State(str(root / "state.sqlite3"))
            payload = json.dumps(
                {
                    "payload": {
                        "id": "a",
                        "userId": "kol-1",
                        "type": "swap_buy",
                        "networkId": 1,
                        "tokenAddress": event().ca,
                        "price": 1,
                        "usdAmount": 500,
                        "marketCap": 1_000_000,
                        "body": {"userHandle": "alice"},
                    }
                }
            )
            cut = len(payload) // 2
            stream.write_text(payload[:cut], encoding="utf-8")
            self.assertEqual(sidecar_events(state, {"kol-1"}, str(stream)), [])
            self.assertEqual(state.load("sidecar_offset", 0), 0)
            with stream.open("a", encoding="utf-8") as output:
                output.write(payload[cut:] + "\n")
            parsed = sidecar_events(state, {"kol-1"}, str(stream))
            self.assertEqual(len(parsed), 1)
            self.assertEqual(state.load("sidecar_offset", 0), 0)
            process_pending_events(
                state, {"timezone": "UTC", "notifications": {}, "copy_trading": {}}, None, None, None, None
            )
            self.assertEqual(state.load("sidecar_offset", 0), stream.stat().st_size)
            state.db.close()

    def test_restart_recovers_inbox_without_duplicate_fill_or_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.sqlite3"
            ledger_path = root / "portfolio.sqlite3"
            first = State(str(state_path))
            self.assertTrue(first.enqueue_event(event(), source="test"))
            crashed_ledger = PortfolioLedger(ledger_path, "UTC")
            crashed_ledger.apply_event(event(), {"status": "accepted", "paperBuyUsd": 10})
            crashed_ledger.close()
            first.db.close()
            state = State(str(state_path))
            ledger = PortfolioLedger(ledger_path, "UTC")
            process_pending_events(state, config(root), None, ledger, None, None)
            self.assertFalse(state.enqueue_event(event(), source="duplicate"))
            process_pending_events(state, config(root), None, ledger, None, None)
            self.assertEqual(ledger.db.execute("SELECT COUNT(*) FROM portfolio_fills").fetchone()[0], 1)
            self.assertEqual(state.db.execute("SELECT COUNT(*) FROM notification_outbox").fetchone()[0], 1)
            ledger.close()
            state.db.close()

    def test_one_failed_event_does_not_block_later_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            state.enqueue_event(event("bad"), source="test")
            state.enqueue_event(event("good"), source="test")

            def fake_process(_state, item, *_args):
                if item.id == "bad":
                    raise RuntimeError("injected")

            with patch("fomo.app.process_event", side_effect=fake_process):
                process_pending_events(state, {"notifications": {}}, None, None, None, None)
            statuses = dict(state.db.execute("SELECT event_id,status FROM event_inbox"))
            self.assertEqual(statuses, {"bad": "retry", "good": "done"})
            state.db.close()

    def test_channel_failure_does_not_resend_successful_channel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(str(root / "state.sqlite3"))
            cfg = {
                "timezone": "UTC",
                "notifications": {"telegram": True, "feishu": True, "max_retries": 3, "retry_base_seconds": 0.01},
            }
            state.enqueue_notifications(event(), cfg)
            worker = NotificationWorker(state.path, cfg)
            calls: list[str] = []

            def deliver(channel, *_args):
                calls.append(channel)
                if channel == "feishu":
                    raise TimeoutError()

            db = sqlite3.connect(state.path)
            db.row_factory = sqlite3.Row
            with patch("fomo.app.notify_channel", side_effect=deliver):
                worker._drain(db)
            statuses = dict(db.execute("SELECT channel,status FROM notification_outbox"))
            self.assertEqual(statuses, {"telegram": "sent", "feishu": "retry"})
            db.execute("UPDATE notification_outbox SET next_attempt_at=0")
            db.commit()
            with patch("fomo.app.notify_channel", side_effect=deliver):
                worker._drain(db)
            self.assertEqual(calls.count("telegram"), 1)
            db.close()
            state.db.close()

    def test_portfolio_failure_rolls_back_and_connection_remains_usable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC")
            with patch.object(ledger, "_daily_delta", side_effect=sqlite3.OperationalError("injected")):
                with self.assertRaises(sqlite3.OperationalError):
                    ledger.apply_event(event(), {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(ledger.db.execute("SELECT COUNT(*) FROM portfolio_events").fetchone()[0], 0)
            result = ledger.apply_event(event(), {"status": "accepted", "paperBuyUsd": 10})
            self.assertEqual(result["status"], "filled")
            ledger.close()

    def test_concurrent_buy_and_exit_never_restore_stale_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = PortfolioLedger(Path(directory) / "portfolio.sqlite3", "UTC")
            try:
                ledger.apply_event(event("initial"), {"status": "accepted", "paperBuyUsd": 10})
                ledger.update_market_marks([{"chainId": 1, "tokenAddress": event().ca, "priceUsd": 0.7}])
                barrier = threading.Barrier(3)
                errors = []

                def buy():
                    try:
                        barrier.wait()
                        ledger.apply_event(event("concurrent", 0.7), {"status": "accepted", "paperBuyUsd": 10})
                    except Exception as exc:
                        errors.append(exc)

                def exit_rule():
                    try:
                        barrier.wait()
                        ledger.evaluate_exit_rules(
                            {
                                "enabled": True,
                                "stopLossPct": 25,
                                "principalRecoveryMultiple": 2,
                                "trailingStopPct": 25,
                                "maxHoldingHours": 168,
                            }
                        )
                    except Exception as exc:
                        errors.append(exc)

                threads = [threading.Thread(target=buy), threading.Thread(target=exit_rule)]
                [thread.start() for thread in threads]
                barrier.wait()
                [thread.join() for thread in threads]
                self.assertEqual(errors, [])
                row = ledger.db.execute(
                    "SELECT quantity,cost_basis_usd_micros,status FROM portfolio_positions"
                ).fetchone()
                self.assertIn((row[1], row[2]), {(0, "closed"), (10_000_000, "open"), (20_000_000, "open")})
                self.assertEqual(
                    ledger.db.execute("SELECT SUM(gross_usd_micros) FROM portfolio_fills WHERE side='buy'").fetchone()[
                        0
                    ],
                    20_000_000,
                )
            finally:
                ledger.close()

    def test_concurrent_readers_do_not_lock_market_writer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio.sqlite3"
            ledger = PortfolioLedger(path, "UTC")
            ledger.apply_event(event("initial"), {"status": "accepted", "paperBuyUsd": 10})
            errors: list[BaseException] = []
            barrier = threading.Barrier(7)

            def reader() -> None:
                try:
                    barrier.wait()
                    for _ in range(50):
                        portfolio_snapshot(path, limit=10)
                except BaseException as exc:
                    errors.append(exc)

            def writer() -> None:
                try:
                    barrier.wait()
                    for index in range(50):
                        ledger.update_market_marks(
                            [{"chainId": 1, "tokenAddress": event().ca, "priceUsd": 1 + index / 1000}]
                        )
                except BaseException as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=reader) for _ in range(6)] + [threading.Thread(target=writer)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            ledger.close()
            self.assertEqual(errors, [])

    def test_large_audit_log_is_incremental_and_limit_is_sql_level(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.ndjson"
            row = '{"recordedAt":"2026-01-01T00:00:00Z","status":"accepted","networkId":1,"paperBuyUsd":1}\n'
            path.write_text(row * 100_000, encoding="utf-8")
            started = time.perf_counter()
            payload = build_dashboard_payload(path, 7)
            first = time.perf_counter() - started
            started = time.perf_counter()
            again = build_dashboard_payload(path, 7)
            second = time.perf_counter() - started
            self.assertEqual(payload["total"], 100_000)
            self.assertEqual(len(payload["orders"]), 7)
            self.assertLess(first, 15)
            self.assertLess(second, 1)
            self.assertEqual(again["total"], 100_000)

    def test_daily_rotation_compresses_and_keeps_index_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.ndjson"
            append_ndjson(path, {"recordedAt": "old", "status": "accepted", "paperBuyUsd": 1})
            self.assertEqual(build_dashboard_payload(path)["total"], 1)
            marker = path.with_name(path.name + ".active-day")
            marker.write_text("2020-01-01", encoding="ascii")
            append_ndjson(path, {"recordedAt": "new", "status": "rejected"})
            self.assertTrue(path.with_name(path.name + ".2020-01-01.gz").exists())
            payload = build_dashboard_payload(path)
            self.assertEqual(payload["total"], 2)
            self.assertEqual(payload["orders"][0]["recordedAt"], "new")

    def test_multiple_clients_share_one_change_watcher(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status"
            bus = DashboardChangeBus({"status": [path]}, 0.05, 4)
            try:
                self.assertTrue(bus.register())
                self.assertTrue(bus.register())
                self.assertEqual(bus.thread.name, "dashboard-file-watcher")
                path.write_text("x", encoding="utf-8")
                revision, changed = bus.wait(0, 1)
                self.assertGreater(revision, 0)
                self.assertEqual(changed, ["status"])
            finally:
                bus.unregister()
                bus.unregister()
                bus.stop()


if __name__ == "__main__":
    unittest.main()
