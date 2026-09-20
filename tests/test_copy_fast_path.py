from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fomo.app import Event, State, process_pending_events
from fomo.execution.fast_path import evaluate_copy_buy


def settings(**changes):
    result = {
        "mode": "paper",
        "network_ids": [1],
        "event_types": ["swap_buy", "single_user_buy"],
        "active_buy_event_types": ["swap_buy", "single_user_buy"],
        "max_signal_age_seconds": 5,
        "min_target_buy_usd": 100,
        "min_market_cap_usd": 100_000,
        "defer_asset_checks": True,
    }
    result.update(changes)
    return result


def event(event_id: str, source_type: str = "swap_buy", market_cap: float = 1_000_000) -> Event:
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
        market_cap=market_cap,
        price=1,
        source_type=source_type,
    )


class CopyFastPathTests(unittest.TestCase):
    def test_passive_asset_event_cannot_be_enabled_accidentally(self) -> None:
        transfer = event("transfer", "large_transfer_in")
        gate = evaluate_copy_buy(
            transfer,
            settings(
                event_types=["swap_buy", "large_transfer_in"],
                active_buy_event_types=["swap_buy", "large_transfer_in"],
            ),
        )
        self.assertEqual(gate.status, "passive_asset_event")
        self.assertFalse(gate.accepted)

    def test_asset_checks_are_deferred_without_blocking_hot_path(self) -> None:
        gate = evaluate_copy_buy(event("new-token", market_cap=0), settings())
        self.assertTrue(gate.accepted)
        self.assertEqual(gate.deferred_checks, ("missing_market_cap",))

    def test_live_signal_requires_original_transaction_reference(self) -> None:
        gate = evaluate_copy_buy(event("live"), settings(mode="live", require_trade_id_in_live=True))
        self.assertEqual(gate.status, "missing_transaction_reference")

    def test_burst_finishes_all_fast_decisions_before_post_trade_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            first, second = event("first"), event("second")
            state.enqueue_event(first, source="test")
            state.enqueue_event(second, source="test")
            order: list[str] = []

            def fast(_state, item, _cfg, _portfolio):
                order.append(f"fast:{item.id}")
                return None

            def post(_state, item, *_args):
                order.append(f"post:{item.id}")

            with (
                patch("fomo.app.process_fast_event", side_effect=fast),
                patch("fomo.app.process_post_trade_event", side_effect=post),
            ):
                processed = process_pending_events(
                    state,
                    {"notifications": {}},
                    SimpleNamespace(),
                    None,
                    None,
                    None,
                )

            self.assertEqual(processed, 2)
            self.assertEqual(order[:2], ["fast:first", "fast:second"])
            self.assertEqual(set(order[2:]), {"post:first", "post:second"})
            state.db.close()

    def test_async_mode_completes_ingress_before_post_trade_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            item = event("async")
            state.enqueue_event(item, source="test")
            cfg = {
                "timezone": "UTC",
                "notifications": {},
                "copy_trading": {"enabled": False},
            }
            processed = process_pending_events(
                state, cfg, None, None, None, None, post_trade_async=True
            )
            self.assertEqual(processed, 1)
            self.assertEqual(
                state.db.execute("SELECT status FROM event_inbox WHERE event_id='async'").fetchone()[0],
                "done",
            )
            self.assertEqual(
                state.db.execute("SELECT status FROM post_trade_outbox WHERE event_id='async'").fetchone()[0],
                "pending",
            )
            state.db.close()


if __name__ == "__main__":
    unittest.main()
