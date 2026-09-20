from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

from fomo.app import Event, NotificationWorker, State


class NotificationReplayTests(unittest.TestCase):
    def test_historical_event_is_neither_enqueued_nor_delivered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            cfg = {"notifications": {"feishu": True, "max_event_age_seconds": 300}}
            stale = Event(
                id="historical-event",
                kind="buy",
                handle="alice",
                created_at="2026-09-11T00:00:00+00:00",
            )
            state.enqueue_notifications(stale, cfg)
            self.assertEqual(state.db.execute("SELECT COUNT(*) FROM notification_outbox").fetchone()[0], 0)

            with state.db:
                state.db.execute(
                    """INSERT INTO notification_outbox
                       (event_id,channel,event_json,created_at) VALUES(?,?,?,?)""",
                    (stale.id, "feishu", json.dumps(asdict(stale)), time.time()),
                )
            worker = NotificationWorker(state.path, cfg)
            db = sqlite3.connect(state.path)
            db.row_factory = sqlite3.Row
            try:
                with patch("fomo.app.notify_channel") as deliver:
                    worker._drain(db)
                deliver.assert_not_called()
                status = db.execute(
                    "SELECT status FROM notification_outbox WHERE event_id=?", (stale.id,)
                ).fetchone()[0]
                self.assertEqual(status, "suppressed_stale")
            finally:
                db.close()
                state.db.close()


if __name__ == "__main__":
    unittest.main()
