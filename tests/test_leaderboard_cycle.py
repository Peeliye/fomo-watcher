import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fomo.intelligence.leaderboard import LeaderboardArchive


def member(user_id: str, handle: str, pnl: float = 1) -> dict:
    return {"id": user_id, "userHandle": handle, "pnl24h": pnl, "followers": 1}


class LeaderboardCycleTests(unittest.TestCase):
    def test_hourly_compare_and_daily_merge_keep_removed_members(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = LeaderboardArchive(root / "rank.sqlite3", root / "leaderboard")
            try:
                first = archive.capture(
                    [member("a", "alice"), member("b", "bob")],
                    captured_at=datetime(2026, 9, 20, 1, tzinfo=timezone.utc),
                )
                second = archive.capture(
                    [member("b", "bob", 2), member("c", "carol", 3)],
                    captured_at=datetime(2026, 9, 20, 2, tzinfo=timezone.utc),
                )
                day = json.loads((root / "leaderboard" / "daily" / "2026-09-20.json").read_text(encoding="utf-8"))
            finally:
                archive.close()
        self.assertFalse(first["hasPrevious"])
        self.assertEqual(second["newCount"], 1)
        self.assertEqual(second["removedCount"], 1)
        self.assertEqual(second["rankChangedCount"], 1)
        self.assertEqual(day["uniqueMembers"], 3)
        self.assertFalse(next(x for x in day["members"] if x["userId"] == "a")["presentInLatest"])

    def test_new_day_finalizes_previous_daily_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = LeaderboardArchive(root / "rank.sqlite3", root / "leaderboard")
            try:
                archive.capture([member("a", "alice")], captured_at=datetime(2026, 9, 20, 15, 59, tzinfo=timezone.utc))
                archive.capture([member("b", "bob")], captured_at=datetime(2026, 9, 20, 16, 1, tzinfo=timezone.utc))
                previous = json.loads((root / "leaderboard" / "daily" / "2026-09-20.json").read_text(encoding="utf-8"))
                current = json.loads((root / "leaderboard" / "daily" / "2026-09-21.json").read_text(encoding="utf-8"))
            finally:
                archive.close()
        self.assertTrue(previous["finalized"])
        self.assertFalse(current["finalized"])

    def test_participant_stats_drop_and_reentry_preserve_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); archive = LeaderboardArchive(root / "rank.sqlite3", root / "leaderboard")
            try:
                archive.capture([member("a", "alice", 50), member("b", "bob", 20)], captured_at=datetime(2026,9,20,0,tzinfo=timezone.utc))
                archive.capture([member("b", "bob", 30)], captured_at=datetime(2026,9,20,1,tzinfo=timezone.utc))
                archive.capture([member("a", "alice", 200), member("c", "carol", 10)], captured_at=datetime(2026,9,20,3,tzinfo=timezone.utc))
                people = {x["userId"]:x for x in archive.participants("2026-09-20")}
                history = archive.participant_detail("2026-09-20", "a")["history"]
            finally: archive.close()
        self.assertEqual(set(people), {"a","b","c"})
        self.assertEqual(people["a"]["appearCount"], 2)
        self.assertEqual(people["a"]["firstPnl"], 50)
        self.assertEqual(people["a"]["latestRankedPnl"], 200)
        self.assertEqual(people["a"]["pnlChange"], 150)
        self.assertEqual(history[10]["status"], "not_ranked")

    def test_duplicate_hour_failure_retry_and_new_day_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); archive = LeaderboardArchive(root / "rank.sqlite3", root / "leaderboard")
            try:
                at=datetime(2026,9,20,1,tzinfo=timezone.utc)
                first=archive.capture([member("a","alice",1)],captured_at=at)
                duplicate=archive.capture([member("b","bob",9)],captured_at=at)
                archive.record_failure("network",captured_at=datetime(2026,9,20,2,tzinfo=timezone.utc))
                retry=archive.capture([member("c","carol",3)],captured_at=datetime(2026,9,20,2,tzinfo=timezone.utc))
                archive.capture([member("z","zed",4)],captured_at=datetime(2026,9,20,16,1,tzinfo=timezone.utc))
                old_ids={x["userId"] for x in archive.participants("2026-09-20")};new_ids={x["userId"] for x in archive.participants("2026-09-21")}
            finally: archive.close()
        self.assertEqual(first["status"],"ok");self.assertEqual(duplicate["status"],"duplicate");self.assertEqual(retry["status"],"ok")
        self.assertNotIn("b",old_ids);self.assertEqual(new_ids,{"z"})


if __name__ == "__main__":
    unittest.main()
