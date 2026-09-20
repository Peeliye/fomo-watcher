import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fomo.intelligence.leaderboard import LeaderboardArchive


def member(user_id: str, handle: str, pnl: float = 1) -> dict:
    return {"id": user_id, "userHandle": handle, "pnl24h": pnl, "followers": 1}


class LeaderboardCycleTests(unittest.TestCase):
    def test_readonly_get_queries_do_not_modify_database_or_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "rank.sqlite3"
            archive = LeaderboardArchive(path, root / "leaderboard")
            archive.capture([member("u1", "alpha")])
            archive.close()
            before = (path.stat().st_mtime_ns, path.stat().st_size)
            readonly = LeaderboardArchive(path, root / "leaderboard", readonly=True)
            business_before = readonly.db.execute("SELECT COUNT(*) FROM ranking_records").fetchone()[0]
            readonly.overview(); readonly.current_ranking(); readonly.history(datetime.now().strftime("%Y-%m-%d"), 0)
            readonly.participants(); readonly.participant_detail(datetime.now().strftime("%Y-%m-%d"), "u1")
            business_after = readonly.db.execute("SELECT COUNT(*) FROM ranking_records").fetchone()[0]
            readonly.close()
            self.assertEqual(before, (path.stat().st_mtime_ns, path.stat().st_size))
            self.assertEqual(business_before, business_after)

    def test_readonly_reader_sees_latest_committed_wal_frames(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "rank.sqlite3"
            writer = LeaderboardArchive(path, root / "leaderboard")
            try:
                writer.capture([member("u1", "alpha")], captured_at=datetime(2026, 9, 20, 1, tzinfo=timezone.utc))
                writer.capture([member("u2", "beta")], captured_at=datetime(2026, 9, 20, 2, tzinfo=timezone.utc))
                self.assertTrue(Path(str(path) + "-wal").exists())
                reader = LeaderboardArchive(path, root / "leaderboard", readonly=True)
                try:
                    self.assertEqual([row["userId"] for row in reader.current_ranking("2026-09-20")], ["u2"])
                finally:
                    reader.close()
            finally:
                writer.close()

    def test_reader_recovers_committed_wal_left_by_abrupt_writer_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "rank.sqlite3"
            archive = LeaderboardArchive(path, root / "leaderboard")
            archive.close()
            script = """
import json,os,sqlite3,sys
db=sqlite3.connect(sys.argv[1]);db.execute('PRAGMA journal_mode=WAL')
db.execute("INSERT INTO cycles VALUES('2026-09-20','Asia/Shanghai','2026-09-20T03:00:00+00:00')")
cur=db.execute("INSERT INTO snapshots(cycle_date,hour_index,scheduled_for,captured_at,status,source_count) VALUES('2026-09-20',11,'2026-09-20T11:01:00+08:00','2026-09-20T03:00:00+00:00','success',1)")
p={'userId':'crash-user','rank':1,'pnlUsd':7,'userHandle':'crash','displayName':'','address':'','evmAddress':'','followers':0,'numTrades':0,'totalVolume':0,'totalHoldings':0,'clan':None,'topHoldings':[]}
db.execute('INSERT INTO ranking_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(cur.lastrowid,'crash-user',1,7,'crash','','','',0,0,0,0,json.dumps(p)))
db.commit();os._exit(0)
"""
            subprocess.run([sys.executable, "-c", script, str(path)], check=True)
            self.assertTrue(Path(str(path) + "-wal").exists())
            reader = LeaderboardArchive(path, root / "leaderboard", readonly=True)
            try:
                self.assertEqual(reader.current_ranking("2026-09-20")[0]["userId"], "crash-user")
            finally:
                reader.close()
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
        self.assertEqual(people["a"]["rankedPnl24hChangeUsd"], 150)
        self.assertEqual(people["a"]["currentPnl24hUsd"], 200)
        self.assertTrue(people["a"]["currentValueAvailable"])
        self.assertIsNone(people["b"]["currentPnl24hUsd"])
        self.assertFalse(people["b"]["currentValueAvailable"])
        self.assertEqual(people["b"]["pnlSource"], "fomo_rolling_24h")
        self.assertEqual(history[10]["status"], "not_ranked")

    def test_rolling_window_change_is_not_natural_day_pnl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); archive = LeaderboardArchive(root / "rank.sqlite3", root / "leaderboard")
            try:
                archive.capture([member("a", "alice", 100)], captured_at=datetime(2026,9,20,0,tzinfo=timezone.utc))
                archive.capture([member("a", "alice", 40)], captured_at=datetime(2026,9,20,1,tzinfo=timezone.utc))
                person = archive.participants("2026-09-20")[0]
                overview = archive.overview("2026-09-20")
            finally: archive.close()
        self.assertEqual(person["currentPnl24hUsd"], 40)
        self.assertEqual(person["rankedPnl24hChangeUsd"], -60)
        self.assertEqual(overview["pnlMetric"], "fomo_rolling_24h")
        self.assertNotIn("pnlChange", person)

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
