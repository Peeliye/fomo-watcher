"""Cross-language durable ingress shared by Fomo push and wallet RPC sources.

The queue does not execute trades. A single leased service consumes it; every
decision is idempotent by source-namespaced signal ID and survives restarts.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from fomo.signals.envelope import TradeSignalEnvelope


SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_queue (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  payload_kind TEXT NOT NULL CHECK(payload_kind IN ('raw_fomo','envelope')),
  payload_json TEXT NOT NULL,
  received_at TEXT NOT NULL,
  enqueued_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('queued','claimed','processed','dropped','blocked')),
  lease_owner TEXT,
  lease_until_ms INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  decision_json TEXT,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signal_queue_claim
  ON signal_queue(status,lease_until_ms,sequence);
CREATE TABLE IF NOT EXISTS signal_queue_service_lock (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  owner TEXT NOT NULL,
  lease_until_ms INTEGER NOT NULL
);
PRAGMA user_version=1;
"""


@dataclass(frozen=True, slots=True)
class QueuedSignal:
    sequence: int
    signal_id: str
    source: str
    payload_kind: str
    payload: dict[str, Any]
    received_at: str
    enqueued_at_ms: int
    attempts: int


class DurableSignalQueue:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000")
        version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if existed and version != 1:
            backup_dir = self.path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            backup = sqlite3.connect(backup_dir / f"{self.path.stem}.pre-v1.{stamp}.sqlite3")
            try:
                self.db.backup(backup)
                if backup.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("signal queue backup integrity check failed")
            finally:
                backup.close()
        if version not in {0, 1}:
            raise ValueError("unsupported_signal_queue_schema")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.close()

    def enqueue_envelope(self, signal: TradeSignalEnvelope) -> bool:
        return self.enqueue(signal.signal_id, signal.source, "envelope", signal.to_dict(), signal.observed_at)

    def enqueue(self, signal_id: str, source: str, payload_kind: str,
                payload: Mapping[str, Any], received_at: str) -> bool:
        if not signal_id or payload_kind not in {"raw_fomo", "envelope"}:
            raise ValueError("invalid signal queue entry")
        now = int(time.time() * 1000)
        inserted = self.db.execute(
            "INSERT OR IGNORE INTO signal_queue(signal_id,source,payload_kind,payload_json,received_at,"
            "enqueued_at_ms,status,updated_at_ms) VALUES(?,?,?,?,?,?,'queued',?)",
            (signal_id, source, payload_kind, json.dumps(dict(payload), separators=(",", ":")),
             received_at, now, now),
        )
        return bool(inserted.rowcount)

    def acquire_service(self, owner: str, lease_ms: int = 5000) -> bool:
        if not owner or lease_ms < 100:
            raise ValueError("invalid service lease")
        now = int(time.time() * 1000)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT owner,lease_until_ms FROM signal_queue_service_lock WHERE singleton=1").fetchone()
            if row and row["owner"] != owner and int(row["lease_until_ms"]) > now:
                self.db.rollback()
                return False
            self.db.execute(
                "INSERT INTO signal_queue_service_lock VALUES(1,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET owner=excluded.owner,lease_until_ms=excluded.lease_until_ms",
                (owner, now + lease_ms),
            )
            self.db.commit()
            return True
        except BaseException:
            self.db.rollback()
            raise

    def claim(self, owner: str, lease_ms: int = 5000) -> QueuedSignal | None:
        now = int(time.time() * 1000)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            lock = self.db.execute(
                "SELECT 1 FROM signal_queue_service_lock WHERE singleton=1 AND owner=? AND lease_until_ms>?",
                (owner, now),
            ).fetchone()
            if lock is None:
                raise ValueError("execution_service_lock_required")
            row = self.db.execute(
                "SELECT * FROM signal_queue WHERE status='queued' OR "
                "(status='claimed' AND lease_until_ms<=?) ORDER BY sequence LIMIT 1", (now,),
            ).fetchone()
            if row is None:
                self.db.commit()
                return None
            self.db.execute(
                "UPDATE signal_queue SET status='claimed',lease_owner=?,lease_until_ms=?,"
                "attempts=attempts+1,updated_at_ms=? WHERE sequence=?",
                (owner, now + lease_ms, now, row["sequence"]),
            )
            self.db.commit()
            return QueuedSignal(
                int(row["sequence"]), str(row["signal_id"]), str(row["source"]),
                str(row["payload_kind"]), json.loads(row["payload_json"]),
                str(row["received_at"]), int(row["enqueued_at_ms"]), int(row["attempts"]) + 1,
            )
        except BaseException:
            self.db.rollback()
            raise

    def finish(self, owner: str, signal_id: str, status: str, decision: Mapping[str, Any]) -> None:
        if status not in {"processed", "dropped", "blocked"}:
            raise ValueError("invalid queue terminal status")
        now = int(time.time() * 1000)
        updated = self.db.execute(
            "UPDATE signal_queue SET status=?,decision_json=?,lease_owner=NULL,lease_until_ms=NULL,"
            "updated_at_ms=? WHERE signal_id=? AND status='claimed' AND lease_owner=? "
            "AND lease_until_ms>? AND EXISTS(SELECT 1 FROM signal_queue_service_lock "
            "WHERE singleton=1 AND owner=? AND lease_until_ms>?)",
            (status, json.dumps(dict(decision), separators=(",", ":")), now, signal_id, owner, now, owner, now),
        )
        if updated.rowcount != 1:
            raise ValueError("queue claim lost")

    def status(self, signal_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT status,attempts,decision_json FROM signal_queue WHERE signal_id=?", (signal_id,),
        ).fetchone()
        return ({"status": row["status"], "attempts": row["attempts"],
                 "decision": json.loads(row["decision_json"]) if row["decision_json"] else None}
                if row else None)

    def cancel_reorged(self, signal_id: str) -> bool:
        """Cancel an unclaimed signal; a claimed intent is fenced in the journal."""
        now = int(time.time() * 1000)
        updated = self.db.execute(
            "UPDATE signal_queue SET status='dropped',decision_json=?,updated_at_ms=? "
            "WHERE signal_id=? AND status='queued'",
            (json.dumps({"reason": "source_reorged"}), now, signal_id),
        )
        return bool(updated.rowcount)
