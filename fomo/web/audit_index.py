"""Incremental SQLite index for append-only dashboard NDJSON archives."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class AuditLogIndex:
    _locks: dict[str, threading.Lock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, log_path: Path, retention_days: int = 30):
        self.log_path = Path(log_path)
        self.retention_days = max(2, int(retention_days))
        self.database = self.log_path.with_suffix(self.log_path.suffix + ".index.sqlite3")
        with self._locks_guard:
            self.lock = self._locks.setdefault(str(self.database.resolve()), threading.Lock())

    def _connect(self) -> sqlite3.Connection:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS source_state(
          path TEXT PRIMARY KEY,offset INTEGER NOT NULL,size INTEGER NOT NULL,generation INTEGER NOT NULL DEFAULT 0,
          file_id TEXT NOT NULL DEFAULT '',writer_generation INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS audit_rows(
          id INTEGER PRIMARY KEY AUTOINCREMENT,generation INTEGER NOT NULL,byte_end INTEGER NOT NULL,
          recorded_at TEXT,status TEXT,outcome TEXT,network_id TEXT,amount REAL NOT NULL DEFAULT 0,
          latency REAL NOT NULL DEFAULT 0,event_key TEXT,payload_json TEXT NOT NULL,UNIQUE(generation,byte_end));
        CREATE INDEX IF NOT EXISTS idx_audit_id ON audit_rows(id DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_status ON audit_rows(status);
        CREATE INDEX IF NOT EXISTS idx_audit_outcome ON audit_rows(outcome);
        CREATE INDEX IF NOT EXISTS idx_audit_latency ON audit_rows(latency);
        CREATE TABLE IF NOT EXISTS audit_blockers(row_id INTEGER NOT NULL,reason TEXT NOT NULL,
          FOREIGN KEY(row_id) REFERENCES audit_rows(id) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS idx_blockers_reason ON audit_blockers(reason);
        CREATE TABLE IF NOT EXISTS audit_metrics(
          name TEXT PRIMARY KEY,value REAL NOT NULL DEFAULT 0,text_value TEXT
        );
        """)
        columns = {str(row[1]) for row in db.execute("PRAGMA table_info(source_state)")}
        if "file_id" not in columns:
            db.execute("ALTER TABLE source_state ADD COLUMN file_id TEXT NOT NULL DEFAULT ''")
        if "writer_generation" not in columns:
            db.execute("ALTER TABLE source_state ADD COLUMN writer_generation INTEGER NOT NULL DEFAULT 0")
        row_columns = {str(row[1]) for row in db.execute("PRAGMA table_info(audit_rows)")}
        if "event_key" not in row_columns:
            db.execute("ALTER TABLE audit_rows ADD COLUMN event_key TEXT")
            db.execute(
                """UPDATE audit_rows SET event_key=COALESCE(
                     NULLIF(json_extract(payload_json,'$.eventId'),''),
                     NULLIF(json_extract(payload_json,'$.signalId'),'')
                   ) WHERE event_key IS NULL"""
            )
            duplicate_ids = [
                int(row[0]) for row in db.execute(
                    """SELECT id FROM audit_rows
                       WHERE event_key IS NOT NULL AND id NOT IN (
                         SELECT MIN(id) FROM audit_rows WHERE event_key IS NOT NULL GROUP BY event_key
                       )"""
                )
            ]
            if duplicate_ids:
                placeholders = ",".join("?" for _ in duplicate_ids)
                db.execute(f"DELETE FROM audit_blockers WHERE row_id IN ({placeholders})", duplicate_ids)
                db.execute(f"DELETE FROM audit_rows WHERE id IN ({placeholders})", duplicate_ids)
                self._rebuild_metrics(db)
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_event_key ON audit_rows(event_key) WHERE event_key IS NOT NULL"
        )
        db.commit()
        return db

    def _rebuild_metrics(self, db: sqlite3.Connection) -> None:
        db.execute("DELETE FROM audit_metrics")
        total = int(db.execute("SELECT COUNT(*) FROM audit_rows").fetchone()[0])
        self._increment(db, "total", total)
        for column, prefix in (("status", "status:"), ("outcome", "outcome:")):
            for value, count in db.execute(
                f"SELECT COALESCE({column},'unknown'),COUNT(*) FROM audit_rows GROUP BY COALESCE({column},'unknown')"
            ):
                self._increment(db, prefix + str(value), float(count))
        accepted, accepted_usd = db.execute(
            """SELECT COUNT(*),COALESCE(SUM(amount),0) FROM audit_rows
               WHERE status='accepted' AND amount>0
                 AND COALESCE(json_extract(payload_json,'$.side'),'buy')='buy'"""
        ).fetchone()
        self._increment(db, "accepted", float(accepted))
        self._increment(db, "accepted_usd", float(accepted_usd))
        eligible = int(db.execute("SELECT COUNT(*) FROM audit_rows WHERE status='eligible'").fetchone()[0])
        self._increment(db, "eligible", eligible)
        for network, count in db.execute(
            """SELECT network_id,COUNT(*) FROM audit_rows
               WHERE status='accepted' AND amount>0
                 AND COALESCE(json_extract(payload_json,'$.side'),'buy')='buy'
               GROUP BY network_id"""
        ):
            self._increment(db, f"accepted_chain:{network}", float(count))
        latency_count, latency_sum = db.execute(
            """SELECT COUNT(*),COALESCE(SUM(latency),0) FROM audit_rows
               WHERE json_extract(payload_json,'$.decisionLatencyMs') IS NOT NULL"""
        ).fetchone()
        self._increment(db, "latency_count", float(latency_count))
        self._increment(db, "latency_sum", float(latency_sum))
        latest = db.execute("SELECT MAX(recorded_at) FROM audit_rows").fetchone()[0]
        if latest:
            self._set_text(db, "latest_at", str(latest))
        for reason, count in db.execute("SELECT reason,COUNT(*) FROM audit_blockers GROUP BY reason"):
            self._increment(db, f"blocker:{reason}", float(count))
        self._set_text(db, "maintenance_at", str(time.time()))

    def _prune_retention(self, db: sqlite3.Connection, batch: int = 500) -> None:
        row = db.execute("SELECT text_value FROM audit_metrics WHERE name='maintenance_at'").fetchone()
        if row is None:
            with db:
                self._set_text(db, "maintenance_at", str(time.time()))
            return
        try:
            if time.time() - float(row[0]) < 3600:
                return
        except (TypeError, ValueError):
            pass
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.retention_days)).isoformat()
        ids = [
            int(row[0]) for row in db.execute(
                """SELECT id FROM audit_rows
                   WHERE recorded_at IS NOT NULL AND recorded_at<? ORDER BY id LIMIT ?""",
                (cutoff, max(10, min(batch, 5000))),
            )
        ]
        with db:
            if ids:
                placeholders = ",".join("?" for _ in ids)
                db.execute(f"DELETE FROM audit_blockers WHERE row_id IN ({placeholders})", ids)
                db.execute(f"DELETE FROM audit_rows WHERE id IN ({placeholders})", ids)
            self._rebuild_metrics(db)

    def _source_identity(self) -> tuple[str, int]:
        if not self.log_path.exists():
            return "", 0
        stat = self.log_path.stat()
        file_id = f"{stat.st_dev}:{stat.st_ino}"
        marker = self.log_path.with_name(self.log_path.name + ".rotation.json")
        try:
            value = json.loads(marker.read_text(encoding="utf-8"))
            writer_generation = max(0, int(value.get("generation") or 0))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            writer_generation = 0
        return file_id, writer_generation

    @staticmethod
    def _increment(db: sqlite3.Connection, name: str, amount: float = 1) -> None:
        db.execute(
            """INSERT INTO audit_metrics(name,value) VALUES(?,?)
               ON CONFLICT(name) DO UPDATE SET value=value+excluded.value""",
            (name, amount),
        )

    @staticmethod
    def _set_text(db: sqlite3.Connection, name: str, value: str) -> None:
        db.execute(
            """INSERT INTO audit_metrics(name,value,text_value) VALUES(?,0,?)
               ON CONFLICT(name) DO UPDATE SET text_value=excluded.text_value""",
            (name, value),
        )

    def sync(self) -> sqlite3.Connection:
        self.lock.acquire()
        db = self._connect()
        try:
            source = str(self.log_path.resolve())
            size = self.log_path.stat().st_size if self.log_path.exists() else 0
            file_id, writer_generation = self._source_identity()
            state = db.execute(
                "SELECT offset,size,generation,file_id,writer_generation FROM source_state WHERE path=?",
                (source,),
            ).fetchone()
            offset = int(state["offset"]) if state else 0
            generation = int(state["generation"]) if state else writer_generation
            rotated = bool(state) and (
                size < offset
                or (bool(state["file_id"]) and str(state["file_id"]) != file_id)
                or int(state["writer_generation"]) != writer_generation
            )
            if rotated:
                with db:
                    generation = max(generation + 1, writer_generation)
                    db.execute(
                        """INSERT OR REPLACE INTO source_state
                           (path,offset,size,generation,file_id,writer_generation)
                           VALUES(?,0,?,?,?,?)""",
                        (source, size, generation, file_id, writer_generation),
                    )
                offset = 0
            if self.log_path.exists() and size > offset:
                with self.log_path.open("rb") as stream, db:
                    stream.seek(offset)
                    while True:
                        raw = stream.readline()
                        if not raw or not raw.endswith(b"\n"):
                            break
                        end = stream.tell()
                        try:
                            value = json.loads(raw.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            self._increment(db, "malformed")
                            offset = end
                            continue
                        if isinstance(value, dict):
                            cursor = db.execute(
                                """INSERT OR IGNORE INTO audit_rows
                                (generation,byte_end,recorded_at,status,outcome,network_id,amount,latency,event_key,payload_json)
                                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                                (
                                    generation,
                                    end,
                                    value.get("recordedAt"),
                                    value.get("status"),
                                    value.get("outcome"),
                                    str(value.get("networkId") or "unknown"),
                                    float(value.get("paperBuyUsd") or 0),
                                    float(value.get("decisionLatencyMs") or 0),
                                    str(value.get("eventId") or value.get("signalId") or "") or None,
                                    json.dumps(value, ensure_ascii=False, separators=(",", ":")),
                                ),
                            )
                            if cursor.rowcount:
                                status = str(value.get("status") or "unknown")
                                outcome = str(value.get("outcome") or "unknown")
                                self._increment(db, "total")
                                self._increment(db, f"status:{status}")
                                self._increment(db, f"outcome:{outcome}")
                                accepted_buy = (
                                    status == "accepted"
                                    and str(value.get("side") or "buy") == "buy"
                                    and float(value.get("paperBuyUsd") or 0) > 0
                                )
                                if accepted_buy:
                                    self._increment(db, "accepted")
                                    self._increment(db, "accepted_usd", float(value.get("paperBuyUsd") or 0))
                                    self._increment(db, f"accepted_chain:{value.get('networkId') or 'unknown'}")
                                if status == "eligible":
                                    self._increment(db, "eligible")
                                if value.get("decisionLatencyMs") is not None:
                                    self._increment(db, "latency_count")
                                    self._increment(db, "latency_sum", float(value.get("decisionLatencyMs") or 0))
                                if value.get("recordedAt"):
                                    self._set_text(db, "latest_at", str(value["recordedAt"]))
                                db.executemany(
                                    "INSERT INTO audit_blockers(row_id,reason) VALUES(?,?)",
                                    [(cursor.lastrowid, str(reason)) for reason in value.get("blockers", []) if reason],
                                )
                                for reason in value.get("blockers", []):
                                    if reason:
                                        self._increment(db, f"blocker:{reason}")
                        offset = end
                    db.execute(
                        """INSERT OR REPLACE INTO source_state
                           (path,offset,size,generation,file_id,writer_generation)
                           VALUES(?,?,?,?,?,?)""",
                        (source, offset, size, generation, file_id, writer_generation),
                    )
            elif state is None:
                with db:
                    db.execute(
                        """INSERT INTO source_state
                           (path,offset,size,generation,file_id,writer_generation)
                           VALUES(?,?,?,?,?,?)""",
                        (source, 0, size, generation, file_id, writer_generation),
                    )
            self._prune_retention(db)
            return db
        except Exception:
            db.close()
            self.lock.release()
            raise

    def close(self, db: sqlite3.Connection) -> None:
        db.close()
        self.lock.release()

    def recent(self, db: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
        rows = db.execute(
            "SELECT payload_json FROM audit_rows ORDER BY id DESC LIMIT ?",
            (max(1, min(int(limit), 2000)),),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    @staticmethod
    def metrics(db: sqlite3.Connection) -> dict[str, tuple[float, str | None]]:
        return {str(row[0]): (float(row[1]), row[2]) for row in db.execute("SELECT * FROM audit_metrics")}
