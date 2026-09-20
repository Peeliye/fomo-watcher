"""Incremental SQLite index for append-only dashboard NDJSON archives."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class AuditLogIndex:
    _locks: dict[str, threading.Lock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, log_path: Path):
        self.log_path = Path(log_path)
        self.database = self.log_path.with_suffix(self.log_path.suffix + ".index.sqlite3")
        with self._locks_guard:
            self.lock = self._locks.setdefault(str(self.database.resolve()), threading.Lock())

    def _connect(self) -> sqlite3.Connection:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS source_state(
          path TEXT PRIMARY KEY,offset INTEGER NOT NULL,size INTEGER NOT NULL,generation INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS audit_rows(
          id INTEGER PRIMARY KEY AUTOINCREMENT,generation INTEGER NOT NULL,byte_end INTEGER NOT NULL,
          recorded_at TEXT,status TEXT,outcome TEXT,network_id TEXT,amount REAL NOT NULL DEFAULT 0,
          latency REAL NOT NULL DEFAULT 0,payload_json TEXT NOT NULL,UNIQUE(generation,byte_end));
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
        return db

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
            state = db.execute("SELECT offset,size,generation FROM source_state WHERE path=?", (source,)).fetchone()
            offset = int(state["offset"]) if state else 0
            generation = int(state["generation"]) if state else 0
            if size < offset:
                with db:
                    generation += 1
                    db.execute("INSERT OR REPLACE INTO source_state VALUES(?,0,?,?)", (source, size, generation))
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
                            offset = end
                            continue
                        if isinstance(value, dict):
                            cursor = db.execute(
                                """INSERT OR IGNORE INTO audit_rows
                                (generation,byte_end,recorded_at,status,outcome,network_id,amount,latency,payload_json)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                                (
                                    generation,
                                    end,
                                    value.get("recordedAt"),
                                    value.get("status"),
                                    value.get("outcome"),
                                    str(value.get("networkId") or "unknown"),
                                    float(value.get("paperBuyUsd") or 0),
                                    float(value.get("decisionLatencyMs") or 0),
                                    json.dumps(value, ensure_ascii=False, separators=(",", ":")),
                                ),
                            )
                            if cursor.rowcount:
                                status = str(value.get("status") or "unknown")
                                outcome = str(value.get("outcome") or "unknown")
                                self._increment(db, "total")
                                self._increment(db, f"status:{status}")
                                self._increment(db, f"outcome:{outcome}")
                                if status == "accepted":
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
                    db.execute("INSERT OR REPLACE INTO source_state VALUES(?,?,?,?)", (source, offset, size, generation))
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
