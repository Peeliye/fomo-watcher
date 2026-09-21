"""Durable wallet-stream checkpoints and event/reorg journal."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from fomo.signals.envelope import TradeSignalEnvelope

from .adapter import ChainCheckpoint, NormalizedWatchEvent


SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_checkpoints(
  adapter_id TEXT NOT NULL, chain_id TEXT NOT NULL, cursor TEXT NOT NULL,
  block_number INTEGER, block_hash TEXT, updated_at TEXT NOT NULL,
  PRIMARY KEY(adapter_id,chain_id)
);
CREATE TABLE IF NOT EXISTS wallet_source_events(
  adapter_id TEXT NOT NULL, chain_id TEXT NOT NULL, event_id TEXT NOT NULL,
  block_number INTEGER, block_hash TEXT, status TEXT NOT NULL,
  observed_at TEXT NOT NULL, PRIMARY KEY(adapter_id,event_id)
);
CREATE INDEX IF NOT EXISTS idx_wallet_events_reorg
  ON wallet_source_events(adapter_id,chain_id,block_number,status);
CREATE TABLE IF NOT EXISTS wallet_delivery_outbox(
  adapter_id TEXT NOT NULL, chain_id TEXT NOT NULL, delivery_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('signal','reorg')),
  event_id TEXT NOT NULL, payload_json TEXT,
  status TEXT NOT NULL CHECK(status IN ('pending','acked','cancelled')),
  created_at TEXT NOT NULL, acked_at TEXT,
  PRIMARY KEY(adapter_id,delivery_id)
);
CREATE INDEX IF NOT EXISTS idx_wallet_outbox_pending
  ON wallet_delivery_outbox(adapter_id,chain_id,status,created_at);
PRAGMA user_version=1;
"""


class CheckpointStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        if existed and not self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='wallet_delivery_outbox'"
        ).fetchone():
            backup_dir = self.path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            backup_path = backup_dir / f"{self.path.stem}.pre-outbox.{stamp}.sqlite3"
            backup = sqlite3.connect(backup_path)
            try:
                self.db.backup(backup)
                if backup.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("wallet checkpoint backup integrity check failed")
            finally:
                backup.close()
        self.db.executescript(SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def close(self) -> None:
        self.db.close()

    def load(self, adapter_id: str, chain_id: str) -> ChainCheckpoint | None:
        row = self.db.execute(
            "SELECT chain_id,cursor,block_number,block_hash FROM wallet_checkpoints "
            "WHERE adapter_id=? AND chain_id=?", (adapter_id, str(chain_id)),
        ).fetchone()
        return ChainCheckpoint(**dict(row)) if row else None

    def has_event(self, adapter_id: str, event_id: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM wallet_source_events WHERE adapter_id=? AND event_id=? AND status='active'",
            (adapter_id, event_id),
        ).fetchone() is not None

    def commit_batch(self, adapter_id: str, checkpoint: ChainCheckpoint,
                     events: list[tuple[NormalizedWatchEvent, int | None, str | None]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction():
            for event, block_number, block_hash in events:
                inserted = self.db.execute(
                    "INSERT INTO wallet_source_events VALUES(?,?,?,?,?,'active',?) "
                    "ON CONFLICT(adapter_id,event_id) DO UPDATE SET status='active',"
                    "block_number=excluded.block_number,block_hash=excluded.block_hash,"
                    "observed_at=excluded.observed_at WHERE wallet_source_events.status='reorged'",
                    (adapter_id, checkpoint.chain_id, event.event_id, block_number, block_hash, now),
                )
                if inserted.rowcount:
                    self.db.execute(
                        "INSERT INTO wallet_delivery_outbox VALUES(?,?,?,'signal',?,?,'pending',?,NULL) "
                        "ON CONFLICT(adapter_id,delivery_id) DO UPDATE SET "
                        "payload_json=excluded.payload_json,status='pending',"
                        "created_at=excluded.created_at,acked_at=NULL",
                        (adapter_id, checkpoint.chain_id, f"signal:{event.event_id}", event.event_id,
                         json.dumps(asdict(event), separators=(",", ":")), now),
                    )
            self.db.execute(
                "INSERT INTO wallet_checkpoints VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(adapter_id,chain_id) DO UPDATE SET cursor=excluded.cursor,"
                "block_number=excluded.block_number,block_hash=excluded.block_hash,updated_at=excluded.updated_at",
                (adapter_id, checkpoint.chain_id, checkpoint.cursor, checkpoint.block_number,
                 checkpoint.block_hash, now),
            )

    def rollback_after(self, adapter_id: str, chain_id: str, block_number: int,
                       block_hash: str | None = None) -> tuple[str, ...]:
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction():
            rows = self.db.execute(
                "SELECT event_id FROM wallet_source_events WHERE adapter_id=? AND chain_id=? "
                "AND block_number>? AND status='active' ORDER BY block_number DESC,event_id",
                (adapter_id, str(chain_id), int(block_number)),
            ).fetchall()
            ids = tuple(str(row[0]) for row in rows)
            self.db.execute(
                "UPDATE wallet_source_events SET status='reorged' WHERE adapter_id=? AND chain_id=? "
                "AND block_number>? AND status='active'", (adapter_id, str(chain_id), int(block_number)),
            )
            for event_id in ids:
                self.db.execute(
                    "UPDATE wallet_delivery_outbox SET status='cancelled' WHERE adapter_id=? AND delivery_id=? "
                    "AND status='pending'", (adapter_id, f"signal:{event_id}"),
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO wallet_delivery_outbox VALUES(?,?,?,'reorg',?,NULL,'pending',?,NULL)",
                    (adapter_id, str(chain_id), f"reorg:{event_id}", event_id, now),
                )
            self.db.execute(
                "UPDATE wallet_checkpoints SET cursor=?,block_number=?,block_hash=?,updated_at=? "
                "WHERE adapter_id=? AND chain_id=?",
                (str(block_number), block_number, block_hash, now, adapter_id, str(chain_id)),
            )
        return ids

    def pending_deliveries(self, adapter_id: str, chain_id: str) -> tuple[tuple[NormalizedWatchEvent, ...], tuple[str, ...]]:
        rows = self.db.execute(
            "SELECT kind,event_id,payload_json FROM wallet_delivery_outbox "
            "WHERE adapter_id=? AND chain_id=? AND status='pending' ORDER BY rowid LIMIT 1000",
            (adapter_id, str(chain_id)),
        ).fetchall()
        events: list[NormalizedWatchEvent] = []
        for row in rows:
            if row["kind"] != "signal":
                continue
            payload = json.loads(row["payload_json"])
            signal = payload.get("signal")
            if signal is not None:
                payload["signal"] = TradeSignalEnvelope(**signal)
            events.append(NormalizedWatchEvent(**payload))
        reverted = tuple(str(row["event_id"]) for row in rows if row["kind"] == "reorg")
        return tuple(events), reverted

    def acknowledge(self, adapter_id: str, delivery_id: str) -> bool:
        """Ack only after the downstream durable consumer commits its own state."""
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction():
            updated = self.db.execute(
                "UPDATE wallet_delivery_outbox SET status='acked',acked_at=? "
                "WHERE adapter_id=? AND delivery_id=? AND status='pending'",
                (now, adapter_id, delivery_id),
            )
        return bool(updated.rowcount)
