"""Durable wallet-stream checkpoints and event/reorg journal."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .adapter import ChainCheckpoint


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
"""


class CheckpointStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
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
                     events: list[tuple[str, int | None, str | None]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction():
            for event_id, block_number, block_hash in events:
                self.db.execute(
                    "INSERT OR IGNORE INTO wallet_source_events VALUES(?,?,?,?,?,'active',?)",
                    (adapter_id, checkpoint.chain_id, event_id, block_number, block_hash, now),
                )
            self.db.execute(
                "INSERT INTO wallet_checkpoints VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(adapter_id,chain_id) DO UPDATE SET cursor=excluded.cursor,"
                "block_number=excluded.block_number,block_hash=excluded.block_hash,updated_at=excluded.updated_at",
                (adapter_id, checkpoint.chain_id, checkpoint.cursor, checkpoint.block_number,
                 checkpoint.block_hash, now),
            )

    def rollback_after(self, adapter_id: str, chain_id: str, block_number: int) -> tuple[str, ...]:
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
        return ids
