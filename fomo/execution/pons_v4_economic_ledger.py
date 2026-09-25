"""Persistent, live-only one-buy-per-chain-and-CA fence."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from scripts.pons_v4_swap_once import SIGNAL_DB


ECONOMIC_DB = SIGNAL_DB.with_name("pons-v4-economic-events.sqlite3")
LOCKED = {"signed", "send_attempted", "submitted", "confirmed", "receipt_failed", "uncertain"}


class PonsV4EconomicLedger:
    def __init__(self, path: Path = ECONOMIC_DB) -> None:
        self.path = Path(path)
        existed = self.path.is_file()
        if not existed:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            self.db.execute("PRAGMA busy_timeout=5000")
            self.db.execute("PRAGMA synchronous=FULL")
            if existed:
                if (self.db.execute("PRAGMA user_version").fetchone()[0] != 2
                        or self.db.execute("PRAGMA integrity_check").fetchone()[0] != "ok"):
                    # A v1 trade-key fence needs an explicit backed-up migration;
                    # never silently clear its live-buy history.
                    raise ValueError("pons_economic_ledger_migration_required")
                columns = {row[1] for row in self.db.execute("PRAGMA table_info(pons_economic_events)")}
                if columns != {"economic_key", "signal_id", "state", "nonce", "tx_hash", "updated_at"}:
                    raise ValueError("pons_economic_ledger_schema_invalid")
            else:
                self.db.executescript("""
                CREATE TABLE pons_economic_events (
                  economic_key TEXT PRIMARY KEY,
                  signal_id TEXT NOT NULL UNIQUE,
                  state TEXT NOT NULL,
                  nonce INTEGER,
                  tx_hash TEXT,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE pons_economic_audit (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  economic_key TEXT NOT NULL,
                  signal_id TEXT NOT NULL,
                  action TEXT NOT NULL,
                  occurred_at TEXT NOT NULL
                );
                PRAGMA user_version=2;
                """)
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def owned(self, economic_key: str, signal_id: str) -> bool:
        row = self.db.execute(
            "SELECT state FROM pons_economic_events WHERE economic_key=? AND signal_id=?",
            (economic_key, signal_id),
        ).fetchone()
        return bool(row and row[0] in {"reserved", "signed", "send_attempted"})

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def claim(self, economic_key: str, signal_id: str) -> None:
        if not economic_key or not signal_id:
            raise ValueError("pons_economic_key_invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO pons_economic_events VALUES(?,?,'reserved',NULL,NULL,?)",
                (economic_key, signal_id, self._now()),
            )
            self.db.execute(
                "INSERT INTO pons_economic_audit(economic_key,signal_id,action,occurred_at) "
                "VALUES(?,?,'reserved',?)", (economic_key, signal_id, self._now()),
            )
            self.db.commit()
        except sqlite3.IntegrityError as error:
            self.db.rollback()
            raise ValueError("pons_signal_duplicate_economic_event") from error
        except BaseException:
            self.db.rollback()
            raise

    def transition(self, economic_key: str, signal_id: str, state: str, *,
                   nonce: int | None = None, tx_hash: str | None = None) -> None:
        if state not in LOCKED | {"preflight_failed"}:
            raise ValueError("pons_economic_state_invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT state FROM pons_economic_events WHERE economic_key=? AND signal_id=?",
                (economic_key, signal_id),
            ).fetchone()
            if row is None or (row[0] in LOCKED and state == "preflight_failed"):
                raise ValueError("pons_economic_state_conflict")
            if state == "preflight_failed":
                self.db.execute("DELETE FROM pons_economic_events WHERE economic_key=? AND signal_id=?",
                                (economic_key, signal_id))
            else:
                self.db.execute(
                    "UPDATE pons_economic_events SET state=?,nonce=COALESCE(?,nonce),"
                    "tx_hash=COALESCE(?,tx_hash),updated_at=? WHERE economic_key=? AND signal_id=?",
                    (state, nonce, tx_hash, self._now(), economic_key, signal_id),
                )
            self.db.execute(
                "INSERT INTO pons_economic_audit(economic_key,signal_id,action,occurred_at) "
                "VALUES(?,?,?,?)", (economic_key, signal_id, state, self._now()),
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def manual_reset(self, economic_key: str, *, operator: str, reason: str,
                     confirmation: str) -> None:
        """Explicit operator-only rearm; never called by the signal consumer."""
        if (not operator.strip() or not reason.strip()
                or confirmation != f"RESET {economic_key}"):
            raise ValueError("pons_economic_manual_reset_confirmation_invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT signal_id,state FROM pons_economic_events WHERE economic_key=?",
                (economic_key,),
            ).fetchone()
            if row is None:
                raise ValueError("pons_economic_manual_reset_missing")
            self.db.execute(
                "INSERT INTO pons_economic_audit(economic_key,signal_id,action,occurred_at) "
                "VALUES(?,?,?,?)",
                (economic_key, row[0],
                 f"manual_reset:{operator.strip()}:{reason.strip()}:{row[1]}", self._now()),
            )
            self.db.execute("DELETE FROM pons_economic_events WHERE economic_key=?", (economic_key,))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
