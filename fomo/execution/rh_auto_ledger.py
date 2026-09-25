"""One successful or uncertain Robinhood buy attempt per token address.

This is a new, isolated SQLite database. It never migrates the execution or
Pons ledgers. A persisted signed hash is never automatically re-signed.
"""

from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")


class RhAutoLedger:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            self.db.execute("PRAGMA busy_timeout=5000")
            if existed and int(self.db.execute("PRAGMA user_version").fetchone()[0]) != 1:
                raise ValueError("rh_auto_ledger_schema_unknown")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS rh_auto_buys (
                  token_out TEXT PRIMARY KEY,
                  signal_id TEXT NOT NULL UNIQUE,
                  wallet TEXT NOT NULL,
                  route TEXT NOT NULL,
                  amount_in_wei TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN
                    ('claimed','signed','broadcast_started','submitted',
                     'confirmed','failed','uncertain')),
                  nonce INTEGER UNIQUE,
                  tx_hash TEXT UNIQUE,
                  unsigned_hash TEXT,
                  receipt_status INTEGER,
                  created_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL
                );
                PRAGMA user_version=1;
            """)
            columns = {str(row[1]) for row in self.db.execute("PRAGMA table_info(rh_auto_buys)")}
            if columns != {"token_out", "signal_id", "wallet", "route", "amount_in_wei",
                           "state", "nonce", "tx_hash", "unsigned_hash", "receipt_status",
                           "created_at_ms", "updated_at_ms"}:
                raise ValueError("rh_auto_ledger_schema_unknown")
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def lookup(self, token_out: str) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT signal_id,state,nonce,tx_hash,receipt_status FROM rh_auto_buys WHERE token_out=?",
            (token_out.lower(),),
        ).fetchone()
        return ({"signalId": row[0], "state": row[1], "nonce": row[2],
                 "txHash": row[3], "receiptStatus": row[4]} if row else None)

    def claim(self, *, token_out: str, signal_id: str, wallet: str,
              route: str, amount_in_wei: int) -> None:
        token = token_out.lower()
        if (not _ADDRESS.fullmatch(token) or not signal_id or not _ADDRESS.fullmatch(wallet.lower())
                or not route or amount_in_wei <= 0):
            raise ValueError("rh_auto_claim_invalid")
        now = int(time.time() * 1000)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO rh_auto_buys(token_out,signal_id,wallet,route,amount_in_wei,"
                "state,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,'claimed',?,?)",
                (token, signal_id, wallet.lower(), route, str(amount_in_wei), now, now),
            )
            self.db.commit()
        except sqlite3.IntegrityError as error:
            self.db.rollback()
            raise ValueError("rh_auto_duplicate_token_or_signal") from error
        except BaseException:
            self.db.rollback()
            raise

    def signed(self, *, token_out: str, signal_id: str, nonce: int,
               tx_hash: str, unsigned_hash: str) -> None:
        if (nonce < 0 or not _HASH.fullmatch(tx_hash.lower())
                or not _HASH.fullmatch(unsigned_hash.lower())):
            raise ValueError("rh_auto_signed_evidence_invalid")
        self._transition(token_out, signal_id, "claimed", "signed", nonce=nonce,
                         tx_hash=tx_hash.lower(), unsigned_hash=unsigned_hash.lower())

    def broadcast_started(self, token_out: str, signal_id: str) -> None:
        self._transition(token_out, signal_id, "signed", "broadcast_started")

    def submitted(self, token_out: str, signal_id: str) -> None:
        self._transition(token_out, signal_id, "broadcast_started", "submitted")

    def receipt(self, token_out: str, signal_id: str, status: int) -> None:
        if status not in (0, 1):
            raise ValueError("rh_auto_receipt_status_invalid")
        target = "confirmed" if status == 1 else "failed"
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE rh_auto_buys SET state=?,receipt_status=?,updated_at_ms=? "
                "WHERE token_out=? AND signal_id=? AND state IN "
                "('signed','broadcast_started','submitted','uncertain') AND tx_hash IS NOT NULL",
                (target, status, int(time.time() * 1000), token_out.lower(), signal_id),
            ).rowcount
            if changed != 1:
                raise ValueError("rh_auto_receipt_transition_invalid")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def uncertain(self, token_out: str, signal_id: str) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE rh_auto_buys SET state='uncertain',updated_at_ms=? "
                "WHERE token_out=? AND signal_id=? AND state IN "
                "('signed','broadcast_started','submitted') AND tx_hash IS NOT NULL",
                (int(time.time() * 1000), token_out.lower(), signal_id),
            ).rowcount
            if changed != 1:
                raise ValueError("rh_auto_uncertain_transition_invalid")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _transition(self, token_out: str, signal_id: str, old: str, new: str,
                    *, nonce: int | None = None, tx_hash: str | None = None,
                    unsigned_hash: str | None = None) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE rh_auto_buys SET state=?,nonce=COALESCE(?,nonce),"
                "tx_hash=COALESCE(?,tx_hash),unsigned_hash=COALESCE(?,unsigned_hash),"
                "updated_at_ms=? WHERE token_out=? AND signal_id=? AND state=?",
                (new, nonce, tx_hash, unsigned_hash, int(time.time() * 1000),
                 token_out.lower(), signal_id, old),
            ).rowcount
            if changed != 1:
                raise ValueError("rh_auto_state_conflict")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
