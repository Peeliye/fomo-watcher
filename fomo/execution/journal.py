"""Durable execution state machine and atomic reservation journal.

No adapter in this module signs or broadcasts. ``live_armed`` remains locked to
zero by default; external execution services must pass every capability and
preflight check before using the state-machine methods.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any


MICROS = Decimal("1000000")


def _micros(value: Any) -> int:
    try:
        return int((Decimal(str(value or 0)) * MICROS).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except Exception:
        return 0


SCHEMA = """
CREATE TABLE IF NOT EXISTS execution_intents (
  intent_id TEXT PRIMARY KEY,
  signal_id TEXT NOT NULL UNIQUE,
  event_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  account_id TEXT NOT NULL,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  requested_usd_micros INTEGER NOT NULL,
  state TEXT NOT NULL,
  risk_outcome TEXT NOT NULL,
  blockers_json TEXT NOT NULL,
  read_only INTEGER NOT NULL CHECK(read_only=1)
);
CREATE TABLE IF NOT EXISTS execution_transitions (
  transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  FOREIGN KEY(intent_id) REFERENCES execution_intents(intent_id)
);
CREATE TABLE IF NOT EXISTS execution_control (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  live_armed INTEGER NOT NULL CHECK(live_armed=0),
  circuit_breaker_tripped INTEGER NOT NULL,
  breaker_reason TEXT NOT NULL,
  consecutive_failures INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_execution_state_time ON execution_intents(state,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_chain_time ON execution_intents(chain_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_created ON execution_intents(created_at DESC);
CREATE TABLE IF NOT EXISTS execution_receipts (
  tx_hash TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('confirmed','failed')),
  block_number INTEGER,
  actual_usd_micros INTEGER NOT NULL,
  fee_usd_micros INTEGER NOT NULL,
  recorded_at TEXT NOT NULL,
  raw_reference TEXT,
  FOREIGN KEY(intent_id) REFERENCES execution_intents(intent_id)
);
CREATE INDEX IF NOT EXISTS idx_receipts_intent ON execution_receipts(intent_id,recorded_at DESC);
CREATE TABLE IF NOT EXISTS execution_jobs (
  intent_id TEXT PRIMARY KEY,
  signal_id TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('buy','sell')),
  token_in TEXT NOT NULL,
  token_out TEXT NOT NULL,
  requested_usd_micros INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN (
    'reserved','quoted','simulated','built','signed','submitted','broadcast',
    'confirmed','failed','replaced','dropped','expired','reorged'
  )),
  tx_hash TEXT UNIQUE,
  serialized_tx_hash TEXT,
  provider TEXT,
  nonce_or_blockhash TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_execution_jobs_recovery
  ON execution_jobs(state,updated_at,intent_id);
CREATE TABLE IF NOT EXISTS capital_accounts (
  account_id TEXT PRIMARY KEY,
  available_usd_micros INTEGER NOT NULL,
  reserved_usd_micros INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_reservations (
  intent_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  amount_usd_micros INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','consumed','released','reversed')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(intent_id) REFERENCES execution_jobs(intent_id),
  FOREIGN KEY(account_id) REFERENCES capital_accounts(account_id)
);
CREATE TABLE IF NOT EXISTS execution_artifacts (
  intent_id TEXT PRIMARY KEY,
  quote_json TEXT,
  simulation_json TEXT,
  serialized_tx_hash TEXT,
  provider TEXT,
  nonce_or_blockhash TEXT,
  receipt_json TEXT,
  fee_usd_micros INTEGER,
  token_delta_json TEXT,
  native_delta TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(intent_id) REFERENCES execution_jobs(intent_id)
);
CREATE TABLE IF NOT EXISTS execution_broadcast_attempts (
  intent_id TEXT PRIMARY KEY,
  tx_hash TEXT NOT NULL UNIQUE,
  signed_tx_hash TEXT NOT NULL,
  attempted_at TEXT NOT NULL,
  FOREIGN KEY(intent_id) REFERENCES execution_jobs(intent_id)
);
CREATE TABLE IF NOT EXISTS execution_evm_nonces (
  intent_id TEXT PRIMARY KEY, chain_id TEXT NOT NULL, account_id TEXT NOT NULL,
  nonce INTEGER NOT NULL, tx_hash TEXT,
  UNIQUE(chain_id,account_id,nonce),
  FOREIGN KEY(intent_id) REFERENCES execution_jobs(intent_id)
);
CREATE TABLE IF NOT EXISTS execution_nonce_lease_events (
  lease_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL, chain_id TEXT NOT NULL, account_id TEXT NOT NULL,
  nonce INTEGER NOT NULL, action TEXT NOT NULL CHECK(action IN ('acquired','bound','released')),
  tx_hash TEXT, reason TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nonce_lease_events_intent ON execution_nonce_lease_events(intent_id,lease_event_id);
CREATE TABLE IF NOT EXISTS execution_source_reorg_fences (
  signal_id TEXT PRIMARY KEY, observed_at TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_solana_blockhashes (
  intent_id TEXT PRIMARY KEY, blockhash TEXT NOT NULL,
  last_valid_block_height INTEGER NOT NULL, tx_hash TEXT,
  FOREIGN KEY(intent_id) REFERENCES execution_jobs(intent_id)
);
PRAGMA user_version=7;
"""

JOB_TRANSITIONS: dict[str, frozenset[str]] = {
    "reserved": frozenset({"quoted", "failed", "dropped", "expired"}),
    "quoted": frozenset({"simulated", "failed", "dropped", "expired"}),
    "simulated": frozenset({"built", "failed", "dropped", "expired"}),
    "built": frozenset({"signed", "failed", "expired"}),
    "signed": frozenset({"submitted", "failed", "expired"}),
    "submitted": frozenset({"broadcast", "failed", "replaced", "dropped", "expired"}),
    "broadcast": frozenset({"confirmed", "failed", "replaced", "dropped", "expired", "reorged"}),
    "confirmed": frozenset({"reorged"}),
    "replaced": frozenset({"broadcast", "confirmed", "failed", "expired"}),
    "reorged": frozenset({"broadcast", "failed", "dropped"}),
    "failed": frozenset(), "dropped": frozenset(), "expired": frozenset(),
}


class ExecutionJournal:
    def __init__(self, path: str | Path, account_id: str = "paper-main"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.account_id = account_id
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._write_lock = threading.RLock()
        version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if version > 7:
            self.db.close()
            raise ValueError("unsupported_execution_journal_schema")
        if existed and version < 7:
            directory = self.path.parent / "backups"
            directory.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            target = sqlite3.connect(directory / f"{self.path.stem}.pre-v7.from-v{version}.{stamp}.sqlite3")
            try:
                self.db.backup(target)
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("execution migration backup integrity check failed")
            finally:
                target.close()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        now = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            "INSERT OR IGNORE INTO execution_control VALUES(1,0,1,'startup_read_only',0,?)", (now,)
        )
        self.db.commit()

    def set_capital_snapshot(self, account_id: str, available_usd: Any) -> None:
        """Store a trusted balance snapshot; reservations never infer zero gas/balance."""
        amount = _micros(available_usd)
        if amount < 0:
            raise ValueError("available balance must be non-negative")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            self.db.execute(
                "INSERT INTO capital_accounts VALUES(?,?,0,?) ON CONFLICT(account_id) DO UPDATE SET "
                "available_usd_micros=excluded.available_usd_micros,updated_at=excluded.updated_at",
                (account_id, amount, now),
            )

    def reserve_intent(self, intent: Any) -> dict[str, Any]:
        """Atomically dedupe the signal and reserve capital across all sources."""
        intent_id = str(intent.intent_id)
        signal_id = str(intent.signal_id)
        account_id = str(getattr(intent, "account_id", self.account_id))
        amount = _micros(getattr(intent, "requested_usd", 0))
        if amount < 0:
            raise ValueError("reservation amount must be non-negative")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            if self.db.execute(
                "SELECT 1 FROM execution_source_reorg_fences WHERE signal_id=?", (signal_id,)
            ).fetchone():
                raise ValueError("source_signal_reorged")
            existing = self.db.execute(
                "SELECT intent_id,state FROM execution_jobs WHERE signal_id=? OR intent_id=?", (signal_id, intent_id)
            ).fetchone()
            if existing:
                return {"intentId": str(existing["intent_id"]), "state": str(existing["state"]), "duplicate": True}
            capital = self.db.execute(
                "SELECT available_usd_micros,reserved_usd_micros FROM capital_accounts WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if capital is None:
                raise ValueError("trusted_balance_snapshot_required")
            spendable = int(capital["available_usd_micros"]) - int(capital["reserved_usd_micros"])
            if amount > spendable:
                raise ValueError("insufficient_unreserved_balance")
            self.db.execute(
                "INSERT INTO execution_jobs(intent_id,signal_id,source,account_id,chain_id,side,token_in,token_out,"
                "requested_usd_micros,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'reserved',?,?)",
                (intent_id, signal_id, str(intent.source), account_id, str(intent.chain_id), str(intent.side),
                 str(intent.token_in), str(intent.token_out), amount, now, now),
            )
            self.db.execute(
                "UPDATE capital_accounts SET reserved_usd_micros=reserved_usd_micros+?,updated_at=? WHERE account_id=?",
                (amount, now, account_id),
            )
            self.db.execute(
                "INSERT INTO execution_reservations VALUES(?,?,?,'active',?,?)",
                (intent_id, account_id, amount, now, now),
            )
        return {"intentId": intent_id, "state": "reserved", "duplicate": False}

    def record_source_reorg(self, signal_id: str) -> None:
        """Durably fence a reverted wallet signal before any further submission."""
        if not signal_id:
            raise ValueError("reorg_signal_id_required")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            self.db.execute(
                "INSERT OR IGNORE INTO execution_source_reorg_fences VALUES(?,?,'wallet_source_reorg')",
                (signal_id, now),
            )
            self.db.execute(
                "UPDATE execution_control SET circuit_breaker_tripped=1,breaker_reason='wallet_source_reorg',"
                "updated_at=? WHERE singleton=1", (now,),
            )

    def source_reorged(self, signal_id: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM execution_source_reorg_fences WHERE signal_id=?", (signal_id,)
        ).fetchone() is not None

    def transition_job(self, intent_id: str, to_state: str, *, metadata: dict[str, Any] | None = None) -> None:
        if to_state in {"submitted", "broadcast"}:
            raise ValueError("submission transitions require a precommitted transaction hash")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            row = self.db.execute("SELECT state FROM execution_jobs WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                raise ValueError("execution intent does not exist")
            current = str(row["state"])
            if to_state == current:
                return
            if to_state not in JOB_TRANSITIONS.get(current, frozenset()):
                raise ValueError(f"invalid execution transition: {current}->{to_state}")
            self.db.execute("UPDATE execution_jobs SET state=?,updated_at=? WHERE intent_id=?", (to_state, now, intent_id))
            if metadata is not None:
                fields = {
                    "quote_json": metadata.get("quote"), "simulation_json": metadata.get("simulation"),
                    "serialized_tx_hash": metadata.get("serializedTxHash"), "provider": metadata.get("provider"),
                    "nonce_or_blockhash": metadata.get("nonceOrBlockhash"),
                }
                encoded = {key: json.dumps(value, separators=(",", ":")) if isinstance(value, (dict, list)) else value
                           for key, value in fields.items()}
                self.db.execute(
                    "INSERT INTO execution_artifacts(intent_id,quote_json,simulation_json,serialized_tx_hash,provider,"
                    "nonce_or_blockhash,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(intent_id) DO UPDATE SET "
                    "quote_json=coalesce(excluded.quote_json,quote_json),simulation_json=coalesce(excluded.simulation_json,simulation_json),"
                    "serialized_tx_hash=coalesce(excluded.serialized_tx_hash,serialized_tx_hash),provider=coalesce(excluded.provider,provider),"
                    "nonce_or_blockhash=coalesce(excluded.nonce_or_blockhash,nonce_or_blockhash),updated_at=excluded.updated_at",
                    (intent_id, encoded["quote_json"], encoded["simulation_json"], encoded["serialized_tx_hash"],
                     encoded["provider"], encoded["nonce_or_blockhash"], now),
                )

    def fail_unsubmitted_job(self, intent_id: str, *, reason: str) -> None:
        """Release only a pre-broadcast reservation; an uncertain send needs reconciliation."""
        if not reason:
            raise ValueError("failure reason required")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            job = self.db.execute(
                "SELECT state,tx_hash FROM execution_jobs WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if job is None or job["tx_hash"] or job["state"] not in {
                "reserved", "quoted", "simulated", "built", "signed"
            }:
                raise ValueError("unsubmitted_job_required_for_release")
            attempt = self.db.execute(
                "SELECT 1 FROM execution_broadcast_attempts WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if attempt is not None:
                raise ValueError("broadcast_attempt_may_have_reached_chain")
            reservation = self.db.execute(
                "SELECT account_id,amount_usd_micros,status FROM execution_reservations WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if reservation is None or reservation["status"] != "active":
                raise ValueError("active_reservation_required")
            self.db.execute(
                "UPDATE capital_accounts SET reserved_usd_micros=reserved_usd_micros-?,updated_at=? "
                "WHERE account_id=? AND reserved_usd_micros>=?",
                (reservation["amount_usd_micros"], now, reservation["account_id"],
                 reservation["amount_usd_micros"]),
            )
            if self.db.execute("SELECT changes()").fetchone()[0] != 1:
                raise ValueError("capital_reservation_inconsistent")
            self.db.execute(
                "UPDATE execution_reservations SET status='released',updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            nonce = self.db.execute(
                "SELECT chain_id,account_id,nonce,tx_hash FROM execution_evm_nonces WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if nonce is not None:
                if nonce["tx_hash"]:
                    raise ValueError("signed_nonce_requires_manual_reconciliation")
                self.db.execute(
                    "INSERT INTO execution_nonce_lease_events"
                    "(intent_id,chain_id,account_id,nonce,action,tx_hash,reason,recorded_at) "
                    "VALUES(?,?,?,?,'released',?,?,?)",
                    (intent_id, nonce["chain_id"], nonce["account_id"], nonce["nonce"],
                     nonce["tx_hash"], reason, now),
                )
                self.db.execute("DELETE FROM execution_evm_nonces WHERE intent_id=?", (intent_id,))
            self.db.execute(
                "UPDATE execution_jobs SET state='failed',updated_at=? WHERE intent_id=?", (now, intent_id)
            )
            self.db.execute(
                "INSERT INTO execution_artifacts(intent_id,simulation_json,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(intent_id) DO UPDATE SET simulation_json=excluded.simulation_json,updated_at=excluded.updated_at",
                (intent_id, json.dumps({"failureReason": reason}, separators=(",", ":")), now),
            )

    def claim_submission(self, intent_id: str, tx_hash: str, provider: str,
                         nonce_or_blockhash: str, serialized_tx_hash: str) -> dict[str, Any]:
        """Commit the signed transaction ID before any network broadcast.

        A repeated claim is not permission to broadcast again. The caller must
        recover by querying the chain for the persisted ID.
        """
        if not all((tx_hash, provider, nonce_or_blockhash)) or len(serialized_tx_hash) != 64:
            raise ValueError("signed transaction identity and SHA-256 are required")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            row = self.db.execute(
                "SELECT state,tx_hash,provider,nonce_or_blockhash,serialized_tx_hash "
                "FROM execution_jobs WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if row is None:
                raise ValueError("execution intent does not exist")
            if row["tx_hash"]:
                if str(row["tx_hash"]) != tx_hash:
                    raise ValueError("intent already has a different transaction hash")
                if (row["provider"], row["nonce_or_blockhash"], row["serialized_tx_hash"]) != (
                    provider, nonce_or_blockhash, serialized_tx_hash
                ):
                    raise ValueError("existing submission identity mismatch")
                return {"intentId": intent_id, "txHash": tx_hash, "duplicate": True}
            if str(row["state"]) != "signed":
                raise ValueError("intent must be signed before submission claim")
            if self.db.execute(
                "SELECT 1 FROM execution_source_reorg_fences WHERE signal_id="
                "(SELECT signal_id FROM execution_jobs WHERE intent_id=?)", (intent_id,),
            ).fetchone():
                raise ValueError("source_signal_reorged")
            self.db.execute(
                "UPDATE execution_jobs SET state='submitted',tx_hash=?,serialized_tx_hash=?,provider=?,"
                "nonce_or_blockhash=?,updated_at=? WHERE intent_id=?",
                (tx_hash, serialized_tx_hash, provider, nonce_or_blockhash, now, intent_id),
            )
            self.db.execute(
                "INSERT INTO execution_artifacts(intent_id,serialized_tx_hash,provider,nonce_or_blockhash,updated_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(intent_id) DO UPDATE SET "
                "serialized_tx_hash=excluded.serialized_tx_hash,provider=excluded.provider,"
                "nonce_or_blockhash=excluded.nonce_or_blockhash,updated_at=excluded.updated_at",
                (intent_id, serialized_tx_hash, provider, nonce_or_blockhash, now),
            )
        return {"intentId": intent_id, "txHash": tx_hash, "duplicate": False}

    def persist_broadcast(self, intent_id: str, tx_hash: str, provider: str,
                          nonce_or_blockhash: str, serialized_tx_hash: str) -> dict[str, Any]:
        """Mark a preclaimed submission broadcast; never create its hash here."""
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            row = self.db.execute(
                "SELECT state,tx_hash,provider,nonce_or_blockhash,serialized_tx_hash "
                "FROM execution_jobs WHERE intent_id=?", (intent_id,),
            ).fetchone()
            if row is None:
                raise ValueError("execution intent does not exist")
            if (row["tx_hash"], row["provider"], row["nonce_or_blockhash"], row["serialized_tx_hash"]) != (
                tx_hash, provider, nonce_or_blockhash, serialized_tx_hash
            ):
                raise ValueError("broadcast identity differs from precommitted submission")
            if row["state"] == "broadcast":
                return {"intentId": intent_id, "txHash": tx_hash, "duplicate": True}
            if row["state"] != "submitted":
                raise ValueError("intent must have a precommitted submission")
            self.db.execute("UPDATE execution_jobs SET state='broadcast',updated_at=? WHERE intent_id=?",
                            (now, intent_id))
        return {"intentId": intent_id, "txHash": tx_hash, "duplicate": False}

    def claim_broadcast_attempt(self, intent_id: str, tx_hash: str, signed_tx_hash: str) -> None:
        """One durable attempt only, committed before the external RPC call.

        If the process dies after this commit, recovery checks the receipt and
        never sends the same intent again, even if the first send was uncertain.
        """
        if len(signed_tx_hash) != 64:
            raise ValueError("signed transaction SHA-256 required")
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            if self.db.execute(
                "SELECT 1 FROM execution_source_reorg_fences WHERE signal_id="
                "(SELECT signal_id FROM execution_jobs WHERE intent_id=?)", (intent_id,),
            ).fetchone():
                raise ValueError("source_signal_reorged")
            control = self.db.execute(
                "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
            ).fetchone()
            if control is None or not control["live_armed"] or control["circuit_breaker_tripped"]:
                raise ValueError("live_execution_disabled")
            row = self.db.execute(
                "SELECT state,tx_hash,serialized_tx_hash FROM execution_jobs WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if (row is None or row["state"] != "submitted" or row["tx_hash"] != tx_hash
                    or row["serialized_tx_hash"] != signed_tx_hash):
                raise ValueError("precommitted_submission_required")
            try:
                self.db.execute(
                    "INSERT INTO execution_broadcast_attempts VALUES(?,?,?,?)",
                    (intent_id, tx_hash, signed_tx_hash, now),
                )
            except sqlite3.IntegrityError as error:
                raise ValueError("broadcast_attempt_already_recorded") from error

    def recoverable_broadcasts(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT intent_id,chain_id,tx_hash,state,provider,nonce_or_blockhash FROM execution_jobs "
            "WHERE tx_hash IS NOT NULL AND state IN ('submitted','broadcast','replaced','reorged') ORDER BY updated_at"
        ).fetchall()
        return [dict(row) for row in rows]

    def reconcile_job_receipt(self, intent_id: str, receipt: dict[str, Any]) -> None:
        """Persist independently tracked finality without inventing fills/deltas.

        Confirmed reservations remain held until receipt token deltas have been
        reconciled to the actual-wallet ledger. A finalized failure releases
        the reservation atomically. Reorgs retain the hold for investigation.
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._transaction():
            row = self.db.execute(
                "SELECT state,tx_hash,chain_id FROM execution_jobs WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if row is None or not row["tx_hash"] or row["tx_hash"] != receipt.get("txHash"):
                raise ValueError("receipt_job_identity_mismatch")
            if str(row["chain_id"]) != str(receipt.get("chainId")):
                raise ValueError("receipt_chain_mismatch")
            status = str(receipt.get("status") or "")
            finality = str(receipt.get("finality") or "")
            if status == "reorged":
                if row["state"] not in {"broadcast", "confirmed", "submitted", "reorged"}:
                    raise ValueError("reorg_job_state_invalid")
                target = "reorged"
            elif status in {"confirmed", "failed"} and finality == "finalized":
                if row["state"] not in {"broadcast", "submitted", "replaced", "reorged", status}:
                    raise ValueError("receipt_job_state_invalid")
                target = status
            else:
                raise ValueError("finalized_receipt_required")
            if row["state"] == target:
                return
            self.db.execute("UPDATE execution_jobs SET state=?,updated_at=? WHERE intent_id=?", (target, now, intent_id))
            self.db.execute(
                "INSERT INTO execution_artifacts(intent_id,receipt_json,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(intent_id) DO UPDATE SET receipt_json=excluded.receipt_json,updated_at=excluded.updated_at",
                (intent_id, json.dumps(receipt, separators=(",", ":")), now),
            )
            if target == "failed":
                reservation = self.db.execute(
                    "SELECT account_id,amount_usd_micros,status FROM execution_reservations WHERE intent_id=?",
                    (intent_id,),
                ).fetchone()
                if reservation is None or reservation["status"] != "active":
                    raise ValueError("active_reservation_required")
                self.db.execute(
                    "UPDATE capital_accounts SET reserved_usd_micros=reserved_usd_micros-?,updated_at=? "
                    "WHERE account_id=? AND reserved_usd_micros>=?",
                    (reservation["amount_usd_micros"], now, reservation["account_id"],
                     reservation["amount_usd_micros"]),
                )
                if self.db.execute("SELECT changes()").fetchone()[0] != 1:
                    raise ValueError("capital_reservation_inconsistent")
                self.db.execute(
                    "UPDATE execution_reservations SET status='released',updated_at=? WHERE intent_id=?",
                    (now, intent_id),
                )

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _transaction(self):
        """Leave the reusable connection clean after every write attempt."""
        with self._write_lock:
            if self.db.in_transaction:
                self.db.rollback()
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self.db.rollback()
                raise
            else:
                self.db.commit()

    def record_risk_decision(self, event: Any, decision: dict[str, Any] | None) -> dict[str, Any] | None:
        if not decision:
            return None
        signal_id = str(decision.get("signalId") or f"fomo:{event.id}")
        intent_id = f"intent:{signal_id}"
        blockers = [str(value) for value in decision.get("blockers", [])]
        outcome = str(decision.get("outcome") or "needs_data")
        phase = str(decision.get("phase") or "pre_trade")
        state = (
            "post_trade_observed" if phase == "post_trade"
            else "shadow_ready" if outcome == "approved_for_shadow"
            else "blocked"
        )
        now = datetime.now(timezone.utc).isoformat()
        inserted_count = 0
        with self._transaction():
            inserted = self.db.execute(
                """INSERT OR IGNORE INTO execution_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (intent_id, signal_id, str(event.id), now, now, self.account_id,
                 str(getattr(event, "user_id", "") or ""), str(event.handle or ""), int(event.network_id or 0),
                 str(event.ca or ""), str(event.symbol or "UNKNOWN"), "sell" if event.kind in {"sell", "clear"} else str(event.kind),
                 _micros(event.amount_usd), state, outcome, json.dumps(blockers, ensure_ascii=False), 1),
            )
            inserted_count = inserted.rowcount
            if inserted_count:
                self.db.execute(
                    "INSERT INTO execution_transitions(intent_id,from_state,to_state,reason,recorded_at,metadata_json) VALUES(?,?,?,?,?,?)",
                    (intent_id, None, state, blockers[0] if blockers else outcome, now,
                     json.dumps({"policyVersion": decision.get("policyVersion"), "registryVersion": decision.get("registryVersion"), "phase": phase}, separators=(",", ":"))),
                )
        return {"intentId": intent_id, "state": state, "duplicate": inserted_count == 0}

    def record_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """Persist a verified chain receipt and reconcile its execution intent.

        The caller must obtain the receipt from an authenticated RPC/indexer. Raw
        payloads are deliberately not stored; ``rawReference`` may identify the
        external evidence without leaking credentials.
        """
        tx_hash = str(receipt.get("txHash") or "").strip()
        intent_id = str(receipt.get("intentId") or "").strip()
        status = str(receipt.get("status") or "").lower()
        if not tx_hash or not intent_id or status not in {"confirmed", "failed"}:
            raise ValueError("receipt requires txHash, intentId and confirmed/failed status")
        chain_id = int(receipt.get("chainId") or 0)
        now = datetime.now(timezone.utc).isoformat()
        inserted_count = 0
        with self._transaction():
            intent = self.db.execute(
                "SELECT state,chain_id FROM execution_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if intent is None:
                raise ValueError("receipt intent does not exist")
            if chain_id != int(intent["chain_id"]):
                raise ValueError("receipt chain does not match intent")
            existing = self.db.execute(
                "SELECT intent_id,chain_id,status FROM execution_receipts WHERE tx_hash=?", (tx_hash,)
            ).fetchone()
            if existing is not None and (
                existing["intent_id"] != intent_id or int(existing["chain_id"]) != chain_id or existing["status"] != status
            ):
                raise ValueError("conflicting receipt already exists for txHash")
            inserted = self.db.execute(
                """INSERT OR IGNORE INTO execution_receipts
                   (tx_hash,intent_id,chain_id,status,block_number,actual_usd_micros,fee_usd_micros,recorded_at,raw_reference)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (tx_hash, intent_id, chain_id, status, receipt.get("blockNumber"),
                 _micros(receipt.get("actualUsd")), _micros(receipt.get("feeUsd")), now,
                 str(receipt.get("rawReference") or "") or None),
            )
            inserted_count = inserted.rowcount
            if inserted_count:
                target_state = "confirmed" if status == "confirmed" else "failed"
                self.db.execute(
                    "UPDATE execution_intents SET state=?,updated_at=? WHERE intent_id=?", (target_state, now, intent_id)
                )
                self.db.execute(
                    "INSERT INTO execution_transitions(intent_id,from_state,to_state,reason,recorded_at,metadata_json) VALUES(?,?,?,?,?,?)",
                    (intent_id, intent["state"], target_state, f"receipt_{status}", now,
                     json.dumps({"txHash": tx_hash, "blockNumber": receipt.get("blockNumber")}, separators=(",", ":"))),
                )
        return {"txHash": tx_hash, "intentId": intent_id, "status": status, "duplicate": inserted_count == 0}


def reconciliation_snapshot(path: str | Path, limit: int = 100) -> dict[str, Any]:
    db_path = Path(path)
    empty = {"receipts": 0, "confirmed": 0, "failed": 0, "unreconciled": 0, "items": []}
    if not db_path.exists():
        return empty
    db = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    try:
        receipts = int(db.execute("SELECT COUNT(*) FROM execution_receipts").fetchone()[0])
        confirmed = int(db.execute("SELECT COUNT(*) FROM execution_receipts WHERE status='confirmed'").fetchone()[0])
        failed = int(db.execute("SELECT COUNT(*) FROM execution_receipts WHERE status='failed'").fetchone()[0])
        unreconciled = int(db.execute(
            "SELECT COUNT(*) FROM execution_intents i LEFT JOIN execution_receipts r ON r.intent_id=i.intent_id "
            "WHERE i.state IN ('submitted','broadcast') AND r.intent_id IS NULL"
        ).fetchone()[0])
        rows = db.execute(
            "SELECT tx_hash,intent_id,chain_id,status,block_number,actual_usd_micros,fee_usd_micros,recorded_at,raw_reference "
            "FROM execution_receipts ORDER BY recorded_at DESC LIMIT ?", (max(1, min(int(limit), 1000)),)
        ).fetchall()
    except sqlite3.OperationalError:
        db.close()
        return empty
    db.close()
    return {"receipts": receipts, "confirmed": confirmed, "failed": failed, "unreconciled": unreconciled, "items": [
        {"txHash": row["tx_hash"], "intentId": row["intent_id"], "chainId": row["chain_id"],
         "status": row["status"], "blockNumber": row["block_number"],
         "actualUsd": float(Decimal(row["actual_usd_micros"]) / MICROS),
         "feeUsd": float(Decimal(row["fee_usd_micros"]) / MICROS), "recordedAt": row["recorded_at"],
         "rawReference": row["raw_reference"]} for row in rows
    ]}


def execution_snapshot(path: str | Path, limit: int = 100) -> dict[str, Any]:
    db_path = Path(path)
    empty = {"total": 0, "states": {}, "control": {"liveArmed": False, "circuitBreakerTripped": True, "breakerReason": "journal_unavailable"}, "intents": []}
    if not db_path.exists():
        return empty
    db = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    try:
        total = int(db.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0])
        states = {row["state"]: int(row["count"]) for row in db.execute("SELECT state,COUNT(*) count FROM execution_intents GROUP BY state")}
        control = db.execute("SELECT * FROM execution_control WHERE singleton=1").fetchone()
        rows = db.execute("SELECT * FROM execution_intents ORDER BY created_at DESC,rowid DESC LIMIT ?", (max(1, min(int(limit), 1000)),)).fetchall()
    except sqlite3.OperationalError:
        db.close()
        return empty
    db.close()
    return {
        "total": total, "states": states,
        "control": {"liveArmed": bool(control["live_armed"]), "circuitBreakerTripped": bool(control["circuit_breaker_tripped"]), "breakerReason": control["breaker_reason"], "consecutiveFailures": control["consecutive_failures"], "updatedAt": control["updated_at"]},
        "intents": [{"intentId": row["intent_id"], "signalId": row["signal_id"], "createdAt": row["created_at"], "handle": row["handle"], "kolId": row["kol_id"], "networkId": row["chain_id"], "ca": row["token_address"], "symbol": row["symbol"], "side": row["side"], "requestedUsd": float(Decimal(row["requested_usd_micros"]) / MICROS), "state": row["state"], "riskOutcome": row["risk_outcome"], "blockers": json.loads(row["blockers_json"]), "readOnly": bool(row["read_only"])} for row in rows],
    }
