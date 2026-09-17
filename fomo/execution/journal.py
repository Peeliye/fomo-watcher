"""Durable, read-only execution intent journal.

The journal deliberately has no transaction builder, signer, or broadcaster.
It makes every risk result auditable and keeps live execution fail-closed.
"""

from __future__ import annotations

import json
import sqlite3
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
PRAGMA user_version=2;
"""


class ExecutionJournal:
    def __init__(self, path: str | Path, account_id: str = "paper-main"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.account_id = account_id
        self.db = sqlite3.connect(self.path, timeout=5)
        self.db.row_factory = sqlite3.Row
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

    def close(self) -> None:
        self.db.close()

    def record_risk_decision(self, event: Any, decision: dict[str, Any] | None) -> dict[str, Any] | None:
        if not decision:
            return None
        signal_id = str(decision.get("signalId") or f"fomo:{event.id}")
        intent_id = f"intent:{signal_id}"
        blockers = [str(value) for value in decision.get("blockers", [])]
        outcome = str(decision.get("outcome") or "needs_data")
        state = "shadow_ready" if outcome == "approved_for_shadow" else "blocked"
        now = datetime.now(timezone.utc).isoformat()
        inserted = self.db.execute(
            """INSERT OR IGNORE INTO execution_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (intent_id, signal_id, str(event.id), now, now, self.account_id,
             str(getattr(event, "user_id", "") or ""), str(event.handle or ""), int(event.network_id or 0),
             str(event.ca or ""), str(event.symbol or "UNKNOWN"), "sell" if event.kind in {"sell", "clear"} else str(event.kind),
             _micros(event.amount_usd), state, outcome, json.dumps(blockers, ensure_ascii=False), 1),
        )
        if inserted.rowcount:
            self.db.execute(
                "INSERT INTO execution_transitions(intent_id,from_state,to_state,reason,recorded_at,metadata_json) VALUES(?,?,?,?,?,?)",
                (intent_id, None, state, blockers[0] if blockers else outcome, now,
                 json.dumps({"policyVersion": decision.get("policyVersion"), "registryVersion": decision.get("registryVersion")}, separators=(",", ":"))),
            )
            self.db.commit()
        return {"intentId": intent_id, "state": state, "duplicate": inserted.rowcount == 0}

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
        intent = self.db.execute(
            "SELECT state,chain_id FROM execution_intents WHERE intent_id=?", (intent_id,)
        ).fetchone()
        if intent is None:
            raise ValueError("receipt intent does not exist")
        chain_id = int(receipt.get("chainId") or 0)
        if chain_id != int(intent["chain_id"]):
            raise ValueError("receipt chain does not match intent")
        existing = self.db.execute(
            "SELECT intent_id,chain_id,status FROM execution_receipts WHERE tx_hash=?", (tx_hash,)
        ).fetchone()
        if existing is not None and (
            existing["intent_id"] != intent_id or int(existing["chain_id"]) != chain_id or existing["status"] != status
        ):
            raise ValueError("conflicting receipt already exists for txHash")
        now = datetime.now(timezone.utc).isoformat()
        inserted = self.db.execute(
            """INSERT OR IGNORE INTO execution_receipts
               (tx_hash,intent_id,chain_id,status,block_number,actual_usd_micros,fee_usd_micros,recorded_at,raw_reference)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (tx_hash, intent_id, chain_id, status, receipt.get("blockNumber"),
             _micros(receipt.get("actualUsd")), _micros(receipt.get("feeUsd")), now,
             str(receipt.get("rawReference") or "") or None),
        )
        if inserted.rowcount:
            target_state = "confirmed" if status == "confirmed" else "failed"
            self.db.execute(
                "UPDATE execution_intents SET state=?,updated_at=? WHERE intent_id=?", (target_state, now, intent_id)
            )
            self.db.execute(
                "INSERT INTO execution_transitions(intent_id,from_state,to_state,reason,recorded_at,metadata_json) VALUES(?,?,?,?,?,?)",
                (intent_id, intent["state"], target_state, f"receipt_{status}", now,
                 json.dumps({"txHash": tx_hash, "blockNumber": receipt.get("blockNumber")}, separators=(",", ":"))),
            )
            self.db.commit()
        return {"txHash": tx_hash, "intentId": intent_id, "status": status, "duplicate": inserted.rowcount == 0}


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
