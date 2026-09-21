"""Persistent EVM nonce and Solana blockhash leases in the execution journal."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fomo.signals.strategy import ExecutionIntent
from fomo.watching.rpc_transport import RpcTransport

from .capabilities import CapabilityStatus
from .journal import ExecutionJournal


def _quantity(value: Any) -> int:
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


class EvmNonceManager:
    def __init__(self, journal: ExecutionJournal, rpc: RpcTransport, *, chain_id: int,
                 account_id: str, wallet: str) -> None:
        self.journal = journal
        self.rpc = rpc
        self.chain_id = int(chain_id)
        self.account_id = account_id
        self.wallet = wallet

    def self_check(self) -> CapabilityStatus:
        try:
            actual = _quantity(self.rpc.call("eth_chainId", []))
            ready = actual == self.chain_id
        except Exception:
            ready = False
        return CapabilityStatus("nonce_blockhash_manager", True, ready,
                                "ok" if ready else "rpc_chain_verification_failed", {})

    def acquire(self, intent: ExecutionIntent) -> str:
        if str(intent.chain_id) != str(self.chain_id):
            raise ValueError("nonce_chain_mismatch")
        pending = _quantity(self.rpc.call("eth_getTransactionCount", [self.wallet, "pending"]))
        with self.journal._transaction():
            job = self.journal.db.execute(
                "SELECT account_id,state FROM execution_jobs WHERE intent_id=?", (intent.intent_id,)
            ).fetchone()
            if job is None or job["account_id"] != self.account_id or job["state"] != "quoted":
                raise ValueError("quoted_reserved_intent_required")
            existing = self.journal.db.execute(
                "SELECT nonce FROM execution_evm_nonces WHERE intent_id=?", (intent.intent_id,)
            ).fetchone()
            if existing:
                return str(existing["nonce"])
            local = self.journal.db.execute(
                "SELECT MAX(nonce) FROM execution_evm_nonces WHERE chain_id=? AND account_id=?",
                (str(self.chain_id), self.account_id),
            ).fetchone()[0]
            nonce = max(pending, int(local) + 1 if local is not None else pending)
            self.journal.db.execute(
                "INSERT INTO execution_evm_nonces VALUES(?,?,?,?,NULL)",
                (intent.intent_id, str(self.chain_id), self.account_id, nonce),
            )
            self.journal.db.execute(
                "INSERT INTO execution_nonce_lease_events"
                "(intent_id,chain_id,account_id,nonce,action,tx_hash,reason,recorded_at) "
                "VALUES(?,?,?,?,'acquired',NULL,'pending_nonce_and_local_leases',?)",
                (intent.intent_id, str(self.chain_id), self.account_id, nonce,
                 datetime.now(timezone.utc).isoformat()),
            )
            return str(nonce)

    def mark_used(self, intent_id: str, value: str, tx_hash: str) -> None:
        nonce = _quantity(value)
        with self.journal._transaction():
            row = self.journal.db.execute(
                "SELECT tx_hash FROM execution_evm_nonces WHERE intent_id=? AND chain_id=? AND account_id=? AND nonce=?",
                (intent_id, str(self.chain_id), self.account_id, nonce),
            ).fetchone()
            if row is None or (row["tx_hash"] and row["tx_hash"] != tx_hash):
                raise ValueError("nonce_lease_missing_or_conflicting")
            self.journal.db.execute(
                "UPDATE execution_evm_nonces SET tx_hash=? WHERE intent_id=? AND chain_id=? AND account_id=? AND nonce=?",
                (tx_hash, intent_id, str(self.chain_id), self.account_id, nonce),
            )
            if not row["tx_hash"]:
                self.journal.db.execute(
                    "INSERT INTO execution_nonce_lease_events"
                    "(intent_id,chain_id,account_id,nonce,action,tx_hash,reason,recorded_at) "
                    "VALUES(?,?,?,?,'bound',?,'signed_transaction_hash',?)",
                    (intent_id, str(self.chain_id), self.account_id, nonce, tx_hash,
                     datetime.now(timezone.utc).isoformat()),
                )


class SolanaBlockhashManager:
    def __init__(self, journal: ExecutionJournal, rpc: RpcTransport) -> None:
        self.journal = journal
        self.rpc = rpc

    def _latest(self) -> tuple[str, int]:
        result = self.rpc.call("getLatestBlockhash", [{"commitment": "confirmed"}])
        value = result["value"]
        return str(value["blockhash"]), int(value["lastValidBlockHeight"])

    def self_check(self) -> CapabilityStatus:
        try:
            blockhash, height = self._latest()
            ready = bool(blockhash and height > 0)
        except Exception:
            ready = False
        return CapabilityStatus("nonce_blockhash_manager", True, ready,
                                "ok" if ready else "solana_blockhash_unavailable", {})

    def acquire(self, intent: ExecutionIntent) -> str:
        if str(intent.chain_id) != "1399811149":
            raise ValueError("blockhash_chain_mismatch")
        blockhash, height = self._latest()
        with self.journal._transaction():
            job = self.journal.db.execute(
                "SELECT state FROM execution_jobs WHERE intent_id=?", (intent.intent_id,)
            ).fetchone()
            if job is None or job["state"] != "quoted":
                raise ValueError("quoted_reserved_intent_required")
            existing = self.journal.db.execute(
                "SELECT blockhash FROM execution_solana_blockhashes WHERE intent_id=?", (intent.intent_id,)
            ).fetchone()
            if existing:
                return str(existing["blockhash"])
            self.journal.db.execute(
                "INSERT INTO execution_solana_blockhashes VALUES(?,?,?,NULL)",
                (intent.intent_id, blockhash, height),
            )
        return blockhash

    def mark_used(self, intent_id: str, value: str, tx_hash: str) -> None:
        height = int(self.rpc.call("getBlockHeight", [{"commitment": "confirmed"}]))
        with self.journal._transaction():
            row = self.journal.db.execute(
                "SELECT tx_hash,last_valid_block_height FROM execution_solana_blockhashes "
                "WHERE intent_id=? AND blockhash=?", (intent_id, value),
            ).fetchone()
            if row is None or row["tx_hash"] and row["tx_hash"] != tx_hash:
                raise ValueError("blockhash_lease_missing_or_conflicting")
            if height > int(row["last_valid_block_height"]):
                raise ValueError("solana_blockhash_expired")
            self.journal.db.execute(
                "UPDATE execution_solana_blockhashes SET tx_hash=? WHERE intent_id=? AND blockhash=?",
                (tx_hash, intent_id, value),
            )
