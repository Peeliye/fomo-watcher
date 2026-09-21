"""Guarded chain broadcasters and receipt trackers using real JSON-RPC methods."""

from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from bip_utils import Base58Encoder

from fomo.watching.rpc_transport import RpcTransport, rpc_view

from .capabilities import CapabilityStatus
from .core import validate_serialized_transaction
from .evm_transaction import EvmTransactionParser, keccak256
from .interfaces import BroadcastResult, TransactionParser
from .journal import ExecutionJournal


def _quantity(value: Any) -> int:
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


class _GuardedBroadcaster:
    def __init__(self, journal: ExecutionJournal, rpc: RpcTransport, *, intent_id: str,
                 wallet: str, token_out: str, maximum_sell_amount: Decimal | str | int,
                 trusted_targets: set[str], parser: TransactionParser) -> None:
        self.journal = journal
        self.rpc = rpc
        self.intent_id = intent_id
        self.wallet = wallet
        self.token_out = token_out
        self.maximum_sell_amount = maximum_sell_amount
        self.trusted_targets = trusted_targets
        self.parser = parser

    def _scope(self, signed: bytes, chain_id: str) -> None:
        if not self.parser.self_check().ready:
            raise ValueError("transaction_scope_parser_unavailable")
        decision, _, _ = validate_serialized_transaction(
            signed, self.parser, expected_chain_id=chain_id,
            expected_wallet=self.wallet, expected_token_out=self.token_out,
            maximum_sell_amount=self.maximum_sell_amount, trusted_targets=self.trusted_targets,
        )
        if not decision.valid:
            raise ValueError("final_serialized_transaction_scope_rejected:" + ",".join(decision.reasons))

    def _control_ready(self) -> bool:
        row = self.journal.db.execute(
            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
        ).fetchone()
        return bool(row and row["live_armed"] and not row["circuit_breaker_tripped"])

    def _prebroadcast(self, signed: bytes, tx_hash: str) -> tuple[str, str, str]:
        digest = hashlib.sha256(signed).hexdigest()
        row = self.journal.db.execute(
            "SELECT provider,nonce_or_blockhash,serialized_tx_hash FROM execution_jobs WHERE intent_id=?",
            (self.intent_id,),
        ).fetchone()
        if row is None or row["serialized_tx_hash"] != digest:
            raise ValueError("signed_transaction_not_precommitted")
        self.journal.claim_broadcast_attempt(self.intent_id, tx_hash, digest)
        return str(row["provider"]), str(row["nonce_or_blockhash"]), digest


class EvmRpcBroadcaster(_GuardedBroadcaster):
    def __init__(self, journal: ExecutionJournal, rpc: RpcTransport, *, chain_id: int,
                 intent_id: str, wallet: str, token_out: str,
                 maximum_sell_amount: Decimal | str | int, trusted_targets: set[str]) -> None:
        super().__init__(journal, rpc, intent_id=intent_id, wallet=wallet, token_out=token_out,
                         maximum_sell_amount=maximum_sell_amount, trusted_targets=trusted_targets,
                         parser=EvmTransactionParser(trusted_targets))
        self.chain_id = int(chain_id)

    def self_check(self) -> CapabilityStatus:
        if not self._control_ready():
            return CapabilityStatus("broadcaster", True, False, "live_disabled_or_circuit_open", {})
        try:
            ready = _quantity(self.rpc.call("eth_chainId", [])) == self.chain_id
        except Exception:
            ready = False
        return CapabilityStatus("broadcaster", True, ready,
                                "ok" if ready else "rpc_chain_verification_failed", {})

    def transaction_id(self, signed_transaction: bytes) -> str:
        self._scope(signed_transaction, str(self.chain_id))
        return "0x" + keccak256(signed_transaction).hex()

    def broadcast(self, signed_transaction: bytes) -> BroadcastResult:
        tx_hash = self.transaction_id(signed_transaction)
        provider, nonce, digest = self._prebroadcast(signed_transaction, tx_hash)
        result = str(self.rpc.call("eth_sendRawTransaction", ["0x" + signed_transaction.hex()]))
        if result.lower() != tx_hash.lower():
            raise ValueError("rpc_broadcast_hash_mismatch")
        self.journal.persist_broadcast(self.intent_id, tx_hash, provider, nonce, digest)
        return BroadcastResult(tx_hash, provider, datetime.now(timezone.utc).isoformat())


def _compact_length(data: bytes) -> tuple[int, int]:
    value = 0
    shift = 0
    for index, byte in enumerate(data[:3]):
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            if value < 1 or value > 16:
                raise ValueError("invalid_solana_signature_count")
            return value, index + 1
        shift += 7
    raise ValueError("invalid_solana_compact_length")


class SolanaRpcBroadcaster(_GuardedBroadcaster):
    def __init__(self, journal: ExecutionJournal, rpc: RpcTransport, *, intent_id: str,
                 wallet: str, token_out: str, maximum_sell_amount: Decimal | str | int,
                 trusted_targets: set[str], parser: TransactionParser) -> None:
        super().__init__(journal, rpc, intent_id=intent_id, wallet=wallet, token_out=token_out,
                         maximum_sell_amount=maximum_sell_amount, trusted_targets=trusted_targets, parser=parser)

    def self_check(self) -> CapabilityStatus:
        if not self._control_ready() or not self.parser.self_check().ready:
            return CapabilityStatus("broadcaster", True, False, "live_or_scope_parser_unavailable", {})
        try:
            ready = int(self.rpc.call("getSlot", [{"commitment": "confirmed"}])) > 0
        except Exception:
            ready = False
        return CapabilityStatus("broadcaster", True, ready, "ok" if ready else "solana_rpc_unavailable", {})

    def transaction_id(self, signed_transaction: bytes) -> str:
        self._scope(signed_transaction, "1399811149")
        count, offset = _compact_length(signed_transaction)
        if offset + count * 64 >= len(signed_transaction):
            raise ValueError("truncated_solana_transaction")
        signature = signed_transaction[offset:offset + 64]
        if not any(signature):
            raise ValueError("unsigned_solana_transaction")
        return Base58Encoder.Encode(signature)

    def broadcast(self, signed_transaction: bytes) -> BroadcastResult:
        signature = self.transaction_id(signed_transaction)
        provider, blockhash, digest = self._prebroadcast(signed_transaction, signature)
        result = str(self.rpc.call("sendTransaction", [base64.b64encode(signed_transaction).decode(), {
            "encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed",
            "maxRetries": 0,
        }]))
        if result != signature:
            raise ValueError("rpc_broadcast_signature_mismatch")
        self.journal.persist_broadcast(self.intent_id, signature, provider, blockhash, digest)
        return BroadcastResult(signature, provider, datetime.now(timezone.utc).isoformat())


class EvmReceiptTracker:
    def __init__(self, rpc: RpcTransport, chain_id: int) -> None:
        self.rpc = rpc
        self.chain_id = int(chain_id)

    def self_check(self) -> CapabilityStatus:
        try:
            ready = _quantity(self.rpc.call("eth_chainId", [])) == self.chain_id
        except Exception:
            ready = False
        return CapabilityStatus("receipt_tracker", True, ready,
                                "ok" if ready else "rpc_chain_verification_failed", {})

    def receipt(self, tx_hash: str) -> Mapping[str, Any] | None:
        with rpc_view(self.rpc):
            return self._receipt(tx_hash)

    def _receipt(self, tx_hash: str) -> Mapping[str, Any] | None:
        value = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
        if value is None:
            return None
        if not isinstance(value, Mapping) or str(value.get("transactionHash") or "").lower() != tx_hash.lower():
            raise ValueError("receipt_identity_mismatch")
        block_number = _quantity(value["blockNumber"])
        block = self.rpc.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(block, Mapping) or block.get("hash") != value.get("blockHash"):
            return {"txHash": tx_hash, "chainId": self.chain_id, "status": "reorged"}
        try:
            finalized = self.rpc.call("eth_getBlockByNumber", ["finalized", False])
        except Exception:
            finalized = None
        finality = ("finalized" if isinstance(finalized, Mapping)
                    and _quantity(finalized["number"]) >= block_number else "confirmed")
        status = "confirmed" if _quantity(value["status"]) == 1 else "failed"
        return {"txHash": tx_hash, "chainId": self.chain_id, "status": status,
                "finality": finality, "blockNumber": block_number,
                "blockHash": value["blockHash"], "gasUsed": _quantity(value["gasUsed"]),
                "effectiveGasPrice": _quantity(value["effectiveGasPrice"]),
                "feeNativeWei": _quantity(value["gasUsed"]) * _quantity(value["effectiveGasPrice"]),
                "logs": value.get("logs") or []}


class SolanaReceiptTracker:
    def __init__(self, rpc: RpcTransport) -> None:
        self.rpc = rpc

    def self_check(self) -> CapabilityStatus:
        try:
            ready = int(self.rpc.call("getSlot", [{"commitment": "confirmed"}])) > 0
        except Exception:
            ready = False
        return CapabilityStatus("receipt_tracker", True, ready,
                                "ok" if ready else "solana_rpc_unavailable", {})

    def receipt(self, tx_hash: str) -> Mapping[str, Any] | None:
        with rpc_view(self.rpc):
            return self._receipt(tx_hash)

    def _receipt(self, tx_hash: str) -> Mapping[str, Any] | None:
        result = self.rpc.call("getSignatureStatuses", [[tx_hash], {"searchTransactionHistory": True}])
        statuses = result.get("value") if isinstance(result, Mapping) else None
        if not isinstance(statuses, list) or not statuses or statuses[0] is None:
            return None
        item = statuses[0]
        finality = str(item.get("confirmationStatus") or "processed")
        if finality not in {"confirmed", "finalized"}:
            return None
        transaction = self.rpc.call("getTransaction", [tx_hash, {
            "commitment": finality, "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
        }])
        if not isinstance(transaction, Mapping) or not isinstance(transaction.get("meta"), Mapping):
            return None
        if int(transaction.get("slot") or -1) != int(item.get("slot") or -2):
            raise ValueError("solana_receipt_slot_mismatch")
        tx = transaction.get("transaction")
        if not isinstance(tx, Mapping) or not tx.get("signatures") or tx["signatures"][0] != tx_hash:
            raise ValueError("solana_receipt_identity_mismatch")
        meta = transaction["meta"]
        return {"txHash": tx_hash, "chainId": 1399811149,
                "status": "failed" if meta.get("err") is not None else "confirmed",
                "finality": finality, "slot": int(transaction["slot"]),
                "feeLamports": int(meta.get("fee") or 0),
                "preTokenBalances": meta.get("preTokenBalances") or [],
                "postTokenBalances": meta.get("postTokenBalances") or [],
                "preBalances": meta.get("preBalances") or [], "postBalances": meta.get("postBalances") or []}
