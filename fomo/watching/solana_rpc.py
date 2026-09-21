"""Bounded Solana signature/transaction stream; no startup history scan."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .adapter import ChainCheckpoint
from .rpc_transport import RpcTransport, RpcUnavailable, rpc_view


def _keys(message: Mapping[str, Any]) -> list[str]:
    return [str(value.get("pubkey") if isinstance(value, Mapping) else value)
            for value in message.get("accountKeys") or []]


def _program_ids(instructions: Sequence[Any], keys: Sequence[str]) -> list[str]:
    output = []
    for item in instructions:
        if not isinstance(item, Mapping):
            continue
        program = item.get("programId")
        if program is None and item.get("programIdIndex") is not None:
            index = int(item["programIdIndex"])
            program = keys[index] if 0 <= index < len(keys) else None
        if program:
            output.append(str(program))
    return output


def _balances(rows: Any, actor: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows or []:
        if not isinstance(row, Mapping) or str(row.get("owner") or "") != actor:
            continue
        amount = row.get("uiTokenAmount")
        if not isinstance(amount, Mapping) or not row.get("mint"):
            continue
        mint = str(row["mint"])
        result[mint] = result.get(mint, 0) + int(amount["amount"])
    return result


class SolanaRpcProvider:
    def __init__(self, rpc: RpcTransport, *, allowed_program_ids: set[str],
                 quote_assets: Mapping[str, tuple[int, Decimal]] | None = None,
                 maximum_slot_gap: int = 128, signature_page_limit: int = 1000) -> None:
        self.rpc = rpc
        self.allowed_program_ids = set(allowed_program_ids)
        self.quote_assets = {str(key): (int(value[0]), Decimal(value[1]))
                             for key, value in (quote_assets or {}).items()}
        self.maximum_slot_gap = max(1, int(maximum_slot_gap))
        self.signature_page_limit = max(1, min(1000, int(signature_page_limit)))

    def _block(self, slot: int) -> Mapping[str, Any] | None:
        value = self.rpc.call("getBlock", [slot, {"commitment": "confirmed", "transactionDetails": "none",
                                                   "rewards": False, "maxSupportedTransactionVersion": 0}])
        return value if isinstance(value, Mapping) else None

    def head(self) -> ChainCheckpoint:
        latest = int(self.rpc.call("getSlot", [{"commitment": "confirmed"}]))
        for slot in range(latest, max(-1, latest - 32), -1):
            block = self._block(slot)
            if block and block.get("blockhash"):
                return ChainCheckpoint("1399811149", str(slot), slot, str(block["blockhash"]))
        raise RpcUnavailable("solana_confirmed_head_unavailable")

    def canonical_hash(self, block_number: int) -> str | None:
        block = self._block(block_number)
        return str(block["blockhash"]) if block and block.get("blockhash") else None

    def _estimated_usd(self, before: Mapping[str, int], after: Mapping[str, int]) -> str | None:
        candidates = []
        for mint, (decimals, usd_price) in self.quote_assets.items():
            delta = after.get(mint, 0) - before.get(mint, 0)
            if delta:
                candidates.append((delta, decimals, usd_price))
        if len(candidates) != 1:
            return None
        amount, decimals, usd_price = candidates[0]
        if decimals < 0 or decimals > 30 or usd_price <= 0:
            return None
        return format(abs(Decimal(amount)) / Decimal(10**decimals) * usd_price, "f")

    def events_after(self, checkpoint: ChainCheckpoint,
                     wallets: Sequence[str]) -> tuple[list[Mapping[str, Any]], ChainCheckpoint]:
        with rpc_view(self.rpc):
            return self._events_after(checkpoint, wallets)

    def _events_after(self, checkpoint: ChainCheckpoint,
                      wallets: Sequence[str]) -> tuple[list[Mapping[str, Any]], ChainCheckpoint]:
        if checkpoint.chain_id != "1399811149" or checkpoint.block_number is None:
            raise ValueError("solana_checkpoint_invalid")
        head = self.head()
        head_slot = head.block_number
        assert head_slot is not None
        gap = head_slot - checkpoint.block_number
        if gap < 0 or gap > self.maximum_slot_gap:
            raise RpcUnavailable("solana_checkpoint_gap_requires_controlled_recovery")
        if gap == 0:
            return [], checkpoint
        signatures: dict[tuple[str, str], Mapping[str, Any]] = {}
        for actor in wallets:
            rows = self.rpc.call("getSignaturesForAddress", [actor, {
                "commitment": "confirmed", "limit": self.signature_page_limit,
            }])
            if not isinstance(rows, list):
                raise RpcUnavailable("solana_signature_page_invalid")
            if len(rows) == self.signature_page_limit and rows and int(rows[-1]["slot"]) > checkpoint.block_number:
                raise RpcUnavailable("solana_signature_page_gap")
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                slot = int(row["slot"])
                if checkpoint.block_number < slot <= head_slot and row.get("err") is None:
                    signatures[(str(row["signature"]), actor)] = row
        events: list[Mapping[str, Any]] = []
        for (signature, actor), info in sorted(signatures.items(), key=lambda item: (int(item[1]["slot"]), item[0])):
            transaction = self.rpc.call("getTransaction", [signature, {
                "commitment": "confirmed", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
            }])
            if not isinstance(transaction, Mapping):
                raise RpcUnavailable("solana_transaction_unavailable")
            meta = transaction.get("meta")
            tx = transaction.get("transaction")
            if not isinstance(meta, Mapping) or meta.get("err") is not None or not isinstance(tx, Mapping):
                continue
            message = tx.get("message")
            if not isinstance(message, Mapping):
                continue
            keys = _keys(message)
            if actor not in keys:
                continue
            outer = message.get("instructions") or []
            inner = meta.get("innerInstructions") or []
            program_ids = _program_ids(outer, keys)
            for group in inner:
                if isinstance(group, Mapping):
                    program_ids.extend(_program_ids(group.get("instructions") or [], keys))
            if not self.allowed_program_ids.intersection(program_ids) or not inner:
                continue
            instruction_index = next((index for index, item in enumerate(outer)
                                      if self.allowed_program_ids.intersection(_program_ids([item], keys))), None)
            if instruction_index is None:
                instruction_index = next((int(group["index"]) for group in inner if isinstance(group, Mapping)
                                          and self.allowed_program_ids.intersection(
                                              _program_ids(group.get("instructions") or [], keys))), None)
            if instruction_index is None:
                continue
            before = _balances(meta.get("preTokenBalances"), actor)
            after = _balances(meta.get("postTokenBalances"), actor)
            slot = int(transaction["slot"])
            block_hash = self.canonical_hash(slot)
            if not block_hash or transaction.get("blockTime") is None:
                raise RpcUnavailable("solana_transaction_block_evidence_unavailable")
            block_time = datetime.fromtimestamp(int(transaction["blockTime"]), timezone.utc).isoformat()
            events.append({
                "actorWallet": actor, "signature": signature, "instructionIndex": instruction_index,
                "slot": slot, "blockHash": block_hash, "blockTime": block_time,
                "confirmationLevel": str(info.get("confirmationStatus") or "confirmed"),
                "programIds": program_ids, "innerInstructions": list(inner),
                "preTokenBalances": before, "postTokenBalances": after,
                "estimatedUsd": self._estimated_usd(before, after),
            })
        return events, head
