"""Bounded EVM block/receipt stream for directly watched EOA swaps."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .adapter import ChainCheckpoint
from .rpc_transport import RpcTransport, RpcUnavailable, rpc_view


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _quantity(value: Any) -> int:
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def _address(value: Any) -> str:
    text = str(value or "").lower()
    if len(text) != 42 or not text.startswith("0x") or any(ch not in "0123456789abcdef" for ch in text[2:]):
        raise ValueError("invalid_evm_address")
    return text


class EvmRpcProvider:
    def __init__(self, chain_id: str, rpc: RpcTransport, *, allowed_routers: set[str],
                 quote_assets: Mapping[str, tuple[int, Decimal]] | None = None,
                 maximum_blocks_per_poll: int = 32) -> None:
        self.chain_id = str(chain_id)
        self.rpc = rpc
        self.allowed_routers = {_address(value) for value in allowed_routers}
        self.quote_assets = {_address(key): (int(value[0]), Decimal(value[1]))
                             for key, value in (quote_assets or {}).items()}
        self.maximum_blocks_per_poll = max(1, int(maximum_blocks_per_poll))

    def _block(self, number: str | int, full: bool) -> Mapping[str, Any] | None:
        tag = number if isinstance(number, str) and not number.isdigit() else hex(int(number))
        value = self.rpc.call("eth_getBlockByNumber", [tag, full])
        return value if isinstance(value, Mapping) else None

    def head(self) -> ChainCheckpoint:
        block = self._block("latest", False)
        if not block or not block.get("hash"):
            raise RpcUnavailable("evm_head_unavailable")
        number = _quantity(block["number"])
        return ChainCheckpoint(self.chain_id, str(number), number, str(block["hash"]))

    def canonical_hash(self, block_number: int) -> str | None:
        block = self._block(block_number, False)
        return str(block["hash"]) if block and block.get("hash") else None

    def _token_deltas(self, receipt: Mapping[str, Any], actor: str) -> tuple[dict[str, int], int | None]:
        deltas: dict[str, int] = {}
        relevant_indexes: list[int] = []
        for log in receipt.get("logs") or []:
            if not isinstance(log, Mapping):
                continue
            topics = log.get("topics")
            if not isinstance(topics, list) or len(topics) != 3 or str(topics[0]).lower() != TRANSFER_TOPIC:
                continue
            try:
                token = _address(log.get("address"))
                sender = _address("0x" + str(topics[1])[-40:])
                recipient = _address("0x" + str(topics[2])[-40:])
                amount = _quantity(log.get("data"))
                index = _quantity(log.get("logIndex"))
            except (ValueError, TypeError):
                continue
            if sender == actor:
                deltas[token] = deltas.get(token, 0) - amount
                relevant_indexes.append(index)
            if recipient == actor:
                deltas[token] = deltas.get(token, 0) + amount
                relevant_indexes.append(index)
        return deltas, min(relevant_indexes) if relevant_indexes else None

    def _estimated_usd(self, deltas: Mapping[str, int]) -> str | None:
        quotes = [(amount, self.quote_assets[token]) for token, amount in deltas.items()
                  if token in self.quote_assets and amount]
        if len(quotes) != 1:
            return None
        amount, (decimals, usd_price) = quotes[0]
        if decimals < 0 or decimals > 30 or usd_price <= 0:
            return None
        return format(abs(Decimal(amount)) / Decimal(10**decimals) * usd_price, "f")

    def events_after(self, checkpoint: ChainCheckpoint,
                     wallets: Sequence[str]) -> tuple[list[Mapping[str, Any]], ChainCheckpoint]:
        with rpc_view(self.rpc):
            return self._events_after(checkpoint, wallets)

    def _events_after(self, checkpoint: ChainCheckpoint,
                      wallets: Sequence[str]) -> tuple[list[Mapping[str, Any]], ChainCheckpoint]:
        if checkpoint.chain_id != self.chain_id or checkpoint.block_number is None:
            raise ValueError("evm_checkpoint_invalid")
        head = self.head()
        gap = head.block_number - checkpoint.block_number  # type: ignore[operator]
        if gap < 0 or gap > self.maximum_blocks_per_poll:
            raise RpcUnavailable("evm_checkpoint_gap_requires_controlled_recovery")
        if gap == 0:
            return [], checkpoint
        watched = {_address(wallet) for wallet in wallets}
        events: list[Mapping[str, Any]] = []
        previous_hash = checkpoint.block_hash
        finalized_height = -1
        try:
            finalized = self._block("finalized", False)
            if finalized:
                finalized_height = _quantity(finalized["number"])
        except (RpcUnavailable, ValueError, TypeError):
            pass
        for number in range(checkpoint.block_number + 1, head.block_number + 1):  # type: ignore[operator]
            block = self._block(number, True)
            if not block or not block.get("hash") or (previous_hash and block.get("parentHash") != previous_hash):
                raise RpcUnavailable("evm_block_chain_discontinuity")
            block_hash = str(block["hash"])
            block_time = datetime.fromtimestamp(_quantity(block["timestamp"]), timezone.utc).isoformat()
            for tx in block.get("transactions") or []:
                if not isinstance(tx, Mapping):
                    continue
                try:
                    actor = _address(tx.get("from"))
                    target = _address(tx.get("to"))
                except ValueError:
                    continue
                if actor not in watched or target not in self.allowed_routers:
                    continue
                tx_hash = str(tx.get("hash") or "")
                receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
                if not isinstance(receipt, Mapping) or str(receipt.get("blockHash")) != block_hash:
                    raise RpcUnavailable("evm_receipt_unavailable_or_reorged")
                if str(receipt.get("status")) != "0x1":
                    continue
                deltas, index = self._token_deltas(receipt, actor)
                if index is None:
                    continue
                before = {token: max(0, -amount) for token, amount in deltas.items()}
                after = {token: max(0, amount) for token, amount in deltas.items()}
                events.append({
                    "actorWallet": actor, "to": target, "txHash": tx_hash,
                    "nonce": _quantity(tx["nonce"]), "logIndex": index,
                    "blockNumber": number, "blockHash": block_hash, "blockTime": block_time,
                    "confirmationLevel": "finalized" if number <= finalized_height else "confirmed",
                    "receipt": dict(receipt), "preBalances": before, "postBalances": after,
                    "estimatedUsd": self._estimated_usd(deltas),
                })
            previous_hash = block_hash
        return events, head
