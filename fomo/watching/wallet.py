"""Common durable orchestration for chain-specific wallet RPC adapters."""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, Sequence

from fomo.signals.envelope import TradeSignalEnvelope, raw_payload_hash

from .adapter import AdapterBatch, AdapterHealth, ChainCheckpoint, NormalizedWatchEvent
from .checkpoints import CheckpointStore
from .decoder import SwapDecoder


class WalletRpcProvider(Protocol):
    def head(self) -> ChainCheckpoint: ...
    def canonical_hash(self, block_number: int) -> str | None: ...
    def events_after(self, checkpoint: ChainCheckpoint, wallets: Sequence[str]) -> tuple[list[Mapping[str, Any]], ChainCheckpoint]: ...


class WalletRpcAdapter(ABC):
    source: str

    def __init__(self, adapter_id: str, chain_id: str, provider: WalletRpcProvider,
                 decoder: SwapDecoder, checkpoint_store: CheckpointStore) -> None:
        self.adapter_id = adapter_id
        self.chain_id = str(chain_id)
        self.provider = provider
        self.decoder = decoder
        self.store = checkpoint_store
        self.last_observed_at: str | None = None

    @abstractmethod
    def _identity(self, transaction: Mapping[str, Any]) -> tuple[str, int | None, int | None]: ...

    @abstractmethod
    def _confirmation(self, transaction: Mapping[str, Any]) -> str: ...

    def poll(self, wallets: Sequence[str], checkpoint: ChainCheckpoint | None = None) -> AdapterBatch:
        current = checkpoint or self.store.load(self.adapter_id, self.chain_id)
        if current is None:
            head = self.provider.head()
            self.store.commit_batch(self.adapter_id, head, [])
            return AdapterBatch((), head, ())
        reverted: tuple[str, ...] = ()
        if current.block_number is not None and current.block_hash:
            canonical = self.provider.canonical_hash(current.block_number)
            if canonical and canonical != current.block_hash:
                rewind_to = max(0, current.block_number - 1)
                reverted = self.store.rollback_after(self.adapter_id, self.chain_id, rewind_to)
                current = ChainCheckpoint(self.chain_id, str(rewind_to), rewind_to,
                                          self.provider.canonical_hash(rewind_to))
        raw_events, next_checkpoint = self.provider.events_after(current, wallets)
        normalized: list[NormalizedWatchEvent] = []
        durable: list[tuple[str, int | None, str | None]] = []
        for transaction in raw_events:
            actor = str(transaction.get("actorWallet") or "")
            if actor not in wallets:
                continue
            decoded = self.decoder.decode(transaction, actor)
            if decoded is None:
                continue
            reference, log_index, instruction_index = self._identity(transaction)
            index = instruction_index if instruction_index is not None else log_index or 0
            event_id = hashlib.sha256(
                f"{self.source}\0{self.chain_id}\0{reference}\0{index}\0{actor}".encode()
            ).hexdigest()
            signal_event_id = f"sig:v1:{self.source}:" + hashlib.sha256(
                f"{self.source}\0{event_id}".encode()
            ).hexdigest()
            if self.store.has_event(self.adapter_id, signal_event_id):
                continue
            observed = datetime.now(timezone.utc).isoformat()
            envelope = TradeSignalEnvelope.create(
                source=self.source, source_event_id=event_id, observed_at=observed,
                source_timestamp=transaction.get("blockTime") or observed,
                delivery_delay_ms=max(0, int(transaction.get("deliveryDelayMs") or 0)),
                chain_id=self.chain_id, actor_wallet=actor, kol_id=None, side=decoded.side,
                token_in=decoded.token_in, token_out=decoded.token_out,
                source_amount=decoded.source_amount, estimated_usd=decoded.estimated_usd,
                tx_hash=reference if self.source == "wallet_rpc_evm" else None,
                signature=reference if self.source == "wallet_rpc_solana" else None,
                log_index=log_index, instruction_index=instruction_index,
                confirmation_level=self._confirmation(transaction),
                reorg_key=str(transaction.get("blockHash") or transaction.get("slot") or reference),
                decoder_version=decoded.decoder_version, raw_payload_hash=raw_payload_hash(transaction),
            )
            normalized.append(NormalizedWatchEvent(
                event_id=envelope.signal_id, chain_id=self.chain_id, wallet=actor,
                kind=decoded.side, token_address=decoded.token_out if decoded.side == "buy" else decoded.token_in,
                token_quantity=decoded.source_amount, usd_value_micros=(
                    int(float(decoded.estimated_usd) * 1_000_000) if decoded.estimated_usd else None
                ), transaction_id=reference, instruction_index=index, observed_at=observed,
            ))
            durable.append((envelope.signal_id, transaction.get("blockNumber") or transaction.get("slot"),
                            str(transaction.get("blockHash") or "") or None))
            self.last_observed_at = observed
        self.store.commit_batch(self.adapter_id, next_checkpoint, durable)
        return AdapterBatch(tuple(normalized), next_checkpoint, reverted)

    def health(self) -> AdapterHealth:
        if not self.last_observed_at:
            return AdapterHealth("stopped", None, None, "checkpoint_only_no_new_events")
        age = int((datetime.now(timezone.utc) - datetime.fromisoformat(self.last_observed_at)).total_seconds())
        return AdapterHealth("healthy" if age < 60 else "stale", self.last_observed_at, age)
