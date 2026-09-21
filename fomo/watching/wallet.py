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
from .watchlist import enabled_wallets


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

    def poll_configured(self, watchlist_path: str) -> AdapterBatch:
        """Select only enabled wallets on this chain from watch-wallets.json."""
        entries = enabled_wallets(watchlist_path, self.chain_id)
        return self.poll([str(entry["address"]) for entry in entries])

    def poll(self, wallets: Sequence[str], checkpoint: ChainCheckpoint | None = None) -> AdapterBatch:
        current = checkpoint or self.store.load(self.adapter_id, self.chain_id)
        if current is None:
            head = self.provider.head()
            self.store.commit_batch(self.adapter_id, head, [])
            return AdapterBatch((), head, ())
        reverted: tuple[str, ...] = ()
        if current.block_number is not None and current.block_hash:
            canonical = self.provider.canonical_hash(current.block_number)
            if not canonical:
                raise ValueError("canonical_checkpoint_hash_required")
            if canonical != current.block_hash:
                rewind_to = max(0, current.block_number - 1)
                rewind_hash = self.provider.canonical_hash(rewind_to)
                if not rewind_hash:
                    raise ValueError("canonical_rewind_hash_required")
                reverted = self.store.rollback_after(self.adapter_id, self.chain_id, rewind_to, rewind_hash)
                current = ChainCheckpoint(self.chain_id, str(rewind_to), rewind_to,
                                          rewind_hash)
        pending_events, pending_reverts = self.store.pending_deliveries(self.adapter_id, self.chain_id)
        if pending_events or pending_reverts:
            return AdapterBatch(pending_events, current, pending_reverts)
        raw_events, next_checkpoint = self.provider.events_after(current, wallets)
        durable: list[tuple[NormalizedWatchEvent, int | None, str | None]] = []
        allowed_wallets = ({value.casefold() for value in wallets} if self.source == "wallet_rpc_evm"
                           else set(wallets))
        for transaction in raw_events:
            actor = str(transaction.get("actorWallet") or "")
            actor_key = actor.casefold() if self.source == "wallet_rpc_evm" else actor
            if actor_key not in allowed_wallets:
                continue
            decoded = self.decoder.decode(transaction, actor)
            if decoded is None:
                continue
            event_height = transaction.get("blockNumber")
            if event_height is None:
                event_height = transaction.get("slot")
            event_block_hash = str(transaction.get("blockHash") or "")
            if event_height is None or (self.source == "wallet_rpc_evm" and not event_block_hash):
                raise ValueError("confirmed_wallet_event_requires_block_position")
            reference, log_index, instruction_index = self._identity(transaction)
            index = instruction_index if instruction_index is not None else log_index or 0
            reorg_key = event_block_hash or str(event_height)
            event_id = hashlib.sha256(
                f"{self.source}\0{self.chain_id}\0{reference}\0{index}\0{actor}\0{reorg_key}".encode()
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
                reorg_key=reorg_key,
                decoder_version=decoded.decoder_version, raw_payload_hash=raw_payload_hash(transaction),
            )
            event = NormalizedWatchEvent(
                event_id=envelope.signal_id, chain_id=self.chain_id, wallet=actor,
                kind=decoded.side, token_address=decoded.token_out if decoded.side == "buy" else decoded.token_in,
                token_quantity=decoded.source_amount, usd_value_micros=(
                    int(float(decoded.estimated_usd) * 1_000_000) if decoded.estimated_usd else None
                ), transaction_id=reference, instruction_index=index, observed_at=observed,
                signal=envelope,
            )
            durable.append((event, int(event_height), event_block_hash or None))
            self.last_observed_at = observed
        self.store.commit_batch(self.adapter_id, next_checkpoint, durable)
        pending_events, pending_reverts = self.store.pending_deliveries(self.adapter_id, self.chain_id)
        return AdapterBatch(pending_events, next_checkpoint, pending_reverts or reverted)

    def health(self) -> AdapterHealth:
        if not self.last_observed_at:
            return AdapterHealth("stopped", None, None, "checkpoint_only_no_new_events")
        age = int((datetime.now(timezone.utc) - datetime.fromisoformat(self.last_observed_at)).total_seconds())
        return AdapterHealth("healthy" if age < 60 else "stale", self.last_observed_at, age)
