"""Stable, read-only interface for future RPC/indexer wallet streams.

No implementation in this module signs or broadcasts transactions. Adapters
must advance durable cursors and emit deterministic event IDs instead of
rescanning complete history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, Sequence


@dataclass(frozen=True)
class ChainCheckpoint:
    chain_id: str
    cursor: str
    block_number: int | None
    block_hash: str | None


@dataclass(frozen=True)
class NormalizedWatchEvent:
    event_id: str
    chain_id: str
    wallet: str
    kind: Literal["buy", "sell", "transfer_in", "transfer_out"]
    token_address: str
    token_quantity: str
    usd_value_micros: int | None
    transaction_id: str
    instruction_index: int
    observed_at: str


@dataclass(frozen=True)
class AdapterHealth:
    status: Literal["healthy", "stale", "degraded", "stopped"]
    last_observed_at: str | None
    age_seconds: int | None
    detail: str | None = None


@dataclass(frozen=True)
class AdapterBatch:
    events: Sequence[NormalizedWatchEvent]
    checkpoint: ChainCheckpoint
    reverted_event_ids: Sequence[str] = ()


class WalletStreamAdapter(Protocol):
    def poll(self, wallets: Sequence[str], checkpoint: ChainCheckpoint | None) -> AdapterBatch:
        """Return events after checkpoint and a durable next checkpoint."""
        ...

    def health(self) -> AdapterHealth:
        """Return adapter freshness without initiating a historical rescan."""
        ...
