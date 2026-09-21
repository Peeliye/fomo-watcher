"""Chain-neutral live execution contracts; implementations must self-check."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityStatus


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    provider: str
    output_amount: str
    minimum_output_amount: str
    captured_at: str
    price_impact_bps: str
    firm: bool
    route_targets: tuple[str, ...]
    execution_payload: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class BuiltTransaction:
    serialized: bytes
    provider: str
    nonce_or_blockhash: str


@dataclass(frozen=True, slots=True)
class BroadcastResult:
    tx_hash: str
    provider: str
    submitted_at: str


class QuoteAdapter(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def quote(self, intent: ExecutionIntent) -> Sequence[ExecutableQuote]: ...


class TransactionBuilder(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def build(self, intent: ExecutionIntent, quote: ExecutableQuote, nonce_or_blockhash: str) -> BuiltTransaction: ...


class Signer(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def sign(self, transaction: BuiltTransaction) -> bytes: ...


class TransactionSimulator(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def simulate(self, transaction: BuiltTransaction) -> Mapping[str, Any]: ...


class Broadcaster(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def transaction_id(self, signed_transaction: bytes) -> str:
        """Derive the chain transaction ID locally, before any network call."""
        ...
    def broadcast(self, signed_transaction: bytes) -> BroadcastResult: ...


class NonceBlockhashManager(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def acquire(self, intent: ExecutionIntent) -> str: ...
    def mark_used(self, intent_id: str, value: str, tx_hash: str) -> None: ...


class ReceiptTracker(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def receipt(self, tx_hash: str) -> Mapping[str, Any] | None: ...


class TransactionParser(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def parse(self, serialized_transaction: bytes) -> Mapping[str, Any]: ...
