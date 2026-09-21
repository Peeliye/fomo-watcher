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


class Broadcaster(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def broadcast(self, signed_transaction: bytes) -> BroadcastResult: ...


class NonceBlockhashManager(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def acquire(self, intent: ExecutionIntent) -> str: ...
    def mark_used(self, value: str, tx_hash: str) -> None: ...


class ReceiptTracker(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def receipt(self, tx_hash: str) -> Mapping[str, Any] | None: ...


class TransactionParser(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def parse(self, serialized_transaction: bytes) -> Mapping[str, Any]: ...
