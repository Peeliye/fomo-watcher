"""Balance-delta swap decoders. One-sided asset movements fail closed."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Mapping, Protocol


@dataclass(frozen=True, slots=True)
class SwapDecodeResult:
    side: Literal["buy", "sell"]
    token_in: str
    token_out: str
    source_amount: str
    estimated_usd: str | None
    decoder_version: str


class SwapDecoder(Protocol):
    version: str

    def decode(self, transaction: Mapping[str, Any], actor_wallet: str) -> SwapDecodeResult | None: ...


def _decimal(value: Any) -> Decimal:
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else Decimal("0")
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _deltas(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Decimal]:
    tokens = set(map(str, before)) | set(map(str, after))
    return {token: _decimal(after.get(token)) - _decimal(before.get(token)) for token in tokens}


def _classify(deltas: Mapping[str, Decimal], quote_assets: set[str], version: str,
              estimated_usd: Any = None) -> SwapDecodeResult | None:
    positive = [(token, amount) for token, amount in deltas.items() if amount > 0]
    negative = [(token, -amount) for token, amount in deltas.items() if amount < 0]
    if not positive or not negative:
        return None
    quote_in = next(((token, amount) for token, amount in negative if token.casefold() in quote_assets), None)
    quote_out = next(((token, amount) for token, amount in positive if token.casefold() in quote_assets), None)
    if quote_in:
        token_out = next(((token, amount) for token, amount in positive if token != quote_in[0]), None)
        if token_out:
            return SwapDecodeResult("buy", quote_in[0], token_out[0], str(token_out[1]),
                                    str(estimated_usd) if estimated_usd is not None else None, version)
    if quote_out:
        token_in = next(((token, amount) for token, amount in negative if token != quote_out[0]), None)
        if token_in:
            return SwapDecodeResult("sell", token_in[0], quote_out[0], str(token_in[1]),
                                    str(estimated_usd) if estimated_usd is not None else None, version)
    return None


class EvmSwapDecoder:
    version = "evm-balance-receipt@1"

    def __init__(self, quote_assets: set[str], allowed_router_addresses: set[str]) -> None:
        self.quote_assets = {value.casefold() for value in quote_assets}
        self.allowed_routers = {value.casefold() for value in allowed_router_addresses}

    def decode(self, transaction: Mapping[str, Any], actor_wallet: str) -> SwapDecodeResult | None:
        receipt = transaction.get("receipt")
        before = transaction.get("preBalances")
        after = transaction.get("postBalances")
        target = str(transaction.get("to") or "").casefold()
        if not isinstance(receipt, Mapping) or receipt.get("status") not in {1, "0x1", True}:
            return None
        if target not in self.allowed_routers or not isinstance(before, Mapping) or not isinstance(after, Mapping):
            return None
        if str(transaction.get("actorWallet") or "").casefold() != actor_wallet.casefold():
            return None
        logs = receipt.get("logs")
        if not isinstance(logs, list) or not logs:
            return None
        return _classify(_deltas(before, after), self.quote_assets, self.version, transaction.get("estimatedUsd"))


class SolanaSwapDecoder:
    version = "solana-balance-inner-instruction@1"

    def __init__(self, quote_assets: set[str], allowed_program_ids: set[str]) -> None:
        self.quote_assets = {value.casefold() for value in quote_assets}
        self.allowed_program_ids = set(allowed_program_ids)

    def decode(self, transaction: Mapping[str, Any], actor_wallet: str) -> SwapDecodeResult | None:
        if str(transaction.get("actorWallet") or "") != actor_wallet:
            return None
        programs = set(map(str, transaction.get("programIds") or []))
        inner = transaction.get("innerInstructions")
        before = transaction.get("preTokenBalances")
        after = transaction.get("postTokenBalances")
        if not programs.intersection(self.allowed_program_ids) or not isinstance(inner, list) or not inner:
            return None
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            return None
        return _classify(_deltas(before, after), self.quote_assets, self.version, transaction.get("estimatedUsd"))
