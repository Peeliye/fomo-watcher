"""Immutable, versioned boundary shared by all signal sources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Mapping


SignalSource = Literal["fomo_push", "wallet_rpc_evm", "wallet_rpc_solana"]
SignalSide = Literal["buy", "sell", "unknown"]
ConfirmationLevel = Literal["pending", "processed", "confirmed", "finalized"]


def _time(value: Any, name: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _amount(value: Any, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return format(number, "f")


def raw_payload_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def deterministic_signal_id(source: str, source_event_id: str) -> str:
    digest = hashlib.sha256(f"{source}\0{source_event_id}".encode()).hexdigest()
    return f"sig:v1:{source}:{digest}"


@dataclass(frozen=True, slots=True)
class TradeSignalEnvelope:
    """Canonical source envelope. Source-specific meaning ends at this type."""

    signal_id: str
    source: SignalSource
    source_event_id: str
    observed_at: str
    source_timestamp: str
    delivery_delay_ms: int
    chain_id: str
    actor_wallet: str | None
    kol_id: str | None
    side: SignalSide
    token_in: str
    token_out: str
    source_amount: str | None
    estimated_usd: str | None
    tx_hash: str | None
    signature: str | None
    log_index: int | None
    instruction_index: int | None
    confirmation_level: ConfirmationLevel
    reorg_key: str
    decoder_version: str
    raw_payload_hash: str

    def __post_init__(self) -> None:
        if self.source not in {"fomo_push", "wallet_rpc_evm", "wallet_rpc_solana"}:
            raise ValueError("unsupported signal source")
        if self.side not in {"buy", "sell", "unknown"}:
            raise ValueError("unsupported signal side")
        if self.confirmation_level not in {"pending", "processed", "confirmed", "finalized"}:
            raise ValueError("unsupported confirmation level")
        for name in ("signal_id", "source_event_id", "chain_id", "reorg_key", "decoder_version"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.signal_id != deterministic_signal_id(self.source, self.source_event_id):
            raise ValueError("signalId must be deterministic for source and sourceEventId")
        if self.delivery_delay_ms < 0:
            raise ValueError("deliveryDelayMs must be non-negative")
        if len(self.raw_payload_hash) != 64:
            raise ValueError("rawPayloadHash must be SHA-256 hex")
        _time(self.observed_at, "observedAt")
        _time(self.source_timestamp, "sourceTimestamp")
        _amount(self.source_amount, "sourceAmount", optional=True)
        _amount(self.estimated_usd, "estimatedUsd", optional=True)

    @classmethod
    def create(cls, **values: Any) -> "TradeSignalEnvelope":
        source = str(values["source"])
        source_event_id = str(values["source_event_id"])
        return cls(
            signal_id=deterministic_signal_id(source, source_event_id),
            source=source,  # type: ignore[arg-type]
            source_event_id=source_event_id,
            observed_at=_time(values["observed_at"], "observedAt"),
            source_timestamp=_time(values["source_timestamp"], "sourceTimestamp"),
            delivery_delay_ms=max(0, int(values.get("delivery_delay_ms", 0))),
            chain_id=str(values["chain_id"]),
            actor_wallet=str(values["actor_wallet"]).strip() if values.get("actor_wallet") else None,
            kol_id=str(values["kol_id"]).strip() if values.get("kol_id") else None,
            side=str(values.get("side", "unknown")),  # type: ignore[arg-type]
            token_in=str(values.get("token_in") or ""),
            token_out=str(values.get("token_out") or ""),
            source_amount=_amount(values.get("source_amount"), "sourceAmount", optional=True),
            estimated_usd=_amount(values.get("estimated_usd"), "estimatedUsd", optional=True),
            tx_hash=str(values["tx_hash"]).strip() if values.get("tx_hash") else None,
            signature=str(values["signature"]).strip() if values.get("signature") else None,
            log_index=int(values["log_index"]) if values.get("log_index") is not None else None,
            instruction_index=(
                int(values["instruction_index"]) if values.get("instruction_index") is not None else None
            ),
            confirmation_level=str(values.get("confirmation_level", "confirmed")),  # type: ignore[arg-type]
            reorg_key=str(values["reorg_key"]),
            decoder_version=str(values["decoder_version"]),
            raw_payload_hash=str(values["raw_payload_hash"]),
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TradeSignalEnvelope":
        aliases = {
            "signal_id": "signalId", "source_event_id": "sourceEventId", "observed_at": "observedAt",
            "source_timestamp": "sourceTimestamp", "delivery_delay_ms": "deliveryDelayMs",
            "chain_id": "chainId", "actor_wallet": "actorWallet", "kol_id": "kolId",
            "token_in": "tokenIn", "token_out": "tokenOut", "source_amount": "sourceAmount",
            "estimated_usd": "estimatedUsd", "tx_hash": "txHash", "log_index": "logIndex",
            "instruction_index": "instructionIndex", "confirmation_level": "confirmationLevel",
            "reorg_key": "reorgKey", "decoder_version": "decoderVersion",
            "raw_payload_hash": "rawPayloadHash",
        }
        values = {name: data.get(alias) for name, alias in aliases.items()}
        values.update({"source": data.get("source"), "side": data.get("side"), "signature": data.get("signature")})
        return cls(**values)  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, Any]:
        values = asdict(self)
        aliases = {
            "signal_id": "signalId", "source_event_id": "sourceEventId", "observed_at": "observedAt",
            "source_timestamp": "sourceTimestamp", "delivery_delay_ms": "deliveryDelayMs",
            "chain_id": "chainId", "actor_wallet": "actorWallet", "kol_id": "kolId",
            "token_in": "tokenIn", "token_out": "tokenOut", "source_amount": "sourceAmount",
            "estimated_usd": "estimatedUsd", "tx_hash": "txHash", "log_index": "logIndex",
            "instruction_index": "instructionIndex", "confirmation_level": "confirmationLevel",
            "reorg_key": "reorgKey", "decoder_version": "decoderVersion",
            "raw_payload_hash": "rawPayloadHash",
        }
        return {aliases.get(name, name): value for name, value in values.items()}
