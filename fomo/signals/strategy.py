"""Source-specific sizing strategies producing source-neutral intents."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .envelope import TradeSignalEnvelope


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    intent_id: str
    signal_id: str
    source: str
    strategy_id: str
    allocation_key: str
    chain_id: str
    side: str
    token_in: str
    token_out: str
    requested_usd: Decimal
    sell_ratio: Decimal | None = None

    def __post_init__(self) -> None:
        if self.requested_usd < 0:
            raise ValueError("requested_usd must be non-negative")
        if self.sell_ratio is not None and not Decimal("0") < self.sell_ratio <= Decimal("1"):
            raise ValueError("sell_ratio must be within (0,1]")


def _decimal(value: Any, default: str = "0") -> Decimal:
    return Decimal(str(default if value is None else value))


class FomoCopyStrategy:
    """Fomo-only fixed-USD and optional first-sell liquidation behavior."""

    def __init__(self, fixed_usd: Any, *, maximum_delay_ms: int = 5000,
                 late_policy: str = "drop", full_on_first_sell: bool = False) -> None:
        self.fixed_usd = _decimal(fixed_usd)
        self.maximum_delay_ms = int(maximum_delay_ms)
        self.late_policy = late_policy
        self.full_on_first_sell = bool(full_on_first_sell)

    def create_intent(self, signal: TradeSignalEnvelope) -> ExecutionIntent | None:
        if signal.source != "fomo_push":
            raise ValueError("FomoCopyStrategy accepts only fomo_push signals")
        if signal.delivery_delay_ms > self.maximum_delay_ms:
            return None
        sell_ratio = Decimal("1") if signal.side == "sell" and self.full_on_first_sell else None
        requested = self.fixed_usd if signal.side == "buy" else Decimal("0")
        return ExecutionIntent(
            f"intent:{signal.signal_id}", signal.signal_id, signal.source, "fomo-copy-v1",
            f"fomo:{signal.kol_id or 'unknown'}", signal.chain_id, signal.side,
            signal.token_in, signal.token_out, requested, sell_ratio,
        )


class WalletCopyStrategy:
    """Wallet sizing has no Fomo/KOL identity or first-sell semantics."""

    def __init__(self, wallet_config: Mapping[str, Any]) -> None:
        self.config = dict(wallet_config)

    def create_intent(self, signal: TradeSignalEnvelope) -> ExecutionIntent | None:
        if signal.source not in {"wallet_rpc_evm", "wallet_rpc_solana"}:
            raise ValueError("WalletCopyStrategy accepts only wallet RPC signals")
        if not signal.actor_wallet or signal.side == "unknown":
            return None
        minimum = _decimal(self.config.get("minimumTradeUsd"))
        observed = _decimal(signal.estimated_usd)
        if observed < minimum:
            return None
        if signal.side == "buy":
            mode = str(self.config.get("buyMode") or "fixed_usd")
            requested = (
                observed * _decimal(self.config.get("buyRatio"), "1")
                if mode == "observed_ratio" else _decimal(self.config.get("fixedUsd"))
            )
            maximum = _decimal(self.config.get("maxUsd"))
            if maximum > 0:
                requested = min(requested, maximum)
            sell_ratio = None
        else:
            requested = Decimal("0")
            mode = str(self.config.get("sellMode") or "source_ratio")
            sell_ratio = _decimal(self.config.get("sellRatio"), "1") if mode == "fixed_ratio" else None
        return ExecutionIntent(
            f"intent:{signal.signal_id}", signal.signal_id, signal.source, "wallet-copy-v1",
            f"wallet:{signal.chain_id}:{signal.actor_wallet}", signal.chain_id, signal.side,
            signal.token_in, signal.token_out, requested, sell_ratio,
        )
