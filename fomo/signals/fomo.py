"""Fomo WebSocket/REST adapter; platform semantics stay in this module."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Mapping

from .envelope import TradeSignalEnvelope, raw_payload_hash


class FomoPushAdapter:
    def __init__(self, followed_user_ids: set[str], decoder_version: str = "fomo-push@1") -> None:
        self.followed_user_ids = {str(value) for value in followed_user_ids}
        self.decoder_version = decoder_version

    def normalize(self, payload: Mapping[str, Any], observed_at: str | None = None) -> TradeSignalEnvelope | None:
        body_value = payload.get("body")
        body = dict(body_value) if isinstance(body_value, Mapping) else {}
        event = {**body, **payload}
        user_id = str(event.get("userId") or "")
        if not user_id or user_id not in self.followed_user_ids:
            return None
        source_type = str(event.get("type") or "").lower()
        if source_type not in {"swap_buy", "single_user_buy", "swap_sell", "single_user_sell"}:
            return None
        side = "buy" if source_type.endswith("buy") else "sell"
        source_event_id = str(event.get("id") or event.get("tradeId") or "").strip()
        chain_id = str(event.get("networkId") or "").strip()
        token = str(event.get("tokenAddress") or "").strip()
        source_timestamp = str(event.get("createdAt") or "").strip()
        if not source_event_id or not chain_id or not token or not source_timestamp:
            return None
        observed = observed_at or datetime.now(timezone.utc).isoformat()
        observed_dt = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        source_dt = datetime.fromisoformat(source_timestamp.replace("Z", "+00:00"))
        delay = max(0, int((observed_dt - source_dt).total_seconds() * 1000))
        raw_tx_hash = str(event.get("txHash") or "").strip()
        tx_hash = raw_tx_hash.lower() if re.fullmatch(r"0x[0-9a-fA-F]{64}", raw_tx_hash) else None
        trade_id = str(event.get("tradeId") or "").strip()
        # Fomo's trade identifier groups multiple platform notifications about
        # one economic event. It is not a chain transaction hash.
        event_group = ("fomo:trade:" + hashlib.sha256(trade_id.encode()).hexdigest()
                       if trade_id else f"fomo:event:{source_event_id}")
        signature = str(event.get("signature") or event.get("tradeId") or "").strip() or None
        return TradeSignalEnvelope.create(
            source="fomo_push", source_event_id=source_event_id, observed_at=observed,
            source_timestamp=source_timestamp, delivery_delay_ms=delay, chain_id=chain_id,
            actor_wallet=event.get("wallet"), kol_id=user_id, side=side,
            # The original quoteToken remains in the durable raw payload. It is
            # source metadata, not a verified asset for our native-ETH route.
            token_in=token if side == "sell" else "",
            token_out=token if side == "buy" else str(event.get("quoteToken") or ""),
            source_amount=event.get("tokenAmount") or event.get("amount"),
            estimated_usd=event.get("usdAmount") or event.get("amountUsd"),
            tx_hash=tx_hash if chain_id != "1399811149" else None,
            signature=signature if chain_id == "1399811149" else None,
            log_index=event.get("logIndex"), instruction_index=event.get("instructionIndex"),
            confirmation_level="pending", reorg_key=event_group,
            decoder_version=self.decoder_version, raw_payload_hash=raw_payload_hash(payload),
        )
