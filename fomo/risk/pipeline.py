"""Connect watcher events to the read-only risk engine.

This module has no signer, key loader, transaction builder, or broadcaster.
It writes append-only decisions for inspection by the local dashboard.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .engine import ReadOnlyRiskEngine, RiskContext, Side, UnifiedSignal


def _utc_timestamp(value: str) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _signal_dict(signal: UnifiedSignal) -> dict[str, Any]:
    return {
        "signalId": signal.signal_id,
        "source": signal.source.value,
        "observedAt": signal.observed_at.isoformat(),
        "sourceTimestamp": signal.source_timestamp.isoformat() if signal.source_timestamp else None,
        "chainId": signal.chain_id,
        "kolId": signal.kol_id,
        "wallet": signal.wallet,
        "walletConfidence": str(signal.wallet_confidence),
        "originalTx": signal.original_tx,
        "side": signal.side.value,
        "tokenIn": signal.token_in,
        "tokenOut": signal.token_out,
        "estimatedUsd": str(signal.estimated_usd),
        "decoder": signal.decoder,
        "rawPayloadHash": signal.raw_payload_hash,
    }


class RiskPipeline:
    """Persistent read-only evaluator and NDJSON audit writer."""

    def __init__(self, project_dir: Path, settings: dict[str, Any]):
        self.project_dir = Path(project_dir)
        self.settings = dict(settings)
        self.enabled = bool(self.settings.get("enabled", True))
        self.policy_path = self._path("policy_path", "risk-policy.example.json")
        self.registry_path = self._path("registry_path", "wallet-registry.json")
        self.log_path = self._path("log_path", "data/risk-decisions.ndjson")
        self.engine = ReadOnlyRiskEngine.from_files(self.policy_path, self.registry_path) if self.enabled else None
        self._source_mtimes = self._mtimes()

    def _path(self, key: str, default: str) -> Path:
        value = Path(str(self.settings.get(key, default)))
        return value if value.is_absolute() else self.project_dir / value

    def _append(self, record: dict[str, Any]) -> dict[str, Any]:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        return record

    def _mtimes(self) -> tuple[int, int]:
        return (self.policy_path.stat().st_mtime_ns, self.registry_path.stat().st_mtime_ns)

    def _reload_if_changed(self) -> None:
        mtimes = self._mtimes()
        if mtimes == self._source_mtimes:
            return
        try:
            replacement = ReadOnlyRiskEngine.from_files(self.policy_path, self.registry_path)
        except (OSError, ValueError, json.JSONDecodeError):
            logging.exception("风控策略或钱包登记表热加载失败；继续使用上一份已验证版本")
            return
        self.engine = replacement
        self._source_mtimes = mtimes
        logging.info(
            "已热加载只读风控：policy v%d / registry v%d",
            replacement.policy_version,
            replacement.registry.version,
        )

    @staticmethod
    def _base(event: Any, now: datetime) -> dict[str, Any]:
        return {
            "recordedAt": now.isoformat(),
            "eventId": str(event.id),
            "signalId": f"fomo:{event.id}",
            "source": "fomo",
            "readOnly": True,
            "handle": str(event.handle),
            "kolId": str(getattr(event, "user_id", "") or ""),
            "networkId": int(event.network_id or 0),
            "symbol": str(event.symbol or "UNKNOWN"),
            "ca": str(event.ca or ""),
            "side": "sell" if event.kind in {"sell", "clear"} else str(event.kind),
            "estimatedUsd": float(event.amount_usd or 0),
        }

    def evaluate_event(self, event: Any, context: RiskContext | None = None) -> dict[str, Any] | None:
        if not self.enabled or self.engine is None or event.kind not in {"buy", "sell", "clear"}:
            return None
        self._reload_if_changed()
        started = time.perf_counter()
        now = datetime.now(timezone.utc)
        record = self._base(event, now)
        kol_id = record["kolId"]
        chain_id = str(record["networkId"])
        candidates = self.engine.registry.wallets_for_kol(chain_id, kol_id) if kol_id and chain_id != "0" else ()
        if not kol_id:
            reason = "missing_kol_id"
        elif chain_id == "0":
            reason = "missing_chain_id"
        elif not candidates:
            reason = "wallet_not_registered"
        elif len(candidates) > 1:
            reason = "ambiguous_wallet_mapping"
        else:
            reason = ""
        if reason:
            record.update({
                "outcome": "needs_identity",
                "wallet": None,
                "checks": [{"name": "identity", "status": "defer", "reason": reason, "details": {"matches": len(candidates)}}],
                "blockers": [reason],
                "latencyMs": round((time.perf_counter() - started) * 1000, 3),
            })
            return self._append(record)

        entry = candidates[0]
        safe_payload = {
            "eventId": event.id,
            "kind": event.kind,
            "userId": kol_id,
            "networkId": event.network_id,
            "ca": event.ca,
            "amountUsd": event.amount_usd,
            "sourceType": event.source_type,
        }
        raw_hash = hashlib.sha256(
            json.dumps(safe_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        signal_payload = {
            "signalId": record["signalId"],
            "source": "fomo",
            "observedAt": now.isoformat(),
            "sourceTimestamp": _utc_timestamp(event.created_at),
            "chainId": chain_id,
            "kolId": kol_id,
            "wallet": entry.address,
            "walletConfidence": str(entry.confidence),
            "originalTx": event.trade_id or None,
            "side": Side.SELL.value if event.kind in {"sell", "clear"} else Side.BUY.value,
            "tokenIn": event.ca if event.kind in {"sell", "clear"} else "",
            "tokenOut": event.ca if event.kind == "buy" else "",
            "estimatedUsd": event.amount_usd,
            "decoder": "fomo-event@1",
            "rawPayloadHash": raw_hash,
        }
        signal = UnifiedSignal.from_dict(signal_payload)
        decision = self.engine.evaluate(signal, context or RiskContext()).to_dict()
        blockers = [check["reason"] for check in decision["checks"] if check["status"] in {"reject", "defer"}]
        record.update({
            "outcome": decision["outcome"],
            "wallet": entry.address,
            "walletConfidence": str(entry.confidence),
            "policyVersion": decision["policyVersion"],
            "registryVersion": decision["registryVersion"],
            "checks": decision["checks"],
            "blockers": blockers,
            "signal": _signal_dict(signal),
            "latencyMs": round((time.perf_counter() - started) * 1000, 3),
        })
        return self._append(record)
