"""Read-only wallet registry, unified signal model, and risk decision engine.

This module deliberately has no signer, private-key, transaction-building, or
broadcasting dependency. Its strongest positive result is
``approved_for_shadow``; it can never authorize a live trade.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


SOLANA_CHAIN_ID = "1399811149"
EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
SOLANA_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field_name} is required")
    if result.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return result.astimezone(timezone.utc)


def decimal_value(value: Any, field_name: str, default: str = "0") -> Decimal:
    if value is None:
        value = default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def chain_family(chain_id: str) -> str:
    return "solana" if str(chain_id) == SOLANA_CHAIN_ID else "evm"


def normalize_wallet(chain_id: str, wallet: str) -> str:
    value = str(wallet or "").strip()
    family = chain_family(chain_id)
    matcher = SOLANA_ADDRESS if family == "solana" else EVM_ADDRESS
    if not matcher.fullmatch(value):
        raise ValueError(f"invalid {family} wallet address")
    return value if family == "solana" else value.lower()


class SignalSource(str, Enum):
    FOMO = "fomo"
    FOMO_PUSH = "fomo_push"
    WALLET_RPC_EVM = "wallet_rpc_evm"
    WALLET_RPC_SOLANA = "wallet_rpc_solana"
    EVM_PENDING = "evm_pending"
    SOLANA_PREPROCESSED = "solana_preprocessed"
    SOLANA_PROCESSED = "solana_processed"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class CheckStatus(str, Enum):
    PASS = "pass"
    REJECT = "reject"
    DEFER = "defer"


@dataclass(frozen=True)
class UnifiedSignal:
    signal_id: str
    source: SignalSource
    observed_at: datetime
    source_timestamp: datetime | None
    chain_id: str
    kol_id: str
    wallet: str
    wallet_confidence: Decimal
    original_tx: str | None
    side: Side
    token_in: str
    token_out: str
    estimated_usd: Decimal
    decoder: str
    raw_payload_hash: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "UnifiedSignal":
        required = ("signalId", "source", "observedAt", "chainId")
        missing = [name for name in required if not str(data.get(name, "")).strip()]
        if missing:
            raise ValueError("missing required signal fields: " + ", ".join(missing))
        try:
            source = SignalSource(str(data["source"]))
        except ValueError as exc:
            raise ValueError(f"unsupported signal source: {data.get('source')}") from exc
        try:
            side = Side(str(data.get("side", "unknown")))
        except ValueError as exc:
            raise ValueError(f"unsupported side: {data.get('side')}") from exc
        source_time = data.get("sourceTimestamp")
        confidence = decimal_value(data.get("walletConfidence"), "walletConfidence")
        if confidence < 0 or confidence > 1:
            raise ValueError("walletConfidence must be between 0 and 1")
        signal_id = str(data["signalId"]).strip()
        if len(signal_id) > 256:
            raise ValueError("signalId is too long")
        wallet_value = data.get("actorWallet") or data.get("wallet")
        if not str(wallet_value or "").strip():
            raise ValueError("missing required signal fields: actorWallet")
        wallet_source = source in {SignalSource.WALLET_RPC_EVM, SignalSource.WALLET_RPC_SOLANA}
        if not wallet_source and not str(data.get("kolId") or "").strip():
            raise ValueError("missing required signal fields: kolId")
        return cls(
            signal_id=signal_id,
            source=source,
            observed_at=parse_time(data["observedAt"], "observedAt"),
            source_timestamp=parse_time(source_time, "sourceTimestamp") if source_time else None,
            chain_id=str(data["chainId"]),
            kol_id=str(data["kolId"]).strip(),
            wallet=normalize_wallet(str(data["chainId"]), str(wallet_value)),
            wallet_confidence=confidence,
            original_tx=str(data["originalTx"]).strip() if data.get("originalTx") else None,
            side=side,
            token_in=str(data.get("tokenIn", "")).strip(),
            token_out=str(data.get("tokenOut", "")).strip(),
            estimated_usd=decimal_value(data.get("estimatedUsd"), "estimatedUsd"),
            decoder=str(data.get("decoder", "unknown")).strip() or "unknown",
            raw_payload_hash=str(data.get("rawPayloadHash", "")).strip(),
        )

    @property
    def effective_timestamp(self) -> datetime:
        return self.source_timestamp or self.observed_at

    def public_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["source"] = self.source.value
        result["side"] = self.side.value
        result["observed_at"] = self.observed_at.isoformat()
        result["source_timestamp"] = (
            self.source_timestamp.isoformat() if self.source_timestamp else None
        )
        result["wallet_confidence"] = str(self.wallet_confidence)
        result["estimated_usd"] = str(self.estimated_usd)
        return result


@dataclass(frozen=True)
class WalletEvidence:
    evidence_type: str
    reference: str
    recorded_at: datetime

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WalletEvidence":
        return cls(
            evidence_type=str(data.get("type", "unknown")),
            reference=str(data.get("reference", "")),
            recorded_at=parse_time(data["recordedAt"], "evidence.recordedAt"),
        )


@dataclass(frozen=True)
class WalletEntry:
    kol_id: str
    handle: str
    chain_ids: tuple[str, ...]
    address: str
    confidence: Decimal
    evidence: tuple[WalletEvidence, ...]
    verified_at: datetime
    expires_at: datetime
    status: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WalletEntry":
        chain_ids = tuple(str(value) for value in data.get("chainIds", ()))
        if not chain_ids:
            raise ValueError("wallet entry requires at least one chainId")
        families = {chain_family(value) for value in chain_ids}
        if len(families) != 1:
            raise ValueError("one wallet entry cannot mix EVM and Solana chain families")
        address = normalize_wallet(chain_ids[0], str(data.get("address", "")))
        confidence = decimal_value(data.get("confidence"), "wallet confidence")
        if confidence < 0 or confidence > 1:
            raise ValueError("wallet confidence must be between 0 and 1")
        status = str(data.get("status", "shadow-only"))
        if status not in {"active", "shadow-only", "revoked"}:
            raise ValueError(f"unsupported wallet status: {status}")
        return cls(
            kol_id=str(data.get("kolId", "")).strip(),
            handle=str(data.get("handle", "")).strip(),
            chain_ids=chain_ids,
            address=address,
            confidence=confidence,
            evidence=tuple(WalletEvidence.from_dict(item) for item in data.get("evidence", ())),
            verified_at=parse_time(data["verifiedAt"], "verifiedAt"),
            expires_at=parse_time(data["expiresAt"], "expiresAt"),
            status=status,
        )


class WalletRegistry:
    """Validated in-memory lookup. Ambiguous ownership fails at load time."""

    def __init__(self, entries: Iterable[WalletEntry], version: int = 1):
        self.version = int(version)
        self.entries = tuple(entries)
        self._index: dict[tuple[str, str], WalletEntry] = {}
        for entry in self.entries:
            if not entry.kol_id:
                raise ValueError("wallet entry requires kolId")
            for chain_id in entry.chain_ids:
                key = (chain_id, normalize_wallet(chain_id, entry.address))
                previous = self._index.get(key)
                if previous and previous.kol_id != entry.kol_id:
                    raise ValueError(
                        f"ambiguous wallet ownership on chain {chain_id}: "
                        f"{previous.kol_id} and {entry.kol_id}"
                    )
                if previous:
                    raise ValueError(f"duplicate wallet registry entry: {chain_id}:{entry.address}")
                self._index[key] = entry

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WalletRegistry":
        return cls(
            (WalletEntry.from_dict(item) for item in data.get("wallets", ())),
            version=int(data.get("version", 1)),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "WalletRegistry":
        with Path(path).open("r", encoding="utf-8") as stream:
            return cls.from_dict(json.load(stream))

    def resolve(self, chain_id: str, wallet: str) -> WalletEntry | None:
        return self._index.get((str(chain_id), normalize_wallet(str(chain_id), wallet)))

    def wallets_for_kol(
        self, chain_id: str, kol_id: str, include_revoked: bool = False
    ) -> tuple[WalletEntry, ...]:
        """Return candidate wallets for one KOL on one chain.

        Revoked entries are historical audit records and must not make an
        otherwise unique Fomo identity mapping appear ambiguous.
        """
        wanted_chain = str(chain_id)
        wanted_kol = str(kol_id).strip()
        return tuple(
            entry for entry in self.entries
            if entry.kol_id == wanted_kol
            and wanted_chain in entry.chain_ids
            and (include_revoked or entry.status != "revoked")
        )

    def wallets_for_verified_handle(self, chain_id: str, handle: str) -> tuple[WalletEntry, ...]:
        """Resolve only exact, evidenced aliases that point to a real platform id."""
        wanted_chain = str(chain_id)
        wanted_handle = str(handle).strip().lstrip("@").casefold()
        return tuple(
            entry for entry in self.entries
            if entry.handle.strip().lstrip("@").casefold() == wanted_handle
            and not entry.kol_id.startswith("handle:")
            and wanted_chain in entry.chain_ids
            and entry.status != "revoked"
            and bool(entry.evidence)
        )


@dataclass(frozen=True)
class AssetSnapshot:
    liquidity_usd: Decimal
    market_cap_usd: Decimal
    buy_tax_bps: Decimal
    sell_tax_bps: Decimal
    top_holder_percent: Decimal
    privileges_known_safe: bool
    sell_simulation_passed: bool
    route_allowlisted: bool

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AssetSnapshot":
        return cls(
            liquidity_usd=decimal_value(data.get("liquidityUsd"), "liquidityUsd"),
            market_cap_usd=decimal_value(data.get("marketCapUsd"), "marketCapUsd"),
            buy_tax_bps=decimal_value(data.get("buyTaxBps"), "buyTaxBps"),
            sell_tax_bps=decimal_value(data.get("sellTaxBps"), "sellTaxBps"),
            top_holder_percent=decimal_value(data.get("topHolderPercent"), "topHolderPercent"),
            privileges_known_safe=bool(data.get("privilegesKnownSafe", False)),
            sell_simulation_passed=bool(data.get("sellSimulationPassed", False)),
            route_allowlisted=bool(data.get("routeAllowlisted", False)),
        )


@dataclass(frozen=True)
class Quote:
    provider: str
    output_amount: Decimal
    price_impact_bps: Decimal
    captured_at: datetime

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Quote":
        return cls(
            provider=str(data.get("provider", "")).strip(),
            output_amount=decimal_value(data.get("outputAmount"), "outputAmount"),
            price_impact_bps=decimal_value(data.get("priceImpactBps"), "priceImpactBps"),
            captured_at=parse_time(data["capturedAt"], "capturedAt"),
        )


@dataclass(frozen=True)
class ExposureSnapshot:
    per_token_usd: Decimal = Decimal("0")
    per_kol_daily_usd: Decimal = Decimal("0")
    global_daily_usd: Decimal = Decimal("0")
    open_positions: int = 0
    chain_exposure_percent: Decimal = Decimal("0")
    native_gas_reserve_usd: Decimal = Decimal("0")
    has_open_position: bool = False

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExposureSnapshot":
        return cls(
            per_token_usd=decimal_value(data.get("perTokenUsd"), "perTokenUsd"),
            per_kol_daily_usd=decimal_value(data.get("perKolDailyUsd"), "perKolDailyUsd"),
            global_daily_usd=decimal_value(data.get("globalDailyUsd"), "globalDailyUsd"),
            open_positions=int(data.get("openPositions", 0)),
            chain_exposure_percent=decimal_value(data.get("chainExposurePercent"), "chainExposurePercent"),
            native_gas_reserve_usd=decimal_value(data.get("nativeGasReserveUsd"), "nativeGasReserveUsd"),
            has_open_position=bool(data.get("hasOpenPosition", False)),
        )


@dataclass(frozen=True)
class HealthSnapshot:
    clock_offset_ms: Decimal = Decimal("0")
    primary_stream_connected: bool = False
    whitelist_fresh: bool = False
    evm_rpc_block_lag: int = 0
    solana_rpc_slot_lag: int = 0
    circuit_breaker_tripped: bool = False

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HealthSnapshot":
        return cls(
            clock_offset_ms=decimal_value(data.get("clockOffsetMs"), "clockOffsetMs"),
            primary_stream_connected=bool(data.get("primaryStreamConnected", False)),
            whitelist_fresh=bool(data.get("whitelistFresh", False)),
            evm_rpc_block_lag=int(data.get("evmRpcBlockLag", 0)),
            solana_rpc_slot_lag=int(data.get("solanaRpcSlotLag", 0)),
            circuit_breaker_tripped=bool(data.get("circuitBreakerTripped", False)),
        )


@dataclass(frozen=True)
class RiskContext:
    asset: AssetSnapshot | None = None
    quotes: tuple[Quote, ...] = ()
    exposure: ExposureSnapshot | None = None
    health: HealthSnapshot | None = None
    exit_plan_ready: bool = False
    transaction_simulation_passed: bool | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "RiskContext":
        data = data or {}
        return cls(
            asset=AssetSnapshot.from_dict(data["asset"]) if data.get("asset") else None,
            quotes=tuple(Quote.from_dict(item) for item in data.get("quotes", ())),
            exposure=ExposureSnapshot.from_dict(data["exposure"]) if data.get("exposure") else None,
            health=HealthSnapshot.from_dict(data["health"]) if data.get("health") else None,
            exit_plan_ready=bool(data.get("exitPlanReady", False)),
            transaction_simulation_passed=(
                bool(data["transactionSimulationPassed"])
                if "transactionSimulationPassed" in data
                else None
            ),
        )


@dataclass(frozen=True)
class RiskCheck:
    name: str
    status: CheckStatus
    reason: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "reason": self.reason,
            "details": self.details,
        }


@dataclass(frozen=True)
class RiskDecision:
    signal_id: str
    outcome: str
    decided_at: datetime
    policy_version: int
    registry_version: int
    checks: tuple[RiskCheck, ...]
    read_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "signalId": self.signal_id,
            "outcome": self.outcome,
            "decidedAt": self.decided_at.isoformat(),
            "policyVersion": self.policy_version,
            "registryVersion": self.registry_version,
            "readOnly": self.read_only,
            "checks": [check.to_dict() for check in self.checks],
        }


class ReadOnlyRiskEngine:
    """Fail-closed evaluator. It never returns live approval."""

    def __init__(
        self,
        policy: Mapping[str, Any],
        registry: WalletRegistry,
        now: Callable[[], datetime] = utc_now,
    ):
        self.policy = dict(policy)
        self.registry = registry
        self.now = now
        self.policy_version = int(self.policy.get("version", 1))
        self._seen: dict[str, datetime] = {}

    @classmethod
    def from_files(
        cls, policy_path: str | Path, registry_path: str | Path
    ) -> "ReadOnlyRiskEngine":
        with Path(policy_path).open("r", encoding="utf-8") as stream:
            policy = json.load(stream)
        return cls(policy, WalletRegistry.from_file(registry_path))

    def _value(self, section: str, name: str, default: Any) -> Any:
        value = self.policy.get(section, {})
        return value.get(name, default) if isinstance(value, Mapping) else default

    @staticmethod
    def _check(name: str, status: CheckStatus, reason: str, **details: Any) -> RiskCheck:
        return RiskCheck(name=name, status=status, reason=reason, details=details)

    def evaluate(
        self, signal: UnifiedSignal, context: RiskContext | None = None
    ) -> RiskDecision:
        context = context or RiskContext()
        now = self.now().astimezone(timezone.utc)
        checks: list[RiskCheck] = []

        dedupe_seconds = int(self._value("signals", "dedupeWindowSeconds", 900))
        cutoff = now.timestamp() - dedupe_seconds
        self._seen = {key: seen for key, seen in self._seen.items() if seen.timestamp() >= cutoff}
        if signal.signal_id in self._seen:
            checks.append(self._check("dedupe", CheckStatus.REJECT, "duplicate_signal"))
        else:
            checks.append(self._check("dedupe", CheckStatus.PASS, "unique_signal"))
            self._seen[signal.signal_id] = now

        allowed_sources = set(self._value("signals", "allowedSources", ()))
        checks.append(
            self._check(
                "source",
                CheckStatus.PASS if signal.source.value in allowed_sources else CheckStatus.REJECT,
                "source_allowed" if signal.source.value in allowed_sources else "source_not_allowed",
                source=signal.source.value,
            )
        )

        age_ms = Decimal(str((now - signal.effective_timestamp).total_seconds() * 1000))
        maximum_ages = self._value("signals", "maximumAgeMs", {})
        maximum_age = Decimal(str(maximum_ages.get(signal.source.value, 0)))
        if age_ms < Decimal("-100"):
            age_status, age_reason = CheckStatus.REJECT, "signal_timestamp_in_future"
        elif maximum_age <= 0 or age_ms > maximum_age:
            age_status, age_reason = CheckStatus.REJECT, "signal_too_old"
        else:
            age_status, age_reason = CheckStatus.PASS, "signal_fresh"
        checks.append(
            self._check(
                "signal_age", age_status, age_reason,
                ageMs=round(float(age_ms), 3), maximumAgeMs=float(maximum_age),
            )
        )

        wallet_source = signal.source in {SignalSource.WALLET_RPC_EVM, SignalSource.WALLET_RPC_SOLANA}
        entry = None if wallet_source else self.registry.resolve(signal.chain_id, signal.wallet)
        if wallet_source:
            checks.append(self._check("identity", CheckStatus.PASS, "wallet_source_actor_present"))
        elif entry is None:
            checks.append(self._check("identity", CheckStatus.REJECT, "wallet_not_registered"))
        elif entry.kol_id != signal.kol_id:
            checks.append(self._check("identity", CheckStatus.REJECT, "wallet_kol_mismatch"))
        elif entry.status == "revoked":
            checks.append(self._check("identity", CheckStatus.REJECT, "wallet_revoked"))
        elif entry.expires_at <= now:
            checks.append(self._check("identity", CheckStatus.REJECT, "wallet_mapping_expired"))
        else:
            minimum = Decimal(str(self._value("identity", "minimumWalletConfidenceForShadow", 0.6)))
            effective_confidence = min(entry.confidence, signal.wallet_confidence)
            checks.append(
                self._check(
                    "identity",
                    CheckStatus.PASS if effective_confidence >= minimum else CheckStatus.REJECT,
                    "wallet_identity_verified" if effective_confidence >= minimum else "wallet_confidence_too_low",
                    registryConfidence=str(entry.confidence),
                    signalConfidence=str(signal.wallet_confidence),
                    requiredConfidence=str(minimum),
                )
            )

        reject_unknown_side = bool(self._value("signals", "rejectUnknownSide", True))
        side_ok = not (reject_unknown_side and signal.side is Side.UNKNOWN)
        checks.append(self._check("side", CheckStatus.PASS if side_ok else CheckStatus.REJECT, "side_known" if side_ok else "unknown_side"))
        decoder_ok = signal.decoder.lower() != "unknown"
        checks.append(self._check("decoder", CheckStatus.PASS if decoder_ok else CheckStatus.REJECT, "decoder_known" if decoder_ok else "unknown_decoder", decoder=signal.decoder))

        if context.asset is None:
            checks.append(self._check("asset", CheckStatus.DEFER, "asset_snapshot_required"))
        else:
            asset = context.asset
            asset_policy = self.policy.get("asset", {})
            problems: list[str] = []
            if asset.liquidity_usd < Decimal(str(asset_policy.get("minimumLiquidityUsd", 0))): problems.append("liquidity_too_low")
            if asset.market_cap_usd < Decimal(str(asset_policy.get("minimumMarketCapUsd", 0))): problems.append("market_cap_too_low")
            if asset.buy_tax_bps > Decimal(str(asset_policy.get("maximumBuyTaxBps", 0))): problems.append("buy_tax_too_high")
            if asset.sell_tax_bps > Decimal(str(asset_policy.get("maximumSellTaxBps", 0))): problems.append("sell_tax_too_high")
            if asset.top_holder_percent > Decimal(str(asset_policy.get("maximumTopHolderPercent", 100))): problems.append("holder_concentration_too_high")
            if not asset.privileges_known_safe: problems.append("mutable_or_unknown_privileges")
            if not asset.sell_simulation_passed: problems.append("sell_simulation_failed")
            if not asset.route_allowlisted: problems.append("route_not_allowlisted")
            checks.append(self._check("asset", CheckStatus.REJECT if problems else CheckStatus.PASS, problems[0] if problems else "asset_checks_passed", problems=problems))

        minimum_quotes = int(self._value("execution", "minimumIndependentQuotes", 2))
        unique_quotes = {quote.provider: quote for quote in context.quotes if quote.provider}
        if len(unique_quotes) < minimum_quotes:
            checks.append(self._check("quotes", CheckStatus.DEFER, "independent_quotes_required", available=len(unique_quotes), required=minimum_quotes))
        else:
            quote_values = list(unique_quotes.values())
            maximum_quote_age = Decimal(str(self._value("execution", "maximumQuoteAgeMs", 750)))
            maximum_impact = Decimal(str(self._value("execution", "maximumPriceImpactBps", 100)))
            stale = [q.provider for q in quote_values if Decimal(str((now - q.captured_at).total_seconds() * 1000)) > maximum_quote_age]
            impacted = [q.provider for q in quote_values if q.price_impact_bps > maximum_impact]
            outputs = [q.output_amount for q in quote_values if q.output_amount > 0]
            divergence = Decimal("Infinity")
            if len(outputs) == len(quote_values) and min(outputs) > 0:
                divergence = (max(outputs) - min(outputs)) / min(outputs) * Decimal("10000")
            maximum_divergence = Decimal(str(self._value("execution", "maximumQuoteDivergenceBps", 100)))
            problems = []
            if stale: problems.append("quote_too_old")
            if impacted: problems.append("price_impact_too_high")
            if divergence > maximum_divergence: problems.append("quote_divergence_too_high")
            checks.append(self._check("quotes", CheckStatus.REJECT if problems else CheckStatus.PASS, problems[0] if problems else "quotes_agree", providers=list(unique_quotes), divergenceBps=str(divergence), problems=problems))

        if context.exposure is None:
            checks.append(self._check("exposure", CheckStatus.DEFER, "exposure_snapshot_required"))
        else:
            exposure = context.exposure
            policy = self.policy.get("exposure", {})
            proposed = Decimal(str(self._value("execution", "paperBuyUsd", 10)))
            problems = []
            if exposure.per_token_usd + proposed > Decimal(str(policy.get("maximumPerTokenUsd", 0))): problems.append("per_token_limit_reached")
            if exposure.per_kol_daily_usd + proposed > Decimal(str(policy.get("maximumPerKolDailyUsd", 0))): problems.append("per_kol_daily_limit_reached")
            if exposure.global_daily_usd + proposed > Decimal(str(policy.get("maximumGlobalDailyUsd", 0))): problems.append("global_daily_limit_reached")
            if not exposure.has_open_position and exposure.open_positions >= int(policy.get("maximumOpenPositions", 0)): problems.append("maximum_open_positions_reached")
            if exposure.chain_exposure_percent > Decimal(str(policy.get("maximumChainExposurePercent", 100))): problems.append("chain_exposure_too_high")
            if exposure.native_gas_reserve_usd < Decimal(str(policy.get("minimumNativeGasReserveUsd", 0))): problems.append("native_gas_reserve_too_low")
            checks.append(self._check("exposure", CheckStatus.REJECT if problems else CheckStatus.PASS, problems[0] if problems else "exposure_within_limits", proposedUsd=str(proposed), problems=problems))

        if context.health is None:
            checks.append(self._check("health", CheckStatus.DEFER, "health_snapshot_required"))
        else:
            health = context.health
            policy = self.policy.get("circuitBreakers", {})
            problems = []
            if health.circuit_breaker_tripped: problems.append("circuit_breaker_tripped")
            if abs(health.clock_offset_ms) > Decimal(str(policy.get("maximumClockOffsetMs", 100))): problems.append("clock_offset_too_high")
            if policy.get("tripOnPrimaryStreamDisconnect", True) and not health.primary_stream_connected: problems.append("primary_stream_disconnected")
            if policy.get("tripOnWhitelistExpiry", True) and not health.whitelist_fresh: problems.append("whitelist_stale")
            if chain_family(signal.chain_id) == "evm" and health.evm_rpc_block_lag > int(policy.get("maximumEvmRpcBlockLag", 1)): problems.append("evm_rpc_lagging")
            if chain_family(signal.chain_id) == "solana" and health.solana_rpc_slot_lag > int(policy.get("maximumSolanaRpcSlotLag", 2)): problems.append("solana_rpc_lagging")
            checks.append(self._check("health", CheckStatus.REJECT if problems else CheckStatus.PASS, problems[0] if problems else "system_healthy", problems=problems))

        if not context.exit_plan_ready:
            checks.append(self._check("exit_plan", CheckStatus.DEFER, "exit_plan_required"))
        else:
            checks.append(self._check("exit_plan", CheckStatus.PASS, "exit_plan_ready"))

        simulation_required = bool(self._value("execution", "requireSimulation", True))
        if simulation_required and context.transaction_simulation_passed is None:
            checks.append(self._check("simulation", CheckStatus.DEFER, "transaction_simulation_required"))
        elif simulation_required and not context.transaction_simulation_passed:
            checks.append(self._check("simulation", CheckStatus.REJECT, "transaction_simulation_failed"))
        else:
            checks.append(self._check("simulation", CheckStatus.PASS, "transaction_simulation_passed"))

        checks.append(self._check("read_only_barrier", CheckStatus.PASS, "live_trading_not_authorized"))
        statuses = {check.status for check in checks}
        if CheckStatus.REJECT in statuses:
            outcome = "rejected"
        elif CheckStatus.DEFER in statuses:
            outcome = "needs_data"
        else:
            outcome = "approved_for_shadow"
        return RiskDecision(signal.signal_id, outcome, now, self.policy_version, self.registry.version, tuple(checks))


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate one unified signal without trading")
    parser.add_argument("--policy", default="risk-policy.example.json")
    parser.add_argument("--registry", default="wallet-registry.json")
    parser.add_argument("--signal", required=True, help="JSON file containing a unified signal")
    parser.add_argument("--context", help="optional JSON file containing risk context")
    args = parser.parse_args()
    engine = ReadOnlyRiskEngine.from_files(args.policy, args.registry)
    with Path(args.signal).open("r", encoding="utf-8") as stream:
        signal = UnifiedSignal.from_dict(json.load(stream))
    if args.context:
        with Path(args.context).open("r", encoding="utf-8") as stream:
            context = RiskContext.from_dict(json.load(stream))
    else:
        context = RiskContext()
    print(json.dumps(engine.evaluate(signal, context).to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
