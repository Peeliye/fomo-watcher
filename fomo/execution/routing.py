"""Deterministic low-latency route selection without network or signing code."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else Decimal("0")
    except (InvalidOperation, ValueError):
        return Decimal("0")


@dataclass(frozen=True)
class RouteQuote:
    provider: str
    chain_id: str
    output_amount: Decimal
    quote_latency_ms: Decimal
    submit_p95_ms: Decimal
    quote_age_ms: Decimal
    price_impact_bps: Decimal
    simulation_passed: bool
    direct: bool = False
    route_allowlisted: bool = True
    scope_validated: bool = True
    minimum_output_amount: Decimal = Decimal("0")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RouteQuote":
        return cls(
            provider=str(data.get("provider") or ""), chain_id=str(data.get("chainId") or ""),
            output_amount=_decimal(data.get("outputAmount")), quote_latency_ms=_decimal(data.get("quoteLatencyMs")),
            submit_p95_ms=_decimal(data.get("submitP95Ms")), quote_age_ms=_decimal(data.get("quoteAgeMs")),
            price_impact_bps=_decimal(data.get("priceImpactBps")),
            simulation_passed=bool(data.get("simulationPassed", False)), direct=bool(data.get("direct", False)),
            route_allowlisted=bool(data.get("routeAllowlisted", False)),
            scope_validated=bool(data.get("scopeValidated", False)),
            minimum_output_amount=_decimal(data.get("minimumOutputAmount")),
        )


@dataclass(frozen=True)
class RouteDecision:
    status: str
    selected_provider: str | None
    reason: str
    eligible_providers: tuple[str, ...]
    rejected: dict[str, tuple[str, ...]]
    expected_output: str | None = None
    expected_latency_ms: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FastRoutePlanner:
    """Choose the fastest safe firm route, not merely the highest quote.

    Firm quotes are expected to be requested concurrently. A warm indicative
    benchmark may be supplied so a sufficiently good early response can win
    without waiting for a slower aggregator.
    """

    def __init__(self, policy: dict[str, Any]):
        self.policy = policy

    def select(
        self,
        chain_id: str | int,
        quotes: Iterable[RouteQuote],
        reference_output_amount: Decimal | str | None = None,
    ) -> RouteDecision:
        wanted_chain = str(chain_id)
        family = "solana" if wanted_chain == "1399811149" else "evm"
        deadline = _decimal(self.policy.get("quote_deadline_ms", {}).get(family, 200))
        maximum_age = _decimal(self.policy.get("maximum_quote_age_ms", 500))
        maximum_impact = _decimal(self.policy.get("maximum_price_impact_bps", 100))
        sacrifice = _decimal(self.policy.get("maximum_output_sacrifice_bps", 30))
        minimum_routes = int(self.policy.get("minimum_independent_routes", 2))
        require_simulation = bool(self.policy.get("require_simulation", True))
        require_allowlisted_route = bool(self.policy.get("require_allowlisted_route", True))
        require_scope_validation = bool(self.policy.get("require_scope_validation", True))
        require_minimum_output = bool(self.policy.get("require_minimum_output", False))
        rejected: dict[str, tuple[str, ...]] = {}
        eligible: list[RouteQuote] = []
        for quote in quotes:
            problems: list[str] = []
            if quote.chain_id != wanted_chain: problems.append("cross_chain_route_forbidden")
            if not quote.provider: problems.append("provider_missing")
            if quote.output_amount <= 0: problems.append("invalid_output")
            if quote.quote_latency_ms > deadline: problems.append("quote_deadline_exceeded")
            if quote.quote_age_ms > maximum_age: problems.append("quote_stale")
            if quote.price_impact_bps > maximum_impact: problems.append("price_impact_too_high")
            if require_simulation and not quote.simulation_passed: problems.append("simulation_required")
            if require_allowlisted_route and not quote.route_allowlisted: problems.append("route_not_allowlisted")
            if require_scope_validation and not quote.scope_validated: problems.append("transaction_scope_invalid")
            if require_minimum_output and quote.minimum_output_amount <= 0: problems.append("minimum_output_required")
            if problems:
                rejected[quote.provider or "unknown"] = tuple(problems)
            else:
                eligible.append(quote)
        providers = {quote.provider for quote in eligible}
        if len(providers) < minimum_routes:
            return RouteDecision("blocked", None, "independent_routes_unavailable", tuple(sorted(providers)), rejected)
        best_received = max(quote.output_amount for quote in eligible)
        reference = max(best_received, _decimal(reference_output_amount or 0))
        minimum_output = reference * (Decimal("1") - sacrifice / Decimal("10000"))
        quality = [quote for quote in eligible if quote.output_amount >= minimum_output]
        if not quality:
            return RouteDecision("blocked", None, "output_protection_failed", tuple(sorted(providers)), rejected)
        selected = min(quality, key=lambda quote: (quote.quote_latency_ms + quote.submit_p95_ms, -quote.output_amount, quote.provider))
        return RouteDecision(
            "selected_for_shadow", selected.provider, "fastest_within_price_guard",
            tuple(sorted(providers)), rejected, str(selected.output_amount),
            str(selected.quote_latency_ms + selected.submit_p95_ms),
        )


def route_readiness(cfg: dict[str, Any]) -> dict[str, Any]:
    settings = cfg.get("routing", {})
    chains = []
    for chain_id, providers in (settings.get("providers", {}) or {}).items():
        output = []
        for provider in providers or []:
            env_key = str(provider.get("api_key_env") or "")
            requires_key = bool(env_key)
            adapter_env = str(provider.get("adapter_env") or "")
            output.append({
                "name": str(provider.get("name") or "unknown"),
                "kind": str(provider.get("kind") or "unknown"),
                "apiKeyEnv": env_key or None,
                "apiKeyConfigured": not requires_key or bool(os.getenv(env_key, "").strip()),
                "requiresRpc": bool(provider.get("requires_rpc", False)),
                "adapterEnv": adapter_env or None,
                # Readiness is supplied only by a registered adapter self-check.
                # Environment strings and arbitrary references are not executable proof.
                "implemented": False,
                "ready": False,
                "selfCheck": "adapter_not_registered",
            })
        ready_count = sum(1 for item in output if item["ready"])
        minimum = int(settings.get("minimum_independent_routes", 2))
        live_blockers = []
        if ready_count < minimum:
            live_blockers.append("independent_executable_routes_required")
        if str(chain_id) == "5042" and not bool(settings.get("arc_reliable_market_data", False)):
            live_blockers.append("arc_reliable_market_data_required")
        if str(chain_id) == "5042" and not bool(settings.get("arc_exit_route_ready", False)):
            live_blockers.append("arc_exit_route_required")
        chains.append({"chainId": int(chain_id) if str(chain_id).isdigit() else chain_id,
                       "providers": output, "liveBlockers": live_blockers,
                       "liveReady": not live_blockers})
    return {
        "mode": str(settings.get("mode", "shadow")), "readOnly": str(settings.get("mode", "shadow")) != "live",
        "crossChainForbidden": bool(settings.get("prohibit_cross_chain", True)),
        "quoteDeadlineMs": settings.get("quote_deadline_ms", {}),
        "minimumIndependentRoutes": int(settings.get("minimum_independent_routes", 2)),
        "maximumOutputSacrificeBps": int(settings.get("maximum_output_sacrifice_bps", 30)),
        "requireSimulation": bool(settings.get("require_simulation", True)),
        "requireAllowlistedRoute": bool(settings.get("require_allowlisted_route", True)),
        "requireScopeValidation": bool(settings.get("require_scope_validation", True)),
        "requireMinimumOutput": bool(settings.get("require_minimum_output", False)),
        "chains": chains,
        "note": "Provider adapters must be explicitly enabled after deployment; configuration alone never marks a route ready.",
    }
