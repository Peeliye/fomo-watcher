"""Source-neutral fail-closed execution core."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from fomo.signals.strategy import ExecutionIntent

from .interfaces import TransactionParser
from .transaction_scope import TransactionScopeDecision, validate_transaction_scope


@dataclass(frozen=True, slots=True)
class PreflightEvidence:
    source_fresh: bool
    source_allowed: bool
    balance_fresh: bool
    gas_reserve_usd: Decimal
    minimum_gas_reserve_usd: Decimal
    exposure_allowed: bool
    reservation_committed: bool
    quote_fresh: bool
    firm_quote_count: int
    independent_sanity_price_count: int
    price_impact_allowed: bool
    slippage_allowed: bool
    minimum_output_present: bool
    route_allowlisted: bool
    simulation_passed: bool
    exit_path_ready: bool
    circuit_breaker_clear: bool
    asset_preapproved: bool = False
    canary_allowed: bool = False

    def blockers(self) -> tuple[str, ...]:
        checks = {
            "source_stale": self.source_fresh,
            "source_not_allowed": self.source_allowed,
            "balance_snapshot_stale": self.balance_fresh,
            "native_gas_reserve_too_low": self.gas_reserve_usd >= self.minimum_gas_reserve_usd,
            "exposure_limit_reached": self.exposure_allowed,
            "funds_not_reserved": self.reservation_committed,
            "quote_stale": self.quote_fresh,
            "firm_executable_quote_required": self.firm_quote_count >= 1,
            "independent_sanity_price_required": self.independent_sanity_price_count >= 1,
            "price_impact_too_high": self.price_impact_allowed,
            "slippage_too_high": self.slippage_allowed,
            "minimum_output_required": self.minimum_output_present,
            "route_not_allowlisted": self.route_allowlisted,
            "transaction_simulation_failed": self.simulation_passed,
            "exit_path_unavailable": self.exit_path_ready,
            "circuit_breaker_tripped": self.circuit_breaker_clear,
            "unknown_asset": self.asset_preapproved or self.canary_allowed,
        }
        return tuple(reason for reason, passed in checks.items() if not passed)


def validate_serialized_transaction(
    serialized_transaction: bytes,
    parser: TransactionParser,
    *,
    expected_chain_id: str | int,
    expected_wallet: str,
    expected_token_out: str,
    maximum_sell_amount: Decimal | str | int,
    trusted_targets: set[str],
) -> tuple[TransactionScopeDecision, str, Mapping[str, Any]]:
    """Parse the exact final bytes and validate that result, never caller metadata."""

    if not serialized_transaction:
        return TransactionScopeDecision(False, ("serialized_transaction_required",)), "", {}
    parsed = parser.parse(serialized_transaction)
    decision = validate_transaction_scope(
        dict(parsed), expected_chain_id=expected_chain_id, expected_wallet=expected_wallet,
        expected_token_out=expected_token_out, maximum_sell_amount=maximum_sell_amount,
        trusted_targets=trusted_targets,
    )
    return decision, hashlib.sha256(serialized_transaction).hexdigest(), parsed


class SharedExecutionCore:
    """Consumes only ExecutionIntent; source adapters and platform fields are absent."""

    def preflight(self, intent: ExecutionIntent, evidence: PreflightEvidence) -> tuple[bool, tuple[str, ...]]:
        del intent
        blockers = evidence.blockers()
        return not blockers, blockers
