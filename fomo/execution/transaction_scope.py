"""Chain-agnostic wallet-safety boundary for fast copy transactions.

Token quality is deliberately not evaluated here. The validator only proves
that a prepared transaction can spend no more than the requested order and can
touch only explicitly trusted routing targets/operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


SAFE_OPERATIONS = frozenset({"swap", "create_ata", "wrap_native", "unwrap_native", "compute_budget"})


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    return result if result.is_finite() else Decimal("0")


@dataclass(frozen=True)
class TransactionScopeDecision:
    valid: bool
    reasons: tuple[str, ...]


def validate_transaction_scope(
    envelope: dict[str, Any],
    *,
    expected_chain_id: str | int,
    expected_wallet: str,
    expected_token_out: str,
    maximum_sell_amount: Decimal | str | int,
    trusted_targets: Iterable[str],
    allowed_operations: Iterable[str] = SAFE_OPERATIONS,
) -> TransactionScopeDecision:
    """Fail closed on unknown targets, extra spend, approvals or operations."""

    reasons: list[str] = []

    def normalize(value: Any) -> str:
        return str(value or "").strip().casefold()

    trusted = {normalize(value) for value in trusted_targets if normalize(value)}
    allowed = {normalize(value) for value in allowed_operations if normalize(value)}
    chain_id = str(envelope.get("chainId") or "")
    wallet = normalize(envelope.get("wallet"))
    token_out = normalize(envelope.get("tokenOut"))
    sell_amount = _decimal(envelope.get("sellAmount"))
    minimum_output = _decimal(envelope.get("minimumOutputAmount"))
    targets = {normalize(value) for value in envelope.get("targets", []) if normalize(value)}
    operations = {normalize(value) for value in envelope.get("operations", []) if normalize(value)}

    if chain_id != str(expected_chain_id):
        reasons.append("chain_mismatch")
    if wallet != normalize(expected_wallet):
        reasons.append("execution_wallet_mismatch")
    if token_out != normalize(expected_token_out):
        reasons.append("output_token_mismatch")
    if sell_amount <= 0 or sell_amount > _decimal(maximum_sell_amount):
        reasons.append("spend_limit_exceeded")
    if minimum_output <= 0:
        reasons.append("minimum_output_required")
    if not targets or not trusted or not targets.issubset(trusted):
        reasons.append("untrusted_transaction_target")
    if not operations or not operations.issubset(allowed):
        reasons.append("unexpected_transaction_operation")

    for approval in envelope.get("approvals", []) or []:
        if not isinstance(approval, dict):
            reasons.append("invalid_approval")
            continue
        spender = normalize(approval.get("spender"))
        amount = _decimal(approval.get("amount"))
        if spender not in trusted:
            reasons.append("untrusted_approval_spender")
        if amount <= 0 or amount > _decimal(maximum_sell_amount):
            reasons.append("approval_exceeds_order")

    return TransactionScopeDecision(not reasons, tuple(dict.fromkeys(reasons)))
