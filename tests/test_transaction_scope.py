from decimal import Decimal

from fomo.execution.transaction_scope import validate_transaction_scope


def envelope(**changes):
    value = {
        "chainId": 1,
        "wallet": "0x1111111111111111111111111111111111111111",
        "tokenOut": "0x2222222222222222222222222222222222222222",
        "sellAmount": "10000000",
        "minimumOutputAmount": "1",
        "targets": ["0x3333333333333333333333333333333333333333"],
        "operations": ["swap"],
        "approvals": [{"spender": "0x3333333333333333333333333333333333333333", "amount": "10000000"}],
    }
    value.update(changes)
    return value


def validate(value):
    return validate_transaction_scope(
        value,
        expected_chain_id=1,
        expected_wallet="0x1111111111111111111111111111111111111111",
        expected_token_out="0x2222222222222222222222222222222222222222",
        maximum_sell_amount=Decimal("10000000"),
        trusted_targets=["0x3333333333333333333333333333333333333333"],
    )


def test_exact_scoped_swap_is_allowed_without_token_safety_checks():
    assert validate(envelope()).valid


def test_unlimited_or_excess_approval_is_rejected():
    decision = validate(envelope(approvals=[{
        "spender": "0x3333333333333333333333333333333333333333",
        "amount": str(2**256 - 1),
    }]))
    assert not decision.valid
    assert "approval_exceeds_order" in decision.reasons


def test_unknown_target_and_extra_transfer_are_rejected():
    decision = validate(envelope(
        targets=["0x4444444444444444444444444444444444444444"],
        operations=["swap", "transfer"],
    ))
    assert not decision.valid
    assert "untrusted_transaction_target" in decision.reasons
    assert "unexpected_transaction_operation" in decision.reasons


def test_minimum_output_is_mandatory():
    decision = validate(envelope(minimumOutputAmount="0"))
    assert not decision.valid
    assert "minimum_output_required" in decision.reasons
