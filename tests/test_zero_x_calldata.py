from __future__ import annotations

import unittest

from fomo.execution.zero_x_calldata import (ALLOWANCE_HOLDER_EXEC, SETTLER_EXECUTE,
                                             inspect_allowance_holder_settler)


WALLET = "0x" + "11" * 20
SELL = "0x" + "22" * 20
BUY = "0x" + "33" * 20
SETTLER = "0x" + "44" * 20


def _word(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _address(value: str) -> bytes:
    return bytes(12) + bytes.fromhex(value[2:])


def _dynamic(value: bytes) -> bytes:
    return _word(len(value)) + value + bytes((-len(value)) % 32)


def allowance_holder_fixture(*, recipient: str = WALLET, sell_amount: int = 10_000_000,
                             minimum: int = 4_500_000, action: bytes | None = None) -> bytes:
    # ABI layout follows the official IAllowanceHolder and ISettlerTakerSubmitted
    # interfaces. The action is intentionally unaudited: this fixture tests only
    # envelope decoding, not permission to execute the route.
    action = action if action is not None else bytes.fromhex("12345678") + _word(1)
    actions = _word(1) + _word(32) + _dynamic(action)
    settler_body = (_address(recipient) + _address(BUY) + _word(minimum)
                    + _word(160) + bytes(32) + actions)
    inner = SETTLER_EXECUTE + settler_body
    outer = (_address(SETTLER) + _address(SELL) + _word(sell_amount)
             + _address(SETTLER) + _word(160) + _dynamic(inner))
    return ALLOWANCE_HOLDER_EXEC + outer


class ZeroXCalldataTests(unittest.TestCase):
    def test_official_abi_selectors_and_canonical_envelope(self):
        self.assertEqual(ALLOWANCE_HOLDER_EXEC.hex(), "2213bc0b")
        self.assertEqual(SETTLER_EXECUTE.hex(), "1fff991f")
        call = inspect_allowance_holder_settler(allowance_holder_fixture())
        self.assertEqual(call.sell_token, SELL)
        self.assertEqual(call.buy_token, BUY)
        self.assertEqual(call.recipient, WALLET)
        self.assertEqual(call.sell_amount, 10_000_000)
        self.assertEqual(call.minimum_buy_amount, 4_500_000)
        self.assertEqual(len(call.actions), 1)

    def test_rejects_noncanonical_offsets_padding_and_extra_data(self):
        fixture = allowance_holder_fixture()
        for changed in (
            fixture[:4 + 128] + _word(192) + fixture[4 + 160:],
            fixture[:-1] + b"\x01",
            fixture + bytes(32),
            fixture[:4 + 160 + 32 + 4 + 96] + _word(192) + fixture[4 + 160 + 32 + 4 + 128:],
        ):
            with self.subTest(changed=changed[-8:]):
                with self.assertRaisesRegex(ValueError, "zero_x_"):
                    inspect_allowance_holder_settler(changed)

    def test_rejects_empty_action_and_zero_minimum(self):
        with self.assertRaisesRegex(ValueError, "zero_x_action_invalid"):
            inspect_allowance_holder_settler(allowance_holder_fixture(action=b"\x01"))
        with self.assertRaisesRegex(ValueError, "zero_x_minimum_output_required"):
            inspect_allowance_holder_settler(allowance_holder_fixture(minimum=0))


if __name__ == "__main__":
    unittest.main()
