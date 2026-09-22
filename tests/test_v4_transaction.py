import time
import unittest

from fomo.execution.direct_v4 import MAINNET_PROBE_KEY, ZERO, USDC
from fomo.execution.v4_transaction import (build_unsigned_swap, decode_single_swap,
                                           decode_unsigned_swap, encode_single_swap)


class V4TransactionTests(unittest.TestCase):
    def test_both_directions_exact_value_and_native_output(self):
        for token, value, output in ((ZERO, 10**12, USDC), (USDC, 0, ZERO)):
            with self.subTest(token=token):
                tx = build_unsigned_swap(chain_id=1, key=MAINNET_PROBE_KEY,
                                         token_in=token, amount_in=10**12,
                                         minimum_out=1, deadline=int(time.time()) + 100)
                decoded = decode_unsigned_swap(tx, chain_id=1)
                self.assertEqual(decoded["msgValue"], value)
                self.assertEqual(decoded["tokenOut"], output)

    def test_unknown_command_hook_and_noncanonical_trailer_rejected(self):
        data = encode_single_swap(chain_id=1, key=MAINNET_PROBE_KEY, token_in=ZERO,
                                  amount_in=100, minimum_out=1, deadline=123)
        self.assertEqual(decode_single_swap(data, chain_id=1)["amountIn"], 100)
        command = bytearray(data)
        command[4 + 96 + 32] = 0x80  # allow-revert flag is forbidden
        with self.assertRaises(ValueError):
            decode_single_swap(bytes(command), chain_id=1)
        with self.assertRaisesRegex(ValueError, "v4_noncanonical_or_extra_calldata"):
            decode_single_swap(data + b"\x00" * 32, chain_id=1)


if __name__ == "__main__":
    unittest.main()
