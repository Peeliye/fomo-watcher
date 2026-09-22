from __future__ import annotations

import time
import unittest
from decimal import Decimal

from coincurve import PrivateKey

from fomo.execution.core import validate_serialized_transaction
from fomo.execution.evm_transaction import (
    EvmEip1559Builder, EvmTransactionParser, decode_v2_swap, keccak256, sign_eip1559,
)
from fomo.execution.interfaces import ExecutableQuote
from fomo.signals.strategy import ExecutionIntent


ROUTER = "0x2222222222222222222222222222222222222222"
USDC = "0x3333333333333333333333333333333333333333"
TOKEN = "0x4444444444444444444444444444444444444444"


def word(value: int) -> bytes:
    return value.to_bytes(32, "big")


def calldata(recipient: str, deadline: int) -> bytes:
    return (bytes.fromhex("38ed1739") + word(10_000_000) + word(4_000_000) + word(160)
            + word(int(recipient, 16)) + word(deadline) + word(2)
            + word(int(USDC, 16)) + word(int(TOKEN, 16)))


class EvmTransactionTests(unittest.TestCase):
    def test_unsigned_builder_signer_and_final_byte_parser(self):
        key = PrivateKey()
        wallet = "0x" + keccak256(key.public_key.format(compressed=False)[1:])[-20:].hex()
        intent = ExecutionIntent("i", "s", "wallet_rpc_evm", "test", "wallet", "1", "buy",
                                 USDC, TOKEN, Decimal("10"))
        data = calldata(wallet, int(time.time()) + 120)
        quote = ExecutableQuote("router", "5000000", "4000000", "2026-09-21T00:00:00Z",
                                "10", True, (ROUTER,), {
                                    "from": wallet, "to": ROUTER, "data": "0x" + data.hex(), "gas": 200_000,
                                    "maxPriorityFeePerGas": 1_000_000_000, "maxFeePerGas": 2_000_000_000, "value": 0,
                                })
        built = EvmEip1559Builder(1, wallet, {ROUTER}).build(intent, quote, "7")
        signed = sign_eip1559(built.serialized, key.secret)
        parser = EvmTransactionParser({ROUTER})
        scope, digest, parsed = validate_serialized_transaction(
            signed, parser, expected_chain_id="1", expected_wallet=wallet,
            expected_token_out=TOKEN, maximum_sell_amount=10_000_000, trusted_targets={ROUTER},
        )
        self.assertTrue(scope.valid)
        self.assertEqual(parsed["nonce"], 7)
        self.assertEqual(parsed["gasLimit"], 200_000)
        self.assertEqual(parsed["maxFeePerGasWei"], 2_000_000_000)
        self.assertEqual(len(digest), 64)
        with self.assertRaises(ValueError):
            parser.parse(signed[:-1] + bytes([signed[-1] ^ 1]))

    def test_unknown_calldata_is_not_interpreted_as_swap(self):
        with self.assertRaisesRegex(ValueError, "unsupported_swap_calldata"):
            decode_v2_swap(bytes.fromhex("deadbeef") + b"\0" * 300)


if __name__ == "__main__":
    unittest.main()
