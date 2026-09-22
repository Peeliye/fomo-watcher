import time
import unittest

from coincurve import PrivateKey

from fomo.execution.direct_v3 import CHAIN_CONFIGS, MAINNET_PROBE_POOL, BASE_PROBE_POOL
from fomo.execution.evm_transaction import Eip1559Fields, decode_eip1559, keccak256, sign_eip1559
from fomo.execution.v3_math import MAX_SQRT_RATIO, MIN_SQRT_RATIO
from fomo.execution.v3_transaction import (EXACT_INPUT_SINGLE, EXACT_INPUT_SINGLE_02, build_unsigned_swap,
                                           decode_exact_input_single,
                                           directional_price_limit,
                                           parse_signed_direct_swap)


class V3TransactionTests(unittest.TestCase):
    def test_base_router02_uses_deadline_free_abi_and_rejects_legacy_bytes(self):
        target = BASE_PROBE_POOL
        self.assertEqual(EXACT_INPUT_SINGLE_02.hex(), "04e45aaf")
        built = build_unsigned_swap(
            chain_id=8453, router=CHAIN_CONFIGS[8453].router,
            token0=target.token0, token1=target.token1, fee=target.fee,
            token_in=target.token0, wallet="0x" + "44" * 20,
            amount_in=10**15, quoted_out=10**6, slippage_bps=100,
            nonce=0, gas_limit=300000, priority_fee_wei=1, maximum_fee_wei=2,
            deadline=int(time.time()) + 60,
        )
        fields, _ = decode_eip1559(built.serialized, signed=False)
        self.assertEqual(fields.data[:4], EXACT_INPUT_SINGLE_02)
        self.assertEqual(len(fields.data), 4 + 7 * 32)
        self.assertEqual(decode_exact_input_single(fields.data, chain_id=8453)["deadline"], 0)
        with self.assertRaisesRegex(ValueError, "v3_swap_selector_or_length_invalid"):
            decode_exact_input_single(fields.data, chain_id=1)
        with self.assertRaisesRegex(ValueError, "v3_unsigned_scope_invalid"):
            build_unsigned_swap(
                chain_id=8453, router=CHAIN_CONFIGS[1].router,
                token0=target.token0, token1=target.token1, fee=target.fee,
                token_in=target.token0, wallet="0x" + "44" * 20,
                amount_in=10**15, quoted_out=10**6, slippage_bps=100,
                nonce=0, gas_limit=300000, priority_fee_wei=1, maximum_fee_wei=2,
                deadline=int(time.time()) + 60,
            )

    def test_exact_input_single_selector_and_direction_boundaries(self):
        self.assertEqual(EXACT_INPUT_SINGLE.hex(), "414bf389")
        target = MAINNET_PROBE_POOL
        self.assertEqual(directional_price_limit(target.token0, target.token0, target.token1),
                         MIN_SQRT_RATIO + 1)
        self.assertEqual(directional_price_limit(target.token1, target.token0, target.token1),
                         MAX_SQRT_RATIO - 1)

    def test_unsigned_and_signed_roundtrip_without_production_signer(self):
        key = b"\x02" * 32
        wallet = "0x" + keccak256(PrivateKey(key).public_key.format(compressed=False)[1:])[-20:].hex()
        target = MAINNET_PROBE_POOL
        built = build_unsigned_swap(
            chain_id=1, router=CHAIN_CONFIGS[1].router,
            token0=target.token0, token1=target.token1, fee=target.fee,
            token_in=target.token0, wallet=wallet, amount_in=10**6,
            quoted_out=10**15, slippage_bps=100, nonce=7,
            gas_limit=300000, priority_fee_wei=10**9, maximum_fee_wei=2 * 10**9,
            deadline=int(time.time()) + 60,
        )
        fields, _ = decode_eip1559(built.serialized, signed=False)
        decoded = decode_exact_input_single(fields.data)
        self.assertEqual(decoded["fee"], 500)
        self.assertEqual(decoded["sqrtPriceLimitX96"], MIN_SQRT_RATIO + 1)
        self.assertEqual(decoded["amountOutMinimum"], 990_000_000_000_000)
        parsed = parse_signed_direct_swap(
            sign_eip1559(built.serialized, key), chain_id=1,
            router=CHAIN_CONFIGS[1].router, token0=target.token0,
            token1=target.token1, fee=target.fee,
        )
        self.assertEqual(parsed["wallet"], wallet)
        self.assertEqual(parsed["nonce"], 7)
        self.assertEqual(parsed["router"], CHAIN_CONFIGS[1].router)

    def test_wrong_router_chain_selector_and_price_limit_reject(self):
        target = MAINNET_PROBE_POOL
        wallet = "0x" + "44" * 20
        built = build_unsigned_swap(
            chain_id=1, router=CHAIN_CONFIGS[1].router,
            token0=target.token0, token1=target.token1, fee=target.fee,
            token_in=target.token1, wallet=wallet, amount_in=10**15,
            quoted_out=10**6, slippage_bps=100, nonce=0,
            gas_limit=300000, priority_fee_wei=1, maximum_fee_wei=2,
            deadline=int(time.time()) + 60,
        )
        fields, _ = decode_eip1559(built.serialized, signed=False)
        key = b"\x03" * 32
        for chain, router in ((56, CHAIN_CONFIGS[1].router), (1, "0x" + "11" * 20)):
            changed = Eip1559Fields(chain, fields.nonce, fields.priority_fee,
                                    fields.maximum_fee, fields.gas_limit,
                                    bytes.fromhex(router[2:]), fields.value, fields.data)
            with self.assertRaises(ValueError):
                parse_signed_direct_swap(sign_eip1559(changed.unsigned_bytes(), key),
                                         chain_id=1, router=CHAIN_CONFIGS[1].router,
                                         token0=target.token0, token1=target.token1, fee=target.fee)
        with self.assertRaisesRegex(ValueError, "v3_swap_selector_or_length_invalid"):
            decode_exact_input_single(b"\0" * len(fields.data))
        wrong_limit = bytearray(fields.data)
        wrong_limit[-1] ^= 1
        changed = Eip1559Fields(1, fields.nonce, fields.priority_fee,
                                fields.maximum_fee, fields.gas_limit, fields.to,
                                fields.value, bytes(wrong_limit))
        with self.assertRaisesRegex(ValueError, "v3_signed_scope_invalid"):
            parse_signed_direct_swap(sign_eip1559(changed.unsigned_bytes(), key),
                                     chain_id=1, router=CHAIN_CONFIGS[1].router,
                                     token0=target.token0, token1=target.token1, fee=target.fee)


if __name__ == "__main__":
    unittest.main()
