"""Uniswap V3 SwapRouter exactInputSingle-only L0 codec.

No signer or broadcaster is exposed. The signed-byte parser is for test scope
verification only; it does not register this route with the execution service.
"""

from __future__ import annotations

import re
import time
from typing import Any, Mapping

from coincurve import PublicKey

from .evm_transaction import (SECP256K1_N, Eip1559Fields, decode_eip1559,
                              keccak256)
from .interfaces import BuiltTransaction
from .v3_math import MAX_SQRT_RATIO, MIN_SQRT_RATIO


EXACT_INPUT_SINGLE = keccak256(
    b"exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
)[:4]
EXACT_INPUT_SINGLE_02 = keccak256(
    b"exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))"
)[:4]
ROUTERS = {
    1: "0xe592427a0aece92de3edee1f18e0157c05861564",
    8453: "0x2626664c2603336e57b271c5c0b26f421741e481",
}
_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _address(value: str) -> str:
    if not isinstance(value, str) or not _ADDRESS.fullmatch(value) or int(value, 16) == 0:
        raise ValueError("v3_address_invalid")
    return value.lower()


def directional_price_limit(token_in: str, token0: str, token1: str) -> int:
    source = _address(token_in)
    if source == _address(token0) and source != _address(token1):
        return MIN_SQRT_RATIO + 1
    if source == _address(token1) and source != _address(token0):
        return MAX_SQRT_RATIO - 1
    raise ValueError("v3_swap_token_pair_invalid")


def minimum_out(quoted_out: int, slippage_bps: int) -> int:
    if quoted_out <= 0 or not 0 <= slippage_bps <= 500:
        raise ValueError("v3_slippage_invalid")
    floor = quoted_out * (10_000 - slippage_bps) // 10_000
    if floor <= 0:
        raise ValueError("v3_minimum_output_zero")
    return floor


def encode_exact_input_single(*, token_in: str, token_out: str, fee: int,
                              recipient: str, deadline: int, amount_in: int,
                              minimum_output: int, sqrt_price_limit_x96: int,
                              token0: str, token1: str, chain_id: int = 1) -> bytes:
    source, target, owner = map(_address, (token_in, token_out, recipient))
    pair = {_address(token0), _address(token1)}
    if (source == target or {source, target} != pair or not 0 < fee < 1_000_000
            or not 0 < deadline < 2**256 or not 0 < amount_in < 2**256
            or not 0 < minimum_output < 2**256
            or sqrt_price_limit_x96 != directional_price_limit(source, token0, token1)):
        raise ValueError("v3_swap_scope_invalid")
    if chain_id not in ROUTERS:
        raise ValueError("v3_chain_unapproved")
    words = ((int(source, 16), int(target, 16), fee, int(owner, 16), deadline,
              amount_in, minimum_output, sqrt_price_limit_x96) if chain_id == 1 else
             (int(source, 16), int(target, 16), fee, int(owner, 16),
              amount_in, minimum_output, sqrt_price_limit_x96))
    selector = EXACT_INPUT_SINGLE if chain_id == 1 else EXACT_INPUT_SINGLE_02
    encoded = selector + b"".join(value.to_bytes(32, "big") for value in words)
    if decode_exact_input_single(encoded, chain_id=chain_id)["amountIn"] != amount_in:
        raise ValueError("v3_swap_roundtrip_failed")
    return encoded


def decode_exact_input_single(data: bytes, *, chain_id: int = 1) -> dict[str, Any]:
    count = {1: 8, 8453: 7}.get(chain_id)
    selector = EXACT_INPUT_SINGLE if chain_id == 1 else EXACT_INPUT_SINGLE_02
    if count is None or len(data) != 4 + count * 32 or data[:4] != selector:
        raise ValueError("v3_swap_selector_or_length_invalid")
    words = [int.from_bytes(data[4 + index * 32:4 + (index + 1) * 32], "big")
             for index in range(count)]
    if chain_id == 8453:
        words.insert(4, 0)  # Router02 has no deadline field.
    if (any(words[index] == 0 or words[index] >> 160 for index in (0, 1, 3))
            or words[0] == words[1] or not 0 < words[2] < 1_000_000
            or any(words[index] <= 0 for index in ((4, 5, 6) if chain_id == 1 else (5, 6)))
            or not MIN_SQRT_RATIO < words[7] < MAX_SQRT_RATIO):
        raise ValueError("v3_swap_fields_invalid")
    return {
        "tokenIn": f"0x{words[0]:040x}", "tokenOut": f"0x{words[1]:040x}",
        "fee": words[2], "recipient": f"0x{words[3]:040x}",
        "deadline": words[4], "amountIn": words[5],
        "amountOutMinimum": words[6], "sqrtPriceLimitX96": words[7],
    }


def build_unsigned_swap(*, chain_id: int, router: str, token0: str, token1: str,
                        fee: int, token_in: str, wallet: str, amount_in: int,
                        quoted_out: int, slippage_bps: int, nonce: int,
                        gas_limit: int, priority_fee_wei: int, maximum_fee_wei: int,
                        deadline: int) -> BuiltTransaction:
    if (chain_id <= 0 or nonce < 0 or not 21_000 <= gas_limit <= 1_000_000
            or not 0 < priority_fee_wei <= maximum_fee_wei
            or not int(time.time()) < deadline <= int(time.time()) + 300
            or _address(router) != ROUTERS.get(chain_id)):
        raise ValueError("v3_unsigned_scope_invalid")
    source, first, second = map(_address, (token_in, token0, token1))
    target = second if source == first else first
    data = encode_exact_input_single(
        token_in=source, token_out=target, fee=fee, recipient=wallet, deadline=deadline,
        amount_in=amount_in, minimum_output=minimum_out(quoted_out, slippage_bps),
        sqrt_price_limit_x96=directional_price_limit(source, first, second),
        token0=first, token1=second,
        chain_id=chain_id,
    )
    fields = Eip1559Fields(chain_id, nonce, priority_fee_wei, maximum_fee_wei,
                           gas_limit, bytes.fromhex(_address(router)[2:]), 0, data)
    return BuiltTransaction(fields.unsigned_bytes(), "uniswap_v3_l0", str(nonce))


def parse_signed_direct_swap(serialized: bytes, *, chain_id: int, router: str,
                             token0: str, token1: str, fee: int) -> Mapping[str, Any]:
    fields, items = decode_eip1559(serialized, signed=True)
    if any(not isinstance(items[index], bytes) or
           (len(items[index]) > 1 and items[index][0] == 0)
           for index in (9, 10, 11)):
        raise ValueError("v3_signature_encoding_invalid")
    parity, r, s = (int.from_bytes(items[index], "big") for index in (9, 10, 11))
    swap = decode_exact_input_single(fields.data, chain_id=chain_id)
    if (fields.chain_id != chain_id or _address(router) != ROUTERS.get(chain_id)
            or "0x" + fields.to.hex() != _address(router)
            or fields.value != 0 or not 21_000 <= fields.gas_limit <= 1_000_000
            or not 0 < fields.priority_fee <= fields.maximum_fee
            or parity not in (0, 1) or not 0 < r < SECP256K1_N
            or not 0 < s <= SECP256K1_N // 2
            or {swap["tokenIn"], swap["tokenOut"]} != {_address(token0), _address(token1)}
            or swap["fee"] != fee
            or (chain_id == 1 and not int(time.time()) < swap["deadline"] <= int(time.time()) + 300)
            or swap["sqrtPriceLimitX96"] != directional_price_limit(swap["tokenIn"], token0, token1)):
        raise ValueError("v3_signed_scope_invalid")
    signature = r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([parity])
    key = PublicKey.from_signature_and_message(signature, keccak256(fields.unsigned_bytes()), hasher=None)
    wallet = "0x" + keccak256(key.format(compressed=False)[1:])[-20:].hex()
    if swap["recipient"] != wallet:
        raise ValueError("v3_signed_recipient_mismatch")
    return {
        "chainId": str(fields.chain_id), "wallet": wallet,
        "router": "0x" + fields.to.hex(), "tokenIn": swap["tokenIn"],
        "tokenOut": swap["tokenOut"], "fee": fee,
        "sellAmount": str(swap["amountIn"]),
        "minimumOutputAmount": str(swap["amountOutMinimum"]),
        "sqrtPriceLimitX96": swap["sqrtPriceLimitX96"],
        "deadline": swap["deadline"], "nonce": fields.nonce,
    }
