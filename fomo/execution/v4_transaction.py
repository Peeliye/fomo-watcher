"""Strict, single-command Universal Router v4 L0 codec; no execution wiring."""

from __future__ import annotations

import time
from typing import Any

from .direct_v4 import CHAIN_CONFIGS, V4PoolKey, ZERO
from .evm_transaction import Eip1559Fields, decode_eip1559, keccak256
from .interfaces import BuiltTransaction

V4_SWAP = b"\x10"
ACTIONS = b"\x06\x0c\x0f"  # exact-in single, settle all, take all
EXECUTE = keccak256(b"execute(bytes,bytes[],uint256)")[:4]


def _word(value: int) -> bytes:
    if not 0 <= value < 1 << 256:
        raise ValueError("v4_abi_word_invalid")
    return value.to_bytes(32, "big")


def _dynamic(data: bytes) -> bytes:
    return _word(len(data)) + data + b"\x00" * ((-len(data)) % 32)


def _array(items: list[bytes]) -> bytes:
    offset = 32 * len(items)
    head, tail = b"", b""
    for item in items:
        head += _word(offset + len(tail))
        tail += _dynamic(item)
    return _word(len(items)) + head + tail


def _pair(currency: str, amount: int) -> bytes:
    return _word(int(currency, 16)) + _word(amount)


def encode_single_swap(*, chain_id: int, key: V4PoolKey, token_in: str,
                       amount_in: int, minimum_out: int, deadline: int) -> bytes:
    config = CHAIN_CONFIGS.get(chain_id)
    if config is None:
        raise ValueError("v4_chain_unapproved")
    key.validate(config)
    if key.hooks != ZERO:
        raise ValueError("v4_hook_encoding_not_verified")
    if (token_in not in {key.currency0, key.currency1}
            or not 0 < amount_in < 1 << 128 or not 0 < minimum_out < 1 << 128
            or not 0 < deadline < 1 << 256):
        raise ValueError("v4_swap_scope_invalid")
    token_out = key.currency1 if token_in == key.currency0 else key.currency0
    swap = b"".join(_word(value) for value in (
        int(key.currency0, 16), int(key.currency1, 16), key.fee,
        key.tick_spacing, int(key.hooks, 16),
        int(token_in == key.currency0), amount_in, minimum_out, 9 * 32,
    )) + _dynamic(b"")
    command_input = _word(64) + _word(64 + len(_dynamic(ACTIONS))) + _dynamic(ACTIONS) + _array([
        swap, _pair(token_in, amount_in), _pair(token_out, minimum_out),
    ])
    data = (EXECUTE + _word(96) + _word(96 + len(_dynamic(V4_SWAP)))
            + _word(deadline) + _dynamic(V4_SWAP) + _array([command_input]))
    if _decode(data, chain_id=chain_id)["amountIn"] != amount_in:
        raise ValueError("v4_codec_roundtrip_failed")
    return data


def _read(data: bytes, offset: int) -> int:
    if offset < 0 or offset + 32 > len(data):
        raise ValueError("v4_abi_offset_invalid")
    return int.from_bytes(data[offset:offset+32], "big")


def _bytes(data: bytes, offset: int) -> bytes:
    length = _read(data, offset)
    end = offset + 32 + length
    if end > len(data):
        raise ValueError("v4_abi_bytes_truncated")
    return data[offset+32:end]


def _items(data: bytes, offset: int) -> list[bytes]:
    count = _read(data, offset)
    if count > 3:
        raise ValueError("v4_abi_array_scope_invalid")
    base = offset + 32
    return [_bytes(data, base + _read(data, base + i * 32)) for i in range(count)]


def _decode(data: bytes, *, chain_id: int) -> dict[str, Any]:
    if data[:4] != EXECUTE:
        raise ValueError("v4_selector_invalid")
    body = data[4:]
    if len(body) < 96:
        raise ValueError("v4_calldata_truncated")
    commands = _bytes(body, _read(body, 0))
    inputs = _items(body, _read(body, 32))
    deadline = _read(body, 64)
    if commands != V4_SWAP or len(inputs) != 1:
        raise ValueError("v4_unknown_command_or_unlock")
    nested = inputs[0]
    actions = _bytes(nested, _read(nested, 0))
    params = _items(nested, _read(nested, 32))
    if actions != ACTIONS or len(params) != 3:
        raise ValueError("v4_unknown_action_or_unlock")
    swap, settle, take = params
    if len(swap) < 320 or len(settle) != 64 or len(take) != 64:
        raise ValueError("v4_swap_params_invalid")
    values = [_read(swap, i*32) for i in range(9)]
    key = V4PoolKey(f"0x{values[0]:040x}", f"0x{values[1]:040x}",
                    values[2], values[3], f"0x{values[4]:040x}")
    config = CHAIN_CONFIGS.get(chain_id)
    if config is None:
        raise ValueError("v4_chain_unapproved")
    key.validate(config)
    if key.hooks != ZERO:
        raise ValueError("v4_hook_encoding_not_verified")
    if values[5] not in (0, 1) or _bytes(swap, values[8]) != b"":
        raise ValueError("v4_hook_or_direction_invalid")
    token_in = key.currency0 if values[5] else key.currency1
    token_out = key.currency1 if values[5] else key.currency0
    if (len(swap) != 320 or _read(settle, 0) != int(token_in, 16)
            or _read(settle, 32) != values[6]
            or _read(take, 0) != int(token_out, 16)
            or _read(take, 32) != values[7]):
        raise ValueError("v4_settlement_mismatch")
    return {"key": key, "tokenIn": token_in, "tokenOut": token_out,
            "amountIn": values[6], "amountOutMinimum": values[7], "deadline": deadline,
            "msgValue": values[6] if token_in == ZERO else 0}


def decode_single_swap(data: bytes, *, chain_id: int) -> dict[str, Any]:
    decoded = _decode(data, chain_id=chain_id)
    if data != encode_single_swap(chain_id=chain_id, key=decoded["key"],
                                  token_in=decoded["tokenIn"], amount_in=decoded["amountIn"],
                                  minimum_out=decoded["amountOutMinimum"], deadline=decoded["deadline"]):
        raise ValueError("v4_noncanonical_or_extra_calldata")
    return decoded


def build_unsigned_swap(*, chain_id: int, key: V4PoolKey, token_in: str,
                        amount_in: int, minimum_out: int, deadline: int,
                        nonce: int = 0, gas_limit: int = 500_000,
                        priority_fee_wei: int = 1, maximum_fee_wei: int = 2) -> BuiltTransaction:
    config = CHAIN_CONFIGS.get(chain_id)
    if (config is None or nonce < 0 or not 21_000 <= gas_limit <= 1_000_000
            or not 0 < priority_fee_wei <= maximum_fee_wei
            or not int(time.time()) < deadline <= int(time.time()) + 300):
        raise ValueError("v4_unsigned_scope_invalid")
    data = encode_single_swap(chain_id=chain_id, key=key, token_in=token_in,
                              amount_in=amount_in, minimum_out=minimum_out, deadline=deadline)
    decoded = decode_single_swap(data, chain_id=chain_id)
    fields = Eip1559Fields(chain_id, nonce, priority_fee_wei, maximum_fee_wei,
                           gas_limit, bytes.fromhex(config.universal_router[2:]),
                           decoded["msgValue"], data)
    return BuiltTransaction(fields.unsigned_bytes(), "uniswap_v4_l0", str(nonce))


def decode_unsigned_swap(transaction: BuiltTransaction, *, chain_id: int) -> dict[str, Any]:
    config = CHAIN_CONFIGS.get(chain_id)
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    decoded = decode_single_swap(fields.data, chain_id=chain_id)
    if (config is None or transaction.provider != "uniswap_v4_l0"
            or fields.chain_id != chain_id
            or "0x" + fields.to.hex() != config.universal_router
            or fields.value != decoded["msgValue"]):
        raise ValueError("v4_unsigned_binding_invalid")
    return decoded
