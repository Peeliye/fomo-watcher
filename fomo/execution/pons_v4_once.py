"""One reviewed Robinhood Pons pool: bounded quote and unsigned swap codec.

This module does not sign or broadcast and does not widen generic V4 support.
"""

from __future__ import annotations

import time
from typing import Any

from .direct_v3 import _BASE_MULTICALL3, _encode_multicall3
from .direct_v4 import (CHAIN_CONFIGS, V4PoolKey, ZERO, _bitmap_position,
                        _call, _initialized_ticks, _selector, _signed, _word)
from .evm_transaction import Eip1559Fields, decode_eip1559, keccak256
from .interfaces import BuiltTransaction
from .pons_v4_hook import HOOK, PonsFeePolicy, verify_pool_key
from .v4_math import quote_single_interval
from .v4_transaction import (ACTIONS, EXECUTE, V4_SWAP, _array, _bytes,
                             _dynamic, _items, _pair, _read, _word as abi_word)

CHAIN_ID = 4663
ROUTE_ID = "pons-v4-single-pool"
TOKEN_OUT = "0x314ad0f11422842d28b4f950a64cd40fafb029fd"
POOL_ID = "0xabf7f005c3d6f2540d3d4fd5a196d77711a176599923b99c41ef6a01904f2cd9"
INITIALIZE_BLOCK = 69886502
KEY = V4PoolKey(ZERO, TOKEN_OUT, 0, 200, HOOK)


def checked_key(key: V4PoolKey, *, expected_pool_id: str = POOL_ID,
                expected_token: str = TOKEN_OUT) -> None:
    verify_pool_key(CHAIN_ID, key, expected_pool_id)
    if (key.currency0 != ZERO or key.currency1 != expected_token
            or key.fee != 0 or key.tick_spacing != 200 or key.hooks != HOOK
            or key.pool_id != expected_pool_id):
        raise ValueError("pons_once_pool_key_mismatch")


def _decode_multicall(value: Any, count: int) -> list[str]:
    if not isinstance(value, str) or not 0 < count <= 48:
        raise ValueError("pons_once_multicall_invalid")
    try:
        raw = bytes.fromhex(value.removeprefix("0x"))
    except ValueError as error:
        raise ValueError("pons_once_multicall_invalid") from error
    if (not value.startswith("0x") or len(raw) < 64 + 32 * count
            or len(raw) % 32 or int.from_bytes(raw[:32], "big") != 32
            or int.from_bytes(raw[32:64], "big") != count):
        raise ValueError("pons_once_multicall_invalid")
    expected = 64 + 32 * count
    results: list[str] = []
    for index in range(count):
        offset = int.from_bytes(raw[64 + 32 * index:96 + 32 * index], "big")
        start = 64 + offset
        if start != expected or start + 96 > len(raw):
            raise ValueError("pons_once_multicall_invalid")
        success = int.from_bytes(raw[start:start + 32], "big")
        data_offset = int.from_bytes(raw[start + 32:start + 64], "big")
        length = int.from_bytes(raw[start + 64:start + 96], "big")
        expected = start + 96 + ((length + 31) // 32 * 32)
        if success != 1 or data_offset != 64 or length == 0 or expected > len(raw):
            raise ValueError("pons_once_multicall_failed")
        results.append("0x" + raw[start + 96:start + 96 + length].hex())
    if expected != len(raw):
        raise ValueError("pons_once_multicall_invalid")
    return results


def _request(to: str, signature: str, tag: str, *args: int) -> tuple[str, list[Any]]:
    data = _selector(signature) + "".join(f"{arg % (1 << 256):064x}" for arg in args)
    return "eth_call", [{"to": to, "data": data}, tag]


def quote_gross(rpc: Any, *, block_tag: str, amount_in: int,
                key: V4PoolKey = KEY
                ) -> tuple[int, dict[str, int], PonsFeePolicy]:
    """One pinned Multicall for dynamic state, fee policy, and full tick bitmap."""
    checked_key(key, expected_pool_id=key.pool_id, expected_token=key.currency1)
    config = CHAIN_CONFIGS[CHAIN_ID]
    pool_word = int(key.pool_id, 16)
    state_slot = int.from_bytes(keccak256(bytes.fromhex(key.pool_id[2:]) + (6).to_bytes(32, "big")), "big")
    requests = [
        _request(config.state_view, "getSlot0(bytes32)", block_tag, pool_word),
        _request(config.state_view, "getLiquidity(bytes32)", block_tag, pool_word),
        _request(config.pool_manager, "extsload(bytes32)", block_tag, state_slot),
        _request(config.pool_manager, "extsload(bytes32)", block_tag, state_slot + 3),
        _request(HOOK, "launches(bytes32)", block_tag, pool_word),
    ]
    requests.extend(_request(config.state_view, "getTickBitmap(bytes32,int16)",
                             block_tag, pool_word, index) for index in range(-18, 18))
    cached_lower = getattr(rpc, "cached_lower_tick", None)
    if type(cached_lower) is int and -887_272 <= cached_lower <= 887_272:
        requests.append(_request(config.state_view, "getTickInfo(bytes32,int24)",
                                 block_tag, pool_word, cached_lower))
    calldata = _encode_multicall3(requests, block_tag)
    recorder = getattr(rpc, "record_multicall_subcalls", None)
    if callable(recorder):
        recorder(len(requests))
    result = rpc.call("eth_call", [{"to": _BASE_MULTICALL3, "data": calldata}, block_tag])
    results = _decode_multicall(result, len(requests))
    raw_slot0 = results[0]
    if not isinstance(raw_slot0, str) or len(raw_slot0) != 258:
        raise ValueError("pons_once_slot0_invalid")
    parts = [int(raw_slot0[2 + i * 64:2 + (i + 1) * 64], 16) for i in range(4)]
    sqrt_price, tick, protocol_fee, lp_fee = parts[0], _signed(parts[1], 24), parts[2], parts[3]
    liquidity = _word(results[1])
    if sqrt_price <= 0 or not 0 < liquidity < 1 << 128 or lp_fee != key.fee:
        raise ValueError("pons_once_pool_uninitialized")
    packed, raw_liquidity = _word(results[2]), _word(results[3])
    if (packed & ((1 << 160) - 1) != sqrt_price
            or _signed(packed >> 160, 24) != tick
            or (packed >> 184) & 0xffffff != protocol_fee
            or (packed >> 208) & 0xffffff != lp_fee
            or raw_liquidity & ((1 << 128) - 1) != liquidity):
        raise ValueError("pons_once_state_mismatch")
    policy = PonsFeePolicy.from_launches_result(results[4], key=key,
                                                 expected_pool_id=key.pool_id)
    _, word_index, _ = _bitmap_position(tick, key.tick_spacing)
    lower = None
    # The protocol tick range and spacing=200 bound all possible bitmap words
    # to [-18,17]. Never infer that a missing bit is a safe crossing.
    for index in range(word_index, -19, -1):
        bitmap = _word(results[5 + index + 18])
        lower = max((value for value in _initialized_ticks(index, bitmap, key.tick_spacing)
                     if value <= tick), default=None)
        if lower is not None:
            break
    if lower is None:
        raise ValueError("pons_once_tick_coverage_missing")
    raw_tick = (results[-1] if cached_lower == lower and len(results) > 41
                else _call(rpc, config.state_view, "getTickInfo(bytes32,int24)",
                           block_tag, pool_word, lower))
    if not isinstance(raw_tick, str) or len(raw_tick) != 258 or int(raw_tick[2:66], 16) <= 0:
        raise ValueError("pons_once_initialized_tick_missing")
    setattr(rpc, "cached_lower_tick", lower)
    quote = quote_single_interval(
        amount_in=amount_in, zero_for_one=True, sqrt_price_x96=sqrt_price,
        tick=tick, liquidity=liquidity, tick_spacing=key.tick_spacing,
        lp_fee=lp_fee, protocol_fee=protocol_fee, nearest_initialized_tick=lower)
    return quote.amount_out, {"sqrtPriceX96": sqrt_price, "tick": tick,
                              "liquidity": liquidity, "lpFee": lp_fee,
                              "protocolFee": protocol_fee, "lowerTick": lower}, policy


def _encode(*, amount_in: int, minimum_out: int, deadline: int,
            key: V4PoolKey = KEY) -> bytes:
    checked_key(key, expected_pool_id=key.pool_id, expected_token=key.currency1)
    if not 0 < amount_in < 1 << 128 or not 0 < minimum_out < 1 << 128:
        raise ValueError("pons_once_amount_invalid")
    # The reviewed afterSwap implementation ignores hookData. Explicit empty bytes.
    swap = abi_word(32) + b"".join(abi_word(value) for value in (
        int(key.currency0, 16), int(key.currency1, 16), key.fee,
        key.tick_spacing, int(key.hooks, 16), 1, amount_in, minimum_out, 9 * 32,
    )) + _dynamic(b"")
    command_input = (abi_word(64) + abi_word(64 + len(_dynamic(ACTIONS)))
                     + _dynamic(ACTIONS) + _array([
                         swap, _pair(ZERO, amount_in), _pair(key.currency1, minimum_out)]))
    return (EXECUTE + abi_word(96) + abi_word(96 + len(_dynamic(V4_SWAP)))
            + abi_word(deadline) + _dynamic(V4_SWAP) + _array([command_input]))


def decode_calldata(data: bytes, *, key: V4PoolKey = KEY) -> dict[str, int | str]:
    if data[:4] != EXECUTE:
        raise ValueError("pons_once_selector_invalid")
    body = data[4:]
    if len(body) < 96 or _bytes(body, _read(body, 0)) != V4_SWAP:
        raise ValueError("pons_once_command_invalid")
    inputs = _items(body, _read(body, 32))
    deadline = _read(body, 64)
    if len(inputs) != 1:
        raise ValueError("pons_once_inputs_invalid")
    nested = inputs[0]
    if _bytes(nested, _read(nested, 0)) != ACTIONS:
        raise ValueError("pons_once_actions_invalid")
    params = _items(nested, _read(nested, 32))
    if len(params) != 3:
        raise ValueError("pons_once_params_invalid")
    swap, settle, take = params
    if len(swap) != 352 or _read(swap, 0) != 32 or len(settle) != 64 or len(take) != 64:
        raise ValueError("pons_once_shape_invalid")
    values = [_read(swap, 32 + i * 32) for i in range(9)]
    parsed_key = V4PoolKey(f"0x{values[0]:040x}", f"0x{values[1]:040x}",
                           values[2], values[3], f"0x{values[4]:040x}")
    checked_key(parsed_key, expected_pool_id=key.pool_id, expected_token=key.currency1)
    if parsed_key != key:
        raise ValueError("pons_once_pool_key_mismatch")
    amount_in, minimum_out = values[6], values[7]
    if (values[5] != 1 or values[8] != 9 * 32 or _bytes(swap, 32 + values[8]) != b""
            or _read(settle, 0) != 0 or _read(settle, 32) != amount_in
            or _read(take, 0) != int(key.currency1, 16) or _read(take, 32) != minimum_out
            or data != _encode(amount_in=amount_in, minimum_out=minimum_out,
                               deadline=deadline, key=key)):
        raise ValueError("pons_once_noncanonical_or_scope_invalid")
    return {"poolId": key.pool_id, "tokenIn": ZERO, "tokenOut": key.currency1,
            "fee": key.fee, "amountIn": amount_in, "minOut": minimum_out,
            "deadline": deadline, "recipientMode": "msg_sender", "msgValue": amount_in}


def build_unsigned(*, amount_in: int, minimum_out: int, deadline: int,
                   nonce: int, gas_limit: int, priority_fee: int,
                   maximum_fee: int, key: V4PoolKey = KEY) -> BuiltTransaction:
    if (not 0 <= nonce or not 21_000 <= gas_limit <= 1_000_000
            or not 0 < priority_fee <= maximum_fee
            or not int(time.time()) < deadline <= int(time.time()) + 300):
        raise ValueError("pons_once_unsigned_scope_invalid")
    data = _encode(amount_in=amount_in, minimum_out=minimum_out, deadline=deadline, key=key)
    decode_calldata(data, key=key)
    fields = Eip1559Fields(CHAIN_ID, nonce, priority_fee, maximum_fee, gas_limit,
                           bytes.fromhex(CHAIN_CONFIGS[CHAIN_ID].universal_router[2:]),
                           amount_in, data)
    return BuiltTransaction(fields.unsigned_bytes(), "pons_v4_once", str(nonce))


def decode_unsigned(transaction: BuiltTransaction, *, key: V4PoolKey = KEY) -> dict[str, int | str]:
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    decoded = decode_calldata(fields.data, key=key)
    if (transaction.provider != "pons_v4_once" or fields.chain_id != CHAIN_ID
            or "0x" + fields.to.hex() != CHAIN_CONFIGS[CHAIN_ID].universal_router
            or fields.value != decoded["msgValue"]):
        raise ValueError("pons_once_unsigned_binding_invalid")
    return {**decoded, "chainId": fields.chain_id, "router": "0x" + fields.to.hex(),
            "nonce": fields.nonce, "gasLimit": fields.gas_limit}
