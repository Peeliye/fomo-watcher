"""Structural decoder for the 0x v2 AllowanceHolder -> Settler call path.

This is deliberately *not* an action-scope proof. Settler's ``bytes[] actions``
can invoke many protocols and must be audited individually before a route can
be built or signed. Unknown actions never become safe merely because the outer
ABI fields match a quote.

ABI references: 0x-settler IAllowanceHolder.exec and
ISettlerTakerSubmitted.execute, not the legacy Uniswap V2 router ABI.
"""

from __future__ import annotations

from dataclasses import dataclass

from Crypto.Hash import keccak


MAX_CALLDATA_BYTES = 64 * 1024
MAX_ACTIONS = 16
ALLOWANCE_HOLDER_EXEC = bytes.fromhex("2213bc0b")


def _selector(signature: str) -> bytes:
    digest = keccak.new(digest_bits=256)
    digest.update(signature.encode("ascii"))
    return digest.digest()[:4]


SETTLER_EXECUTE = _selector("execute((address,address,uint256),bytes[],bytes32)")


def _word(data: bytes, position: int) -> bytes:
    if position < 0 or position + 32 > len(data):
        raise ValueError("zero_x_abi_truncated")
    return data[position:position + 32]


def _uint(data: bytes, position: int) -> int:
    return int.from_bytes(_word(data, position), "big")


def _address(data: bytes, position: int) -> str:
    word = _word(data, position)
    if word[:12] != bytes(12) or word[12:] == bytes(20):
        raise ValueError("zero_x_abi_invalid_address")
    return "0x" + word[12:].hex()


def _dynamic_bytes(data: bytes, head_size: int, offset: int) -> bytes:
    if offset != head_size or offset % 32 or offset + 32 > len(data):
        raise ValueError("zero_x_abi_noncanonical_offset")
    size = _uint(data, offset)
    start = offset + 32
    end = start + size
    padded_end = start + ((size + 31) // 32) * 32
    if end > len(data) or padded_end != len(data) or any(data[end:padded_end]):
        raise ValueError("zero_x_abi_noncanonical_bytes")
    return data[start:end]


def _actions(data: bytes, offset: int) -> tuple[bytes, ...]:
    if offset != 160 or offset + 32 > len(data):
        raise ValueError("zero_x_actions_offset_invalid")
    count = _uint(data, offset)
    if not 1 <= count <= MAX_ACTIONS:
        raise ValueError("zero_x_actions_count_invalid")
    head = offset + 32
    tail = head + 32 * count
    if tail > len(data):
        raise ValueError("zero_x_actions_truncated")
    actions: list[bytes] = []
    for index in range(count):
        relative = _uint(data, head + index * 32)
        if head + relative != tail or tail + 32 > len(data):
            raise ValueError("zero_x_action_offset_invalid")
        size = _uint(data, tail)
        start = tail + 32
        end = start + size
        padded_end = start + ((size + 31) // 32) * 32
        if size < 4 or end > len(data) or padded_end > len(data) or any(data[end:padded_end]):
            raise ValueError("zero_x_action_invalid")
        actions.append(data[start:end])
        tail = padded_end
    if tail != len(data):
        raise ValueError("zero_x_actions_trailing_bytes")
    return tuple(actions)


@dataclass(frozen=True, slots=True)
class AllowanceHolderSettlerCall:
    operator: str
    sell_token: str
    sell_amount: int
    settler: str
    recipient: str
    buy_token: str
    minimum_buy_amount: int
    actions: tuple[bytes, ...]
    affiliate: bytes


def inspect_allowance_holder_settler(data: bytes) -> AllowanceHolderSettlerCall:
    """Decode canonical ABI envelopes; do not treat returned actions as safe."""
    if len(data) > MAX_CALLDATA_BYTES or data[:4] != ALLOWANCE_HOLDER_EXEC:
        raise ValueError("zero_x_allowance_holder_exec_required")
    outer = data[4:]
    if len(outer) < 192:
        raise ValueError("zero_x_exec_truncated")
    operator = _address(outer, 0)
    sell_token = _address(outer, 32)
    sell_amount = _uint(outer, 64)
    settler = _address(outer, 96)
    inner = _dynamic_bytes(outer, 160, _uint(outer, 128))
    if sell_amount <= 0 or inner[:4] != SETTLER_EXECUTE:
        raise ValueError("zero_x_settler_execute_required")
    body = inner[4:]
    if len(body) < 192:
        raise ValueError("zero_x_settler_truncated")
    recipient = _address(body, 0)
    buy_token = _address(body, 32)
    minimum = _uint(body, 64)
    affiliate = _word(body, 128)
    actions = _actions(body, _uint(body, 96))
    if minimum <= 0:
        raise ValueError("zero_x_minimum_output_required")
    return AllowanceHolderSettlerCall(operator, sell_token, sell_amount, settler,
                                      recipient, buy_token, minimum, actions, affiliate)
