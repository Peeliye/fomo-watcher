"""Isolated read-only Uniswap v4 no-hook PoolManager state and quote binding."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, replace
from typing import Any, Mapping

from fomo.watching.rpc_transport import RpcTransport, RpcUnavailable, rpc_view

from .evm_transaction import keccak256
from .v4_math import V4Quote, quote_single_interval
from .evm_transaction import decode_eip1559
from .interfaces import BuiltTransaction

ZERO = "0x" + "00" * 20
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")


@dataclass(frozen=True, slots=True)
class V4ChainConfig:
    chain_id: int
    pool_manager: str
    state_view: str
    universal_router: str
    permitted_currencies: frozenset[str]


@dataclass(frozen=True, slots=True)
class V4PoolKey:
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str = ZERO

    def validate(self, config: V4ChainConfig) -> None:
        if (not all(_ADDRESS.fullmatch(value) for value in
                    (self.currency0, self.currency1, self.hooks))
                or not int(self.currency0, 16) < int(self.currency1, 16)
                or not {self.currency0, self.currency1} <= config.permitted_currencies
                or self.hooks != ZERO or not 0 < self.fee < 1_000_000
                or not 0 < self.tick_spacing <= 32767):
            raise ValueError("v4_pool_key_unapproved")

    @property
    def pool_id(self) -> str:
        words = (int(self.currency0, 16), int(self.currency1, 16), self.fee,
                 self.tick_spacing, int(self.hooks, 16))
        return "0x" + keccak256(b"".join(value.to_bytes(32, "big") for value in words)).hex()


CHAIN_CONFIGS: Mapping[int, V4ChainConfig] = {
    1: V4ChainConfig(
        1, "0x000000000004444c5dc75cb358380d2e3de08a90",
        "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
        "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
        frozenset({ZERO, USDC}),
    ),
    8453: V4ChainConfig(
        8453, "0x498581ff718922c3f8e6a244956af099b2652b2b",
        "0xa3c0c9b65bad0b08107aa264b0f3db444b867a71",
        "0x6ff5693b99212da76ad316178a184ab56d299b43",
        frozenset({ZERO, "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"}),
    ),
    4663: V4ChainConfig(
        4663, "0x8366a39cc670b4001a1121b8f6a443a643e40951",
        "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b",
        "0x8876789976decbfcbbbe364623c63652db8c0904",
        frozenset({ZERO, "0x0bd7d308f8e1639fab988df18a8011f41eacad73",
                   "0x5fc5360d0400a0fd4f2af552add042d716f1d168",
                   "0x314ad0f11422842d28b4f950a64cd40fafb029fd"}),
    ),
}
MAINNET_PROBE_KEY = V4PoolKey(ZERO, USDC, 500, 10)
BASE_PROBE_KEY = V4PoolKey(ZERO, "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", 3000, 60)
ROBINHOOD_PROBE_KEY = V4PoolKey(ZERO, "0x5fc5360d0400a0fd4f2af552add042d716f1d168", 500, 10)
ROBINHOOD_PROBE_POOL_ID = "0x387bf619da4d3fb62bb276482693dba1b9b3520f573cabdfe033384a24125982"


def _selector(signature: str) -> str:
    return "0x" + keccak256(signature.encode())[:4].hex()


def _word(value: Any) -> int:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise ValueError("v4_rpc_word_invalid")
    return int(value, 16)


def _signed(value: int, width: int) -> int:
    value &= (1 << width) - 1
    return value - (1 << width) if value & (1 << (width - 1)) else value


def _call(rpc: RpcTransport, to: str, signature: str, block_tag: str,
          *args: int) -> Any:
    if not re.fullmatch(r"0x[0-9a-f]+", block_tag):
        raise ValueError("v4_block_tag_missing")
    data = _selector(signature) + "".join(f"{arg % (1 << 256):064x}" for arg in args)
    return rpc.call("eth_call", [{"to": to, "data": data}, block_tag])


def _read_tick_batch(rpc: RpcTransport, requests: list[tuple[str, list[Any]]],
                     tag: str) -> list[Any]:
    """Bounded same-height batch; fixture transports retain individual calls."""
    if not 0 < len(requests) <= 3 or any(
        method != "eth_call" or len(params) != 2 or params[1] != tag
        for method, params in requests
    ):
        raise ValueError("v4_tick_batch_scope_invalid")
    batch = getattr(rpc, "call_batch", None)
    if callable(batch):
        result = batch(requests)
        if not isinstance(result, list):
            raise ValueError("v4_tick_batch_response_invalid")
        return result
    return [rpc.call(method, params) for method, params in requests]


def _tick_request(to: str, signature: str, tag: str, *args: int) -> tuple[str, list[Any]]:
    data = _selector(signature) + "".join(f"{arg % (1 << 256):064x}" for arg in args)
    return "eth_call", [{"to": to, "data": data}, tag]


def _bitmap_position(tick: int, tick_spacing: int) -> tuple[int, int, int]:
    """Uniswap v4 TickBitmap.compress/position, including negative ticks."""
    if not -887272 <= tick <= 887272 or not 0 < tick_spacing <= 32767:
        raise ValueError("v4_tick_position_invalid")
    compressed = tick // tick_spacing  # Solidity sdiv plus negative remainder fix.
    return compressed, compressed >> 8, compressed & 0xff


def _initialized_ticks(word_index: int, bitmap: int, tick_spacing: int) -> tuple[int, ...]:
    """Decode only set bits in one already-fetched 256-bit bitmap word."""
    if not -(1 << 15) <= word_index < 1 << 15 or not 0 <= bitmap < 1 << 256:
        raise ValueError("v4_bitmap_word_invalid")
    ticks = []
    remaining = bitmap
    while remaining:
        lowest = remaining & -remaining
        bit = lowest.bit_length() - 1
        tick = ((word_index << 8) + bit) * tick_spacing
        if not -887272 <= tick <= 887272:
            raise ValueError("v4_bitmap_tick_out_of_range")
        ticks.append(tick)
        remaining ^= lowest
    return tuple(ticks)


@dataclass(frozen=True, slots=True)
class V4PoolSnapshot:
    chain_id: int
    key: V4PoolKey
    pool_id: str
    block_height: int
    block_hash: str
    sqrt_price_x96: int
    tick: int
    protocol_fee: int
    lp_fee: int
    liquidity: int
    nearest_lower_tick: int | None
    nearest_upper_tick: int | None
    observed_at_ms: int
    bitmap_words_read: int = 1
    tick_info_read: int = 0
    compressed_tick: int = 0
    bitmap_word_index: int = 0
    bitmap_bit_index: int = 0
    bitmap_diagnostics: tuple[tuple[int, int, tuple[int, ...]], ...] = ()

    def quote(self, *, token_in: str, amount_in: int) -> V4Quote:
        config = CHAIN_CONFIGS.get(self.chain_id)
        if config is None or self.key.pool_id != self.pool_id:
            raise ValueError("v4_quote_pool_identity_invalid")
        self.key.validate(config)
        if token_in not in {self.key.currency0, self.key.currency1}:
            raise ValueError("v4_quote_currency_invalid")
        zero_for_one = token_in == self.key.currency0
        quote = quote_single_interval(
            amount_in=amount_in, zero_for_one=zero_for_one,
            sqrt_price_x96=self.sqrt_price_x96, tick=self.tick,
            liquidity=self.liquidity, tick_spacing=self.key.tick_spacing,
            lp_fee=self.lp_fee, protocol_fee=self.protocol_fee,
            nearest_initialized_tick=(self.nearest_lower_tick if zero_for_one
                                      else self.nearest_upper_tick),
        )
        return replace(quote, block_hash=self.block_hash, pool_id=self.pool_id)


class UniswapV4PoolReader:
    def __init__(self, *, rpc: RpcTransport, chain_id: int = 1,
                 key: V4PoolKey | None = None) -> None:
        config = CHAIN_CONFIGS.get(chain_id)
        key = key or {1: MAINNET_PROBE_KEY, 8453: BASE_PROBE_KEY,
                      4663: ROBINHOOD_PROBE_KEY}.get(chain_id)
        if config is None or key is None:
            raise ValueError("v4_chain_unapproved")
        key.validate(config)
        self.rpc, self.config, self.key = rpc, config, key
        self.last_header_diagnostic: dict[str, Any] = {}

    def snapshot(self) -> V4PoolSnapshot:
        with rpc_view(self.rpc):
            if self.config.chain_id in (8453, 4663):
                head = int(str(self.rpc.call("eth_blockNumber", [])), 16)
                self.last_header_diagnostic = {"head": head, "selected": None,
                                               "fallback": False, "attempted": []}
                block = None
                for candidate in (head, head - 1) if head > 1 else (head,):
                    self.last_header_diagnostic["attempted"].append(candidate)
                    try:
                        response = self.rpc.call("eth_getBlockByNumber", [hex(candidate), False])
                    except RpcUnavailable:
                        if candidate == head and head > 1:
                            continue
                        raise
                    if isinstance(response, Mapping) and response.get("hash"):
                        block = response
                        self.last_header_diagnostic.update(
                            selected=candidate, fallback=candidate != head)
                        break
                    if candidate != head or head <= 1:
                        raise ValueError("v4_block_missing")
                if block is None:
                    raise ValueError("v4_block_missing")
            else:
                block = self.rpc.call("eth_getBlockByNumber", ["latest", False])
            if not isinstance(block, Mapping):
                raise ValueError("v4_block_missing")
            height = int(str(block["number"]), 16)
            block_hash = str(block.get("hash") or "")
            timestamp = int(str(block["timestamp"]), 16)
            if (height <= 0 or not _HASH.fullmatch(block_hash)
                    or not 0 <= time.time() - timestamp <= 60):
                raise ValueError("v4_block_stale_or_invalid")
            if self.config.chain_id in (8453, 4663) and height != self.last_header_diagnostic["selected"]:
                raise ValueError("v4_block_stale_or_invalid")
            tag, config, key = hex(height), self.config, self.key
            for address in (config.pool_manager, config.state_view, config.universal_router):
                code = self.rpc.call("eth_getCode", [address, tag])
                if not isinstance(code, str) or not re.fullmatch(r"0x[0-9a-fA-F]{2,}", code):
                    raise ValueError("v4_deployment_code_missing")
            pool_id = key.pool_id
            pool_word = int(pool_id, 16)
            raw_slot0 = _call(self.rpc, config.state_view, "getSlot0(bytes32)", tag, pool_word)
            if not isinstance(raw_slot0, str) or not re.fullmatch(r"0x[0-9a-fA-F]{256}", raw_slot0):
                raise ValueError("v4_slot0_invalid")
            parts = [int(raw_slot0[2+i*64:2+(i+1)*64], 16) for i in range(4)]
            sqrt_price, tick, protocol_fee, lp_fee = parts[0], _signed(parts[1], 24), parts[2], parts[3]
            liquidity = _word(_call(self.rpc, config.state_view, "getLiquidity(bytes32)", tag, pool_word))
            if (sqrt_price <= 0 or liquidity <= 0 or liquidity >= 1 << 128
                    or lp_fee != key.fee or protocol_fee >= 1 << 24):
                raise ValueError("v4_pool_uninitialized_or_fee_mismatch")
            # PoolManager storage is an independent same-block check of StateView's binding.
            state_slot = int.from_bytes(keccak256(bytes.fromhex(pool_id[2:]) + (6).to_bytes(32, "big")), "big")
            packed = _word(_call(self.rpc, config.pool_manager, "extsload(bytes32)", tag, state_slot))
            raw_liquidity = _word(_call(self.rpc, config.pool_manager, "extsload(bytes32)", tag,
                                       state_slot + 3))
            if (packed & ((1 << 160) - 1) != sqrt_price
                    or _signed(packed >> 160, 24) != tick
                    or (packed >> 184) & 0xffffff != protocol_fee
                    or (packed >> 208) & 0xffffff != lp_fee
                    or raw_liquidity & ((1 << 128) - 1) != liquidity):
                raise ValueError("v4_state_view_manager_mismatch")
            compressed, bitmap_word, position = _bitmap_position(tick, key.tick_spacing)
            words = [bitmap_word]
            bitmap_raw = _read_tick_batch(self.rpc, [
                _tick_request(config.state_view, "getTickBitmap(bytes32,int16)",
                              tag, pool_word, index) for index in words], tag)
            bitmaps = {index: _word(raw) for index, raw in zip(words, bitmap_raw)}
            diagnostics = tuple((index, bitmaps[index],
                                 _initialized_ticks(index, bitmaps[index], key.tick_spacing))
                                for index in words)
            parsed = [candidate for _, _, ticks in diagnostics for candidate in ticks]
            lower = max((candidate for candidate in parsed if candidate <= tick), default=None)
            upper = min((candidate for candidate in parsed if candidate > tick), default=None)
            boundaries = [boundary for boundary in (lower, upper) if boundary is not None]
            tick_raw = _read_tick_batch(self.rpc, [
                _tick_request(config.state_view, "getTickInfo(bytes32,int24)",
                              tag, pool_word, boundary) for boundary in boundaries], tag) if boundaries else []
            for raw in tick_raw:
                if (not isinstance(raw, str) or len(raw) != 2 + 64 * 4
                        or int(raw[2:66], 16) <= 0):
                    raise ValueError("v4_initialized_tick_missing")
            check = self.rpc.call("eth_getBlockByNumber", [tag, False])
            if not isinstance(check, Mapping) or check.get("hash") != block_hash:
                raise ValueError("v4_block_reorged")
        observed = int(time.time() * 1000)
        if observed - timestamp * 1000 > 60_000:
            raise ValueError("v4_block_stale_after_read")
        return V4PoolSnapshot(config.chain_id, key, pool_id, height, block_hash,
                              sqrt_price, tick, protocol_fee, lp_fee, liquidity,
                              lower, upper, observed,
                              len(words), len(tick_raw), compressed, bitmap_word,
                              position, diagnostics)


def attempt_same_block_simulation(rpc: RpcTransport, *, transaction: BuiltTransaction,
                                  snapshot: V4PoolSnapshot, quote: V4Quote,
                                  wallet: str, state_override: Mapping[str, Any] | None = None) -> bool:
    """Exercise eth_call, but never claim output equality from execute's void return.

    Universal Router execute returns no token amount. A successful call alone
    cannot prove the local integer quote equals the settled amount; L0 remains
    unverified until an independently decoded, same-block output is available.
    """
    from .v4_transaction import decode_unsigned_swap

    config = CHAIN_CONFIGS.get(snapshot.chain_id)
    decoded = decode_unsigned_swap(transaction, chain_id=snapshot.chain_id)
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    if (config is None or decoded["key"] != snapshot.key
            or decoded["amountIn"] != quote.amount_in
            or quote.pool_id != snapshot.pool_id or quote.block_hash != snapshot.block_hash
            or decoded["amountOutMinimum"] > quote.amount_out
            or not _ADDRESS.fullmatch(wallet.lower())
            or not 0 <= int(time.time() * 1000) - snapshot.observed_at_ms <= 60_000):
        raise ValueError("v4_simulation_scope_invalid")
    call = {"from": wallet.lower(), "to": config.universal_router,
            "data": "0x" + fields.data.hex(), "value": hex(fields.value),
            "gas": hex(fields.gas_limit)}
    params: list[Any] = [call, hex(snapshot.block_height)]
    if state_override is not None:
        params.append(dict(state_override))
    with rpc_view(rpc):
        result = rpc.call("eth_call", params)
        block = rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
    if (result != "0x" or not isinstance(block, Mapping)
            or block.get("hash") != snapshot.block_hash):
        raise ValueError("v4_simulation_return_or_block_invalid")
    return False
