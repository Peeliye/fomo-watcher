"""Single-pair Uniswap V2 reads, integer math, and local unsigned simulation."""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from fomo.watching.rpc_transport import RpcTransport, rpc_view

from .evm_transaction import SWAP_EXACT_TOKENS_FOR_TOKENS, decode_v2_swap, keccak256
from .evm_transaction import Eip1559Fields, EvmTransactionParser, decode_eip1559
from .interfaces import BuiltTransaction
from .simulators import EvmTransactionSimulator


FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
ROUTER02 = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
MAINNET_WETH9 = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
MAINNET_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
BASE_WETH9 = "0x4200000000000000000000000000000000000006"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
BASE_PROBE_PAIR = "0x88a43bbdf9d098eec7bceda4e2494615dfd9bb9c"
ROBINHOOD_WETH9 = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
ROBINHOOD_USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


@dataclass(frozen=True, slots=True)
class V2ChainConfig:
    factory: str
    router02: str
    weth: str
    usdc: str
    probe_pair: str


CHAIN_CONFIGS: Mapping[str, V2ChainConfig] = {
    "1": V2ChainConfig(FACTORY, ROUTER02, MAINNET_WETH9, MAINNET_USDC,
                       "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
    "8453": V2ChainConfig("0x8909dc15e40173ff4699343b6eb8132c65e18ec6",
                          "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24",
                          BASE_WETH9, BASE_USDC, BASE_PROBE_PAIR),
    "4663": V2ChainConfig("0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f",
                           "0x89e5db8b5aa49aa85ac63f691524311aeb649eba",
                           ROBINHOOD_WETH9, ROBINHOOD_USDG, ""),
}
_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_WORD = re.compile(r"^0x[0-9a-fA-F]{64}$")
_RESERVES = re.compile(r"^0x[0-9a-fA-F]{192}$")
_BLOCK_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _address(value: str) -> str:
    if not _ADDRESS.fullmatch(value) or int(value, 16) == 0:
        raise ValueError("v2_address_invalid")
    return value.lower()


def _decode_address(value: Any) -> str:
    if not isinstance(value, str) or not _WORD.fullmatch(value) or int(value[2:26], 16):
        raise ValueError("v2_address_response_invalid")
    return _address("0x" + value[-40:])


def _decode_word(value: Any) -> int:
    if not isinstance(value, str) or not _WORD.fullmatch(value):
        raise ValueError("v2_word_response_invalid")
    return int(value, 16)


def _eth_call(rpc: RpcTransport, to: str, data: str, block: str) -> Any:
    return rpc.call("eth_call", [{"to": to, "data": data}, block])


def amount_out(amount_in: int, reserve_in: int, reserve_out: int) -> int:
    """Canonical V2 getAmountOut: 0.30% input fee, rounded down."""
    if min(amount_in, reserve_in, reserve_out) <= 0 or max(amount_in, reserve_in, reserve_out) >= 2**112:
        raise ValueError("v2_quote_amount_or_reserve_invalid")
    after_fee = amount_in * 997
    output = after_fee * reserve_out // (reserve_in * 1000 + after_fee)
    if output <= 0 or output >= reserve_out:
        raise ValueError("v2_quote_output_zero_or_excessive")
    return output


def amount_in(amount_out_required: int, reserve_in: int, reserve_out: int) -> int:
    """Canonical V2 getAmountIn: ceil via integer quotient plus one."""
    if (amount_out_required <= 0 or reserve_in <= 0 or reserve_out <= 0
            or amount_out_required >= reserve_out or max(reserve_in, reserve_out) >= 2**112):
        raise ValueError("v2_quote_amount_or_reserve_invalid")
    return reserve_in * amount_out_required * 1000 // ((reserve_out - amount_out_required) * 997) + 1


def minimum_out(quoted_out: int, slippage_bps: int) -> int:
    if quoted_out <= 0 or not 0 <= slippage_bps <= 500:
        raise ValueError("v2_slippage_invalid")
    result = quoted_out * (10_000 - slippage_bps) // 10_000
    if result <= 0:
        raise ValueError("v2_minimum_output_zero")
    return result


def build_swap_calldata(*, amount_in_units: int, minimum_out_units: int,
                        token_in: str, token_out: str, recipient: str,
                        deadline: int) -> bytes:
    """Encode only Router02 swapExactTokensForTokens with a two-token path."""
    token_in, token_out, recipient = (_address(value) for value in (token_in, token_out, recipient))
    if (token_in == token_out or not 0 < amount_in_units < 2**256
            or not 0 < minimum_out_units < 2**256 or deadline <= 0):
        raise ValueError("v2_swap_scope_invalid")
    words = (amount_in_units, minimum_out_units, 160, int(recipient, 16), deadline,
             2, int(token_in, 16), int(token_out, 16))
    calldata = SWAP_EXACT_TOKENS_FOR_TOKENS + b"".join(value.to_bytes(32, "big") for value in words)
    parsed = decode_v2_swap(calldata)
    if (parsed["path"] != [token_in, token_out] or parsed["recipient"] != recipient
            or parsed["amountIn"] != amount_in_units or parsed["minimumOut"] != minimum_out_units):
        raise ValueError("v2_swap_roundtrip_failed")
    return calldata


def build_approve_calldata(*, spender: str = ROUTER02, amount_units: int) -> bytes:
    """A distinct ERC-20 approval instruction; never hidden inside a swap."""
    if _address(spender) not in {config.router02 for config in CHAIN_CONFIGS.values()} or not 0 < amount_units < 2**256:
        raise ValueError("v2_approval_scope_invalid")
    return bytes.fromhex("095ea7b3") + int(spender, 16).to_bytes(32, "big") + amount_units.to_bytes(32, "big")


@dataclass(frozen=True, slots=True)
class V2PoolSnapshot:
    chain_id: str
    pair: str
    base_token: str
    quote_token: str
    reserve_base: int
    reserve_quote: int
    base_decimals: int
    quote_decimals: int
    block_height: int
    block_hash: str
    observed_at_ms: int
    provider: str

    @property
    def spot_quote_per_base(self) -> Decimal:
        return ((Decimal(self.reserve_quote) / Decimal(10**self.quote_decimals))
                / (Decimal(self.reserve_base) / Decimal(10**self.base_decimals)))

    def quote_buy(self, quote_units_in: int, slippage_bps: int) -> tuple[int, int]:
        output = amount_out(quote_units_in, self.reserve_quote, self.reserve_base)
        return output, minimum_out(output, slippage_bps)

    def quote_sell(self, base_units_in: int, slippage_bps: int) -> tuple[int, int]:
        output = amount_out(base_units_in, self.reserve_base, self.reserve_quote)
        return output, minimum_out(output, slippage_bps)


class UniswapV2PoolReader:
    """Discover one canonical pair once, then read all state at one block."""

    def __init__(self, *, rpc: RpcTransport, base_token: str, quote_token: str,
                 chain_id: str = "1", cache_ttl_ms: int = 400) -> None:
        if chain_id not in CHAIN_CONFIGS or not 0 <= cache_ttl_ms <= 5_000:
            raise ValueError("v2_chain_or_cache_policy_unapproved")
        self.base_token = _address(base_token)
        self.quote_token = _address(quote_token)
        if self.base_token == self.quote_token:
            raise ValueError("v2_identical_tokens")
        self.rpc = rpc
        self.chain_id = chain_id
        self.config = CHAIN_CONFIGS[chain_id]
        self.cache_ttl_ms = cache_ttl_ms
        self._pair: str | None = None
        self._snapshot: V2PoolSnapshot | None = None
        self._lock = threading.RLock()

    def snapshot(self, *, now_ms: int | None = None) -> V2PoolSnapshot:
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        with self._lock:
            cached = self._snapshot
            if cached and 0 <= current - cached.observed_at_ms <= self.cache_ttl_ms:
                return cached
            with rpc_view(self.rpc):
                block = self.rpc.call("eth_getBlockByNumber", ["latest", False])
                if not isinstance(block, Mapping):
                    raise ValueError("v2_block_missing")
                height = _decode_word("0x" + str(block.get("number") or "").removeprefix("0x").zfill(64))
                block_hash = str(block.get("hash") or "")
                timestamp = int(str(block.get("timestamp") or "0x0"), 16)
                if (height <= 0 or not _BLOCK_HASH.fullmatch(block_hash)
                        or not 0 <= current - timestamp * 1000 <= 60_000):
                    raise ValueError("v2_block_stale_or_invalid")
                tag = hex(height)
                pair = self._pair
                if pair is None:
                    data = ("0xe6a43905" + self.base_token[2:].rjust(64, "0")
                            + self.quote_token[2:].rjust(64, "0"))
                    pair = _decode_address(_eth_call(self.rpc, self.config.factory, data, tag))
                if (self.config.probe_pair and pair != self.config.probe_pair
                        and {self.base_token, self.quote_token} == {self.config.weth, self.config.usdc}):
                    raise ValueError("v2_pair_identity_mismatch")
                token0 = _decode_address(_eth_call(self.rpc, pair, "0x0dfe1681", tag))
                token1 = _decode_address(_eth_call(self.rpc, pair, "0xd21220a7", tag))
                if {token0, token1} != {self.base_token, self.quote_token}:
                    raise ValueError("v2_pair_token_mismatch")
                raw = _eth_call(self.rpc, pair, "0x0902f1ac", tag)
                if not isinstance(raw, str) or not _RESERVES.fullmatch(raw):
                    raise ValueError("v2_reserves_response_invalid")
                reserve0, reserve1 = int(raw[2:66], 16), int(raw[66:130], 16)
                if min(reserve0, reserve1) <= 0 or max(reserve0, reserve1) >= 2**112:
                    raise ValueError("v2_reserves_invalid")
                base_decimals = _decode_word(_eth_call(self.rpc, self.base_token, "0x313ce567", tag))
                quote_decimals = _decode_word(_eth_call(self.rpc, self.quote_token, "0x313ce567", tag))
                if not 0 <= base_decimals <= 30 or not 0 <= quote_decimals <= 30:
                    raise ValueError("v2_decimals_invalid")
                check = self.rpc.call("eth_getBlockByNumber", [tag, False])
                if not isinstance(check, Mapping) or check.get("hash") != block_hash:
                    raise ValueError("v2_block_reorged")
                provider = str(getattr(self.rpc, "last_provider", "") or "")
                if not provider:
                    raise ValueError("v2_rpc_provider_unknown")
            base_reserve = reserve0 if token0 == self.base_token else reserve1
            quote_reserve = reserve1 if token0 == self.base_token else reserve0
            observed_at_ms = current if now_ms is not None else int(time.time() * 1000)
            result = V2PoolSnapshot(self.chain_id, pair, self.base_token, self.quote_token,
                                    base_reserve, quote_reserve, base_decimals, quote_decimals,
                                    height, block_hash, observed_at_ms, provider)
            self._pair = pair
            self._snapshot = result
            return result

    def allowance(self, *, owner: str, token_in: str, snapshot: V2PoolSnapshot) -> int:
        """Read Router02 approval at the exact snapshot block; no approval write."""
        wallet = _address(owner)
        token = _address(token_in)
        if (snapshot.chain_id != self.chain_id or snapshot.pair != self._pair
                or token not in {self.base_token, self.quote_token}):
            raise ValueError("v2_allowance_scope_invalid")
        data = "0xdd62ed3e" + wallet[2:].rjust(64, "0") + self.config.router02[2:].rjust(64, "0")
        with rpc_view(self.rpc):
            value = _decode_word(_eth_call(self.rpc, token, data, hex(snapshot.block_height)))
            block = self.rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
        if not isinstance(block, Mapping) or block.get("hash") != snapshot.block_hash:
            raise ValueError("v2_allowance_block_reorged")
        return value

    def token_balance(self, *, owner: str, token: str, snapshot: V2PoolSnapshot) -> int:
        """Read ERC-20 balanceOf at the same verified block as the pool state."""
        wallet = _address(owner)
        asset = _address(token)
        if (snapshot.chain_id != self.chain_id or snapshot.pair != self._pair
                or asset not in {self.base_token, self.quote_token}):
            raise ValueError("v2_balance_scope_invalid")
        data = "0x70a08231" + wallet[2:].rjust(64, "0")
        with rpc_view(self.rpc):
            value = _decode_word(_eth_call(self.rpc, asset, data, hex(snapshot.block_height)))
            block = self.rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
        if not isinstance(block, Mapping) or block.get("hash") != snapshot.block_hash:
            raise ValueError("v2_balance_block_reorged")
        return value


def build_unsigned_swap(*, snapshot: V2PoolSnapshot, wallet: str, token_in: str,
                        amount_in_units: int, slippage_bps: int, nonce: int,
                        gas_limit: int, priority_fee_wei: int, maximum_fee_wei: int,
                        deadline: int, now_ms: int | None = None) -> BuiltTransaction:
    """Build an unsigned direct V2 EIP-1559 swap; never sends or signs it."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    sender = _address(wallet)
    source = _address(token_in)
    config = CHAIN_CONFIGS.get(snapshot.chain_id)
    if (config is None or not 0 <= current - snapshot.observed_at_ms <= 5_000
            or source not in {snapshot.base_token, snapshot.quote_token}
            or nonce < 0 or not 21_000 <= gas_limit <= 1_000_000
            or not 0 < priority_fee_wei <= maximum_fee_wei
            or not current // 1000 < deadline <= current // 1000 + 300):
        raise ValueError("v2_unsigned_swap_scope_invalid")
    target = snapshot.base_token if source == snapshot.quote_token else snapshot.quote_token
    expected_out = amount_out(
        amount_in_units,
        snapshot.reserve_quote if source == snapshot.quote_token else snapshot.reserve_base,
        snapshot.reserve_base if source == snapshot.quote_token else snapshot.reserve_quote,
    )
    floor = minimum_out(expected_out, slippage_bps)
    calldata = build_swap_calldata(
        amount_in_units=amount_in_units, minimum_out_units=floor,
        token_in=source, token_out=target, recipient=sender, deadline=deadline,
    )
    fields = Eip1559Fields(int(snapshot.chain_id), nonce, priority_fee_wei, maximum_fee_wei,
                           gas_limit, bytes.fromhex(config.router02[2:]), 0, calldata)
    return BuiltTransaction(fields.unsigned_bytes(), "uniswap_v2_direct", str(nonce))


def parse_signed_direct_swap(serialized: bytes) -> Mapping[str, Any]:
    """Recover signer and reject multi-hop/non-Router02 signed transactions."""
    fields, _ = decode_eip1559(serialized, signed=True)
    if fields.chain_id == 4663:
        raise ValueError("v2_signed_direct_scope_invalid")  # Robinhood remains L0-only.
    config = CHAIN_CONFIGS.get(str(fields.chain_id))
    if config is None or "0x" + fields.to.hex() != config.router02:
        raise ValueError("v2_signed_direct_scope_invalid")
    parsed = EvmTransactionParser({config.router02}).parse(serialized)
    swap = decode_v2_swap(fields.data)
    if (len(swap["path"]) != 2
            or fields.gas_limit > 1_000_000 or fields.maximum_fee < fields.priority_fee
            or not fields.priority_fee or not fields.maximum_fee):
        raise ValueError("v2_signed_direct_scope_invalid")
    return parsed


def simulate_unsigned_swap(rpc: RpcTransport, *, wallet: str,
                           transaction: BuiltTransaction) -> Mapping[str, Any]:
    """RPC eth_call/eth_estimateGas only; this function cannot broadcast."""
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    swap = decode_v2_swap(fields.data)
    config = CHAIN_CONFIGS.get(str(fields.chain_id))
    if (transaction.provider != "uniswap_v2_direct" or config is None
            or "0x" + fields.to.hex() != config.router02 or fields.value != 0
            or len(swap["path"]) != 2 or swap["recipient"] != _address(wallet)):
        raise ValueError("v2_simulation_scope_invalid")
    with rpc_view(rpc):
        return EvmTransactionSimulator(rpc, fields.chain_id, wallet).simulate(transaction)


def _token_state_override(*, token: str, wallet: str, balance_units: int,
                          allowance_units: int, balance_slot_number: int,
                          allowance_slot_number: int) -> dict[str, Any]:
    owner = _address(wallet)
    if not 0 < balance_units < 2**112 or not 0 < allowance_units < 2**112:
        raise ValueError("v2_override_amount_invalid")
    owner_word = int(owner, 16).to_bytes(32, "big")
    spender_word = int(ROUTER02, 16).to_bytes(32, "big")
    balance_slot = keccak256(owner_word + balance_slot_number.to_bytes(32, "big"))
    allowance_outer = keccak256(owner_word + allowance_slot_number.to_bytes(32, "big"))
    allowance_slot = keccak256(spender_word + allowance_outer)
    return {
        token: {"stateDiff": {
            "0x" + balance_slot.hex(): "0x" + balance_units.to_bytes(32, "big").hex(),
            "0x" + allowance_slot.hex(): "0x" + allowance_units.to_bytes(32, "big").hex(),
        }},
        owner: {"balance": hex(10**18)},
    }


def weth9_state_override(*, wallet: str, balance_units: int,
                         allowance_units: int) -> dict[str, Any]:
    """Ephemeral WETH9 balance/allowance mapping slots 3/4."""
    return _token_state_override(token=MAINNET_WETH9, wallet=wallet,
                                 balance_units=balance_units, allowance_units=allowance_units,
                                 balance_slot_number=3, allowance_slot_number=4)


def usdc_state_override(*, wallet: str, balance_units: int,
                        allowance_units: int) -> dict[str, Any]:
    """Ephemeral Circle USDC proxy balance/allowance mapping slots 9/10."""
    return _token_state_override(token=MAINNET_USDC, wallet=wallet,
                                 balance_units=balance_units, allowance_units=allowance_units,
                                 balance_slot_number=9, allowance_slot_number=10)


def verify_token_override(rpc: RpcTransport, *, token: str, wallet: str, block_height: int,
                          override: Mapping[str, Any], expected_units: int) -> None:
    """Prove the deployed token reads the overridden slots at the pinned block."""
    owner = _address(wallet)
    asset = _address(token)
    if block_height <= 0 or expected_units <= 0 or asset not in {MAINNET_WETH9, MAINNET_USDC}:
        raise ValueError("v2_override_scope_invalid")
    tag = hex(block_height)
    balance_data = "0x70a08231" + owner[2:].rjust(64, "0")
    allowance_data = "0xdd62ed3e" + owner[2:].rjust(64, "0") + ROUTER02[2:].rjust(64, "0")
    with rpc_view(rpc):
        balance = rpc.call("eth_call", [{"to": asset, "data": balance_data}, tag, override])
        allowance = rpc.call("eth_call", [{"to": asset, "data": allowance_data}, tag, override])
    if _decode_word(balance) != expected_units or _decode_word(allowance) != expected_units:
        raise ValueError("v2_override_not_applied")


def verify_weth9_override(rpc: RpcTransport, *, wallet: str, block_height: int,
                          override: Mapping[str, Any], expected_units: int) -> None:
    verify_token_override(rpc, token=MAINNET_WETH9, wallet=wallet, block_height=block_height,
                          override=override, expected_units=expected_units)


def simulate_direct_pair_swap(rpc: RpcTransport, *, wallet: str,
                              transaction: BuiltTransaction, snapshot: V2PoolSnapshot,
                              override: Mapping[str, Any]) -> int:
    """Exact-block eth_call with state override; return Router02 output units."""
    owner = _address(wallet)
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    swap = decode_v2_swap(fields.data)
    if (transaction.provider != "uniswap_v2_direct" or fields.chain_id != 1
            or "0x" + fields.to.hex() != ROUTER02 or fields.value != 0
            or swap["path"] not in ([MAINNET_WETH9, MAINNET_USDC],
                                    [MAINNET_USDC, MAINNET_WETH9])
            or swap["recipient"] != owner or snapshot.base_token != MAINNET_WETH9
            or snapshot.quote_token != MAINNET_USDC
            or snapshot.chain_id != "1" or snapshot.block_height <= 0
            or swap["path"][0] not in override or owner not in override):
        raise ValueError("v2_override_simulation_scope_invalid")
    call = {"from": owner, "to": ROUTER02, "data": "0x" + fields.data.hex(),
            "value": "0x0", "gas": hex(fields.gas_limit)}
    with rpc_view(rpc):
        result = rpc.call("eth_call", [call, hex(snapshot.block_height), override])
        block = rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
    if not isinstance(block, Mapping) or block.get("hash") != snapshot.block_hash:
        raise ValueError("v2_override_simulation_block_reorged")
    if not isinstance(result, str) or not result.startswith("0x") or len(result) != 2 + 32 * 4 * 2:
        raise ValueError("v2_override_simulation_return_invalid")
    try:
        words = [int(result[2 + index * 64:2 + (index + 1) * 64], 16) for index in range(4)]
    except ValueError as error:
        raise ValueError("v2_override_simulation_return_invalid") from error
    if words[0] != 32 or words[1] != 2 or words[2] != swap["amountIn"] or words[3] < swap["minimumOut"]:
        raise ValueError("v2_override_simulation_output_invalid")
    return words[3]


def simulate_direct_weth_swap(rpc: RpcTransport, *, wallet: str,
                              transaction: BuiltTransaction, snapshot: V2PoolSnapshot,
                              override: Mapping[str, Any]) -> int:
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    if decode_v2_swap(fields.data)["path"][0] != MAINNET_WETH9:
        raise ValueError("v2_weth_simulation_scope_invalid")
    return simulate_direct_pair_swap(rpc, wallet=wallet, transaction=transaction,
                                     snapshot=snapshot, override=override)
