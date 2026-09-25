"""Finite read-only Robinhood native-ETH route preflights.

V2/V3 are delegated to the existing direct single-pool probe. V4 zero-Hook
checks only four explicit PoolKeys. Unknown Hook and unrelated quote assets
are not silently routed through an aggregator or a multi-hop path.
"""

from __future__ import annotations

import time
from typing import Any, Mapping

from .direct_v4 import CHAIN_CONFIGS, UniswapV4PoolReader, V4PoolKey, ZERO
from .evm_transaction import decode_eip1559
from .v4_transaction import build_unsigned_swap, decode_unsigned_swap


CHAIN_ID = 4663
ZERO_HOOK_KEYS = ((100, 1), (500, 10), (3000, 60), (10_000, 200))
MAX_GAS = 1_000_000


def simulate_zero_hook(rpc: Any, *, token_out: str,
                       amount_in_wei: int, slippage_bps: int) -> dict[str, Any] | None:
    from scripts.uniswap_swap_once import _simulate
    from scripts.uniswap_v3_swap_once import _address, _fees, _quantity, _wallet_profile

    token = _address(token_out)
    if (token == ZERO or not 0 < amount_in_wei < 1 << 128
            or not 0 <= slippage_bps <= 500):
        raise ValueError("rh_auto_zero_hook_input_invalid")
    if _quantity(rpc.call("eth_chainId", [])) != CHAIN_ID:
        raise ValueError("rh_auto_wrong_chain")
    candidates = []
    for fee, spacing in ZERO_HOOK_KEYS:
        key = V4PoolKey(ZERO, token, fee, spacing)
        try:
            snapshot = UniswapV4PoolReader(rpc=rpc, chain_id=CHAIN_ID, key=key).snapshot()
        except ValueError as error:
            if str(error) == "v4_pool_uninitialized_or_fee_mismatch":
                continue
            raise
        try:
            quote = snapshot.quote(token_in=ZERO, amount_in=amount_in_wei)
        except ValueError:
            continue
        if quote.amount_out > 0:
            candidates.append((snapshot, quote))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError("rh_auto_ambiguous_zero_hook_pools")
    snapshot, quote = candidates[0]
    minimum = quote.amount_out * (10_000 - slippage_bps) // 10_000
    if minimum <= 0:
        raise ValueError("rh_auto_zero_hook_minimum_invalid")
    wallet, _ = getattr(rpc, "wallet_profile", None) or _wallet_profile()
    priority, maximum = _fees(rpc)
    nonce = _quantity(rpc.call("eth_getTransactionCount", [wallet, "pending"]))
    balance = _quantity(rpc.call("eth_getBalance", [wallet, "pending"]))
    if balance < amount_in_wei + 21_000 * maximum:
        raise ValueError("rh_auto_native_balance_insufficient")
    deadline = int(time.time()) + 180
    def build(gas: int):
        built = build_unsigned_swap(chain_id=CHAIN_ID, key=snapshot.key,
                                   token_in=ZERO, amount_in=amount_in_wei,
                                   minimum_out=minimum, deadline=deadline,
                                   nonce=nonce, gas_limit=gas,
                                   priority_fee_wei=priority,
                                   maximum_fee_wei=maximum)
        decoded = decode_unsigned_swap(built, chain_id=CHAIN_ID)
        fields, _ = decode_eip1559(built.serialized, signed=False)
        if (decoded["key"] != snapshot.key or decoded["tokenOut"] != token
                or decoded["amountIn"] != amount_in_wei
                or decoded["amountOutMinimum"] != minimum
                or decoded["deadline"] != deadline or fields.nonce != nonce
                or fields.value != amount_in_wei or fields.chain_id != CHAIN_ID
                or "0x" + fields.to.hex() != CHAIN_CONFIGS[CHAIN_ID].universal_router):
            raise ValueError("rh_auto_zero_hook_final_scope_invalid")
        return built
    tag = hex(snapshot.block_height)
    estimated = _simulate(rpc, build(MAX_GAS), wallet=wallet, tag=tag)
    gas = min(MAX_GAS, estimated * 120 // 100)
    if balance < amount_in_wei + gas * maximum:
        raise ValueError("rh_auto_native_balance_insufficient")
    final = build(gas)
    _simulate(rpc, final, wallet=wallet, tag=tag)
    header = rpc.call("eth_getBlockByNumber", [tag, False])
    if (not isinstance(header, Mapping)
            or str(header.get("hash") or "").lower() != snapshot.block_hash.lower()):
        raise ValueError("rh_auto_zero_hook_block_reorged")
    return {"chainId": CHAIN_ID, "protocol": "uniswap_v4_zero_hook",
            "poolId": snapshot.pool_id, "tokenOut": token,
            "block": snapshot.block_height, "blockHash": snapshot.block_hash.lower(),
            "amountInWei": str(amount_in_wei), "quotedOut": str(quote.amount_out),
            "minOut": str(minimum), "estimatedGas": estimated,
            "recipient": wallet, "nonce": nonce, "deadline": deadline,
            "simulation_success": True, "broadcast": False,
            "unsignedTransaction": final}
