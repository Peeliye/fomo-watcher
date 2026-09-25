"""Read-only, per-token Pons V4 native buy preflight for the reviewed Hook.

No signer, send method, or persistent live switch is reachable from this module.
An unknown or differently configured Hook remains unsupported.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from typing import Any, Mapping

from .direct_v4 import CHAIN_CONFIGS, V4PoolKey, ZERO, _call
from .evm_transaction import decode_eip1559
from .pons_v4_hook import HOOK, REVIEWED_RUNTIME_CODE_HASH, verify_runtime_code
from .pons_v4_once import build_unsigned, checked_key, decode_unsigned, quote_gross


CHAIN_ID = 4663
MAX_GAS = 1_000_000
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")


def candidate_key(token_out: str) -> V4PoolKey:
    token = str(token_out).lower()
    if not _ADDRESS.fullmatch(token) or token == ZERO:
        raise ValueError("pons_dynamic_token_invalid")
    key = V4PoolKey(ZERO, token, 0, 200, HOOK)
    checked_key(key, expected_pool_id=key.pool_id, expected_token=token)
    return key


def _header(rpc: Any, height: int) -> Mapping[str, Any]:
    header = rpc.call("eth_getBlockByNumber", [hex(height), False])
    if (not isinstance(header, Mapping)
            or header.get("number") != hex(height)
            or not _HASH.fullmatch(str(header.get("hash") or "").lower())):
        raise ValueError("pons_dynamic_header_invalid")
    return header


def has_reviewed_launch(rpc: Any, *, key: V4PoolKey, block_tag: str) -> bool:
    """An absent launch is not a routing failure; malformed RPC data is."""
    raw = _call(rpc, HOOK, "launches(bytes32)", block_tag, int(key.pool_id, 16))
    if not isinstance(raw, str) or not re.fullmatch(r"0x[0-9a-fA-F]{832}", raw):
        raise ValueError("pons_dynamic_launch_response_invalid")
    registered = int(raw[2:66], 16)
    if registered == 0:
        if int(raw[66:], 16) != 0:
            raise ValueError("pons_dynamic_unregistered_launch_data")
        return False
    if registered != 1:
        raise ValueError("pons_dynamic_launch_response_invalid")
    return True


def simulate_buy(rpc: Any, *, token_out: str, amount_in_wei: int,
                 slippage_bps: int) -> dict[str, Any] | None:
    """Return a verified simulation, or None only when this Hook has no launch."""
    from scripts.pons_v4_swap_once import _fee_values
    from scripts.uniswap_v3_swap_once import _quantity, _wallet_profile

    if not 0 < amount_in_wei < 1 << 128 or not 0 <= slippage_bps <= 500:
        raise ValueError("pons_dynamic_input_invalid")
    key = candidate_key(token_out)
    if _quantity(rpc.call("eth_chainId", [])) != CHAIN_ID:
        raise ValueError("pons_dynamic_wrong_chain")
    head = _quantity(rpc.call("eth_blockNumber", []))
    if head < 2:
        raise ValueError("pons_dynamic_head_invalid")
    height = head - 1
    header = _header(rpc, height)
    tag = hex(height)
    if not has_reviewed_launch(rpc, key=key, block_tag=tag):
        return None
    verify_runtime_code(rpc.call("eth_getCode", [HOOK, tag]), REVIEWED_RUNTIME_CODE_HASH)
    manager = _call(rpc, HOOK, "poolManager()", tag)
    if (not isinstance(manager, str) or len(manager) != 66
            or int(manager, 16) != int(CHAIN_CONFIGS[CHAIN_ID].pool_manager, 16)):
        raise ValueError("pons_dynamic_manager_mismatch")
    gross, pool_state, policy = quote_gross(rpc, block_tag=tag,
                                            amount_in=amount_in_wei, key=key)
    if policy.memecoin != key.currency1 or policy.quote_token != ZERO:
        raise ValueError("pons_dynamic_launch_asset_mismatch")
    hook_fee, creator_tax, net = policy.fee_components(gross)
    minimum = net * (10_000 - slippage_bps) // 10_000
    if minimum <= 0:
        raise ValueError("pons_dynamic_minimum_zero")
    wallet, _ = getattr(rpc, "wallet_profile", None) or _wallet_profile()
    nonce = _quantity(rpc.call("eth_getTransactionCount", [wallet, "pending"]))
    balance = _quantity(rpc.call("eth_getBalance", [wallet, "pending"]))
    priority = rpc.call("eth_maxPriorityFeePerGas", [])
    pending = rpc.call("eth_getBlockByNumber", ["pending", False])
    priority_fee, maximum_fee = _fee_values(priority, pending)
    deadline = int(time.time()) + 180
    def build(gas: int):
        built = build_unsigned(amount_in=amount_in_wei, minimum_out=minimum,
                               deadline=deadline, nonce=nonce, gas_limit=gas,
                               priority_fee=priority_fee, maximum_fee=maximum_fee,
                               key=key)
        decoded = decode_unsigned(built, key=key)
        fields, _ = decode_eip1559(built.serialized, signed=False)
        if (decoded["poolId"] != key.pool_id or decoded["tokenOut"] != key.currency1
                or decoded["amountIn"] != amount_in_wei or decoded["minOut"] != minimum
                or decoded["nonce"] != nonce or decoded["deadline"] != deadline
                or fields.value != amount_in_wei or fields.chain_id != CHAIN_ID
                or "0x" + fields.to.hex() != CHAIN_CONFIGS[CHAIN_ID].universal_router):
            raise ValueError("pons_dynamic_final_scope_invalid")
        return built, fields
    _, fields = build(MAX_GAS)
    if balance < amount_in_wei + 21_000 * maximum_fee:
        raise ValueError("pons_dynamic_native_balance_insufficient")
    call = {"from": wallet, "to": "0x" + fields.to.hex(),
            "data": "0x" + fields.data.hex(), "value": hex(fields.value),
            "maxFeePerGas": hex(fields.maximum_fee),
            "maxPriorityFeePerGas": hex(fields.priority_fee)}
    estimate = _quantity(rpc.call("eth_estimateGas", [call, tag]))
    if not 21_000 <= estimate <= MAX_GAS:
        raise ValueError("pons_dynamic_estimated_gas_invalid")
    gas = min(MAX_GAS, estimate * 120 // 100)
    if balance < amount_in_wei + gas * maximum_fee:
        raise ValueError("pons_dynamic_native_balance_insufficient")
    final, final_fields = build(gas)
    call.update(data="0x" + final_fields.data.hex(), gas=hex(gas))
    if rpc.call("eth_call", [call, tag]) != "0x":
        raise ValueError("pons_dynamic_call_return_invalid")
    final_header = _header(rpc, height)
    if str(final_header["hash"]).lower() != str(header["hash"]).lower():
        raise ValueError("pons_dynamic_block_reorged")
    return {"chainId": CHAIN_ID, "protocol": "pons_v4_reviewed_hook",
            "poolId": key.pool_id, "tokenOut": key.currency1,
            "block": height, "blockHash": str(header["hash"]).lower(),
            "poolState": pool_state, "amountInWei": str(amount_in_wei),
            "grossOut": str(gross), "hookFee": str(hook_fee),
            "creatorTax": str(creator_tax), "netOut": str(net),
            "minOut": str(minimum), "estimatedGas": estimate,
            "recipient": wallet, "nonce": nonce, "deadline": deadline,
            "simulation_success": True, "broadcast": False,
            "unsignedTransaction": final}


def main(argv: list[str] | None = None) -> int:
    from scripts.pons_v4_swap_once import PonsRpc
    from scripts.uniswap_v3_swap_once import _rpc_url

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-out", required=True)
    parser.add_argument("--amount-in-wei", required=True, type=int)
    parser.add_argument("--slippage-bps", default=100, type=int)
    args = parser.parse_args(argv)
    rpc: PonsRpc | None = None
    try:
        rpc = PonsRpc(_rpc_url())
        result = simulate_buy(rpc, token_out=args.token_out,
                              amount_in_wei=args.amount_in_wei,
                              slippage_bps=args.slippage_bps)
        if result is None:
            print(json.dumps({"simulation_success": False, "reason": "pons_launch_not_registered"}))
            return 1
        public = {key: value for key, value in result.items()
                  if key != "unsignedTransaction"}
        print(json.dumps(public))
        return 0
    except Exception as error:
        reason = str(error)
        if not reason.startswith(("pons_", "swap_once_")):
            reason = type(error).__name__
        print(json.dumps({"simulation_success": False, "reason": reason}))
        return 1
    finally:
        if rpc is not None:
            rpc.close()


if __name__ == "__main__":
    raise SystemExit(main())
