"""Bounded Robinhood V4 Initialize discovery and native ETH simulation only."""

from __future__ import annotations

import argparse
import json
import re
import time
from typing import Any, Mapping

from fomo.execution.direct_v4 import (CHAIN_CONFIGS, ZERO, UniswapV4PoolReader,
                                      V4PoolKey, _call, _signed)
from fomo.execution.evm_transaction import decode_eip1559, keccak256
from fomo.execution.pons_v4_hook import (HOOK as REVIEWED_HOOK,
                                         REVIEWED_RUNTIME_CODE_HASH,
                                         is_target_key)
from fomo.execution.v4_discovery import INITIALIZE_TOPIC, decode_initialize
from fomo.execution.v4_transaction import build_unsigned_swap, decode_unsigned_swap
from scripts.discover_two_ca_4663 import first_code_block
from scripts.uniswap_swap_once import _simulate
from scripts.uniswap_v3_swap_once import (_address, _fees, _quantity, _rpc_url,
                                         _wallet_profile, SwapOnceRpc)

CHAIN_ID = 4663
TARGET = "0x314ad0f11422842d28b4f950a64cd40fafb029fd"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
WINDOW_BLOCKS = 128
MAX_WINDOW_BLOCKS = 256
LOG_CHUNK_BLOCKS = 4
MAX_GAS = 1_000_000


def _logs(rpc: SwapOnceRpc, *, start: int, end: int, topic_index: int,
          token: str = TARGET) -> list[Mapping[str, Any]]:
    topic = "0x" + token[2:].rjust(64, "0")
    topics = [INITIALIZE_TOPIC, None, topic] if topic_index == 2 else [INITIALIZE_TOPIC, None, None, topic]

    def read(low: int, high: int) -> list[Mapping[str, Any]]:
        query = {"address": CHAIN_CONFIGS[CHAIN_ID].pool_manager,
                 "fromBlock": hex(low), "toBlock": hex(high), "topics": topics}
        try:
            result = rpc.call("eth_getLogs", [query])
        except Exception:
            if low == high:
                raise
            middle = (low + high) // 2
            return read(low, middle) + read(middle + 1, high)
        if not isinstance(result, list) or any(not isinstance(item, Mapping) for item in result):
            raise ValueError("v4_once_logs_invalid")
        return result

    found: list[Mapping[str, Any]] = []
    for low in range(start, end + 1, LOG_CHUNK_BLOCKS):
        found.extend(read(low, min(low + LOG_CHUNK_BLOCKS - 1, end)))
    return found


def _pool_state(rpc: SwapOnceRpc, key: V4PoolKey, tag: str) -> dict[str, int]:
    config = CHAIN_CONFIGS[CHAIN_ID]
    slot = _call(rpc, config.state_view, "getSlot0(bytes32)", tag, int(key.pool_id, 16))
    if not isinstance(slot, str) or len(slot) != 258:
        raise ValueError("v4_once_slot0_invalid")
    parts = [int(slot[2 + i * 64:2 + (i + 1) * 64], 16) for i in range(4)]
    liquidity = _call(rpc, config.state_view, "getLiquidity(bytes32)", tag, int(key.pool_id, 16))
    if not isinstance(liquidity, str) or len(liquidity) != 66:
        raise ValueError("v4_once_liquidity_invalid")
    return {"sqrtPriceX96": parts[0], "tick": _signed(parts[1], 24),
            "protocolFee": parts[2], "lpFee": parts[3], "liquidity": int(liquidity, 16)}


def discover(rpc: SwapOnceRpc, *, token_out: str = TARGET,
             window_blocks: int = WINDOW_BLOCKS
             ) -> tuple[list[dict[str, Any]], dict[str, int]]:
    token = _address(token_out)
    if not 1 <= window_blocks <= MAX_WINDOW_BLOCKS:
        raise ValueError("v4_once_window_invalid")
    if _quantity(rpc.call("eth_chainId", [])) != CHAIN_ID:
        raise ValueError("v4_once_wrong_chain")
    head = rpc.call("eth_getBlockByNumber", ["latest", False])
    if not isinstance(head, Mapping) or not isinstance(head.get("number"), str):
        raise ValueError("v4_once_head_invalid")
    height = _quantity(head["number"])
    head_hash = str(head.get("hash") or "").lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", head_hash):
        raise ValueError("v4_once_head_invalid")
    deployment = first_code_block(rpc, token, height)
    start, end = deployment, min(height, deployment + window_blocks - 1)
    found: dict[tuple[str, str], Mapping[str, Any]] = {}
    for index in (2, 3):
        for log in _logs(rpc, start=start, end=end, topic_index=index, token=token):
            block_number = log.get("blockNumber")
            if (not isinstance(block_number, str)
                    or not re.fullmatch(r"0x[0-9a-fA-F]+", block_number)
                    or not start <= int(block_number, 16) <= end):
                raise ValueError("v4_once_log_range_invalid")
            identity = (str(log.get("transactionHash") or "").lower(),
                        str(log.get("logIndex") or "").lower())
            if (not re.fullmatch(r"0x[0-9a-f]{64}", identity[0])
                    or not re.fullmatch(r"0x[0-9a-f]+", identity[1])):
                raise ValueError("v4_once_log_identity_invalid")
            found[identity] = log
    records: list[dict[str, Any]] = []
    for log in found.values():
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 4:
            raise ValueError("v4_once_log_topics_invalid")
        pool_id = str(topics[1]).lower()
        decoded = decode_initialize(log, chain_id=CHAIN_ID,
                                    expected_pool_id=pool_id, expected_token=token)
        if not decoded.identity_verified:
            raise ValueError("v4_once_pool_identity_invalid")
        header = rpc.call("eth_getBlockByNumber", [hex(decoded.block_height), False])
        if (not isinstance(header, Mapping)
                or str(header.get("hash") or "").lower() != decoded.block_hash):
            raise ValueError("v4_once_initialize_reorged")
        key = decoded.key
        other = key.currency1 if key.currency0 == token else key.currency0
        tag = hex(height)
        state = _pool_state(rpc, key, tag)
        hook_code_hash: str | None = None
        if key.hooks != ZERO:
            code = rpc.call("eth_getCode", [key.hooks, tag])
            if not isinstance(code, str) or not code.startswith("0x") or len(code) % 2:
                raise ValueError("v4_once_hook_code_invalid")
            hook_code_hash = "0x" + keccak256(bytes.fromhex(code[2:])).hex()
        hook_review = ("zero_hook" if key.hooks == ZERO else
                       "reviewed_pool_key" if is_target_key(CHAIN_ID, key)
                       and hook_code_hash == REVIEWED_RUNTIME_CODE_HASH else
                       "reviewed_runtime_other_pool_key" if key.hooks == REVIEWED_HOOK
                       and hook_code_hash == REVIEWED_RUNTIME_CODE_HASH else
                       "unreviewed_hook")
        records.append({
            "poolId": pool_id, "poolKey": {"currency0": key.currency0,
                "currency1": key.currency1, "fee": key.fee,
                "tickSpacing": key.tick_spacing, "hooks": key.hooks},
            "initializeBlock": decoded.block_height, "otherCurrency": other,
            "state": state, "hookCodeHash": hook_code_hash,
            "hookReview": hook_review,
            "inputPath": "native_eth" if other == ZERO else
                         "weth_needs_wrap" if other == WETH else "unsupported_input_asset",
        })
    final_head = rpc.call("eth_getBlockByNumber", [hex(height), False])
    if (not isinstance(final_head, Mapping)
            or str(final_head.get("hash") or "").lower() != head_hash):
        raise ValueError("v4_once_scan_head_reorged")
    return records, {"deploymentBlock": deployment, "scanFrom": start,
                     "scanTo": end, "confirmedHead": height}


def execute_once(*, token_out: str = TARGET, native_in_wei: int,
                 slippage_bps: int, rpc: SwapOnceRpc | None = None,
                 window_blocks: int = WINDOW_BLOCKS) -> Mapping[str, Any]:
    started = time.monotonic()
    if not 0 < native_in_wei < 1 << 128 or not 0 <= slippage_bps <= 500:
        raise ValueError("v4_once_input_invalid")
    client = rpc or SwapOnceRpc(_rpc_url())
    token = _address(token_out)
    pools, window = discover(client, token_out=token, window_blocks=window_blocks)
    common: dict[str, Any] = {"chainId": CHAIN_ID, "tokenOut": token,
                              "scan": window, "pools": pools,
                              "simulation_success": False, "broadcast": False}
    if not pools:
        return {**common, "result": "bounded_window_no_initialize",
                "totalElapsedMs": int((time.monotonic() - started) * 1000)}
    if any(pool["poolKey"]["hooks"] != ZERO for pool in pools):
        # The sole reviewed nonzero hook belongs to a different PoolKey.
        if all(pool["poolKey"]["hooks"] != ZERO for pool in pools):
            return {**common, "result": "unsupported_v4_hook",
                    "totalElapsedMs": int((time.monotonic() - started) * 1000)}
    candidates: list[tuple[int, Any, Any]] = []
    for pool in pools:
        if (pool["poolKey"]["hooks"] != ZERO or pool["otherCurrency"] != ZERO
                or pool["state"]["liquidity"] <= 0 or pool["state"]["sqrtPriceX96"] <= 0):
            continue
        details = pool["poolKey"]
        key = V4PoolKey(details["currency0"], details["currency1"], details["fee"],
                        details["tickSpacing"], details["hooks"])
        snapshot = UniswapV4PoolReader(rpc=client, chain_id=CHAIN_ID, key=key).snapshot()
        try:
            quote = snapshot.quote(token_in=ZERO, amount_in=native_in_wei)
        except ValueError:
            continue
        candidates.append((quote.amount_out, snapshot, quote))
    if not candidates:
        return {**common, "result": "v4_no_supported_native_quote",
                "totalElapsedMs": int((time.monotonic() - started) * 1000)}
    amount_out, snapshot, _ = max(candidates, key=lambda item: item[0])
    minimum = amount_out * (10_000 - slippage_bps) // 10_000
    if minimum <= 0:
        raise ValueError("v4_once_minimum_out_zero")
    wallet, _ = _wallet_profile()
    priority, maximum = _fees(client)
    nonce = _quantity(client.call("eth_getTransactionCount", [wallet, "pending"]))
    balance = _quantity(client.call("eth_getBalance", [wallet, "pending"]))
    if balance < native_in_wei + 21_000 * maximum:
        raise ValueError("v4_once_native_balance_insufficient")
    deadline = int(time.time()) + 180
    def build(gas: int):
        transaction = build_unsigned_swap(
            chain_id=CHAIN_ID, key=snapshot.key, token_in=ZERO,
            amount_in=native_in_wei, minimum_out=minimum, deadline=deadline,
            nonce=nonce, gas_limit=gas, priority_fee_wei=priority,
            maximum_fee_wei=maximum,
        )
        decoded = decode_unsigned_swap(transaction, chain_id=CHAIN_ID)
        fields, _ = decode_eip1559(transaction.serialized, signed=False)
        if (decoded["key"] != snapshot.key or decoded["tokenOut"] != token
                or decoded["amountIn"] != native_in_wei
                or decoded["amountOutMinimum"] != minimum
                or decoded["deadline"] != deadline or fields.nonce != nonce
                or fields.value != native_in_wei or fields.chain_id != CHAIN_ID
                or "0x" + fields.to.hex() != CHAIN_CONFIGS[CHAIN_ID].universal_router):
            raise ValueError("v4_once_final_scope_invalid")
        # Universal Router TAKE_ALL sends output to msg.sender; the command
        # codec has no arbitrary recipient field to silently redirect output.
        return transaction
    initial = build(MAX_GAS)
    tag = hex(snapshot.block_height)
    estimated = _simulate(client, initial, wallet=wallet, tag=tag)
    gas = min(MAX_GAS, estimated * 120 // 100)
    if balance < native_in_wei + gas * maximum:
        raise ValueError("v4_once_native_balance_insufficient")
    final = build(gas)
    _simulate(client, final, wallet=wallet, tag=tag)
    check = client.call("eth_getBlockByNumber", [tag, False])
    if not isinstance(check, Mapping) or check.get("hash") != snapshot.block_hash:
        raise ValueError("v4_once_simulation_reorged")
    return {**common, "result": "v4_eth_call_success", "simulation_success": True,
            "selectedPoolId": snapshot.pool_id, "amountInWei": str(native_in_wei),
            "quotedOut": str(amount_out), "minOut": str(minimum),
            "recipient": wallet, "estimatedGas": estimated,
            "totalElapsedMs": int((time.monotonic() - started) * 1000)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-out", default=TARGET)
    parser.add_argument("--native-in-wei", required=True, type=int)
    parser.add_argument("--slippage-bps", type=int, default=100)
    parser.add_argument("--window-blocks", type=int, default=WINDOW_BLOCKS)
    parser.add_argument("--simulate", action="store_true")
    options = parser.parse_args(argv)
    try:
        result = execute_once(token_out=options.token_out,
                              native_in_wei=options.native_in_wei,
                              slippage_bps=options.slippage_bps,
                              window_blocks=options.window_blocks)
    except Exception as error:
        reason = str(error)
        safe = reason if reason.startswith("v4_") else type(error).__name__
        print(json.dumps({"simulation_success": False, "reason": safe}))
        return 1
    print(json.dumps(result))
    return 0 if result["simulation_success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
