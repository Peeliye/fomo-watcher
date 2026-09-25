"""One Robinhood Chain native ETH buy through the best verified V2/V3 single pool."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, replace
from typing import Any, Mapping

from fomo.execution.direct_v2 import CHAIN_CONFIGS as V2_CONFIGS
from fomo.execution.direct_v2 import amount_out as v2_amount_out
from fomo.execution.direct_v3 import CHAIN_CONFIGS as V3_CONFIGS
from fomo.execution.direct_v3 import UniswapV3PoolReader, V3PoolTarget, pin_v3_block_context
from fomo.execution.evm_transaction import Eip1559Fields, decode_eip1559, keccak256
from fomo.execution.interfaces import BuiltTransaction
from fomo.execution.v3_transaction import (decode_exact_input_single,
                                           directional_price_limit, encode_exact_input_single,
                                           minimum_out)
from scripts.uniswap_v3_swap_once import (_address, _eth_call_word, _factory_pool,
                                         _fees, _quantity, _rpc_url, _send_and_wait,
                                         _signer, _wallet_profile, SwapOnceRpc)

CHAIN_ID = 4663
WETH = V2_CONFIGS[str(CHAIN_ID)].weth
FEE_TIERS = (100, 500, 3000, 10_000)
MAX_GAS = 1_000_000
V2_ETH_SWAP = keccak256(b"swapExactETHForTokens(uint256,address[],address,uint256)")[:4]


@dataclass(frozen=True, slots=True)
class Candidate:
    protocol: str
    pool: str
    fee: int | None
    amount_out: int
    token0: str
    token1: str
    block_hash: str | None = None


def _address_word(rpc: SwapOnceRpc, target: str, data: str, tag: str) -> str:
    value = _eth_call_word(rpc, target=target, data=data, tag=tag)
    if value <= 0 or value >= 2**160:
        raise ValueError("swap_once_pool_identity_invalid")
    return f"0x{value:040x}"


def _v2_pair(rpc: SwapOnceRpc, token: str, tag: str) -> str | None:
    factory = V2_CONFIGS[str(CHAIN_ID)].factory
    data = "0xe6a43905" + WETH[2:].rjust(64, "0") + token[2:].rjust(64, "0")
    value = _eth_call_word(rpc, target=factory, data=data, tag=tag)
    if value == 0:
        return None
    if value >= 2**160:
        raise ValueError("swap_once_v2_pair_invalid")
    return f"0x{value:040x}"


def _v2_candidate(rpc: SwapOnceRpc, token: str, amount: int, tag: str,
                  pair: str) -> Candidate | None:
    factory = V2_CONFIGS[str(CHAIN_ID)].factory
    token0 = _address_word(rpc, pair, "0x0dfe1681", tag)
    token1 = _address_word(rpc, pair, "0xd21220a7", tag)
    if ({token0, token1} != {WETH, token}
            or _address_word(rpc, pair, "0xc45a0155", tag) != factory
            or _v2_pair(rpc, token, tag) != pair):
        raise ValueError("swap_once_v2_identity_mismatch")
    reserves = rpc.call("eth_call", [{"to": pair, "data": "0x0902f1ac"}, tag])
    if not isinstance(reserves, str) or len(reserves) != 194:
        raise ValueError("swap_once_v2_reserves_invalid")
    reserve0, reserve1 = int(reserves[2:66], 16), int(reserves[66:130], 16)
    reserve_in, reserve_out = ((reserve0, reserve1) if token0 == WETH
                               else (reserve1, reserve0))
    if not 0 < reserve_in < 2**112 or not 0 < reserve_out < 2**112:
        return None
    try:
        output = v2_amount_out(amount, reserve_in, reserve_out)
    except ValueError:
        return None
    return Candidate("v2", pair, None, output, token0, token1)


def _v3_spacing(rpc: SwapOnceRpc, fee: int, tag: str) -> int:
    data = "0x22afcccb" + f"{fee:064x}"
    value = _eth_call_word(rpc, target=V3_CONFIGS[CHAIN_ID].factory, data=data, tag=tag)
    if value == 0:
        return 0
    if value >= 2**24 or value >= 32768:
        raise ValueError("swap_once_v3_spacing_invalid")
    return value


def discover(rpc: SwapOnceRpc, *, token: str, amount: int,
             confirmations: int = 0
             ) -> tuple[Candidate | None, list[dict[str, Any]], str]:
    if _quantity(rpc.call("eth_chainId", [])) != CHAIN_ID:
        raise ValueError("swap_once_wrong_chain")
    if confirmations not in (0, 1):
        raise ValueError("swap_once_confirmations_invalid")
    context = pin_v3_block_context(rpc, chain_id=CHAIN_ID, confirmations=confirmations)
    tag = hex(context.block_height)
    records: list[dict[str, Any]] = []
    candidates: list[Candidate] = []
    pair = _v2_pair(rpc, token, tag)
    v2 = _v2_candidate(rpc, token, amount, tag, pair) if pair else None
    records.append({"protocol": "v2", "pool": pair, "status": "valid" if v2 else
                    "no_quote" if pair else "missing", "amountOut": str(v2.amount_out) if v2 else None})
    if v2:
        candidates.append(v2)
    token0, token1 = sorted((WETH, token))
    for fee in FEE_TIERS:
        spacing = _v3_spacing(rpc, fee, tag)
        if not spacing:
            records.append({"protocol": "v3", "fee": fee, "pool": None, "status": "fee_disabled"})
            continue
        pool = _factory_pool(rpc, token0=token0, token1=token1, fee=fee, tag=tag)
        if pool is None:
            records.append({"protocol": "v3", "fee": fee, "pool": None, "status": "missing"})
            continue
        target = V3PoolTarget(pool, token0, token1, fee, spacing, -1, -1)
        try:
            snapshot = UniswapV3PoolReader(rpc=rpc, chain_id=CHAIN_ID, target=target).snapshot(
                context=context,
            )
        except ValueError as error:
            # A factory pool can exist while its current in-range liquidity is
            # zero. It cannot quote this buy, but must not hide other fee tiers.
            if (str(error) != "v3_pool_state_invalid"
                    or _eth_call_word(rpc, target=pool, data="0x1a686502", tag=tag) != 0):
                raise
            records.append({"protocol": "v3", "fee": fee, "pool": pool,
                            "status": "inactive_zero_liquidity"})
            continue
        try:
            output = snapshot.quote(token_in=WETH, amount_in=amount).amount_out
        except ValueError:
            output = 0
        candidate = (Candidate("v3", pool, fee, output, token0, token1)
                     if output > 0 else None)
        records.append({"protocol": "v3", "fee": fee, "pool": pool,
                        "status": "valid" if candidate else "no_quote",
                        "amountOut": str(output) if candidate else None})
        if candidate:
            candidates.append(candidate)
    final_block = rpc.call("eth_getBlockByNumber", [tag, False])
    if (not isinstance(final_block, Mapping)
            or final_block.get("hash") != context.block_hash):
        raise ValueError("swap_once_block_reorged")
    best = max(candidates, key=lambda item: (item.amount_out, item.protocol == "v3"), default=None)
    return (replace(best, block_hash=context.block_hash) if best else None), records, tag


def _v2_calldata(*, token: str, min_out: int, recipient: str, deadline: int) -> bytes:
    words = (min_out, 128, int(recipient, 16), deadline, 2, int(WETH, 16), int(token, 16))
    return V2_ETH_SWAP + b"".join(value.to_bytes(32, "big") for value in words)


def _verify_v2(data: bytes, *, token: str, min_out: int,
               recipient: str, deadline: int) -> None:
    if len(data) != 4 + 7 * 32 or data[:4] != V2_ETH_SWAP:
        raise ValueError("swap_once_v2_calldata_invalid")
    words = [int.from_bytes(data[4 + 32 * index:36 + 32 * index], "big") for index in range(7)]
    if words != [min_out, 128, int(recipient, 16), deadline, 2, int(WETH, 16), int(token, 16)]:
        raise ValueError("swap_once_v2_calldata_invalid")


def _transaction(*, selected: Candidate, token: str, wallet: str, amount: int,
                 slippage_bps: int, nonce: int, gas: int, priority: int,
                 maximum: int, deadline: int) -> BuiltTransaction:
    min_out = minimum_out(selected.amount_out, slippage_bps)
    if selected.protocol == "v2":
        router = V2_CONFIGS[str(CHAIN_ID)].router02
        calldata = _v2_calldata(token=token, min_out=min_out, recipient=wallet, deadline=deadline)
    else:
        router = V3_CONFIGS[CHAIN_ID].router
        assert selected.fee is not None
        calldata = encode_exact_input_single(
            token_in=WETH, token_out=token, fee=selected.fee, recipient=wallet,
            deadline=deadline, amount_in=amount, minimum_output=min_out,
            sqrt_price_limit_x96=directional_price_limit(
                WETH, selected.token0, selected.token1),
            token0=selected.token0, token1=selected.token1, chain_id=CHAIN_ID,
        )
    fields = Eip1559Fields(CHAIN_ID, nonce, priority, maximum, gas,
                           bytes.fromhex(router[2:]), amount, calldata)
    built = BuiltTransaction(fields.unsigned_bytes(), "uniswap_native_once", str(nonce))
    parsed, _ = decode_eip1559(built.serialized, signed=False)
    if (parsed.chain_id != CHAIN_ID or parsed.nonce != nonce or parsed.value != amount
            or "0x" + parsed.to.hex() != router or parsed.data != calldata):
        raise ValueError("swap_once_final_scope_invalid")
    if selected.protocol == "v2":
        _verify_v2(parsed.data, token=token, min_out=min_out, recipient=wallet,
                   deadline=deadline)
    else:
        swap = decode_exact_input_single(parsed.data, chain_id=CHAIN_ID)
        if (swap["tokenIn"] != WETH or swap["tokenOut"] != token
                or swap["fee"] != selected.fee or swap["recipient"] != wallet
                or swap["amountIn"] != amount or swap["amountOutMinimum"] != min_out
                or swap["deadline"] != deadline):
            raise ValueError("swap_once_final_scope_invalid")
    return built


def _simulate(rpc: SwapOnceRpc, built: BuiltTransaction, *, wallet: str,
              tag: str) -> int:
    fields, _ = decode_eip1559(built.serialized, signed=False)
    call = {"from": wallet, "to": "0x" + fields.to.hex(),
            "data": "0x" + fields.data.hex(), "value": hex(fields.value),
            "gas": hex(fields.gas_limit), "maxFeePerGas": hex(fields.maximum_fee),
            "maxPriorityFeePerGas": hex(fields.priority_fee)}
    estimate_call = dict(call)
    estimate_call.pop("gas")
    gas = _quantity(rpc.call("eth_estimateGas", [estimate_call, tag]))
    if not 21_000 <= gas <= MAX_GAS:
        raise ValueError("swap_once_estimated_gas_invalid")
    rpc.call("eth_call", [call, tag])
    return gas


def execute_once(*, chain_id: int, token_out: str, native_in_wei: int,
                 slippage_bps: int, broadcast: bool = False,
                 include_prepared: bool = False,
                 rpc: SwapOnceRpc | None = None) -> Mapping[str, Any]:
    started = time.monotonic()
    token = _address(token_out)
    if (chain_id != CHAIN_ID or token == WETH or not 0 < native_in_wei < 2**112
            or not 0 <= slippage_bps <= 500):
        raise ValueError("swap_once_input_invalid")
    if broadcast and include_prepared:
        raise ValueError("swap_once_prepared_broadcast_conflict")
    client = rpc or SwapOnceRpc(_rpc_url())
    selected, records, tag = discover(client, token=token, amount=native_in_wei,
                                      confirmations=1 if include_prepared or broadcast else 0)
    if selected is None:
        return {"chainId": CHAIN_ID, "tokenOut": token, "candidates": records,
                "reason": "swap_once_no_supported_v2_v3_pool",
                "possibleNextProtocol": "v4_unverified",
                "simulation_success": False, "broadcast": False,
                "totalElapsedMs": int((time.monotonic() - started) * 1000)}
    wallet, signer_profile = _wallet_profile()
    priority, maximum = _fees(client)
    nonce = _quantity(client.call("eth_getTransactionCount", [wallet, "pending"]))
    native_balance = _quantity(client.call("eth_getBalance", [wallet, "pending"]))
    deadline = int(time.time()) + 180
    built = _transaction(selected=selected, token=token, wallet=wallet,
                         amount=native_in_wei, slippage_bps=slippage_bps,
                         nonce=nonce, gas=MAX_GAS, priority=priority,
                         maximum=maximum, deadline=deadline)
    if native_balance < native_in_wei + 21_000 * maximum:
        raise ValueError("swap_once_native_balance_insufficient")
    estimated = _simulate(client, built, wallet=wallet, tag=tag)
    final_gas = min(MAX_GAS, estimated * 120 // 100)
    if native_balance < native_in_wei + final_gas * maximum:
        raise ValueError("swap_once_native_balance_insufficient")
    built = _transaction(selected=selected, token=token, wallet=wallet,
                         amount=native_in_wei, slippage_bps=slippage_bps,
                         nonce=nonce, gas=final_gas,
                         priority=priority, maximum=maximum, deadline=deadline)
    _simulate(client, built, wallet=wallet, tag=tag)
    final_header = client.call("eth_getBlockByNumber", [tag, False])
    if (not isinstance(final_header, Mapping)
            or str(final_header.get("hash") or "").lower() != selected.block_hash):
        raise ValueError("swap_once_final_header_invalid")
    result: dict[str, Any] = {
        "chainId": CHAIN_ID, "tokenIn": WETH, "tokenOut": token,
        "inputAsset": "native_eth", "amountInWei": str(native_in_wei),
        "selectedProtocol": selected.protocol, "pool": selected.pool,
        "fee": selected.fee, "quotedOut": str(selected.amount_out),
        "minOut": str(minimum_out(selected.amount_out, slippage_bps)),
        "candidates": records, "estimatedGas": estimated,
        "simulation_success": True, "broadcast": False,
        "totalElapsedMs": int((time.monotonic() - started) * 1000),
    }
    if include_prepared:
        result.update({"block": int(tag, 16), "blockHash": final_header["hash"].lower(),
                       "nonce": nonce, "deadline": deadline, "recipient": wallet,
                       "unsignedTransaction": built})
    if broadcast:
        signer = _signer(signer_profile, wallet)
        tx_hash, status = _send_and_wait(client, signer=signer, transaction=built)
        result.update({"broadcast": True, "transactionHash": tx_hash,
                       "receiptStatus": status})
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain-id", required=True, type=int)
    parser.add_argument("--token-out", required=True)
    parser.add_argument("--native-in-wei", required=True, type=int)
    parser.add_argument("--slippage-bps", type=int, default=100)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--simulate", action="store_true")
    mode.add_argument("--broadcast", action="store_true")
    options = parser.parse_args(argv)
    try:
        result = execute_once(chain_id=options.chain_id, token_out=options.token_out,
                              native_in_wei=options.native_in_wei,
                              slippage_bps=options.slippage_bps,
                              broadcast=options.broadcast)
    except Exception as error:
        reason = str(error)
        safe = reason if reason.startswith(("swap_once_", "v2_", "v3_")) else type(error).__name__
        print(json.dumps({"simulation_success": False, "reason": safe}))
        return 1
    print(json.dumps(result))
    return 0 if result["simulation_success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
