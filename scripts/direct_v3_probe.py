"""Read-only, fixed-target V3 L0 probe using the configured local RPC.

Loads the git-ignored local .env without overriding a process value. Never
signs, approves, estimates gas, or broadcasts. Output omits provider secrets.
"""

from __future__ import annotations

import json
import os
import time
import argparse
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fomo.execution.direct_v3 import (CHAIN_CONFIGS, MAINNET_PROBE_POOL, BASE_PROBE_POOL,
                                      UniswapV3PoolReader, mainnet_probe_override,
                                      simulate_same_block, verify_mainnet_probe_override)
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.v3_transaction import (build_unsigned_swap, decode_exact_input_single,
                                           encode_exact_input_single)
from fomo.execution.evm_transaction import decode_eip1559
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcUnavailable, rpc_view


PUBLIC_WALLET = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"
SELL_WETH_UNITS = 10**15
BUY_USDC_UNITS = 10**6
SYNTHETIC_WETH_UNITS = 10**18
SYNTHETIC_USDC_UNITS = 10 * 10**6


def main(argv: list[str] | None = None) -> int:
    args = argparse.ArgumentParser(description="V3 L0 read-only probe")
    args.add_argument("--chain", choices=("1", "8453"), default="1")
    chain = int(args.parse_args([] if argv is None else argv).chain)
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    rpc_env = "RPC_BASE_URL" if chain == 8453 else "RPC_ETHEREUM_URL"
    if not os.getenv(rpc_env, "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}))
        return 2
    if chain == 8453:
        started = time.monotonic()
        stage = "read_pool"
        reader = None
        rpc = None
        try:
            config, target = CHAIN_CONFIGS[chain], BASE_PROBE_POOL
            rpc = FailoverJsonRpc(str(chain), [RpcEndpoint(str(chain), "direct-v3-base-l0",
                                                           "primary", http_env=rpc_env)],
                                  previous_header_fallback=True)
            reader = UniswapV3PoolReader(rpc=rpc, chain_id=chain, target=target)
            snapshot = reader.snapshot()
            stage = "local_quote_and_codec"
            rows = []
            for token, initial in ((target.token0, 10**15), (target.token1, 10**6)):
                amount = initial
                for _ in range(4):
                    try:
                        quote = snapshot.quote(token_in=token, amount_in=amount)
                        break
                    except ValueError as error:
                        if str(error) not in {"v3_initialized_tick_missing", "v3_bitmap_word_missing"}:
                            raise
                        amount //= 10
                else:
                    raise ValueError("v3_probe_amount_not_covered")
                if quote.amount_in != amount or quote.block_hash != snapshot.block_hash:
                    raise ValueError("v3_probe_partial_quote")
                unsigned = build_unsigned_swap(
                    chain_id=chain, router=config.router, token0=target.token0,
                    token1=target.token1, fee=target.fee, token_in=token,
                    wallet=PUBLIC_WALLET, amount_in=amount, quoted_out=quote.amount_out,
                    slippage_bps=500, nonce=0, gas_limit=400_000,
                    priority_fee_wei=1, maximum_fee_wei=2, deadline=int(time.time())+300,
                )
                fields, _ = decode_eip1559(unsigned.serialized, signed=False)
                decoded = decode_exact_input_single(fields.data, chain_id=chain)
                if (fields.chain_id != chain or "0x" + fields.to.hex() != config.router
                        or decoded["amountIn"] != amount or decoded["fee"] != target.fee):
                    raise ValueError("v3_base_codec_mismatch")
                rows.append({"tokenIn": token, "amountIn": str(amount),
                             "localAmountOut": str(quote.amount_out),
                             "codecRoundtrip": True, "simulationVerified": False})
            print(json.dumps({"readOnlyOk": True, "simulationVerified": False,
                              "tradingReady": False, "chainId": chain,
                              "block": snapshot.block_height, "blockHash": snapshot.block_hash,
                              "blockDiagnostic": reader.last_block_diagnostic,
                              "factory": config.factory, "pool": snapshot.pool,
                              "fee": snapshot.fee, "rows": rows,
                              "totalElapsedMs": round((time.monotonic()-started)*1000)}))
            return 0
        except Exception as error:
            print(json.dumps({"readOnlyOk": False, "simulationVerified": False,
                              "tradingReady": False, "chainId": chain, "stage": stage,
                              "errorType": type(error).__name__,
                              "reason": str(error) if str(error).startswith("v3_") else None,
                              "blockDiagnostic": (reader.last_block_diagnostic
                                                  if reader is not None else None),
                              "rpcFailure": ({"method": rpc.last_request_method,
                                              "blockTagged": rpc.last_block_tagged,
                                              "httpStatus": rpc.last_http_status,
                                              "providerErrorCode": rpc.last_provider_error_code,
                                              "transportErrorType": rpc.last_transport_error_type,
                                              "rpcDiagnostic": rpc.last_diagnostic}
                                             if rpc is not None else None),
                              "totalElapsedMs": round((time.monotonic()-started)*1000)}))
            return 1
    stage = "rpc_identity_and_pool"
    evidence: dict[str, Any] | None = None
    started = time.monotonic()
    rpc: FailoverJsonRpc | None = None
    try:
        config = CHAIN_CONFIGS[1]
        target = MAINNET_PROBE_POOL
        rpc = FailoverJsonRpc("1", [RpcEndpoint("1", "direct-v3-l0", "primary",
                                             http_env="RPC_ETHEREUM_URL")])
        snapshot = UniswapV3PoolReader(rpc=rpc, chain_id=1, target=target).snapshot()
        stage = "same_block_quote"
        if (snapshot.pool != target.pool or snapshot.block_hash == ""
                or snapshot.router != config.router):
            raise ValueError("v3_probe_pool_identity_invalid")
        quote_rows = []
        for name, token_in, amount_in, synthetic_balance in (
            ("WETH->USDC", target.token1, SELL_WETH_UNITS, SYNTHETIC_WETH_UNITS),
            ("USDC->WETH", target.token0, BUY_USDC_UNITS, SYNTHETIC_USDC_UNITS),
        ):
            # Probe amounts only shrink; never fetch more bitmap words or invent ticks.
            for _ in range(4):
                try:
                    quote = snapshot.quote(token_in=token_in, amount_in=amount_in)
                    break
                except ValueError as error:
                    if str(error) not in {"v3_initialized_tick_missing", "v3_bitmap_word_missing"}:
                        raise
                    amount_in //= 10
                    if amount_in == 0:
                        raise ValueError("v3_probe_amount_not_covered") from error
            else:
                raise ValueError("v3_probe_amount_not_covered")
            if quote.amount_in != amount_in or quote.block_hash != snapshot.block_hash:
                raise ValueError("v3_probe_partial_quote")
            quote_rows.append((name, token_in, amount_in, synthetic_balance, quote))
        evidence = {"chainId": 1, "factory": config.factory, "router": config.router,
                    "pool": snapshot.pool, "fee": snapshot.fee,
                    "token0": snapshot.token0, "token1": snapshot.token1,
                    "blockHeight": snapshot.block_height, "blockHash": snapshot.block_hash,
                    "tick": snapshot.tick, "sqrtPriceX96": str(snapshot.sqrt_price_x96),
                    "liquidity": str(snapshot.liquidity),
                    "spotToken0PerToken1": str(snapshot.spot_quote_per_base),
                    "bitmapWords": len(snapshot.bitmaps),
                    "readElapsedMs": round((time.monotonic() - started) * 1000)}
        directions = []
        for name, token_in, amount_in, synthetic_balance, quote in quote_rows:
            stage = "unsigned_build"
            unsigned = build_unsigned_swap(
                chain_id=1, router=config.router, token0=target.token0, token1=target.token1,
                fee=target.fee, token_in=token_in, wallet=PUBLIC_WALLET,
                amount_in=amount_in, quoted_out=quote.amount_out, slippage_bps=500,
                nonce=0, gas_limit=400_000, priority_fee_wei=1, maximum_fee_wei=2,
                deadline=int(time.time()) + 300,
            )
            fields, _ = decode_eip1559(unsigned.serialized, signed=False)
            swap = decode_exact_input_single(fields.data)
            if (swap["amountIn"] != amount_in or swap["fee"] != target.fee
                    or swap["amountOutMinimum"] > quote.amount_out):
                raise ValueError("v3_probe_unsigned_scope_invalid")
            stage = "synthetic_override_verification"
            override = mainnet_probe_override(wallet=PUBLIC_WALLET, token=token_in,
                                              amount_units=synthetic_balance)
            verify_mainnet_probe_override(
                rpc, wallet=PUBLIC_WALLET, token=token_in, amount_units=synthetic_balance,
                snapshot=snapshot, override=override,
            )
            stage = "same_block_eth_call"
            simulated = simulate_same_block(
                rpc, wallet=PUBLIC_WALLET, transaction=unsigned,
                snapshot=snapshot, quote=quote, state_override=override,
            )
            stage = "excessive_min_out_eth_call"
            bad_data = encode_exact_input_single(
                token_in=swap["tokenIn"], token_out=swap["tokenOut"], fee=swap["fee"],
                recipient=swap["recipient"], deadline=swap["deadline"],
                amount_in=amount_in, minimum_output=quote.amount_out + 1,
                sqrt_price_limit_x96=swap["sqrtPriceLimitX96"],
                token0=snapshot.token0, token1=snapshot.token1,
            )
            bad_call = {"from": PUBLIC_WALLET, "to": snapshot.router,
                        "data": "0x" + bad_data.hex(), "value": "0x0",
                        "gas": hex(fields.gas_limit)}
            rejected = False
            with rpc_view(rpc):
                try:
                    rpc.call("eth_call", [bad_call, hex(snapshot.block_height), override])
                except RpcUnavailable:
                    if rpc.last_diagnostic != "rpc_method_unavailable":
                        raise
                    rejected = True
                block = rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
            if not rejected or block.get("hash") != snapshot.block_hash:
                raise ValueError("v3_excessive_min_out_not_rejected_or_block_changed")
            directions.append({"direction": name, "inputUnits": str(amount_in),
                               "quotedOutputUnits": str(quote.amount_out),
                               "simulatedOutputUnits": str(simulated),
                               "excessiveMinOutRejected": rejected,
                               "crossedTicks": len(quote.crossed_ticks),
                               "blockHeight": snapshot.block_height,
                               "blockHash": snapshot.block_hash})
    except Exception as error:
        print(json.dumps({"readOnlyOk": evidence is not None, "simulationOk": False,
                          "tradingReady": False, "stage": stage,
                          "errorType": type(error).__name__,
                          "rpcDiagnostic": getattr(rpc, "last_diagnostic", None),
                          "readFailure": str(error) if str(error).startswith("v3_rpc_failure:") else None,
                          "totalElapsedMs": round((time.monotonic() - started) * 1000),
                          "evidence": evidence}, ensure_ascii=False))
        return 1
    print(json.dumps({"readOnlyOk": True, "simulationOk": True,
                      "tradingReady": False, "evidence": evidence,
                      "simulation": {"sameBlock": True, "stateOverride": True,
                                     "directions": directions,
                                     "totalElapsedMs": round((time.monotonic() - started) * 1000)}},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
