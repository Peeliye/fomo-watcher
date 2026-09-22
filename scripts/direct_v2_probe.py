"""Fixed-pair, fixed-public-wallet V2 L0 read and state-override simulation.

Only RPC_ETHEREUM_URL already injected into this process is used. No .env read,
signature, approval, state change, or broadcast is performed.
"""

from __future__ import annotations

import json
import os
import time
import argparse
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fomo.execution.direct_v2 import (UniswapV2PoolReader, build_swap_calldata,
                                      build_unsigned_swap, simulate_direct_pair_swap,
                                      usdc_state_override, verify_token_override,
                                      weth9_state_override)
from fomo.execution.direct_v2 import CHAIN_CONFIGS
from fomo.execution.evm_transaction import Eip1559Fields, decode_eip1559, decode_v2_swap
from fomo.execution.interfaces import BuiltTransaction
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcUnavailable


WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
PAIR = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
PUBLIC_WALLET = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"
SYNTHETIC_WETH_UNITS = 10**18
SYNTHETIC_USDC_UNITS = 10 * 10**6
SELL_WETH_UNITS = 10**15
BUY_USDC_UNITS = 10**6
EXPECTED_MIN_OUT_REVERT = "UniswapV2Router: INSUFFICIENT_OUTPUT_AMOUNT"


def _excessive_minimum(transaction: BuiltTransaction, *, reserve_out: int) -> BuiltTransaction:
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    original = decode_v2_swap(fields.data)
    swap_data = build_swap_calldata(
        amount_in_units=original["amountIn"], minimum_out_units=reserve_out,
        token_in=original["path"][0], token_out=original["path"][1],
        recipient=PUBLIC_WALLET,
        deadline=int.from_bytes(fields.data[132:164], "big"),
    )
    changed = Eip1559Fields(fields.chain_id, fields.nonce, fields.priority_fee,
                            fields.maximum_fee, fields.gas_limit, fields.to,
                            fields.value, swap_data)
    return BuiltTransaction(changed.unsigned_bytes(), transaction.provider,
                            transaction.nonce_or_blockhash)


def main(argv: list[str] | None = None) -> int:
    args = argparse.ArgumentParser(description="V2 L0 read-only probe")
    args.add_argument("--chain", choices=("1", "8453"), default="1")
    chain = args.parse_args([] if argv is None else argv).chain
    if chain == "8453":
        load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    rpc_env = "RPC_BASE_URL" if chain == "8453" else "RPC_ETHEREUM_URL"
    if not os.getenv(rpc_env, "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}))
        return 2
    if chain == "8453":
        started = time.monotonic()
        try:
            config = CHAIN_CONFIGS[chain]
            rpc = FailoverJsonRpc(chain, [RpcEndpoint(chain, "direct-v2-base-l0", "primary",
                                                      http_env=rpc_env)])
            snapshot = UniswapV2PoolReader(rpc=rpc, chain_id=chain,
                                            base_token=config.weth, quote_token=config.usdc).snapshot()
            if snapshot.pair != config.probe_pair:
                raise ValueError("v2_pair_identity_mismatch")
            rows = []
            for token, amount in ((config.weth, 10**15), (config.usdc, 10**6)):
                output = (snapshot.quote_sell(amount, 500)[0] if token == config.weth
                          else snapshot.quote_buy(amount, 500)[0])
                unsigned = build_unsigned_swap(
                    snapshot=snapshot, wallet=PUBLIC_WALLET, token_in=token,
                    amount_in_units=amount, slippage_bps=500, nonce=0,
                    gas_limit=300_000, priority_fee_wei=1, maximum_fee_wei=2,
                    deadline=int(time.time()) + 300,
                )
                fields, _ = decode_eip1559(unsigned.serialized, signed=False)
                swap = decode_v2_swap(fields.data)
                if fields.chain_id != 8453 or "0x" + fields.to.hex() != config.router02 or swap["amountIn"] != amount:
                    raise ValueError("v2_base_codec_mismatch")
                rows.append({"tokenIn": token, "amountIn": str(amount), "localAmountOut": str(output),
                             "codecRoundtrip": True, "simulationVerified": False})
            print(json.dumps({"readOnlyOk": True, "simulationVerified": False,
                              "tradingReady": False, "chainId": 8453,
                              "block": snapshot.block_height, "blockHash": snapshot.block_hash,
                              "factory": config.factory, "pair": snapshot.pair,
                              "rows": rows, "totalElapsedMs": round((time.monotonic()-started)*1000)}))
            return 0
        except Exception as error:
            print(json.dumps({"readOnlyOk": False, "simulationVerified": False,
                              "tradingReady": False, "chainId": 8453,
                              "errorType": type(error).__name__,
                              "reason": str(error) if str(error).startswith("v2_") else None}))
            return 1
    evidence: dict[str, Any] | None = None
    stage = "rpc_identity_and_pool"
    try:
        started = time.monotonic()
        rpc = FailoverJsonRpc("1", [RpcEndpoint("1", "direct-v2-l0", "primary",
                                               http_env="RPC_ETHEREUM_URL")])
        reader = UniswapV2PoolReader(rpc=rpc, base_token=WETH, quote_token=USDC)
        snapshot = reader.snapshot()
        stage = "fixed_pair_check"
        if snapshot.pair != PAIR:
            raise ValueError("fixed_pair_identity_mismatch")
        # Local construction uses the fresh read immediately. Subsequent
        # allowance/override RPC calls may be slow but are pinned to this block.
        stage = "local_unsigned_build"
        sell_unsigned = build_unsigned_swap(
            snapshot=snapshot, wallet=PUBLIC_WALLET, token_in=WETH,
            amount_in_units=SELL_WETH_UNITS, slippage_bps=500, nonce=0,
            gas_limit=300_000, priority_fee_wei=1, maximum_fee_wei=2,
            deadline=int(time.time()) + 300,
        )
        buy_unsigned = build_unsigned_swap(
            snapshot=snapshot, wallet=PUBLIC_WALLET, token_in=USDC,
            amount_in_units=BUY_USDC_UNITS, slippage_bps=500, nonce=0,
            gas_limit=300_000, priority_fee_wei=1, maximum_fee_wei=2,
            deadline=int(time.time()) + 300,
        )
        stage = "actual_allowance"
        actual_weth_allowance = reader.allowance(owner=PUBLIC_WALLET, token_in=WETH,
                                                 snapshot=snapshot)
        actual_usdc_allowance = reader.allowance(owner=PUBLIC_WALLET, token_in=USDC,
                                                 snapshot=snapshot)
        buy_out, _ = snapshot.quote_buy(BUY_USDC_UNITS, 500)
        sell_out, sell_minimum = snapshot.quote_sell(SELL_WETH_UNITS, 500)
        evidence = {
            "chainId": "1", "pair": PAIR, "publicWallet": PUBLIC_WALLET,
            "blockHeight": snapshot.block_height, "blockHash": snapshot.block_hash,
            "reserveBaseUnits": str(snapshot.reserve_base),
            "reserveQuoteUnits": str(snapshot.reserve_quote),
            "spotQuotePerBase": str(snapshot.spot_quote_per_base),
            "buyQuoteInputUnits": str(BUY_USDC_UNITS),
            "buyBaseOutputUnits": str(buy_out),
            "sellBaseInputUnits": str(SELL_WETH_UNITS),
            "sellQuoteOutputUnits": str(sell_out),
            "sellMinimumQuoteUnits": str(sell_minimum),
            "actualWethAllowanceUnits": str(actual_weth_allowance),
            "actualUsdcAllowanceUnits": str(actual_usdc_allowance),
            "readElapsedMs": round((time.monotonic() - started) * 1000),
        }
        sell_override = weth9_state_override(
            wallet=PUBLIC_WALLET, balance_units=SYNTHETIC_WETH_UNITS,
            allowance_units=SYNTHETIC_WETH_UNITS,
        )
        buy_override = usdc_state_override(
            wallet=PUBLIC_WALLET, balance_units=SYNTHETIC_USDC_UNITS,
            allowance_units=SYNTHETIC_USDC_UNITS,
        )
        stage = "state_override_verification"
        for token, override, amount in ((WETH, sell_override, SYNTHETIC_WETH_UNITS),
                                        (USDC, buy_override, SYNTHETIC_USDC_UNITS)):
            verify_token_override(rpc, token=token, wallet=PUBLIC_WALLET,
                                  block_height=snapshot.block_height,
                                  override=override, expected_units=amount)
        directions = []
        stage = "two_direction_simulation"
        for name, unsigned, override, reserve_out, expected_out in (
            ("WETH->USDC", sell_unsigned, sell_override, snapshot.reserve_quote, sell_out),
            ("USDC->WETH", buy_unsigned, buy_override, snapshot.reserve_base, buy_out),
        ):
            success_out = simulate_direct_pair_swap(
                rpc, wallet=PUBLIC_WALLET, transaction=unsigned,
                snapshot=snapshot, override=override,
            )
            if success_out != expected_out or expected_out >= reserve_out:
                raise ValueError("v2_same_block_quote_simulation_mismatch")
            bad = _excessive_minimum(unsigned, reserve_out=reserve_out)
            rejected = False
            try:
                simulate_direct_pair_swap(rpc, wallet=PUBLIC_WALLET, transaction=bad,
                                          snapshot=snapshot, override=override)
            except RpcUnavailable:
                rejected = rpc.last_diagnostic == "rpc_method_unavailable"
            if not rejected:
                raise ValueError("excessive_minimum_not_rejected")
            # The transport masks RPC error payloads. The exact reason follows
            # from the verified Router02 minOut require and this reserve bound.
            if simulate_direct_pair_swap(rpc, wallet=PUBLIC_WALLET, transaction=unsigned,
                                         snapshot=snapshot, override=override) != success_out:
                raise ValueError("post_revert_positive_control_failed")
            directions.append({"direction": name, "blockHeight": snapshot.block_height,
                               "blockHash": snapshot.block_hash,
                               "successOutputUnits": str(success_out),
                               "successSimulated": True,
                               "excessiveMinOutRejected": True,
                               "expectedRevertReason": EXPECTED_MIN_OUT_REVERT,
                               "revertReasonProvenance": "Router02-source-derived; RPC payload masked"})
    except Exception as error:
        # Never serialize RPC exception text: URLs can contain credentials.
        safe_code = str(error) if isinstance(error, ValueError) and str(error).startswith((
            "v2_", "rpc_", "fixed_")) else None
        print(json.dumps({"readOnlyOk": evidence is not None, "simulationOk": False,
                          "tradingReady": False, "errorType": type(error).__name__,
                          "stage": stage, "errorCode": safe_code,
                          "evidence": evidence}, ensure_ascii=False))
        return 1
    print(json.dumps({"readOnlyOk": True, "simulationOk": True,
                      "tradingReady": False, "evidence": evidence,
                      "simulation": {"stateOverride": True, "directions": directions,
                                     "elapsedMs": round((time.monotonic() - started) * 1000)},
                      }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
