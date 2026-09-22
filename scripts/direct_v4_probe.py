"""Read-only V4 L0 local quote probe; never signs, broadcasts, or claims simulation."""

from __future__ import annotations

import json
import os
import time
import argparse
from pathlib import Path
from urllib.parse import urlparse

from curl_cffi import requests as cf
from curl_cffi.const import CurlOpt

from dotenv import load_dotenv

from fomo.execution.direct_v4 import (CHAIN_CONFIGS, MAINNET_PROBE_KEY, BASE_PROBE_KEY,
                                      UniswapV4PoolReader)
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.url_safety import validate_endpoint_url
from fomo.execution.v4_transaction import build_unsigned_swap, decode_unsigned_swap
from fomo.watching.rpc_transport import FailoverJsonRpc


class _BaseHttpDiagnostic:
    """Probe-only transport with secret-free HTTP and RPC failure metadata."""

    def __init__(self) -> None:
        self.method: str | None = None
        self.block_tagged = False
        self.http_status: int | None = None
        self.provider_error_code: int | None = None
        self.transport_error_type: str | None = None

    def request(self, endpoint: RpcEndpoint, method: str, params: list | tuple) -> object:
        self.method = method
        self.block_tagged = (method in {"eth_call", "eth_getCode", "eth_getBlockByNumber"}
                             and len(params) >= 2 and isinstance(params[1], str)
                             and params[1].startswith("0x")) or (
                                 method == "eth_getBlockByNumber" and bool(params)
                                 and isinstance(params[0], str) and params[0].startswith("0x"))
        self.http_status = None
        self.provider_error_code = None
        self.transport_error_type = None
        url, first = validate_endpoint_url(endpoint.resolved_http_url)
        _, second = validate_endpoint_url(endpoint.resolved_http_url)
        if first != second:
            raise ValueError("rpc_dns_rebinding_rejected")
        parsed = urlparse(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = sorted(address for address in second if ":" not in address)
        address = (addresses or sorted(second))[0]
        pinned = f"[{address}]" if ":" in address else address
        try:
            response = cf.post(
                url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
                headers={"Accept": "application/json"}, timeout=3.0,
                allow_redirects=False, proxy="",
                curl_options={CurlOpt.RESOLVE: [f"{parsed.hostname}:{port}:{pinned}"],
                              CurlOpt.PROXY: ""},
            )
        except cf.RequestsError as error:
            self.transport_error_type = type(error).__name__
            raise
        self.http_status = int(response.status_code)
        if response.primary_ip and response.primary_ip not in second:
            raise ValueError("rpc_connected_address_mismatch")
        if 300 <= response.status_code < 400:
            raise ValueError("rpc_redirect_rejected")
        if response.status_code != 200:
            raise ValueError("rpc_http_status_invalid")
        body = response.json()
        if not isinstance(body, dict) or body.get("id") != 1 or body.get("jsonrpc") != "2.0":
            raise ValueError("rpc_response_invalid")
        rpc_error = body.get("error")
        if isinstance(rpc_error, dict) and type(rpc_error.get("code")) is int:
            self.provider_error_code = rpc_error["code"]
        if rpc_error is not None or "result" not in body:
            raise ValueError("rpc_method_unavailable")
        return body["result"]

    def public(self, rpc: FailoverJsonRpc | None) -> dict:
        return {"method": self.method, "blockTagged": self.block_tagged,
                "httpStatus": self.http_status,
                "providerErrorCode": self.provider_error_code,
                "transportErrorType": self.transport_error_type,
                "rpcDiagnostic": getattr(rpc, "last_diagnostic", None)}


def main(argv: list[str] | None = None) -> int:
    args = argparse.ArgumentParser(description="V4 L0 read-only probe")
    args.add_argument("--chain", choices=("1", "8453"), default="1")
    chain = int(args.parse_args([] if argv is None else argv).chain)
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    rpc_env = "RPC_BASE_URL" if chain == 8453 else "RPC_ETHEREUM_URL"
    if not os.getenv(rpc_env, "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}, ensure_ascii=False))
        return 2
    started = time.monotonic()
    stage = "read_pool"
    evidence = None
    diagnostic = _BaseHttpDiagnostic() if chain == 8453 else None
    rpc = None
    reader = None
    try:
        rpc = FailoverJsonRpc(str(chain), [RpcEndpoint(str(chain), "direct-v4-l0", "primary",
                                                       http_env=rpc_env)],
                              **({"requester": diagnostic.request,
                                  "previous_header_fallback": True,
                                  "pin_health_during_view": True} if diagnostic else {}))
        key = MAINNET_PROBE_KEY if chain == 1 else BASE_PROBE_KEY
        reader = UniswapV4PoolReader(rpc=rpc, chain_id=chain, key=key)
        snapshot = reader.snapshot()
        read_ms = round((time.monotonic() - started) * 1000)
        stage = "local_quote_and_codec"
        rows = []
        for name, token, amount in (
            ("ETH->USDC", key.currency0, 10**15),
            ("USDC->ETH", key.currency1, 10**6),
        ):
            quote = snapshot.quote(token_in=token, amount_in=amount)
            transaction = build_unsigned_swap(
                chain_id=chain, key=snapshot.key, token_in=token,
                amount_in=amount, minimum_out=max(1, quote.amount_out * 95 // 100),
                deadline=int(time.time()) + 300,
            )
            decoded = decode_unsigned_swap(transaction, chain_id=chain)
            if (decoded["msgValue"] != (amount if token == snapshot.key.currency0 else 0)
                    or decoded["tokenOut"] == token):
                raise ValueError("v4_probe_codec_or_value_invalid")
            rows.append({"direction": name, "amountIn": str(amount),
                         "localAmountOut": str(quote.amount_out),
                         "msgValue": str(decoded["msgValue"]),
                         "outputCurrency": decoded["tokenOut"],
                         "codecRoundtrip": True,
                         "simulationVerified": False})
        evidence = {"chainId": chain, "block": snapshot.block_height,
                    "blockHash": snapshot.block_hash, "poolManager": CHAIN_CONFIGS[chain].pool_manager,
                    "headerDiagnostic": (reader.last_header_diagnostic if chain == 8453 else None),
                    "poolId": snapshot.pool_id, "poolKey": {
                        "currency0": snapshot.key.currency0, "currency1": snapshot.key.currency1,
                        "fee": snapshot.key.fee, "tickSpacing": snapshot.key.tick_spacing,
                        "hooks": snapshot.key.hooks,
                    }, "readElapsedMs": read_ms, "directions": rows}
        evidence["initialized"] = True
        evidence["slot0"] = {"sqrtPriceX96": str(snapshot.sqrt_price_x96),
                             "tick": snapshot.tick, "lpFee": snapshot.lp_fee,
                             "protocolFee": snapshot.protocol_fee}
        evidence["liquidity"] = str(snapshot.liquidity)
    except Exception as error:
        print(json.dumps({"readOnlyOk": evidence is not None, "simulationVerified": False,
                          "tradingReady": False, "stage": stage,
                          "errorType": type(error).__name__,
                          "reason": str(error) if str(error).startswith("v4_") else None,
                          "rpcFailure": diagnostic.public(rpc) if diagnostic else None,
                          "headerDiagnostic": (reader.last_header_diagnostic
                                               if reader is not None and chain == 8453 else None),
                          "totalElapsedMs": round((time.monotonic() - started) * 1000)},
                         ensure_ascii=False))
        return 1
    print(json.dumps({"readOnlyOk": True, "simulationVerified": False,
                      "tradingReady": False, "evidence": evidence,
                      "totalElapsedMs": round((time.monotonic() - started) * 1000)},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
