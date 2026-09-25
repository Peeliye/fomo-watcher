"""Robinhood V4 PoolKey discovery only; no quote, simulation, or transaction."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.v4_discovery import discover_pool
from fomo.watching.rpc_transport import FailoverJsonRpc
from scripts._robinhood_probe_transport import (RobinhoodDiscoveryRpc,
                                                proxy_configured)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Robinhood V4 PoolKey discovery")
    parser.add_argument("--chain", choices=("4663",), default="4663")
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--created-at", help="UTC timestamp hint; identity still comes from RPC logs")
    options = parser.parse_args([] if argv is None else argv)
    chain = options.chain
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    if not os.getenv("RPC_ROBINHOOD_URL", "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}, ensure_ascii=False))
        return 2
    started = time.monotonic()
    rpc = None
    try:
        endpoint = RpcEndpoint(chain, "direct-v4-discovery-l0", "primary",
                               http_env="RPC_ROBINHOOD_URL")
        rpc = (RobinhoodDiscoveryRpc(endpoint) if proxy_configured() else
               FailoverJsonRpc(chain, [endpoint]))
        if int(str(rpc.call("eth_chainId", [])), 16) != 4663:
            raise ValueError("v4_discovery_chain_id_invalid")
        hint = (int(datetime.fromisoformat(options.created_at.replace("Z", "+00:00"))
                    .astimezone(timezone.utc).timestamp()) if options.created_at else None)
        found = discover_pool(rpc, chain_id=int(chain), pool_id=options.pool_id.lower(),
                              token=options.token.lower(), timestamp_hint=hint)
        print(json.dumps({
            "chainId": int(chain), "block": found.block_height,
            "blockHash": found.block_hash,
            "poolKey": {"currency0": found.key.currency0,
                        "currency1": found.key.currency1, "fee": found.key.fee,
                        "tickSpacing": found.key.tick_spacing,
                        "hooks": found.key.hooks},
            "candidatePoolId": found.pool_id,
            "computedPoolId": found.key.pool_id,
            "identityVerified": found.identity_verified,
            "quoteRejected": True,
            "rejectReason": found.reject_reason or "v4_discovery_only_no_quote",
            "elapsedMs": round((time.monotonic() - started) * 1000),
            "tradingReady": False,
        }))
        return 0 if found.identity_verified else 1
    except Exception as error:
        print(json.dumps({
            "chainId": int(chain), "stage": "pool_discovery",
            "errorType": type(error).__name__,
            "reason": str(error) if str(error).startswith("v4_") else None,
            "method": getattr(rpc, "last_request_method", None),
            "httpStatus": getattr(rpc, "last_http_status", None),
            "transportErrorType": getattr(rpc, "last_transport_error_type", None),
            "elapsedMs": round((time.monotonic() - started) * 1000),
            "tradingReady": False,
        }))
        return 1


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
