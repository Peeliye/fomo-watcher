"""Read-only independent-price, balance and fee-method probe; never broadcasts."""

from __future__ import annotations

import argparse
import json
import re

from fomo.execution.market_evidence import (EvmMarketEvidenceProvider, PythHermesPriceAdapter,
                                            SolanaMarketEvidenceProvider, approved_feed_id)
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.watching.rpc_transport import FailoverJsonRpc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain-id", choices=("1", "1399811149"), required=True)
    parser.add_argument("--wallet", required=True, help="public wallet address; never supply a private key")
    parser.add_argument("--token-in", required=True)
    parser.add_argument("--token-decimals", type=int, default=18)
    parser.add_argument("--rpc-primary-env", required=True, help="environment variable NAME, not URL")
    parser.add_argument("--rpc-backup-env", help="optional environment variable NAME, not URL")
    args = parser.parse_args()
    names = [args.rpc_primary_env, *([args.rpc_backup_env] if args.rpc_backup_env else [])]
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in names):
        parser.error("RPC arguments must be environment variable names")
    endpoints = [RpcEndpoint(args.chain_id, f"probe-{index}", "primary" if index == 1 else "backup",
                             http_env=name, priority=index)
                 for index, name in enumerate(names, 1)]
    rpc = FailoverJsonRpc(args.chain_id, endpoints)
    asset = "SOL" if args.chain_id == "1399811149" else "ETH"
    price = PythHermesPriceAdapter(chain_id=args.chain_id, asset=asset,
                                   feed_id=approved_feed_id(asset))
    if args.chain_id == "1399811149":
        provider = SolanaMarketEvidenceProvider(wallet=args.wallet, token_in=args.token_in,
                                                price_adapter=price, rpc=rpc)
    else:
        provider = EvmMarketEvidenceProvider(chain_id=args.chain_id, wallet=args.wallet,
                                             token_in=args.token_in, token_decimals=args.token_decimals,
                                             price_adapter=price, rpc=rpc)
    status = provider.self_check()
    print(json.dumps({"chainId": args.chain_id, "ready": status.ready,
                      "reason": status.reason, "evidence": status.evidence,
                      "broadcastAttempted": False}, ensure_ascii=False))
    return 0 if status.ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
