"""Read-only warm-process Pons benchmark. Never sign or broadcast."""

from __future__ import annotations

import argparse
import json
import math
import time

from fomo.execution.pons_v4_once import INITIALIZE_BLOCK
from scripts import pons_v4_swap_once as pons


def _percentile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--amount-in-wei", type=int, required=True)
    parser.add_argument("--slippage-bps", type=int, default=100)
    parser.add_argument("--runs", type=int, default=30)
    args = parser.parse_args(argv)
    if args.runs < 30:
        parser.error("at least 30 read-only runs are required")
    rpc = pons.PonsRpc(pons._rpc_url())
    try:
        cold_started = time.monotonic()
        if pons._quantity(rpc.call("eth_chainId", [])) != 4663:
            raise ValueError("pons_once_wrong_chain")
        header = pons._pinned_header(rpc)
        if pons._quantity(header["number"]) <= INITIALIZE_BLOCK:
            raise ValueError("pons_once_unconfirmed_pool")
        pons._verify_static_identity(rpc, hex(pons._quantity(header["number"])))
        rpc.wallet_profile = pons._wallet_profile()
        cold_ms = int((time.monotonic() - cold_started) * 1000)
        results = []
        for _ in range(args.runs):
            summary, _, _ = pons._prepare_once(native_in_wei=args.amount_in_wei,
                                                slippage_bps=args.slippage_bps,
                                                rpc=rpc, fast=True)
            if summary["simulation_success"] is not True:
                raise ValueError("pons_once_simulation_failed")
            results.append(summary)
        elapsed = [int(item["hotPathMs"]) for item in results]
        stages = {key: [int(item["stagesMs"][key]) for item in results]
                  for key in results[0]["stagesMs"]}
        print(json.dumps({
            "runs": len(results), "coldStartupMs": cold_ms,
            "hotPathMs": {"p50": _percentile(elapsed, .50),
                          "p95": _percentile(elapsed, .95),
                          "p99": _percentile(elapsed, .99), "max": max(elapsed)},
            "slowestStageByP95": max(stages, key=lambda key: _percentile(stages[key], .95)),
            "stageP95Ms": {key: _percentile(values, .95) for key, values in stages.items()},
            "httpRoundTripsMax": max(item["rpc"]["httpRoundTrips"] for item in results),
            "jsonRpcCallsMax": max(item["rpc"]["jsonRpcCalls"] for item in results),
            "simulationSuccess": True, "signed": False, "broadcast": False,
        }))
        return 0
    finally:
        rpc.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"simulationSuccess": False, "signed": False,
                          "broadcast": False, "errorType": type(error).__name__}))
        raise SystemExit(1) from None
