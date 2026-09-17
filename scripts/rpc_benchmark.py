"""Probe configured RPC endpoints and persist latency/height samples.

This command never prints or stores endpoint URLs:
    python -m scripts.rpc_benchmark --samples 5
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from dotenv import load_dotenv

from fomo.execution.rpc_pool import run_rpc_probe_cycle


PROJECT_DIR = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark configured RPC primary/backup endpoints")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args()
    load_dotenv(PROJECT_DIR / ".env")
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = PROJECT_DIR / config_path
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    snapshot = run_rpc_probe_cycle(PROJECT_DIR, cfg, args.samples, args.timeout)
    if not snapshot["configured"]:
        print("No configured RPC endpoints. Fill the RPC_* variables in .env first.")
        return 2
    for item in snapshot["endpoints"]:
        if item["httpConfigured"]:
            latency = "—" if item["p95LatencyMs"] is None else f'{item["p95LatencyMs"]:.1f}ms'
            print(f'{item["chainId"]} {item["provider"]}/{item["role"]}: {item["status"]}, p95={latency}, lag={item["blockLag"]}')
    return 0 if snapshot["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
