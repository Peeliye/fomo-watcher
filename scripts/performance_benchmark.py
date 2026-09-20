"""Deterministic 100k-row benchmark for verified performance materialization."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
import tracemalloc
from pathlib import Path

from fomo.intelligence.performance import VerifiedPerformanceStore


def run_benchmark(rows: int = 100_000) -> dict[str, float | int]:
    rows = max(1_000, int(rows))
    with tempfile.TemporaryDirectory() as directory:
        store = VerifiedPerformanceStore(
            Path(directory) / "performance.sqlite3",
            {"maximum_data_age_seconds": 999_999_999},
        )
        tracemalloc.start()
        try:
            fills = []
            for index in range(rows):
                token_index = (index // 2) % 1_000
                side = "buy" if index % 2 == 0 else "sell"
                fills.append({
                    "txHash": f"tx-{index}", "instructionIndex": 0,
                    "kolId": f"kol-{token_index % 100}", "handle": f"kol{token_index % 100}",
                    "wallet": f"0x{token_index % 100:040x}", "chainId": 1,
                    "tokenAddress": f"0x{token_index:040x}", "symbol": "BENCH", "side": side,
                    "tokenQuantity": 1, "grossUsd": 10 if side == "buy" else 11,
                    "executedAt": f"2026-09-{1 + (index % 20):02d}T00:{index % 60:02d}:00Z",
                    "sourceConfidence": 1,
                })
            started = time.perf_counter()
            inserted_fills = store.ingest_fills(fills)
            fill_seconds = time.perf_counter() - started
            del fills

            markets = []
            for index in range(rows):
                token_index = index % 1_000
                markets.append({
                    "chainId": 1, "tokenAddress": f"0x{token_index:040x}",
                    "observedAt": f"2026-09-{1 + (index % 20):02d}T01:{index % 60:02d}:{index % 60:02d}Z",
                    "priceUsd": "11", "source": f"benchmark-{index // 1_000}",
                })
            started = time.perf_counter()
            inserted_markets = store.ingest_market_history(markets)
            market_seconds = time.perf_counter() - started
            del markets

            started = time.perf_counter()
            store.ingest_market_history([{
                "chainId": 1, "tokenAddress": f"0x{0:040x}", "observedAt": "2026-09-21T00:00:00Z",
                "priceUsd": "12", "source": "benchmark-incremental",
            }])
            incremental_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            snapshot = store.snapshot(200)
            snapshot_ms = (time.perf_counter() - started) * 1000
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
            store.close()
    return {
        "requestedRowsPerTable": rows,
        "insertedFills": inserted_fills,
        "insertedMarketObservations": inserted_markets,
        "fillImportSeconds": round(fill_seconds, 3),
        "marketImportSeconds": round(market_seconds, 3),
        "incrementalUpdateMs": round(incremental_ms, 3),
        "snapshotMs": round(snapshot_ms, 3),
        "snapshotProfiles": len(snapshot["profiles"]),
        "peakMemoryMiB": round(peak / 1024 / 1024, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(args.rows), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
