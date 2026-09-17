from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from fomo.intelligence.performance import VerifiedPerformanceStore


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else list(value.get("rows", []))
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                rows.append(value)
        except json.JSONDecodeError:
            continue
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Import transaction-proven fills and historical market observations")
    parser.add_argument("kind", choices=("fills", "market", "social"))
    parser.add_argument("source")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    cfg = yaml.safe_load((project / args.config).read_text(encoding="utf-8"))
    target = Path(str(cfg.get("verified_performance", {}).get("database", "data/verified-performance.sqlite3")))
    if not target.is_absolute():
        target = project / target
    source = Path(args.source)
    if not source.is_absolute():
        source = project / source
    store = VerifiedPerformanceStore(target)
    try:
        rows = load_rows(source)
        inserted = {
            "fills": store.ingest_fills,
            "market": store.ingest_market_history,
            "social": store.ingest_social_identities,
        }[args.kind](rows)
        snapshot = store.snapshot(1)
    finally:
        store.close()
    print(json.dumps({"kind": args.kind, "read": len(rows), "inserted": inserted, **{k: snapshot[k] for k in ("verifiedFills", "marketObservations", "profiled", "verifiedProfiles")}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
