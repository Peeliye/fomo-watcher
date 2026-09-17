from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from fomo.intelligence.profile import WalletIntelligenceStore, load_ndjson


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill observation-only wallet intelligence")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--source", default="data/paper-orders.ndjson")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    cfg = yaml.safe_load((project / args.config).read_text(encoding="utf-8"))
    settings = cfg.get("smart_money", {})
    database = Path(str(settings.get("database", "data/wallet-intelligence.sqlite3")))
    if not database.is_absolute():
        database = project / database
    source = Path(args.source)
    if not source.is_absolute():
        source = project / source
    store = WalletIntelligenceStore(database, settings)
    try:
        inserted = store.ingest_order_rows(load_ndjson(source))
        snapshot = store.snapshot(1)
    finally:
        store.close()
    print(json.dumps({"inserted": inserted, "totalEvents": snapshot["totalEvents"], "profiled": snapshot["profiled"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
