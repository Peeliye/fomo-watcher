"""Idempotently backfill the SQLite paper portfolio from existing audit logs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from fomo.app import feed_events
from fomo.portfolio.ledger import PortfolioLedger


def _rows(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    yield value
            except json.JSONDecodeError:
                continue


def backfill(project_dir: Path, database: Path, timezone_name: str, account_id: str) -> dict[str, int]:
    decisions = {
        str(row.get("eventId")): row
        for row in _rows(project_dir / "data" / "paper-orders.ndjson")
        if row.get("status") == "accepted" and row.get("eventId")
    }
    watched_positions = {
        (str(row.get("handle") or "").lower(), int(row.get("networkId") or 0), str(row.get("ca") or "").lower())
        for row in decisions.values()
    }
    ledger = PortfolioLedger(database, timezone_name, account_id)
    counts = {"acceptedDecisions": len(decisions), "eventsMatched": 0, "fillsCreated": 0, "duplicates": 0, "missingPrice": 0}
    try:
        for envelope in _rows(project_dir / "data" / "ws-events.ndjson"):
            payload = envelope.get("payload") or {}
            for event in feed_events([payload]):
                key = (event.handle.lower(), int(event.network_id), event.ca.lower())
                decision = decisions.get(event.id) if event.kind == "buy" else None
                if decision is None and not (event.kind in {"sell", "clear"} and key in watched_positions):
                    continue
                counts["eventsMatched"] += 1
                result = ledger.apply_event(event, decision)
                if result and result.get("status") == "filled":
                    counts["fillsCreated"] += 1
                elif result and result.get("status") == "duplicate":
                    counts["duplicates"] += 1
                elif result and result.get("status") == "needs_price":
                    counts["missingPrice"] += 1
    finally:
        ledger.close()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill paper portfolio from append-only logs")
    parser.add_argument("--project", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--database", type=Path, default=Path("data/portfolio.sqlite3"))
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--account-id", default="paper-main")
    args = parser.parse_args()
    project = args.project.resolve()
    database = args.database if args.database.is_absolute() else project / args.database
    print(json.dumps(backfill(project, database, args.timezone, args.account_id), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
