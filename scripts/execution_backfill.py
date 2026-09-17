"""Idempotently backfill the execution intent journal from risk decisions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

from fomo.execution.journal import ExecutionJournal, execution_snapshot


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/risk-decisions.ndjson")
    parser.add_argument("--database", default="data/execution.sqlite3")
    parser.add_argument("--account-id", default="paper-main")
    args = parser.parse_args()
    journal = ExecutionJournal(args.database, args.account_id)
    inserted = duplicates = invalid = 0
    source = Path(args.source)
    if source.exists():
        for line in source.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
                event = SimpleNamespace(
                    id=row.get("eventId", ""), user_id=row.get("kolId", ""), handle=row.get("handle", ""),
                    network_id=row.get("networkId", 0), ca=row.get("ca", ""), symbol=row.get("symbol", "UNKNOWN"),
                    kind=row.get("side", "unknown"), amount_usd=row.get("estimatedUsd", 0),
                )
                result = journal.record_risk_decision(event, row)
                duplicates += int(bool(result and result["duplicate"]))
                inserted += int(bool(result and not result["duplicate"]))
            except (json.JSONDecodeError, TypeError, ValueError):
                invalid += 1
    journal.close()
    snapshot = execution_snapshot(args.database, limit=1)
    print(json.dumps({"inserted": inserted, "duplicates": duplicates, "invalid": invalid, "total": snapshot["total"], "states": snapshot["states"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
