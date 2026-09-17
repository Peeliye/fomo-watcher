"""Non-destructive maintenance commands for the persistent portfolio ledger."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from fomo.portfolio.ledger import backup_database, portfolio_snapshot


def check_database(path: Path, account_id: str) -> dict[str, object]:
    if not path.exists():
        return {"ok": False, "database": str(path), "error": "database_not_found"}
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
    try:
        integrity = str(db.execute("PRAGMA integrity_check").fetchone()[0])
        counts = {
            table: int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("portfolio_events", "portfolio_fills", "portfolio_positions", "portfolio_daily")
        }
    finally:
        db.close()
    summary = portfolio_snapshot(path, account_id, limit=1)
    return {
        "ok": integrity == "ok", "database": str(path), "integrity": integrity,
        "counts": counts, "accountId": account_id,
        "openPositions": summary["openPositions"], "totalPnlUsd": summary["totalPnlUsd"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check or back up the Fomo portfolio SQLite ledger")
    parser.add_argument("command", choices=("check", "backup"))
    parser.add_argument("--database", default="data/portfolio.sqlite3")
    parser.add_argument("--account-id", default="paper-main")
    parser.add_argument("--destination")
    args = parser.parse_args()
    source = Path(args.database).resolve()
    if args.command == "check":
        print(json.dumps(check_database(source, args.account_id), ensure_ascii=False, indent=2))
        return
    destination = Path(args.destination).resolve() if args.destination else source.parent / "backups" / f"portfolio-{datetime.now():%Y%m%d-%H%M%S}.sqlite3"
    backup_database(source, destination)
    result = check_database(destination, args.account_id)
    result["backup"] = str(destination)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
