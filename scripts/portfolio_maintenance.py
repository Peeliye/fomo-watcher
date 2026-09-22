"""Non-destructive maintenance commands for the persistent portfolio ledger."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from fomo.portfolio.ledger import backup_database, portfolio_snapshot


def _accepted_paper_event_ids(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    accepted: set[str] = set()
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            event_id = str(row.get("eventId") or "").strip() if isinstance(row, dict) else ""
            side = str(row.get("side") or "").casefold() if isinstance(row, dict) else ""
            source_type = str(row.get("sourceType") or "").casefold() if isinstance(row, dict) else ""
            if (event_id and row.get("status") == "accepted" and side != "sell"
                    and source_type not in {"swap_sell", "single_user_sell"}):
                accepted.add(event_id)
    return accepted


def reconciliation_report(
    path: Path,
    account_id: str,
    timezone_name: str = "Asia/Shanghai",
    paper_orders_path: Path | None = None,
) -> dict[str, object]:
    """Check event/fill/position/daily invariants without changing the ledger."""

    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        missing_fill_rows = db.execute(
            """SELECT e.event_id,e.kind,e.reason FROM portfolio_events e
               LEFT JOIN portfolio_fills f ON f.event_id=e.event_id
               WHERE e.status='filled' AND f.event_id IS NULL ORDER BY e.event_time"""
        ).fetchall()
        orphan_fills = int(db.execute(
            """SELECT COUNT(*) FROM portfolio_fills f LEFT JOIN portfolio_events e
               ON e.event_id=f.event_id WHERE e.event_id IS NULL"""
        ).fetchone()[0])
        fill_counts = db.execute(
            "SELECT event_id,COUNT(*) AS count FROM portfolio_fills GROUP BY event_id HAVING count<>1"
        ).fetchall()
        expected: dict[tuple[str, str, int, str], tuple[Decimal, int]] = {}
        for row in db.execute(
            """SELECT account_id,kol_id,chain_id,token_address,side,quantity,realized_pnl_micros
               FROM portfolio_fills WHERE account_id=? ORDER BY executed_at,fill_id""",
            (account_id,),
        ):
            key = (str(row["account_id"]), str(row["kol_id"]), int(row["chain_id"]), str(row["token_address"]))
            quantity, realized = expected.get(key, (Decimal(0), 0))
            delta = Decimal(str(row["quantity"]))
            expected[key] = (
                quantity + delta if row["side"] == "buy" else quantity - delta,
                realized + int(row["realized_pnl_micros"]),
            )
        position_mismatches = 0
        seen: set[tuple[str, str, int, str]] = set()
        for row in db.execute("SELECT * FROM portfolio_positions WHERE account_id=?", (account_id,)):
            key = (str(row["account_id"]), str(row["kol_id"]), int(row["chain_id"]), str(row["token_address"]))
            seen.add(key)
            quantity, realized = expected.get(key, (Decimal(0), 0))
            if abs(quantity - Decimal(str(row["quantity"]))) > Decimal("0.000000000001"):
                position_mismatches += 1
            elif realized != int(row["realized_pnl_micros"]):
                position_mismatches += 1
        position_mismatches += sum(1 for key, (quantity, _) in expected.items() if key not in seen and quantity > 0)

        tz = ZoneInfo(timezone_name)
        daily_realized: dict[str, int] = {}
        for row in db.execute(
            "SELECT executed_at,realized_pnl_micros FROM portfolio_fills WHERE account_id=?", (account_id,)
        ):
            parsed = datetime.fromisoformat(str(row["executed_at"]).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            day = parsed.astimezone(tz).strftime("%Y-%m-%d")
            daily_realized[day] = daily_realized.get(day, 0) + int(row["realized_pnl_micros"])
        stored_daily = {
            str(row["local_day"]): int(row["realized_pnl_micros"])
            for row in db.execute("SELECT local_day,realized_pnl_micros FROM portfolio_daily WHERE account_id=?", (account_id,))
        }
        daily_mismatches = sum(
            1 for day in set(daily_realized) | set(stored_daily)
            if daily_realized.get(day, 0) != stored_daily.get(day, 0)
        )
        accepted_ids = _accepted_paper_event_ids(paper_orders_path)
        accepted_failures: list[dict[str, str]] = []
        for event_id in sorted(accepted_ids):
            row = db.execute(
                """SELECT e.status,e.reason,f.side FROM portfolio_events e
                   LEFT JOIN portfolio_fills f ON f.event_id=e.event_id AND f.account_id=?
                   WHERE e.event_id=?""",
                (account_id, event_id),
            ).fetchone()
            if row is None:
                accepted_failures.append({"eventId": event_id, "reason": "missing_portfolio_event"})
            elif row["side"] == "buy":
                continue
            elif row["status"] in {"ignored", "needs_price", "failed", "legacy_observed"} and row["reason"]:
                continue
            else:
                accepted_failures.append({
                    "eventId": event_id,
                    "reason": f"status={row['status']};reason={row['reason'] or 'missing'}",
                })
    finally:
        db.close()
    return {
        "filledEventsWithoutFill": len(missing_fill_rows),
        "legacyCandidates": [
            {"eventId": str(row["event_id"]), "kind": str(row["kind"]), "reason": str(row["reason"])}
            for row in missing_fill_rows
        ],
        "fillsWithoutEvent": orphan_fills,
        "eventsWithNonUniqueFillCount": len(fill_counts),
        "positionMismatches": position_mismatches,
        "dailyRealizedMismatches": daily_mismatches,
        "acceptedPaperBuysChecked": len(accepted_ids),
        "acceptedPaperBuysWithoutFillOrFailure": accepted_failures,
        "ok": not missing_fill_rows and not orphan_fills and not fill_counts and not position_mismatches
        and not daily_mismatches and not accepted_failures,
    }


def reconcile_legacy_events(path: Path, account_id: str, *, apply: bool = False) -> dict[str, object]:
    """Downgrade unreconstructable legacy events; never synthesize fills."""

    before = reconciliation_report(path, account_id)
    candidates_value = before["legacyCandidates"]
    candidates = candidates_value if isinstance(candidates_value, list) else []
    result: dict[str, object] = {"mode": "apply" if apply else "dry-run", "planned": len(candidates), "changed": 0}
    if not apply or not candidates:
        result["before"] = before
        return result
    destination = path.parent / "backups" / f"portfolio-pre-reconcile-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.sqlite3"
    backup_database(path, destination)
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        changed = db.execute(
            """UPDATE portfolio_events SET status='legacy_observed',reason='missing_legacy_fill'
               WHERE status='filled' AND NOT EXISTS(
                 SELECT 1 FROM portfolio_fills f WHERE f.event_id=portfolio_events.event_id
               )"""
        ).rowcount
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    result.update({"changed": changed, "backup": str(destination), "after": reconciliation_report(path, account_id)})
    return result


def check_database(path: Path, account_id: str, paper_orders_path: Path | None = None) -> dict[str, object]:
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
    reconciliation = reconciliation_report(path, account_id, paper_orders_path=paper_orders_path)
    return {
        "ok": integrity == "ok" and bool(reconciliation["ok"]), "database": str(path), "integrity": integrity,
        "counts": counts, "accountId": account_id,
        "openPositions": summary["openPositions"], "totalPnlUsd": summary["totalPnlUsd"],
        "reconciliation": reconciliation,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check, reconcile, or back up the Fomo portfolio SQLite ledger")
    parser.add_argument("command", choices=("check", "backup", "reconcile"))
    parser.add_argument("--database", default="data/portfolio.sqlite3")
    parser.add_argument("--account-id", default="paper-main")
    parser.add_argument("--destination")
    parser.add_argument("--paper-orders", help="optional NDJSON paper decisions to reconcile against fills")
    parser.add_argument("--apply", action="store_true", help="apply reconcile changes; default is dry-run")
    args = parser.parse_args()
    source = Path(args.database).resolve()
    paper_orders = Path(args.paper_orders).resolve() if args.paper_orders else None
    if args.command == "check":
        print(json.dumps(check_database(source, args.account_id, paper_orders), ensure_ascii=False, indent=2))
        return
    if args.command == "reconcile":
        print(json.dumps(reconcile_legacy_events(source, args.account_id, apply=args.apply), ensure_ascii=False, indent=2))
        return
    destination = Path(args.destination).resolve() if args.destination else source.parent / "backups" / f"portfolio-{datetime.now():%Y%m%d-%H%M%S}.sqlite3"
    backup_database(source, destination)
    result = check_database(destination, args.account_id, paper_orders)
    result["backup"] = str(destination)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
