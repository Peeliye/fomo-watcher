"""Operator-only Pons V4 arming switch. This command never signs or broadcasts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fomo.execution.journal import migrate_execution_database
from fomo.execution.pons_v4_once import CHAIN_ID, ROUTE_ID, TOKEN_OUT
from fomo.execution.rh_auto_executor import ROUTE_SCOPE as AUTO_ROUTE_SCOPE

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_DIR / "data" / "execution.sqlite3"


def _connect(path: Path, *, readonly: bool) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError("execution_database_missing")
    mode = "ro" if readonly else "rw"
    db = sqlite3.connect(path.resolve().as_uri() + f"?mode={mode}", uri=True, timeout=5)
    db.execute("PRAGMA busy_timeout=5000")
    if readonly:
        db.execute("PRAGMA query_only=ON")
    return db


def _require_v8(db: sqlite3.Connection) -> None:
    if int(db.execute("PRAGMA user_version").fetchone()[0]) != 8:
        raise ValueError("execution_control_migration_required")
    sql = db.execute("SELECT sql FROM sqlite_master WHERE name='execution_control'").fetchone()
    if sql is None or "CHECK(live_armed IN (0,1))" not in str(sql[0]):
        raise ValueError("execution_control_schema_invalid")


def status(path: Path = DEFAULT_DB) -> dict[str, Any]:
    with closing(_connect(path, readonly=True)) as db:
        version = int(db.execute("PRAGMA user_version").fetchone()[0])
        control = db.execute(
            "SELECT live_armed,circuit_breaker_tripped,breaker_reason,consecutive_failures,updated_at "
            "FROM execution_control WHERE singleton=1"
        ).fetchone()
        if control is None:
            raise ValueError("execution_control_missing")
        scope = None
        if version == 8:
            _require_v8(db)
            scope = db.execute(
                "SELECT chain_id,token_out,native_in_wei,route,confirmed_at "
                "FROM execution_operator_scope WHERE singleton=1"
            ).fetchone()
    return {
        "schemaVersion": version,
        "liveArmed": control[0] == 1,
        "circuitBreakerTripped": control[1] == 1,
        "breakerReason": control[2],
        "consecutiveFailures": int(control[3]),
        "updatedAt": control[4],
        "broadcastParameters": {
            "chainId": int(scope[0]) if scope else CHAIN_ID,
            "tokenOut": str(scope[1]) if scope else TOKEN_OUT,
            "nativeInWei": str(scope[2]) if scope else None,
            "route": str(scope[3]) if scope else ROUTE_ID,
            "scopeConfirmedAt": str(scope[4]) if scope else None,
            "serviceAllowBroadcast": "runtime_flag_required_not_stored",
            "tradeIdRequired": False,
        },
    }


def confirmation_text(native_in_wei: int) -> str:
    return f"ARM {CHAIN_ID} {TOKEN_OUT} {native_in_wei} {ROUTE_ID}"


def auto_confirmation_text(native_in_wei: int) -> str:
    return f"ARM AUTO {CHAIN_ID} ANY_SUPPORTED_CA {native_in_wei} {AUTO_ROUTE_SCOPE}"


def arm_auto(path: Path, *, chain_id: int, native_in_wei: int,
             route: str, confirmation: str) -> None:
    """Operator scope for reviewed single-pool routes only; never broadcasts."""
    if (chain_id != CHAIN_ID or type(native_in_wei) is not int
            or not 0 < native_in_wei < 1 << 128 or route != AUTO_ROUTE_SCOPE
            or confirmation != auto_confirmation_text(native_in_wei)):
        raise ValueError("execution_auto_arm_confirmation_invalid")
    with closing(_connect(path, readonly=False)) as db:
        _require_v8(db)
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                "SELECT live_armed,circuit_breaker_tripped,breaker_reason,consecutive_failures "
                "FROM execution_control WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != 0 or row[3] != 0:
                raise ValueError("execution_auto_arm_control_invalid")
            if row[1] not in (0, 1) or (row[1] == 1 and row[2] != "startup_read_only"):
                raise ValueError("execution_auto_arm_active_breaker")
            now = datetime.now(timezone.utc).isoformat()
            db.execute(
                "INSERT INTO execution_operator_scope VALUES(1,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET chain_id=excluded.chain_id,"
                "token_out=excluded.token_out,native_in_wei=excluded.native_in_wei,"
                "route=excluded.route,confirmed_at=excluded.confirmed_at",
                (CHAIN_ID, "*", str(native_in_wei), AUTO_ROUTE_SCOPE, now),
            )
            if db.execute(
                "UPDATE execution_control SET live_armed=1,circuit_breaker_tripped=0,"
                "breaker_reason='operator_armed',updated_at=? WHERE singleton=1 "
                "AND live_armed=0 AND circuit_breaker_tripped=? AND breaker_reason=? "
                "AND consecutive_failures=0", (now, row[1], row[2]),
            ).rowcount != 1:
                raise ValueError("execution_auto_arm_control_changed")
            db.commit()
        except BaseException:
            db.rollback()
            raise


def arm(path: Path, *, chain_id: int, token_out: str,
        native_in_wei: int, route: str, confirmation: str) -> None:
    if (chain_id != CHAIN_ID or token_out.lower() != TOKEN_OUT
            or type(native_in_wei) is not int or not 0 < native_in_wei < 1 << 128
            or route != ROUTE_ID or confirmation != confirmation_text(native_in_wei)):
        raise ValueError("execution_arm_confirmation_invalid")
    with closing(_connect(path, readonly=False)) as db:
        _require_v8(db)
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                "SELECT live_armed,circuit_breaker_tripped,breaker_reason,consecutive_failures "
                "FROM execution_control WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != 0:
                raise ValueError("execution_arm_requires_disarmed_control")
            if row[3] != 0:
                raise ValueError("execution_arm_active_failure_history")
            if row[1] not in (0, 1):
                raise ValueError("execution_arm_control_invalid")
            if row[1] == 1 and row[2] != "startup_read_only":
                raise ValueError("execution_arm_active_circuit_breaker")
            now = datetime.now(timezone.utc).isoformat()
            db.execute(
                "INSERT INTO execution_operator_scope VALUES(1,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET chain_id=excluded.chain_id, "
                "token_out=excluded.token_out,native_in_wei=excluded.native_in_wei, "
                "route=excluded.route,confirmed_at=excluded.confirmed_at",
                (CHAIN_ID, TOKEN_OUT, str(native_in_wei), ROUTE_ID, now),
            )
            if db.execute(
                "UPDATE execution_control SET live_armed=1,circuit_breaker_tripped=0,"
                "breaker_reason='operator_armed',updated_at=? "
                "WHERE singleton=1 AND live_armed=0 AND circuit_breaker_tripped=? "
                "AND breaker_reason=? AND consecutive_failures=0",
                (now, row[1], row[2]),
            ).rowcount != 1:
                raise ValueError("execution_arm_control_changed")
            db.commit()
        except BaseException:
            db.rollback()
            raise


def disarm(path: Path = DEFAULT_DB) -> None:
    with closing(_connect(path, readonly=False)) as db:
        _require_v8(db)
        db.execute("BEGIN IMMEDIATE")
        try:
            now = datetime.now(timezone.utc).isoformat()
            if db.execute("UPDATE execution_control SET live_armed=0,updated_at=? "
                          "WHERE singleton=1", (now,)).rowcount != 1:
                raise ValueError("execution_control_missing")
            db.commit()
        except BaseException:
            db.rollback()
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("status", help="read-only control and confirmed scope")
    sub.add_parser("migrate", help="SQLite backup API, integrity checks, transactional v8 migration")
    arm_parser = sub.add_parser("arm", help="explicitly arm only the pinned Pons route")
    arm_parser.add_argument("--chain-id", type=int, required=True)
    arm_parser.add_argument("--token-out", required=True)
    arm_parser.add_argument("--native-in-wei", type=int, required=True)
    arm_parser.add_argument("--route", required=True)
    arm_parser.add_argument("--confirm", required=True,
                            help="type: ARM 4663 <target-ca> <native-in-wei> pons-v4-single-pool")
    auto_parser = sub.add_parser("arm-auto", help="reviewed RH single-pool routes, any supported CA")
    auto_parser.add_argument("--chain-id", type=int, required=True)
    auto_parser.add_argument("--native-in-wei", type=int, required=True)
    auto_parser.add_argument("--route", required=True)
    auto_parser.add_argument("--confirm", required=True)
    sub.add_parser("disarm", help="switch live_armed back to zero")
    args = parser.parse_args()
    try:
        if args.action == "migrate":
            backup = migrate_execution_database(args.database)
            output: dict[str, Any] = {"migration": "v8", "backup": str(backup) if backup else None}
        elif args.action == "arm":
            arm(args.database, chain_id=args.chain_id, token_out=args.token_out,
                native_in_wei=args.native_in_wei, route=args.route,
                confirmation=args.confirm)
            output = status(args.database)
        elif args.action == "arm-auto":
            arm_auto(args.database, chain_id=args.chain_id,
                     native_in_wei=args.native_in_wei, route=args.route,
                     confirmation=args.confirm)
            output = status(args.database)
        elif args.action == "disarm":
            disarm(args.database)
            output = status(args.database)
        else:
            output = status(args.database)
        label = ("repo:data/execution.sqlite3" if args.database.resolve() == DEFAULT_DB
                 else "custom:sha256:" + hashlib.sha256(str(args.database.resolve()).encode()).hexdigest()[:12])
        print(json.dumps({"database": label, **output}, ensure_ascii=False))
        return 0
    except (OSError, sqlite3.Error, ValueError) as error:
        code = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(json.dumps({"status": "error", "reason": code}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
