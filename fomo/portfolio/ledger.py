"""Durable paper/live portfolio ledger with no signing capability.

Money is stored as integer USD micros and token quantities as decimal strings.
SQLite WAL mode keeps watcher writes and dashboard reads independent.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..risk.engine import ExposureSnapshot


MICROS = Decimal("1000000")


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value or 0))
        return result if result.is_finite() else Decimal("0")
    except Exception:
        return Decimal("0")


def _micros(value: Any) -> int:
    return int((_decimal(value) * MICROS).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _usd(value: int | None) -> float:
    return float(Decimal(int(value or 0)) / MICROS)


def _event_time(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_events (
  event_id TEXT PRIMARY KEY,
  received_at TEXT NOT NULL,
  event_time TEXT NOT NULL,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS portfolio_fills (
  fill_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  account_id TEXT NOT NULL,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  quantity TEXT NOT NULL,
  price_usd TEXT NOT NULL,
  gross_usd_micros INTEGER NOT NULL,
  cost_usd_micros INTEGER NOT NULL,
  realized_pnl_micros INTEGER NOT NULL,
  fee_usd_micros INTEGER NOT NULL DEFAULT 0,
  executed_at TEXT NOT NULL,
  mode TEXT NOT NULL,
  FOREIGN KEY(event_id) REFERENCES portfolio_events(event_id)
);
CREATE TABLE IF NOT EXISTS portfolio_positions (
  account_id TEXT NOT NULL,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  quantity TEXT NOT NULL,
  cost_basis_usd_micros INTEGER NOT NULL,
  realized_pnl_micros INTEGER NOT NULL DEFAULT 0,
  first_bought_at TEXT NOT NULL,
  last_trade_at TEXT NOT NULL,
  last_price_usd TEXT NOT NULL,
  last_mark_at TEXT NOT NULL,
  status TEXT NOT NULL,
  PRIMARY KEY(account_id, kol_id, chain_id, token_address)
);
CREATE TABLE IF NOT EXISTS portfolio_daily (
  local_day TEXT NOT NULL,
  account_id TEXT NOT NULL,
  buy_usd_micros INTEGER NOT NULL DEFAULT 0,
  sell_usd_micros INTEGER NOT NULL DEFAULT 0,
  realized_pnl_micros INTEGER NOT NULL DEFAULT 0,
  fee_usd_micros INTEGER NOT NULL DEFAULT 0,
  trades INTEGER NOT NULL DEFAULT 0,
  unrealized_pnl_micros INTEGER NOT NULL DEFAULT 0,
  market_value_usd_micros INTEGER NOT NULL DEFAULT 0,
  snapshot_at TEXT,
  PRIMARY KEY(local_day, account_id)
);
CREATE INDEX IF NOT EXISTS idx_fills_executed_at ON portfolio_fills(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_fills_token ON portfolio_fills(chain_id, token_address, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_status ON portfolio_positions(account_id, status, last_trade_at DESC);
CREATE INDEX IF NOT EXISTS idx_daily_day ON portfolio_daily(account_id, local_day DESC);
PRAGMA user_version=1;
"""


class PortfolioLedger:
    def __init__(self, path: str | Path, timezone_name: str, account_id: str = "paper-main"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timezone = ZoneInfo(timezone_name)
        self.account_id = account_id
        self.db = sqlite3.connect(self.path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        self.db.commit()
        self._last_backup_day: str | None = None

    def close(self) -> None:
        self.db.close()

    def open_position_tokens(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT DISTINCT chain_id,token_address,symbol FROM portfolio_positions WHERE account_id=? AND status='open'",
            (self.account_id,),
        ).fetchall()
        return [{"chainId": int(row["chain_id"]), "tokenAddress": str(row["token_address"]),
                 "symbol": str(row["symbol"])} for row in rows]

    def update_market_marks(self, marks: list[dict[str, Any]]) -> int:
        """Apply independently sourced prices to every matching open position."""
        updated = 0
        now = datetime.now(timezone.utc)
        for mark in marks:
            price = _decimal(mark.get("priceUsd"))
            if price <= 0:
                continue
            captured = _event_time(str(mark.get("capturedAt") or now.isoformat()))
            cursor = self.db.execute(
                """UPDATE portfolio_positions SET last_price_usd=?,last_mark_at=?
                   WHERE account_id=? AND chain_id=? AND lower(token_address)=lower(?) AND status='open'""",
                (str(price), captured.isoformat(), self.account_id, int(mark.get("chainId") or 0),
                 str(mark.get("tokenAddress") or "")),
            )
            updated += cursor.rowcount
        if updated:
            self._refresh_daily_snapshot(self._day(now), now)
            self.db.commit()
        return updated

    def maybe_daily_backup(self, directory: str | Path) -> Path | None:
        day = datetime.now(self.timezone).strftime("%Y%m%d")
        if day == self._last_backup_day:
            return None
        destination = Path(directory) / f"portfolio-{day}.sqlite3"
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            target = sqlite3.connect(destination)
            try:
                self.db.backup(target)
            finally:
                target.close()
        self._last_backup_day = day
        return destination

    def exposure_snapshot(self, event: Any) -> ExposureSnapshot:
        """Build authoritative pre-trade exposure from the durable ledger.

        Native gas reserve stays zero until an RPC-backed balance provider is
        configured, intentionally keeping that live-trading check closed.
        """
        now_local = datetime.now(self.timezone)
        day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start = day_start_local.astimezone(timezone.utc)
        day_end = (day_start_local + timedelta(days=1)).astimezone(timezone.utc)
        kol_id = str(getattr(event, "user_id", "") or f"handle:{str(event.handle).lower()}")
        token_cost = self.db.execute(
            "SELECT COALESCE(SUM(cost_basis_usd_micros),0) FROM portfolio_positions WHERE account_id=? AND chain_id=? AND token_address=? AND status='open'",
            (self.account_id, int(event.network_id or 0), str(event.ca or "")),
        ).fetchone()[0]
        kol_daily = self.db.execute(
            "SELECT COALESCE(SUM(gross_usd_micros),0) FROM portfolio_fills WHERE account_id=? AND kol_id=? AND side='buy' AND executed_at>=? AND executed_at<?",
            (self.account_id, kol_id, day_start.isoformat(), day_end.isoformat()),
        ).fetchone()[0]
        global_daily = self.db.execute(
            "SELECT COALESCE(SUM(gross_usd_micros),0) FROM portfolio_fills WHERE account_id=? AND side='buy' AND executed_at>=? AND executed_at<?",
            (self.account_id, day_start.isoformat(), day_end.isoformat()),
        ).fetchone()[0]
        rows = self.db.execute(
            "SELECT chain_id,quantity,last_price_usd FROM portfolio_positions WHERE account_id=? AND status='open'",
            (self.account_id,),
        ).fetchall()
        values = [(int(row["chain_id"]), _micros(_decimal(row["quantity"]) * _decimal(row["last_price_usd"]))) for row in rows]
        total_value = sum(value for _, value in values)
        chain_value = sum(value for chain_id, value in values if chain_id == int(event.network_id or 0))
        chain_percent = Decimal(chain_value) / Decimal(total_value) * 100 if total_value > 0 else Decimal("0")
        has_open_position = self.db.execute(
            "SELECT 1 FROM portfolio_positions WHERE account_id=? AND kol_id=? AND chain_id=? AND token_address=? AND status='open'",
            (self.account_id, kol_id, int(event.network_id or 0), str(event.ca or "")),
        ).fetchone() is not None
        return ExposureSnapshot(
            per_token_usd=Decimal(int(token_cost)) / MICROS,
            per_kol_daily_usd=Decimal(int(kol_daily)) / MICROS,
            global_daily_usd=Decimal(int(global_daily)) / MICROS,
            open_positions=len(rows),
            chain_exposure_percent=chain_percent,
            native_gas_reserve_usd=Decimal("0"),
            has_open_position=has_open_position,
        )

    def pretrade_guard(self, event: Any, proposed_usd: float, settings: dict[str, Any]) -> str | None:
        exposure = self.exposure_snapshot(event)
        proposed = _decimal(proposed_usd)
        if exposure.per_token_usd + proposed > _decimal(settings.get("per_token_limit_usd", 20)):
            return "per_token_limit_reached"
        if exposure.per_kol_daily_usd + proposed > _decimal(settings.get("per_kol_daily_limit_usd", 50)):
            return "per_kol_daily_limit_reached"
        if exposure.global_daily_usd + proposed > _decimal(settings.get("daily_limit_usd", 100)):
            return "daily_limit_reached"
        if not exposure.has_open_position and exposure.open_positions >= int(settings.get("max_open_positions", 5)):
            return "maximum_open_positions_reached"
        return None

    def _day(self, event_time: datetime) -> str:
        return event_time.astimezone(self.timezone).strftime("%Y-%m-%d")

    def _daily_delta(self, day: str, buy: int = 0, sell: int = 0, pnl: int = 0, fee: int = 0, trades: int = 0) -> None:
        self.db.execute(
            """INSERT INTO portfolio_daily(local_day,account_id,buy_usd_micros,sell_usd_micros,realized_pnl_micros,fee_usd_micros,trades)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(local_day,account_id) DO UPDATE SET
                 buy_usd_micros=buy_usd_micros+excluded.buy_usd_micros,
                 sell_usd_micros=sell_usd_micros+excluded.sell_usd_micros,
                 realized_pnl_micros=realized_pnl_micros+excluded.realized_pnl_micros,
                 fee_usd_micros=fee_usd_micros+excluded.fee_usd_micros,
                 trades=trades+excluded.trades""",
            (day, self.account_id, buy, sell, pnl, fee, trades),
        )

    def _refresh_daily_snapshot(self, day: str, now: datetime) -> None:
        rows = self.db.execute(
            "SELECT quantity,cost_basis_usd_micros,last_price_usd FROM portfolio_positions WHERE account_id=? AND status='open'",
            (self.account_id,),
        ).fetchall()
        market = sum(_micros(_decimal(row["quantity"]) * _decimal(row["last_price_usd"])) for row in rows)
        cost = sum(int(row["cost_basis_usd_micros"]) for row in rows)
        self.db.execute(
            """INSERT INTO portfolio_daily(local_day,account_id,unrealized_pnl_micros,market_value_usd_micros,snapshot_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(local_day,account_id) DO UPDATE SET
                 unrealized_pnl_micros=excluded.unrealized_pnl_micros,
                 market_value_usd_micros=excluded.market_value_usd_micros,
                 snapshot_at=excluded.snapshot_at""",
            (day, self.account_id, market - cost, market, now.isoformat()),
        )

    def apply_event(self, event: Any, paper_decision: dict[str, Any] | None) -> dict[str, Any] | None:
        if event.kind not in {"buy", "sell", "clear"} or not event.ca or not event.network_id:
            return None
        now = datetime.now(timezone.utc)
        executed_at = _event_time(event.created_at)
        kol_id = str(getattr(event, "user_id", "") or f"handle:{str(event.handle).lower()}")
        inserted = self.db.execute(
            """INSERT OR IGNORE INTO portfolio_events(event_id,received_at,event_time,kol_id,handle,chain_id,token_address,symbol,kind,status,reason)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (event.id, now.isoformat(), executed_at.isoformat(), kol_id, event.handle, int(event.network_id), event.ca, event.symbol, event.kind, "observed", "pending"),
        )
        if inserted.rowcount == 0:
            return {"status": "duplicate", "eventId": event.id}

        key = (self.account_id, kol_id, int(event.network_id), str(event.ca))
        price = _decimal(event.price)
        if price > 0:
            self.db.execute(
                """UPDATE portfolio_positions SET last_price_usd=?,last_mark_at=?,symbol=?,handle=?
                   WHERE account_id=? AND kol_id=? AND chain_id=? AND token_address=?""",
                (str(price), executed_at.isoformat(), event.symbol, event.handle, *key),
            )

        result: dict[str, Any]
        if event.kind == "buy":
            if not paper_decision or paper_decision.get("status") != "accepted":
                reason = str((paper_decision or {}).get("status") or "buy_not_accepted")
                result = {"status": "ignored", "reason": reason, "eventId": event.id}
            elif price <= 0:
                result = {"status": "needs_price", "reason": "missing_execution_price", "eventId": event.id}
            else:
                gross = _micros(paper_decision.get("paperBuyUsd"))
                quantity = (Decimal(gross) / MICROS) / price
                row = self.db.execute(
                    "SELECT quantity,cost_basis_usd_micros,realized_pnl_micros,first_bought_at FROM portfolio_positions WHERE account_id=? AND kol_id=? AND chain_id=? AND token_address=?",
                    key,
                ).fetchone()
                old_quantity = _decimal(row["quantity"]) if row else Decimal("0")
                old_cost = int(row["cost_basis_usd_micros"]) if row else 0
                realized = int(row["realized_pnl_micros"]) if row else 0
                first_bought = row["first_bought_at"] if row and old_quantity > 0 else executed_at.isoformat()
                self.db.execute(
                    """INSERT INTO portfolio_positions(account_id,kol_id,handle,chain_id,token_address,symbol,quantity,cost_basis_usd_micros,realized_pnl_micros,first_bought_at,last_trade_at,last_price_usd,last_mark_at,status)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(account_id,kol_id,chain_id,token_address) DO UPDATE SET
                         handle=excluded.handle,symbol=excluded.symbol,quantity=excluded.quantity,
                         cost_basis_usd_micros=excluded.cost_basis_usd_micros,first_bought_at=excluded.first_bought_at,
                         last_trade_at=excluded.last_trade_at,last_price_usd=excluded.last_price_usd,
                         last_mark_at=excluded.last_mark_at,status='open'""",
                    (*key[:2], event.handle, *key[2:], event.symbol, str(old_quantity + quantity), old_cost + gross, realized, first_bought, executed_at.isoformat(), str(price), executed_at.isoformat(), "open"),
                )
                self.db.execute(
                    "INSERT INTO portfolio_fills VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"paper:{event.id}:buy", event.id, self.account_id, kol_id, event.handle, int(event.network_id), event.ca, event.symbol, "buy", str(quantity), str(price), gross, gross, 0, 0, executed_at.isoformat(), "paper"),
                )
                self._daily_delta(self._day(executed_at), buy=gross, trades=1)
                result = {"status": "filled", "side": "buy", "eventId": event.id, "quantity": str(quantity), "grossUsd": _usd(gross)}
        else:
            row = self.db.execute(
                "SELECT quantity,cost_basis_usd_micros,realized_pnl_micros FROM portfolio_positions WHERE account_id=? AND kol_id=? AND chain_id=? AND token_address=? AND status='open'",
                key,
            ).fetchone()
            if row is None:
                result = {"status": "ignored", "reason": "no_open_paper_position", "eventId": event.id}
            elif price <= 0:
                result = {"status": "needs_price", "reason": "missing_execution_price", "eventId": event.id}
            else:
                quantity = _decimal(row["quantity"])
                cost = int(row["cost_basis_usd_micros"])
                gross = _micros(quantity * price)
                pnl = gross - cost
                realized_total = int(row["realized_pnl_micros"]) + pnl
                self.db.execute(
                    """UPDATE portfolio_positions SET quantity='0',cost_basis_usd_micros=0,realized_pnl_micros=?,
                       last_trade_at=?,last_price_usd=?,last_mark_at=?,status='closed'
                       WHERE account_id=? AND kol_id=? AND chain_id=? AND token_address=?""",
                    (realized_total, executed_at.isoformat(), str(price), executed_at.isoformat(), *key),
                )
                self.db.execute(
                    "INSERT INTO portfolio_fills VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"paper:{event.id}:sell", event.id, self.account_id, kol_id, event.handle, int(event.network_id), event.ca, event.symbol, "sell", str(quantity), str(price), gross, cost, pnl, 0, executed_at.isoformat(), "paper"),
                )
                self._daily_delta(self._day(executed_at), sell=gross, pnl=pnl, trades=1)
                result = {"status": "filled", "side": "sell", "eventId": event.id, "quantity": str(quantity), "grossUsd": _usd(gross), "realizedPnlUsd": _usd(pnl)}

        self.db.execute(
            "UPDATE portfolio_events SET status=?,reason=? WHERE event_id=?",
            (result["status"], str(result.get("reason") or result.get("side") or "recorded"), event.id),
        )
        self._refresh_daily_snapshot(self._day(executed_at), now)
        self.db.commit()
        return result


def portfolio_snapshot(
    path: str | Path,
    account_id: str = "paper-main",
    limit: int = 200,
    mark_stale_seconds: int = 300,
) -> dict[str, Any]:
    db_path = Path(path)
    empty = {"accountId": account_id, "openPositions": 0, "costBasisUsd": 0, "marketValueUsd": 0, "unrealizedPnlUsd": 0, "realizedPnlUsd": 0, "totalPnlUsd": 0, "positions": [], "fills": [], "daily": []}
    if not db_path.exists():
        return empty
    db = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    db.execute("PRAGMA busy_timeout=5000")
    try:
        summary_positions = db.execute(
            "SELECT quantity,cost_basis_usd_micros,realized_pnl_micros,last_price_usd,status FROM portfolio_positions WHERE account_id=?",
            (account_id,),
        ).fetchall()
        positions = db.execute(
            "SELECT * FROM portfolio_positions WHERE account_id=? ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END,last_trade_at DESC,rowid DESC LIMIT ?",
            (account_id, max(1, min(int(limit), 1000))),
        ).fetchall()
        fills = db.execute(
            "SELECT * FROM portfolio_fills WHERE account_id=? ORDER BY executed_at DESC,rowid DESC LIMIT ?",
            (account_id, max(1, min(int(limit), 1000))),
        ).fetchall()
        daily = db.execute(
            "SELECT * FROM portfolio_daily WHERE account_id=? ORDER BY local_day DESC LIMIT 90",
            (account_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        db.close()
        return empty
    db.close()
    output_positions = []
    cost_total = market_total = unrealized_total = realized_total = 0
    open_count = 0
    for row in summary_positions:
        realized_total += int(row["realized_pnl_micros"])
        if row["status"] != "open":
            continue
        open_count += 1
        cost = int(row["cost_basis_usd_micros"])
        market = _micros(_decimal(row["quantity"]) * _decimal(row["last_price_usd"]))
        cost_total += cost
        market_total += market
        unrealized_total += market - cost
    now = datetime.now(timezone.utc)
    for row in positions:
        quantity = _decimal(row["quantity"])
        cost = int(row["cost_basis_usd_micros"])
        market = _micros(quantity * _decimal(row["last_price_usd"])) if row["status"] == "open" else 0
        unrealized = market - cost if row["status"] == "open" else 0
        realized = int(row["realized_pnl_micros"])
        mark_time = _event_time(row["last_mark_at"])
        mark_age = max(0.0, (now - mark_time).total_seconds())
        output_positions.append({
            "kolId": row["kol_id"], "handle": row["handle"], "networkId": row["chain_id"],
            "ca": row["token_address"], "symbol": row["symbol"], "quantity": str(quantity),
            "costBasisUsd": _usd(cost), "marketValueUsd": _usd(market), "unrealizedPnlUsd": _usd(unrealized),
            "realizedPnlUsd": _usd(realized), "firstBoughtAt": row["first_bought_at"],
            "lastTradeAt": row["last_trade_at"], "lastPriceUsd": float(_decimal(row["last_price_usd"])),
            "lastMarkAt": row["last_mark_at"], "status": row["status"],
            "markAgeSeconds": round(mark_age, 3),
            "markStale": row["status"] == "open" and mark_age > max(1, int(mark_stale_seconds)),
        })
    output_fills = [{
        "fillId": row["fill_id"], "eventId": row["event_id"], "handle": row["handle"],
        "networkId": row["chain_id"], "ca": row["token_address"], "symbol": row["symbol"],
        "side": row["side"], "quantity": row["quantity"], "priceUsd": float(_decimal(row["price_usd"])),
        "grossUsd": _usd(row["gross_usd_micros"]), "costUsd": _usd(row["cost_usd_micros"]),
        "realizedPnlUsd": _usd(row["realized_pnl_micros"]), "feeUsd": _usd(row["fee_usd_micros"]),
        "executedAt": row["executed_at"], "mode": row["mode"],
    } for row in fills]
    output_daily = [{
        "day": row["local_day"], "buyUsd": _usd(row["buy_usd_micros"]), "sellUsd": _usd(row["sell_usd_micros"]),
        "realizedPnlUsd": _usd(row["realized_pnl_micros"]), "unrealizedPnlUsd": _usd(row["unrealized_pnl_micros"]),
        "totalPnlUsd": _usd(int(row["realized_pnl_micros"]) + int(row["unrealized_pnl_micros"])),
        "marketValueUsd": _usd(row["market_value_usd_micros"]), "feesUsd": _usd(row["fee_usd_micros"]),
        "trades": row["trades"], "snapshotAt": row["snapshot_at"],
    } for row in daily]
    return {
        "accountId": account_id, "openPositions": open_count,
        "costBasisUsd": _usd(cost_total), "marketValueUsd": _usd(market_total),
        "unrealizedPnlUsd": _usd(unrealized_total), "realizedPnlUsd": _usd(realized_total),
        "totalPnlUsd": _usd(unrealized_total + realized_total),
        "positions": output_positions, "fills": output_fills, "daily": output_daily,
    }


def backup_database(source: str | Path, destination: str | Path) -> None:
    source_db = sqlite3.connect(Path(source), timeout=5)
    target_path = Path(destination)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_db = sqlite3.connect(target_path)
    try:
        source_db.backup(target_db)
    finally:
        target_db.close()
        source_db.close()
