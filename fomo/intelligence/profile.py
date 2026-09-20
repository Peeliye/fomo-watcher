"""Persistent, read-only behavior profiles built from observed Fomo trades.

The store deliberately does not call behavior statistics a win rate or PnL.
Those require complete source-wallet round trips and historical pricing, which
the Fomo feed alone cannot prove.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable


MICROS = 1_000_000


SCHEMA = """
CREATE TABLE IF NOT EXISTS intelligence_events (
  event_id TEXT PRIMARY KEY,
  logical_key TEXT NOT NULL UNIQUE,
  observed_at TEXT NOT NULL,
  event_time TEXT NOT NULL,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  amount_usd_micros INTEGER NOT NULL,
  market_cap_usd_micros INTEGER NOT NULL,
  price_usd TEXT NOT NULL,
  source_type TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intelligence_kol_time
  ON intelligence_events(kol_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_intelligence_token_time
  ON intelligence_events(chain_id, token_address, event_time DESC);
CREATE TABLE IF NOT EXISTS intelligence_cache (
  cache_key TEXT PRIMARY KEY,event_revision INTEGER NOT NULL,
  generated_at TEXT NOT NULL,payload_json TEXT NOT NULL
);
"""
SCHEMA_VERSION = 2


DEFAULT_SETTINGS: dict[str, Any] = {
    "minimum_buy_events": 20,
    "minimum_distinct_tokens": 10,
    "minimum_active_days": 7,
    "low_cap_usd": 1_000_000,
    "pvp_daily_buy_rate": 10,
    "large_buy_thresholds": [
        {"maximum_market_cap_usd": 1_000_000, "minimum_buy_usd": 5_000},
        {"maximum_market_cap_usd": 3_000_000, "minimum_buy_usd": 10_000},
        {"maximum_market_cap_usd": None, "minimum_buy_usd": 20_000},
    ],
}


def _number(value: Any) -> float:
    try:
        result = float(value or 0)
        return result if result == result and abs(result) != float("inf") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _micros(value: Any) -> int:
    return round(_number(value) * MICROS)


def _time(value: Any) -> str:
    text = str(value or "").strip()
    if text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def _logical_key(data: dict[str, Any]) -> str:
    trade_id = str(data.get("trade_id") or data.get("tradeId") or "").strip()
    if trade_id:
        raw = f"trade:{data.get('chain_id')}:{trade_id}:{data.get('side')}"
    else:
        raw = "|".join((
            str(data.get("kol_id") or ""), str(data.get("chain_id") or 0),
            str(data.get("token_address") or ""), str(data.get("side") or ""),
            f"{_number(data.get('amount_usd')):.6f}", _time(data.get("event_time")),
        ))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _large_buy(amount: float, market_cap: float, settings: dict[str, Any]) -> bool:
    for tier in settings.get("large_buy_thresholds", DEFAULT_SETTINGS["large_buy_thresholds"]):
        maximum = tier.get("maximum_market_cap_usd")
        if maximum is None or market_cap <= _number(maximum):
            return amount >= _number(tier.get("minimum_buy_usd"))
    return False


class WalletIntelligenceStore:
    """SQLite WAL store for observation-only wallet/KOL behavior metrics."""

    def __init__(self, path: str | Path, settings: dict[str, Any] | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.settings = {**DEFAULT_SETTINGS, **(settings or {})}
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=5)
        self.db.row_factory = sqlite3.Row
        version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if version < SCHEMA_VERSION:
            if existed:
                backup_dir = self.path.parent / "backups"
                backup_dir.mkdir(parents=True, exist_ok=True)
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                target = sqlite3.connect(backup_dir / f"{self.path.stem}.pre-v{SCHEMA_VERSION}.{stamp}.sqlite3")
                try:
                    self.db.backup(target)
                finally:
                    target.close()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        if version < SCHEMA_VERSION:
            self.db.executescript(SCHEMA)
            self.db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self.db.commit()

    def close(self) -> None:
        self.db.close()

    def record_event(self, event: Any) -> bool:
        if str(getattr(event, "kind", "")) not in {"buy", "sell", "clear"}:
            return False
        kol_id = str(getattr(event, "user_id", "") or f"handle:{getattr(event, 'handle', 'unknown')}")
        data = {
            "event_id": str(getattr(event, "id", "")),
            "event_time": getattr(event, "created_at", ""),
            "kol_id": kol_id,
            "handle": str(getattr(event, "handle", "unknown")),
            "chain_id": int(getattr(event, "network_id", 0) or 0),
            "token_address": str(getattr(event, "ca", "")),
            "symbol": str(getattr(event, "symbol", "UNKNOWN")),
            "side": "sell" if getattr(event, "kind", "") in {"sell", "clear"} else "buy",
            "amount_usd": _number(getattr(event, "amount_usd", 0)),
            "market_cap_usd": _number(getattr(event, "market_cap", 0)),
            "price_usd": _number(getattr(event, "price", 0)),
            "source_type": str(getattr(event, "source_type", "") or getattr(event, "kind", "")),
            "trade_id": str(getattr(event, "trade_id", "")),
        }
        if not data["event_id"] or not data["token_address"] or not data["chain_id"]:
            return False
        inserted = self.db.execute(
            """INSERT OR IGNORE INTO intelligence_events
               (event_id,logical_key,observed_at,event_time,kol_id,handle,chain_id,
                token_address,symbol,side,amount_usd_micros,market_cap_usd_micros,
                price_usd,source_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data["event_id"], _logical_key(data), datetime.now(timezone.utc).isoformat(),
                _time(data["event_time"]), data["kol_id"], data["handle"], data["chain_id"],
                data["token_address"], data["symbol"], data["side"],
                _micros(data["amount_usd"]), _micros(data["market_cap_usd"]),
                str(data["price_usd"]), data["source_type"],
            ),
        )
        self.db.commit()
        return inserted.rowcount == 1

    def ingest_order_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        """Backfill historical observation rows without treating them as fills."""
        inserted = 0
        for row in rows:
            source = str(row.get("sourceType") or "")
            if "buy" in source:
                side = "buy"
            elif "sell" in source:
                side = "sell"
            else:
                continue
            data = {
                "event_id": str(row.get("eventId") or ""),
                "event_time": row.get("recordedAt"),
                "kol_id": str(row.get("userId") or f"handle:{row.get('handle', 'unknown')}"),
                "handle": str(row.get("handle") or "unknown"),
                "chain_id": int(row.get("networkId") or 0),
                "token_address": str(row.get("ca") or ""),
                "symbol": str(row.get("symbol") or "UNKNOWN"),
                "side": side,
                "amount_usd": _number(row.get("targetBuyUsd") if side == "buy" else row.get("targetSellUsd")),
                "market_cap_usd": _number(row.get("marketCapUsd")),
                "price_usd": _number(row.get("priceUsd")),
                "source_type": source,
                "trade_id": str(row.get("tradeId") or ""),
            }
            if not data["event_id"] or not data["token_address"] or not data["chain_id"]:
                continue
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO intelligence_events
                   (event_id,logical_key,observed_at,event_time,kol_id,handle,chain_id,
                    token_address,symbol,side,amount_usd_micros,market_cap_usd_micros,
                    price_usd,source_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    data["event_id"], _logical_key(data), datetime.now(timezone.utc).isoformat(),
                    _time(data["event_time"]), data["kol_id"], data["handle"], data["chain_id"],
                    data["token_address"], data["symbol"], data["side"],
                    _micros(data["amount_usd"]), _micros(data["market_cap_usd"]),
                    str(data["price_usd"]), data["source_type"],
                ),
            )
            inserted += cursor.rowcount
        self.db.commit()
        return inserted

    def snapshot(self, limit: int = 200) -> dict[str, Any]:
        requested = max(1, min(int(limit), 1000))
        revision = int(self.db.execute("SELECT COALESCE(MAX(rowid),0) FROM intelligence_events").fetchone()[0])
        cached = self.db.execute("SELECT * FROM intelligence_cache WHERE cache_key='profiles-v1'").fetchone()
        if cached and int(cached["event_revision"]) == revision:
            payload = json.loads(str(cached["payload_json"]))
            payload["profiles"] = payload.get("profiles", [])[:requested]
            return payload
        payload = self._compute_snapshot(1000)
        self.db.execute(
            "INSERT OR REPLACE INTO intelligence_cache VALUES('profiles-v1',?,?,?)",
            (revision, datetime.now(timezone.utc).isoformat(), json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
        )
        self.db.commit()
        payload["profiles"] = payload["profiles"][:requested]
        return payload

    def _compute_snapshot(self, limit: int = 1000) -> dict[str, Any]:
        rows = self.db.execute(
            "SELECT * FROM intelligence_events ORDER BY event_time ASC"
        ).fetchall()
        # Historical imports did not always include the platform user id and
        # therefore used ``handle:<name>``.  Once the real id arrives, fold
        # those synthetic rows into it so one person does not get two profiles.
        actual_ids_by_handle: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            handle_key = str(row["handle"]).strip().lstrip("@").casefold()
            kol_id = str(row["kol_id"])
            if handle_key and not kol_id.startswith("handle:"):
                actual_ids_by_handle[handle_key].add(kol_id)
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        aliases: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            kol_id = str(row["kol_id"])
            handle_key = str(row["handle"]).strip().lstrip("@").casefold()
            actual_ids = actual_ids_by_handle.get(handle_key, set())
            canonical_id = next(iter(actual_ids)) if kol_id.startswith("handle:") and len(actual_ids) == 1 else kol_id
            grouped[canonical_id].append(row)
            aliases[canonical_id].add(kol_id)

        profiles: list[dict[str, Any]] = []
        style_counts: Counter[str] = Counter()
        sufficient = 0
        for kol_id, events in grouped.items():
            buys = [row for row in events if row["side"] == "buy"]
            sells = [row for row in events if row["side"] == "sell"]
            if not buys:
                continue
            amounts = [row["amount_usd_micros"] / MICROS for row in buys]
            caps = [row["market_cap_usd_micros"] / MICROS for row in buys if row["market_cap_usd_micros"] > 0]
            tokens = {(row["chain_id"], row["token_address"]) for row in buys}
            days = {str(row["event_time"])[:10] for row in buys}
            chains = sorted({int(row["chain_id"]) for row in events})
            total_buy = sum(amounts)
            per_token: Counter[tuple[int, str]] = Counter()
            for row, amount in zip(buys, amounts):
                per_token[(int(row["chain_id"]), str(row["token_address"]))] += amount
            concentration = max(per_token.values(), default=0) / total_buy if total_buy > 0 else 0
            low_cap = sum(1 for cap in caps if cap < _number(self.settings["low_cap_usd"]))
            low_cap_rate = low_cap / len(caps) if caps else 0
            daily_rate = len(buys) / max(1, len(days))
            large_buys = sum(
                1 for row in buys
                if _large_buy(
                    row["amount_usd_micros"] / MICROS,
                    row["market_cap_usd_micros"] / MICROS,
                    self.settings,
                )
            )
            enough = (
                len(buys) >= int(self.settings["minimum_buy_events"])
                and len(tokens) >= int(self.settings["minimum_distinct_tokens"])
                and len(days) >= int(self.settings["minimum_active_days"])
            )
            if daily_rate >= _number(self.settings["pvp_daily_buy_rate"]) and low_cap_rate >= 0.35:
                style = "pvp_short_term"
            elif large_buys >= 3 or (sum(amounts) / len(amounts)) >= 5_000:
                style = "whale_momentum"
            elif (median(caps) if caps else 0) >= 1_000_000 and (sum(amounts) / len(amounts)) >= 500:
                style = "narrative_momentum"
            else:
                style = "unclassified"
            flags = ["pnl_unverified"]
            if concentration >= 0.6 and len(buys) >= 5:
                flags.append("concentrated_activity")
            if daily_rate >= 30:
                flags.append("high_frequency")
            if low_cap_rate >= 0.6 and len(caps) >= 5:
                flags.append("ultra_low_cap_exposure")
            evidence_score = min(100, round(
                min(1, len(buys) / int(self.settings["minimum_buy_events"])) * 40
                + min(1, len(tokens) / int(self.settings["minimum_distinct_tokens"])) * 35
                + min(1, len(days) / int(self.settings["minimum_active_days"])) * 25
            ))
            profile = {
                "kolId": kol_id,
                "kolAliases": sorted(aliases[kol_id]),
                "handle": str(events[-1]["handle"]),
                "chains": chains,
                "buyEvents": len(buys),
                "sellEvents": len(sells),
                "distinctTokens": len(tokens),
                "activeDays": len(days),
                "observedBuyUsd": round(total_buy, 2),
                "averageBuyUsd": round(total_buy / len(buys), 2),
                "medianMarketCapUsd": round(median(caps), 2) if caps else None,
                "lowCapBuyRate": round(low_cap_rate, 4),
                "largeBuyEvents": large_buys,
                "topTokenConcentration": round(concentration, 4),
                "evidenceScore": evidence_score,
                "evidenceStatus": "sufficient_behavior_sample" if enough else "insufficient_behavior_sample",
                "primaryStyle": style,
                "performanceVerified": False,
                "riskFlags": flags,
                "recommendedMode": "observe_only",
                "latestAt": str(events[-1]["event_time"]),
                "source": "fomo_feed_behavior_observation",
                "asOf": str(events[-1]["event_time"]),
                "freshness": "latest_observed_feed_event",
                "verificationStatus": "observation_only_not_pnl",
                "metrics": {
                    "buyEvents": {"value": len(buys), "source": "fomo_feed", "verificationStatus": "observed"},
                    "sellEvents": {"value": len(sells), "source": "fomo_feed", "verificationStatus": "observed"},
                    "observedBuyUsd": {
                        "valueUsd": round(total_buy, 2),
                        "source": "fomo_feed_reported_amount",
                        "verificationStatus": "not_pnl",
                    },
                },
            }
            profiles.append(profile)
            style_counts[style] += 1
            sufficient += int(enough)
        profiles.sort(key=lambda item: (item["evidenceScore"], item["buyEvents"]), reverse=True)
        return {
            "observationOnly": True,
            "performanceVerified": False,
            "source": "fomo_feed_behavior_observation",
            "verificationStatus": "observation_only_not_pnl",
            "totalEvents": len(rows),
            "profiled": len(profiles),
            "sufficientBehaviorSamples": sufficient,
            "styles": dict(style_counts),
            "profiles": profiles[:max(1, min(int(limit), 1000))],
        }


def intelligence_snapshot(path: str | Path, settings: dict[str, Any] | None = None, limit: int = 200) -> dict[str, Any]:
    store = WalletIntelligenceStore(path, settings)
    try:
        return store.snapshot(limit)
    finally:
        store.close()


def load_ndjson(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source = Path(path)
    if not source.exists():
        return rows
    with source.open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
            except json.JSONDecodeError:
                continue
    return rows
