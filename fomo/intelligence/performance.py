"""Transaction-proven wallet performance with durable FIFO materialization.

Feed observations, paper fills, leaderboard values, and verified wallet fills
are separate data products. This store accepts normalized chain fills with
stable transaction identities and values open inventory only with independent
market observations.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


MICROS = Decimal("1000000")
EPSILON = Decimal("0.000000000001")
SOLANA_CHAIN_ID = 1399811149
DEFAULT_SETTINGS: dict[str, Any] = {
    "minimum_closed_tokens": 3,
    "minimum_distinct_tokens": 3,
    "maximum_data_age_seconds": 86400,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS verified_fills (
  fill_id TEXT PRIMARY KEY, tx_hash TEXT NOT NULL,
  instruction_index INTEGER NOT NULL DEFAULT 0,
  kol_id TEXT NOT NULL, handle TEXT NOT NULL,
  wallet TEXT NOT NULL, wallet_key TEXT NOT NULL DEFAULT '',
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL, token_key TEXT NOT NULL DEFAULT '',
  symbol TEXT NOT NULL, side TEXT NOT NULL CHECK(side IN ('buy','sell')),
  token_quantity TEXT NOT NULL, gross_usd_micros INTEGER NOT NULL,
  fee_usd_micros INTEGER NOT NULL DEFAULT 0, price_usd TEXT NOT NULL,
  market_cap_usd_micros INTEGER, executed_at TEXT NOT NULL,
  source TEXT NOT NULL, source_confidence TEXT NOT NULL,
  history_complete INTEGER NOT NULL DEFAULT 0, raw_payload_hash TEXT NOT NULL,
  receipt_verified INTEGER NOT NULL DEFAULT 0,
  finality TEXT NOT NULL DEFAULT '',
  indexer_checkpoint TEXT NOT NULL DEFAULT '',
  UNIQUE(chain_id, tx_hash, instruction_index, token_address, side)
);
CREATE INDEX IF NOT EXISTS idx_verified_fills_kol_time
  ON verified_fills(kol_id, executed_at ASC);
CREATE INDEX IF NOT EXISTS idx_verified_fills_inventory_time
  ON verified_fills(kol_id, wallet_key, chain_id, token_key, executed_at ASC);
CREATE INDEX IF NOT EXISTS idx_verified_fills_token_kol
  ON verified_fills(chain_id, token_key, kol_id);

CREATE TABLE IF NOT EXISTS token_market_history (
  chain_id INTEGER NOT NULL, token_address TEXT NOT NULL,
  token_key TEXT NOT NULL DEFAULT '', observed_at TEXT NOT NULL,
  price_usd TEXT NOT NULL, market_cap_usd_micros INTEGER,
  liquidity_usd_micros INTEGER, source TEXT NOT NULL,
  PRIMARY KEY(chain_id, token_address, observed_at, source)
);
CREATE INDEX IF NOT EXISTS idx_market_token_time
  ON token_market_history(chain_id, token_key, observed_at ASC);
CREATE INDEX IF NOT EXISTS idx_market_token_latest
  ON token_market_history(chain_id, token_key, observed_at DESC);

CREATE TABLE IF NOT EXISTS social_identities (
  kol_id TEXT NOT NULL, platform TEXT NOT NULL, account_handle TEXT NOT NULL,
  profile_url TEXT, evidence_reference TEXT NOT NULL, confidence TEXT NOT NULL,
  verified_at TEXT NOT NULL, PRIMARY KEY(kol_id, platform, account_handle)
);
CREATE TABLE IF NOT EXISTS performance_profiles (
  kol_id TEXT PRIMARY KEY, performance_verified INTEGER NOT NULL,
  total_pnl_usd_micros INTEGER, generated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_performance_profiles_rank
  ON performance_profiles(performance_verified DESC, total_pnl_usd_micros DESC, kol_id);

-- Retained so old databases and tooling remain readable. Requests use the
-- bounded performance_profiles table instead of this revision cache.
CREATE TABLE IF NOT EXISTS performance_cache (
  cache_key TEXT PRIMARY KEY, fill_revision INTEGER NOT NULL,
  market_revision INTEGER NOT NULL, social_revision INTEGER NOT NULL,
  generated_at TEXT NOT NULL, payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS performance_stats (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  fill_count INTEGER NOT NULL, market_count INTEGER NOT NULL,
  social_count INTEGER NOT NULL
);
INSERT OR IGNORE INTO performance_stats(singleton,fill_count,market_count,social_count)
  SELECT 1,
    (SELECT COUNT(*) FROM verified_fills),
    (SELECT COUNT(*) FROM token_market_history),
    (SELECT COUNT(*) FROM social_identities);
CREATE TRIGGER IF NOT EXISTS verified_fills_stats_insert AFTER INSERT ON verified_fills
BEGIN UPDATE performance_stats SET fill_count=fill_count+1 WHERE singleton=1; END;
CREATE TRIGGER IF NOT EXISTS verified_fills_stats_delete AFTER DELETE ON verified_fills
BEGIN UPDATE performance_stats SET fill_count=fill_count-1 WHERE singleton=1; END;
CREATE TRIGGER IF NOT EXISTS market_history_stats_insert AFTER INSERT ON token_market_history
BEGIN UPDATE performance_stats SET market_count=market_count+1 WHERE singleton=1; END;
CREATE TRIGGER IF NOT EXISTS market_history_stats_delete AFTER DELETE ON token_market_history
BEGIN UPDATE performance_stats SET market_count=market_count-1 WHERE singleton=1; END;
CREATE TRIGGER IF NOT EXISTS social_identities_stats_insert AFTER INSERT ON social_identities
BEGIN UPDATE performance_stats SET social_count=social_count+1 WHERE singleton=1; END;
CREATE TRIGGER IF NOT EXISTS social_identities_stats_delete AFTER DELETE ON social_identities
BEGIN UPDATE performance_stats SET social_count=social_count-1 WHERE singleton=1; END;
"""
SCHEMA_VERSION = 5


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value if value is not None else 0))
        return result if result.is_finite() else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _micros(value: Any) -> int:
    return int((_decimal(value) * MICROS).quantize(Decimal("1")))


def _time(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _age_seconds(value: str, now: datetime) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0, int((now - parsed.astimezone(timezone.utc)).total_seconds()))


def _hash(raw: Any) -> str:
    text = str(raw or "")
    return text if len(text) == 64 else hashlib.sha256(text.encode()).hexdigest()


def canonical_address(chain_id: int, value: Any) -> str:
    """Normalize EVM addresses while preserving Solana case."""

    text = str(value or "").strip()
    return text if int(chain_id) == SOLANA_CHAIN_ID else text.lower()


def _inventory_status(*, history_complete: bool, orphan: bool, oversold: bool) -> str:
    if orphan:
        return "orphan_sell"
    if oversold:
        return "oversold"
    if not history_complete:
        return "inventory_incomplete"
    return "complete"


class VerifiedPerformanceStore:
    """WAL-backed FIFO store whose snapshot path never scans raw history."""

    def __init__(self, path: str | Path, settings: dict[str, Any] | None = None):
        self.path = Path(path)
        self.settings = {**DEFAULT_SETTINGS, **(settings or {})}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if version < SCHEMA_VERSION and existed:
            self._backup_before_migration(version)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=10000")
        if version < SCHEMA_VERSION:
            # Existing v1/v2 tables do not have the canonical-key columns, so
            # add them before creating indexes that reference those columns.
            if existed:
                self.db.execute("BEGIN IMMEDIATE")
                try:
                    self._ensure_columns()
                    self.db.commit()
                except Exception:
                    self.db.rollback()
                    raise
            else:
                self.db.executescript(SCHEMA)
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_columns()
                self.db.executescript(SCHEMA)
                self.db.execute("DELETE FROM performance_profiles")
                self.db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        self._materialize_missing_profiles()

    def _backup_before_migration(self, version: int) -> None:
        directory = self.path.parent / "backups"
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target = sqlite3.connect(directory / f"{self.path.stem}.pre-v{SCHEMA_VERSION}.from-v{version}.{stamp}.sqlite3")
        try:
            self.db.backup(target)
        finally:
            target.close()

    def _ensure_columns(self) -> None:
        columns = {str(row[1]) for row in self.db.execute("PRAGMA table_info(verified_fills)")}
        for name, definition in (
            ("wallet_key", "TEXT NOT NULL DEFAULT ''"),
            ("token_key", "TEXT NOT NULL DEFAULT ''"),
            ("history_complete", "INTEGER NOT NULL DEFAULT 0"),
            ("receipt_verified", "INTEGER NOT NULL DEFAULT 0"),
            ("finality", "TEXT NOT NULL DEFAULT ''"),
            ("indexer_checkpoint", "TEXT NOT NULL DEFAULT ''"),
        ):
            if name not in columns:
                self.db.execute(f"ALTER TABLE verified_fills ADD COLUMN {name} {definition}")
        market_columns = {str(row[1]) for row in self.db.execute("PRAGMA table_info(token_market_history)")}
        if "token_key" not in market_columns:
            self.db.execute("ALTER TABLE token_market_history ADD COLUMN token_key TEXT NOT NULL DEFAULT ''")
        self.db.execute(
            "UPDATE verified_fills SET wallet_key=CASE WHEN chain_id=? THEN wallet ELSE lower(wallet) END "
            "WHERE wallet_key=''",
            (SOLANA_CHAIN_ID,),
        )
        self.db.execute(
            "UPDATE verified_fills SET token_key=CASE WHEN chain_id=? THEN token_address ELSE lower(token_address) END "
            "WHERE token_key=''",
            (SOLANA_CHAIN_ID,),
        )
        self.db.execute(
            "UPDATE token_market_history SET token_key=CASE WHEN chain_id=? THEN token_address ELSE lower(token_address) END "
            "WHERE token_key=''",
            (SOLANA_CHAIN_ID,),
        )

    def _materialize_missing_profiles(self) -> None:
        missing = [
            str(row[0])
            for row in self.db.execute(
                "SELECT DISTINCT f.kol_id FROM verified_fills f LEFT JOIN performance_profiles p "
                "ON p.kol_id=f.kol_id WHERE p.kol_id IS NULL"
            )
        ]
        if missing:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self._rebuild_profiles(missing)
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise

    def close(self) -> None:
        self.db.close()

    def ingest_fills(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        affected: set[str] = set()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                side = str(row.get("side") or "").lower()
                tx_hash = str(row.get("txHash") or row.get("signature") or "").strip()
                kol_id = str(row.get("kolId") or "").strip()
                wallet_raw = str(row.get("wallet") or "").strip()
                qty, gross = _decimal(row.get("tokenQuantity")), _decimal(row.get("grossUsd"))
                # sourceConfidence is retained for audit compatibility but is
                # never accepted as chain verification evidence.
                confidence = _decimal(row.get("sourceConfidence", 0))
                if side not in {"buy", "sell"} or not tx_hash or not kol_id or not wallet_raw:
                    continue
                if qty <= 0 or gross <= 0:
                    continue
                chain_id = int(row.get("chainId") or 0)
                token_raw = str(row.get("tokenAddress") or "").strip()
                if not chain_id or not token_raw:
                    continue
                wallet_key = canonical_address(chain_id, wallet_raw)
                token_key = canonical_address(chain_id, token_raw)
                executed_at = _time(row.get("executedAt"))
                index = int(row.get("instructionIndex") or 0)
                fill_id = str(
                    row.get("fillId")
                    or hashlib.sha256(f"{chain_id}|{tx_hash}|{index}|{token_key}|{side}".encode()).hexdigest()
                )
                duplicate = self.db.execute(
                    "SELECT 1 FROM verified_fills WHERE fill_id=? OR "
                    "(chain_id=? AND tx_hash=? AND instruction_index=? AND token_key=? AND side=?) LIMIT 1",
                    (fill_id, chain_id, tx_hash, index, token_key, side),
                ).fetchone()
                if duplicate:
                    continue
                cursor = self.db.execute(
                    """INSERT INTO verified_fills
                    (fill_id,tx_hash,instruction_index,kol_id,handle,wallet,wallet_key,chain_id,
                     token_address,token_key,symbol,side,token_quantity,gross_usd_micros,
                     fee_usd_micros,price_usd,market_cap_usd_micros,executed_at,
                     source,source_confidence,history_complete,raw_payload_hash,
                     receipt_verified,finality,indexer_checkpoint)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        fill_id, tx_hash, index, kol_id, str(row.get("handle") or "unknown"),
                        wallet_key, wallet_key, chain_id, token_key, token_key,
                        str(row.get("symbol") or "UNKNOWN"), side, str(qty), _micros(gross),
                        _micros(row.get("feeUsd")), str(_decimal(row.get("priceUsd")) or gross / qty),
                        _micros(row.get("marketCapUsd")) if row.get("marketCapUsd") is not None else None,
                        executed_at, str(row.get("source") or "normalized_chain_indexer"), str(confidence),
                        int(bool(row.get("historyComplete", row.get("inventoryComplete", False)))),
                        _hash(row.get("rawPayloadHash") or tx_hash),
                        int(bool(row.get("receiptVerified", False))),
                        str(row.get("finality") or ""),
                        str(row.get("indexerCheckpoint") or ""),
                    ),
                )
                inserted += cursor.rowcount
                affected.add(kol_id)
            self._rebuild_profiles(affected)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return inserted

    def ingest_market_history(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        changed: set[tuple[int, str]] = set()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                chain_id = int(row.get("chainId") or 0)
                token_raw = str(row.get("tokenAddress") or "").strip()
                price = _decimal(row.get("priceUsd"))
                if not chain_id or not token_raw or price <= 0:
                    continue
                token_key = canonical_address(chain_id, token_raw)
                observed_at = _time(row.get("observedAt"))
                source = str(row.get("source") or "market_history")
                duplicate = self.db.execute(
                    "SELECT 1 FROM token_market_history WHERE chain_id=? AND token_key=? "
                    "AND observed_at=? AND source=?",
                    (chain_id, token_key, observed_at, source),
                ).fetchone()
                if duplicate:
                    continue
                cursor = self.db.execute(
                    """INSERT INTO token_market_history
                    (chain_id,token_address,token_key,observed_at,price_usd,market_cap_usd_micros,
                     liquidity_usd_micros,source) VALUES(?,?,?,?,?,?,?,?)""",
                    (
                        chain_id, token_key, token_key, observed_at, str(price),
                        _micros(row.get("marketCapUsd")) if row.get("marketCapUsd") is not None else None,
                        _micros(row.get("liquidityUsd")) if row.get("liquidityUsd") is not None else None,
                        source,
                    ),
                )
                inserted += cursor.rowcount
                changed.add((chain_id, token_key))
            affected: set[str] = set()
            ordered = sorted(changed)
            for start in range(0, len(ordered), 300):
                chunk = ordered[start : start + 300]
                clauses = " OR ".join("(chain_id=? AND token_key=?)" for _ in chunk)
                params = [value for pair in chunk for value in pair]
                affected.update(
                    str(result[0])
                    for result in self.db.execute(
                        f"SELECT DISTINCT kol_id FROM verified_fills WHERE {clauses}", params
                    )
                )
            self._rebuild_profiles(affected)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return inserted

    def ingest_social_identities(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        affected: set[str] = set()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                kol_id = str(row.get("kolId") or "").strip()
                platform = str(row.get("platform") or "").strip().lower()
                handle = str(row.get("accountHandle") or "").strip()
                evidence = str(row.get("evidenceReference") or "").strip()
                confidence = _decimal(row.get("confidence"))
                if not kol_id or not platform or not handle or not evidence or confidence < Decimal("0.8"):
                    continue
                cursor = self.db.execute(
                    """INSERT INTO social_identities
                    (kol_id,platform,account_handle,profile_url,evidence_reference,confidence,verified_at)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT(kol_id,platform,account_handle) DO UPDATE SET
                    profile_url=excluded.profile_url,evidence_reference=excluded.evidence_reference,
                    confidence=excluded.confidence,verified_at=excluded.verified_at""",
                    (kol_id, platform, handle, row.get("profileUrl"), evidence, str(confidence), _time(row.get("verifiedAt"))),
                )
                inserted += int(cursor.rowcount > 0)
                affected.add(kol_id)
            self._rebuild_profiles(affected)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return inserted

    def snapshot(self, limit: int = 200) -> dict[str, Any]:
        """Return bounded materialized rows without scanning fill or market history."""

        requested = max(1, min(int(limit), 1000))
        profiles = [
            json.loads(str(row[0]))
            for row in self.db.execute(
                "SELECT payload_json FROM performance_profiles "
                "ORDER BY performance_verified DESC,total_pnl_usd_micros DESC,kol_id LIMIT ?",
                (requested,),
            )
        ]
        counts = self.db.execute(
            "SELECT fill_count,market_count,social_count,"
            "(SELECT COUNT(*) FROM performance_profiles),"
            "(SELECT COUNT(*) FROM performance_profiles WHERE performance_verified=1) "
            "FROM performance_stats WHERE singleton=1"
        ).fetchone()
        verified_fills, markets, socials, profiled, verified_profiles = map(int, counts)
        generated_at = datetime.now(timezone.utc).isoformat()
        return {
            "source": "verified_wallet_chain_fills", "asOf": generated_at,
            "freshness": "materialized",
            "verificationStatus": "verified" if verified_profiles else "not_verified",
            "integrationStatus": "data_available" if verified_fills or markets else "not_connected_or_empty",
            "performanceVerified": bool(verified_profiles), "verifiedFills": verified_fills,
            "marketObservations": markets, "socialIdentities": socials,
            "profiled": profiled, "verifiedProfiles": verified_profiles, "profiles": profiles,
        }

    def _market_rows(self, keys: set[tuple[int, str]]) -> list[sqlite3.Row]:
        rows: list[sqlite3.Row] = []
        ordered = sorted(keys)
        for start in range(0, len(ordered), 300):
            chunk = ordered[start : start + 300]
            clauses = " OR ".join("(chain_id=? AND token_key=?)" for _ in chunk)
            params = [value for pair in chunk for value in pair]
            rows.extend(self.db.execute(
                f"SELECT * FROM token_market_history WHERE {clauses} "
                "ORDER BY chain_id,token_key,observed_at ASC", params
            ).fetchall())
        return rows

    def _rebuild_profiles(self, kol_ids: Iterable[str]) -> None:
        for kol_id in sorted({str(value) for value in kol_ids if str(value)}):
            payload = self._compute_profile(kol_id)
            if payload is None:
                self.db.execute("DELETE FROM performance_profiles WHERE kol_id=?", (kol_id,))
                continue
            total = payload.get("totalPnlUsd")
            self.db.execute(
                """INSERT INTO performance_profiles
                (kol_id,performance_verified,total_pnl_usd_micros,generated_at,payload_json)
                VALUES(?,?,?,?,?) ON CONFLICT(kol_id) DO UPDATE SET
                performance_verified=excluded.performance_verified,
                total_pnl_usd_micros=excluded.total_pnl_usd_micros,
                generated_at=excluded.generated_at,payload_json=excluded.payload_json""",
                (kol_id, int(bool(payload["performanceVerified"])), _micros(total) if total is not None else None,
                 payload["asOf"], json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
            )

    def _compute_profile(self, kol_id: str) -> dict[str, Any] | None:
        rows = self.db.execute(
            "SELECT * FROM verified_fills WHERE kol_id=? ORDER BY executed_at ASC,rowid ASC", (kol_id,)
        ).fetchall()
        if not rows:
            return None
        socials = self.db.execute(
            "SELECT * FROM social_identities WHERE kol_id=? ORDER BY platform,account_handle", (kol_id,)
        ).fetchall()
        grouped: dict[tuple[str, int, str], list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[(str(row["wallet_key"]), int(row["chain_id"]), str(row["token_key"]))].append(row)
        market_by_token: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
        for market in self._market_rows({(key[1], key[2]) for key in grouped}):
            market_by_token[(int(market["chain_id"]), str(market["token_key"]))].append(market)

        now = datetime.now(timezone.utc)
        maximum_age = int(self.settings["maximum_data_age_seconds"])
        token_results: list[dict[str, Any]] = []
        closed_outcomes: list[tuple[str, Decimal]] = []
        total_buy = total_sell = realized = estimated_unrealized = Decimal(0)
        wins = losses = early_wins = rebound_wins = 0
        all_inventory_complete = all_marks_fresh = True
        has_open_inventory = False
        latest_market_at: str | None = None

        for (wallet, chain_id, token), token_rows in grouped.items():
            lots: deque[list[Decimal]] = deque()
            token_realized = Decimal(0)
            orphan = oversold = False
            history_complete = all(
                bool(row["history_complete"])
                and bool(row["receipt_verified"])
                and str(row["finality"]) in {"confirmed", "finalized"}
                and bool(str(row["indexer_checkpoint"]))
                for row in token_rows
            )
            first_buy = next((row for row in token_rows if row["side"] == "buy"), None)
            for row in token_rows:
                quantity = _decimal(row["token_quantity"])
                gross = Decimal(int(row["gross_usd_micros"])) / MICROS
                fee = Decimal(int(row["fee_usd_micros"])) / MICROS
                if row["side"] == "buy":
                    lots.append([quantity, gross + fee])
                    total_buy += gross + fee
                    continue
                available = sum(lot[0] for lot in lots)
                if available <= EPSILON:
                    orphan = True
                    continue
                matched = min(available, quantity)
                oversold = oversold or quantity > available + EPSILON
                allocated_cost = Decimal(0)
                remaining = matched
                while remaining > EPSILON and lots:
                    lot_quantity, lot_cost = lots[0]
                    consumed = min(lot_quantity, remaining)
                    consumed_cost = lot_cost * consumed / lot_quantity
                    allocated_cost += consumed_cost
                    lot_quantity -= consumed
                    lot_cost -= consumed_cost
                    remaining -= consumed
                    if lot_quantity <= EPSILON:
                        lots.popleft()
                    else:
                        lots[0] = [lot_quantity, lot_cost]
                proceeds = max(Decimal(0), gross - fee) * matched / quantity
                token_realized += proceeds - allocated_cost
                total_sell += proceeds

            remaining_quantity = sum(lot[0] for lot in lots)
            remaining_cost = sum(lot[1] for lot in lots)
            inventory_status = _inventory_status(history_complete=history_complete, orphan=orphan, oversold=oversold)
            inventory_complete = inventory_status == "complete"
            all_inventory_complete = all_inventory_complete and inventory_complete
            realized += token_realized
            history = market_by_token.get((chain_id, token), [])
            latest_market = history[-1] if history else None
            observed_at = str(latest_market["observed_at"]) if latest_market else None
            if observed_at and (latest_market_at is None or observed_at > latest_market_at):
                latest_market_at = observed_at
            market_age = _age_seconds(observed_at, now) if observed_at else None
            mark_status = "missing" if not latest_market else "fresh" if market_age is not None and market_age <= maximum_age else "stale"
            all_marks_fresh = all_marks_fresh and mark_status == "fresh"
            has_open = remaining_quantity > EPSILON
            has_open_inventory = has_open_inventory or has_open
            unrealized: Decimal | None = None
            if has_open and latest_market:
                unrealized = remaining_quantity * _decimal(latest_market["price_usd"]) - remaining_cost
                estimated_unrealized += unrealized
            entry_cap = Decimal(int(first_buy["market_cap_usd_micros"] or 0)) / MICROS if first_buy else Decimal(0)
            caps = [Decimal(int(item["market_cap_usd_micros"])) / MICROS for item in history if item["market_cap_usd_micros"]]
            peak_cap = max(caps + ([entry_cap] if entry_cap else []), default=Decimal(0))
            current_cap = caps[-1] if caps else Decimal(0)
            pre_entry_caps = [
                Decimal(int(item["market_cap_usd_micros"])) / MICROS
                for item in history
                if item["market_cap_usd_micros"] and first_buy and item["observed_at"] < first_buy["executed_at"]
            ]
            prior_peak = max(pre_entry_caps, default=Decimal(0))
            prior_low = min(pre_entry_caps[-20:], default=Decimal(0)) if pre_entry_caps else Decimal(0)
            early = bool(entry_cap and peak_cap and entry_cap <= peak_cap * Decimal("0.2"))
            rebound = bool(prior_peak and prior_low <= prior_peak * Decimal("0.2") and entry_cap >= prior_low * Decimal("1.25"))
            closed = bool(first_buy and remaining_quantity <= EPSILON)
            verified_closed = closed and inventory_complete
            if verified_closed:
                wins += int(token_realized > 0)
                losses += int(token_realized <= 0)
                early_wins += int(early and token_realized > 0)
                rebound_wins += int(rebound and token_realized > 0)
                closed_outcomes.append((str(token_rows[-1]["executed_at"]), token_realized))
            total_pnl = token_realized + unrealized if unrealized is not None else None
            token_results.append({
                "wallet": wallet, "chainId": chain_id, "tokenAddress": token,
                "symbol": token_rows[-1]["symbol"],
                "firstBoughtAt": first_buy["executed_at"] if first_buy else None,
                "lastFillAt": token_rows[-1]["executed_at"],
                "entryMarketCapUsd": float(entry_cap) if entry_cap else None,
                "peakMarketCapUsd": float(peak_cap) if peak_cap else None,
                "currentMarketCapUsd": float(current_cap) if current_cap else None,
                "entryToPeakMultiple": float(peak_cap / entry_cap) if entry_cap and peak_cap else None,
                "remainingQuantity": str(remaining_quantity), "remainingCostUsd": float(remaining_cost),
                "realizedPnlUsd": float(token_realized),
                "unrealizedPnlUsd": float(unrealized) if unrealized is not None else None,
                "totalPnlUsd": float(total_pnl) if total_pnl is not None else None,
                "closed": closed, "verifiedClosed": verified_closed,
                "inventoryStatus": inventory_status,
                "realizedVerificationStatus": "verified" if inventory_complete else "inventory_incomplete",
                "unrealizedVerificationStatus": "not_applicable" if not has_open else (
                    "verified" if inventory_complete and mark_status == "fresh" else "unverified"
                ),
                "lastMarketObservedAt": observed_at, "marketAgeSeconds": market_age,
                "markStatus": mark_status, "markSource": latest_market["source"] if latest_market else None,
                "earlyEntry": early, "reboundEntry": rebound,
                "collapsedFromPeak": bool(peak_cap and current_cap <= peak_cap * Decimal("0.1")),
                "zeroThenRecovered": rebound,
            })

        closed_count = wins + losses
        buy_rows = [row for row in rows if row["side"] == "buy"]
        active_days = len({str(row["executed_at"])[:10] for row in buy_rows})
        avg_buy = total_buy / max(1, len(buy_rows))
        win_rate = Decimal(wins) / closed_count if closed_count else None
        if closed_count:
            z = 1.959963984540054
            p = wins / closed_count
            denominator = 1 + z * z / closed_count
            center = (p + z * z / (2 * closed_count)) / denominator
            margin = z * math.sqrt((p * (1 - p) + z * z / (4 * closed_count)) / closed_count) / denominator
            win_rate_interval: list[float] | None = [round(max(0, center - margin), 4), round(min(1, center + margin), 4)]
        else:
            win_rate_interval = None
        equity = peak = max_drawdown = Decimal(0)
        for _, outcome in sorted(closed_outcomes):
            equity += outcome
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)
        data_age = _age_seconds(latest_market_at, now) if latest_market_at else None
        data_fresh = bool(latest_market_at and all_marks_fresh)
        mark_status = "fresh" if data_fresh else "missing" if latest_market_at is None else "stale"
        historically_verified = (
            all_inventory_complete
            and closed_count >= int(self.settings["minimum_closed_tokens"])
            and len(grouped) >= int(self.settings["minimum_distinct_tokens"])
        )
        verified = historically_verified and data_fresh
        unrealized_verified = all_inventory_complete and (not has_open_inventory or all_marks_fresh)
        aggregate_unrealized: Decimal | None = estimated_unrealized if (not has_open_inventory or all_marks_fresh) else None
        aggregate_total = realized + aggregate_unrealized if aggregate_unrealized is not None and all_inventory_complete else None
        roi = aggregate_total / total_buy if aggregate_total is not None and total_buy else None
        early_rate = sum(int(item["earlyEntry"]) for item in token_results) / max(1, len(token_results))
        rebound_rate = sum(int(item["reboundEntry"]) for item in token_results) / max(1, len(token_results))
        daily_rate = len(buy_rows) / max(1, active_days)
        if daily_rate >= 10 and (win_rate or Decimal(0)) >= Decimal("0.55"):
            style = "pvp_verified"
        elif early_rate >= 0.35 and (win_rate or Decimal(0)) >= Decimal("0.55"):
            style = "early_alpha"
        elif rebound_rate >= 0.25:
            style = "rebound_specialist"
        elif avg_buy >= 5000:
            style = "whale_confirmation"
        else:
            style = "unclassified"
        inventory_status = "complete" if all_inventory_complete else "inventory_incomplete"
        performance_status = (
            "verified" if verified else "inventory_incomplete" if not all_inventory_complete
            else "stale_verified_history" if historically_verified and not data_fresh
            else "market_missing" if latest_market_at is None else "market_stale" if not data_fresh
            else "insufficient_closed_round_trips"
        )
        realized_status = "verified" if all_inventory_complete else "inventory_incomplete"
        unrealized_status = "verified" if unrealized_verified else "unverified"
        as_of = now.isoformat()
        return {
            "kolId": kol_id, "handle": rows[-1]["handle"], "wallet": rows[-1]["wallet_key"],
            "wallets": sorted({str(row["wallet_key"]) for row in rows}),
            "chains": sorted({int(row["chain_id"]) for row in rows}),
            "verifiedFills": len(rows), "tokens": len(grouped), "closedTokens": closed_count,
            "wins": wins, "losses": losses, "winRate": float(win_rate) if win_rate is not None else None,
            "winRateConfidence95": win_rate_interval, "realizedPnlUsd": float(realized),
            "unrealizedPnlUsd": float(aggregate_unrealized) if aggregate_unrealized is not None else None,
            "estimatedUnrealizedPnlUsd": float(estimated_unrealized) if has_open_inventory else 0.0,
            "totalPnlUsd": float(aggregate_total) if aggregate_total is not None else None,
            "roi": float(roi) if roi is not None else None, "maxDrawdownUsd": float(max_drawdown),
            "observedBuyUsd": float(total_buy), "observedSellUsd": float(total_sell),
            "averageBuyUsd": float(avg_buy), "activeDays": active_days,
            "earlyEntryRate": round(early_rate, 4), "reboundEntryRate": round(rebound_rate, 4),
            "earlyClosedWins": early_wins, "reboundClosedWins": rebound_wins,
            "historicallyVerified": historically_verified, "performanceVerified": verified,
            "realizedVerified": all_inventory_complete,
            "unrealizedVerified": unrealized_verified,
            "latestFillAt": str(rows[-1]["executed_at"]), "lastMarketObservedAt": latest_market_at,
            "marketAgeSeconds": data_age, "markStatus": mark_status, "inventoryStatus": inventory_status,
            "realizedVerificationStatus": realized_status,
            "unrealizedVerificationStatus": unrealized_status,
            "dataAgeSeconds": data_age, "dataFresh": data_fresh,
            "sampleConfidence": "high" if closed_count >= 30 else "medium" if closed_count >= 10 else "low",
            "performanceStatus": performance_status, "primaryStyle": style,
            "source": "verified_wallet_chain_fills", "asOf": as_of,
            "freshness": {"lastMarketObservedAt": latest_market_at, "marketAgeSeconds": data_age, "markStatus": mark_status},
            "verificationStatus": "verified" if verified else performance_status,
            "metrics": {
                "realizedPnl": {"valueUsd": float(realized), "source": "verified_wallet_chain_fills",
                    "asOf": str(rows[-1]["executed_at"]), "freshness": "transaction_finality",
                    "verificationStatus": realized_status},
                "unrealizedPnl": {"valueUsd": float(aggregate_unrealized) if aggregate_unrealized is not None else None,
                    "source": "independent_market_observations", "asOf": latest_market_at,
                    "freshness": mark_status, "verificationStatus": unrealized_status},
            },
            "socialIdentities": [{"platform": row["platform"], "handle": row["account_handle"],
                "profileUrl": row["profile_url"], "confidence": float(row["confidence"])} for row in socials],
            "socialIdentityStatus": "verified" if socials else "not_available",
            "tokensDetail": sorted(token_results, key=lambda item: item["firstBoughtAt"] or "", reverse=True),
        }

    def _compute_snapshot(self, limit: int = 1000) -> dict[str, Any]:
        """Explicit full rebuild hook for maintenance; HTTP requests do not call it."""

        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._rebuild_profiles(str(row[0]) for row in self.db.execute("SELECT DISTINCT kol_id FROM verified_fills"))
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return self.snapshot(limit)


def performance_snapshot(path: str | Path, limit: int = 200, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    store = VerifiedPerformanceStore(path, settings)
    try:
        return store.snapshot(limit)
    finally:
        store.close()
