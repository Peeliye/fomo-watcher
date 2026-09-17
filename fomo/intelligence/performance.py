"""Verified wallet performance reconstructed from normalized on-chain fills.

Only rows backed by a transaction identifier and an explicit valuation are
eligible for performance statistics.  This keeps Fomo feed observations and
chain-derived PnL strictly separated.
"""

from __future__ import annotations

import hashlib
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


MICROS = Decimal("1000000")

DEFAULT_SETTINGS: dict[str, Any] = {
    "minimum_closed_tokens": 3,
    "minimum_distinct_tokens": 3,
    "maximum_data_age_seconds": 86400,
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS verified_fills (
  fill_id TEXT PRIMARY KEY,
  tx_hash TEXT NOT NULL,
  instruction_index INTEGER NOT NULL DEFAULT 0,
  kol_id TEXT NOT NULL,
  handle TEXT NOT NULL,
  wallet TEXT NOT NULL,
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('buy','sell')),
  token_quantity TEXT NOT NULL,
  gross_usd_micros INTEGER NOT NULL,
  fee_usd_micros INTEGER NOT NULL DEFAULT 0,
  price_usd TEXT NOT NULL,
  market_cap_usd_micros INTEGER,
  executed_at TEXT NOT NULL,
  source TEXT NOT NULL,
  source_confidence TEXT NOT NULL,
  raw_payload_hash TEXT NOT NULL,
  UNIQUE(chain_id, tx_hash, instruction_index, token_address, side)
);
CREATE INDEX IF NOT EXISTS idx_verified_fills_kol_time
  ON verified_fills(kol_id, executed_at ASC);
CREATE INDEX IF NOT EXISTS idx_verified_fills_token_time
  ON verified_fills(chain_id, token_address, executed_at ASC);

CREATE TABLE IF NOT EXISTS token_market_history (
  chain_id INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  price_usd TEXT NOT NULL,
  market_cap_usd_micros INTEGER,
  liquidity_usd_micros INTEGER,
  source TEXT NOT NULL,
  PRIMARY KEY(chain_id, token_address, observed_at, source)
);
CREATE INDEX IF NOT EXISTS idx_market_token_time
  ON token_market_history(chain_id, token_address, observed_at ASC);

CREATE TABLE IF NOT EXISTS social_identities (
  kol_id TEXT NOT NULL,
  platform TEXT NOT NULL,
  account_handle TEXT NOT NULL,
  profile_url TEXT,
  evidence_reference TEXT NOT NULL,
  confidence TEXT NOT NULL,
  verified_at TEXT NOT NULL,
  PRIMARY KEY(kol_id, platform, account_handle)
);
PRAGMA user_version=1;
"""


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value if value is not None else 0))
        return result if result.is_finite() else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _micros(value: Any) -> int:
    return int((_decimal(value) * MICROS).quantize(Decimal("1")))


def _usd(value: int) -> float:
    return float((Decimal(value) / MICROS).quantize(Decimal("0.000001")))


def _time(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _hash(raw: Any) -> str:
    text = str(raw or "")
    return text if len(text) == 64 else hashlib.sha256(text.encode("utf-8")).hexdigest()


class VerifiedPerformanceStore:
    """WAL-backed store for normalized, transaction-proven wallet history."""

    def __init__(self, path: str | Path, settings: dict[str, Any] | None = None):
        self.path = Path(path)
        self.settings = {**DEFAULT_SETTINGS, **(settings or {})}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def ingest_fills(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        for row in rows:
            side = str(row.get("side") or "").lower()
            tx_hash = str(row.get("txHash") or row.get("signature") or "").strip()
            qty, gross = _decimal(row.get("tokenQuantity")), _decimal(row.get("grossUsd"))
            confidence = _decimal(row.get("sourceConfidence", 1))
            if side not in {"buy", "sell"} or not tx_hash or qty <= 0 or gross <= 0 or confidence < Decimal("0.8"):
                continue
            chain_id = int(row.get("chainId") or 0)
            token = str(row.get("tokenAddress") or "").strip()
            executed_at = _time(row.get("executedAt"))
            index = int(row.get("instructionIndex") or 0)
            fill_id = str(row.get("fillId") or hashlib.sha256(
                f"{chain_id}|{tx_hash}|{index}|{token}|{side}".encode("utf-8")
            ).hexdigest())
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO verified_fills
                   (fill_id,tx_hash,instruction_index,kol_id,handle,wallet,chain_id,
                    token_address,symbol,side,token_quantity,gross_usd_micros,
                    fee_usd_micros,price_usd,market_cap_usd_micros,executed_at,
                    source,source_confidence,raw_payload_hash)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    fill_id, tx_hash, index, str(row.get("kolId") or ""),
                    str(row.get("handle") or "unknown"), str(row.get("wallet") or ""),
                    chain_id, token, str(row.get("symbol") or "UNKNOWN"), side,
                    str(qty), _micros(gross), _micros(row.get("feeUsd")),
                    str(_decimal(row.get("priceUsd")) or gross / qty),
                    _micros(row.get("marketCapUsd")) if row.get("marketCapUsd") is not None else None,
                    executed_at, str(row.get("source") or "normalized_chain_indexer"),
                    str(confidence), _hash(row.get("rawPayloadHash") or tx_hash),
                ),
            )
            inserted += cursor.rowcount
        self.db.commit()
        return inserted

    def ingest_market_history(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        for row in rows:
            chain_id, token = int(row.get("chainId") or 0), str(row.get("tokenAddress") or "").strip()
            if not chain_id or not token:
                continue
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO token_market_history
                   (chain_id,token_address,observed_at,price_usd,market_cap_usd_micros,
                    liquidity_usd_micros,source) VALUES(?,?,?,?,?,?,?)""",
                (
                    chain_id, token, _time(row.get("observedAt")), str(_decimal(row.get("priceUsd"))),
                    _micros(row.get("marketCapUsd")) if row.get("marketCapUsd") is not None else None,
                    _micros(row.get("liquidityUsd")) if row.get("liquidityUsd") is not None else None,
                    str(row.get("source") or "market_history"),
                ),
            )
            inserted += cursor.rowcount
        self.db.commit()
        return inserted

    def ingest_social_identities(self, rows: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        for row in rows:
            kol_id = str(row.get("kolId") or "").strip()
            platform = str(row.get("platform") or "").strip().lower()
            handle = str(row.get("accountHandle") or "").strip()
            evidence = str(row.get("evidenceReference") or "").strip()
            confidence = _decimal(row.get("confidence"))
            if not kol_id or not platform or not handle or not evidence or confidence < Decimal("0.8"):
                continue
            cursor = self.db.execute(
                """INSERT OR REPLACE INTO social_identities
                   (kol_id,platform,account_handle,profile_url,evidence_reference,confidence,verified_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (kol_id, platform, handle, row.get("profileUrl"), evidence, str(confidence), _time(row.get("verifiedAt"))),
            )
            inserted += int(cursor.rowcount > 0)
        self.db.commit()
        return inserted

    def snapshot(self, limit: int = 200) -> dict[str, Any]:
        fills = self.db.execute("SELECT * FROM verified_fills ORDER BY executed_at ASC, rowid ASC").fetchall()
        markets = self.db.execute("SELECT * FROM token_market_history ORDER BY observed_at ASC").fetchall()
        socials = self.db.execute("SELECT * FROM social_identities ORDER BY platform,account_handle").fetchall()
        market_by_token: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
        for row in markets:
            market_by_token[(int(row["chain_id"]), str(row["token_address"]))].append(row)
        social_by_kol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in socials:
            social_by_kol[str(row["kol_id"])].append({
                "platform": row["platform"], "handle": row["account_handle"],
                "profileUrl": row["profile_url"], "confidence": float(row["confidence"]),
            })
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in fills:
            grouped[str(row["kol_id"])].append(row)

        profiles = []
        for kol_id, rows in grouped.items():
            tokens: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
            for row in rows:
                tokens[(int(row["chain_id"]), str(row["token_address"]))].append(row)
            token_results = []
            closed_outcomes: list[tuple[str, Decimal]] = []
            total_buy = total_sell = realized = unrealized = Decimal(0)
            closed_wins = closed_losses = early_wins = rebound_wins = 0
            for key, token_rows in tokens.items():
                qty = cost = token_realized = Decimal(0)
                first_buy = next((row for row in token_rows if row["side"] == "buy"), None)
                for row in token_rows:
                    row_qty = _decimal(row["token_quantity"])
                    gross = Decimal(row["gross_usd_micros"]) / MICROS
                    fee = Decimal(row["fee_usd_micros"]) / MICROS
                    if row["side"] == "buy":
                        qty += row_qty
                        cost += gross + fee
                        total_buy += gross + fee
                    else:
                        sold = min(qty, row_qty)
                        allocated = cost * sold / qty if qty > 0 else Decimal(0)
                        proceeds = gross - fee
                        token_realized += proceeds - allocated
                        realized += proceeds - allocated
                        total_sell += proceeds
                        qty -= sold
                        cost -= allocated
                history = market_by_token.get(key, [])
                latest_price = _decimal(history[-1]["price_usd"]) if history else _decimal(token_rows[-1]["price_usd"])
                token_unrealized = qty * latest_price - cost
                unrealized += token_unrealized
                entry_cap = Decimal(first_buy["market_cap_usd_micros"] or 0) / MICROS if first_buy else Decimal(0)
                caps = [Decimal(row["market_cap_usd_micros"]) / MICROS for row in history if row["market_cap_usd_micros"]]
                peak_cap = max(caps + ([entry_cap] if entry_cap else []), default=Decimal(0))
                current_cap = caps[-1] if caps else Decimal(0)
                pre_entry_caps = [
                    Decimal(row["market_cap_usd_micros"]) / MICROS for row in history
                    if row["market_cap_usd_micros"] and first_buy and row["observed_at"] < first_buy["executed_at"]
                ]
                prior_peak = max(pre_entry_caps, default=Decimal(0))
                prior_low = min(pre_entry_caps[-20:], default=Decimal(0)) if pre_entry_caps else Decimal(0)
                early = bool(entry_cap and peak_cap and entry_cap <= peak_cap * Decimal("0.2"))
                rebound = bool(prior_peak and prior_low <= prior_peak * Decimal("0.2") and entry_cap >= prior_low * Decimal("1.25"))
                closed = qty <= Decimal("0.000000000001")
                pnl = token_realized + token_unrealized
                if closed:
                    closed_wins += int(token_realized > 0)
                    closed_losses += int(token_realized <= 0)
                    early_wins += int(early and token_realized > 0)
                    rebound_wins += int(rebound and token_realized > 0)
                    closed_outcomes.append((str(token_rows[-1]["executed_at"]), token_realized))
                token_results.append({
                    "chainId": key[0], "tokenAddress": key[1], "symbol": token_rows[-1]["symbol"],
                    "firstBoughtAt": first_buy["executed_at"] if first_buy else None,
                    "entryMarketCapUsd": float(entry_cap) if entry_cap else None,
                    "peakMarketCapUsd": float(peak_cap) if peak_cap else None,
                    "currentMarketCapUsd": float(current_cap) if current_cap else None,
                    "entryToPeakMultiple": float(peak_cap / entry_cap) if entry_cap and peak_cap else None,
                    "realizedPnlUsd": float(token_realized), "unrealizedPnlUsd": float(token_unrealized),
                    "totalPnlUsd": float(pnl), "closed": closed, "earlyEntry": early,
                    "reboundEntry": rebound,
                    "collapsedFromPeak": bool(peak_cap and current_cap <= peak_cap * Decimal("0.1")),
                    "zeroThenRecovered": rebound,
                })
            closed_count = closed_wins + closed_losses
            buy_rows = [row for row in rows if row["side"] == "buy"]
            active_days = len({str(row["executed_at"])[:10] for row in buy_rows})
            avg_buy = total_buy / max(1, len(buy_rows))
            win_rate = Decimal(closed_wins) / closed_count if closed_count else None
            latest_fill_at = str(rows[-1]["executed_at"])
            latest_dt = datetime.fromisoformat(latest_fill_at.replace("Z", "+00:00"))
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            data_age = max(0, int((datetime.now(timezone.utc) - latest_dt.astimezone(timezone.utc)).total_seconds()))
            data_fresh = data_age <= int(self.settings["maximum_data_age_seconds"])
            historically_verified = (
                closed_count >= int(self.settings["minimum_closed_tokens"])
                and len(tokens) >= int(self.settings["minimum_distinct_tokens"])
            )
            verified = historically_verified and data_fresh
            if closed_count:
                z = 1.959963984540054
                p = closed_wins / closed_count
                denominator = 1 + z * z / closed_count
                center = (p + z * z / (2 * closed_count)) / denominator
                margin = z * math.sqrt((p * (1 - p) + z * z / (4 * closed_count)) / closed_count) / denominator
                win_rate_interval = [round(max(0, center - margin), 4), round(min(1, center + margin), 4)]
            else:
                win_rate_interval = None
            equity = peak = Decimal(0)
            max_drawdown = Decimal(0)
            for _, outcome in sorted(closed_outcomes):
                equity += outcome
                peak = max(peak, equity)
                max_drawdown = max(max_drawdown, peak - equity)
            total_pnl = realized + unrealized
            roi = total_pnl / total_buy if total_buy else None
            early_rate = sum(int(row["earlyEntry"]) for row in token_results) / max(1, len(token_results))
            rebound_rate = sum(int(row["reboundEntry"]) for row in token_results) / max(1, len(token_results))
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
            profiles.append({
                "kolId": kol_id, "handle": rows[-1]["handle"], "wallet": rows[-1]["wallet"],
                "chains": sorted({int(row["chain_id"]) for row in rows}), "verifiedFills": len(rows),
                "tokens": len(tokens), "closedTokens": closed_count, "wins": closed_wins,
                "losses": closed_losses, "winRate": float(win_rate) if win_rate is not None else None,
                "winRateConfidence95": win_rate_interval,
                "realizedPnlUsd": float(realized), "unrealizedPnlUsd": float(unrealized),
                "totalPnlUsd": float(total_pnl), "roi": float(roi) if roi is not None else None,
                "maxDrawdownUsd": float(max_drawdown), "observedBuyUsd": float(total_buy),
                "observedSellUsd": float(total_sell), "averageBuyUsd": float(avg_buy),
                "activeDays": active_days, "earlyEntryRate": round(early_rate, 4),
                "reboundEntryRate": round(rebound_rate, 4), "earlyClosedWins": early_wins,
                "reboundClosedWins": rebound_wins, "historicallyVerified": historically_verified,
                "performanceVerified": verified, "latestFillAt": latest_fill_at,
                "dataAgeSeconds": data_age, "dataFresh": data_fresh,
                "sampleConfidence": "high" if closed_count >= 30 else "medium" if closed_count >= 10 else "low",
                "performanceStatus": "verified" if verified else "stale_verified_history" if historically_verified else "insufficient_closed_round_trips",
                "primaryStyle": style, "socialIdentities": social_by_kol.get(kol_id, []),
                "tokensDetail": sorted(token_results, key=lambda item: item["firstBoughtAt"] or "", reverse=True),
            })
        profiles.sort(key=lambda item: (item["performanceVerified"], item["totalPnlUsd"]), reverse=True)
        return {
            "performanceVerified": any(row["performanceVerified"] for row in profiles),
            "verifiedFills": len(fills), "marketObservations": len(markets),
            "profiled": len(profiles), "verifiedProfiles": sum(int(row["performanceVerified"]) for row in profiles),
            "profiles": profiles[:max(1, min(int(limit), 1000))],
        }


def performance_snapshot(path: str | Path, limit: int = 200, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    store = VerifiedPerformanceStore(path, settings)
    try:
        return store.snapshot(limit)
    finally:
        store.close()
