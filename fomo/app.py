from __future__ import annotations

import argparse
import atexit
import base64
import json
import hashlib
import logging
import os
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import keyring
import yaml
from dotenv import dotenv_values, load_dotenv
from keyring.errors import PasswordDeleteError

PROJECT_DIR = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_DIR / ".env"
SESSION_ENV_PATH = PROJECT_DIR / "data" / ".fomo-session.env"
load_dotenv(DOTENV_PATH)
load_dotenv(SESSION_ENV_PATH, override=True)
from curl_cffi import requests as cf
from fomo.execution.journal import ExecutionJournal
from fomo.execution.fast_path import evaluate_copy_buy
from fomo.execution.networks import apply_network_settings
from fomo.audit import append_ndjson
from fomo.intelligence.profile import WalletIntelligenceStore
from fomo.intelligence.leaderboard_scheduler import LeaderboardScheduler
from fomo.monitoring import BackgroundMonitors
from fomo.portfolio.ledger import PortfolioLedger
from fomo.risk.engine import RiskContext
from fomo.risk.pipeline import RiskPipeline
from fomo.web.server import start_dashboard

API_BASE = "https://prod-api.fomo.family"
AUTH_BASE = "https://auth.privy.io"
CHAINS = "1,56,4663,5042,8453,1399811149"
CHAIN_NAMES = {1: "eth", 56: "bsc", 137: "polygon", 8453: "base", 4663: "robinhood", 5042: "arc", 1399811149: "sol"}
FOMO_CHAIN_NAMES = {1: "ethereum", 56: "bnb", 137: "polygon", 8453: "base", 4663: "robinhood", 5042: "arc", 1399811149: "solana"}
CHAIN_LABELS = {1: "ETH", 56: "BSC", 137: "POLYGON", 8453: "BASE", 4663: "RH", 5042: "ARC", 1399811149: "SOL"}
KEYRING_SERVICE = "codex-fomo-watcher"
FEED_TYPES = (
    "large_buy", "large_sell", "large_transfer_in", "large_transfer_out",
    "single_user_transfer_out", "single_user_sell", "single_user_buy",
    "thesis_created", "multi_user_buy", "multi_user_sell", "whale_move", "token_deploy",
)
STABLE_SYMBOLS = {"USDC", "USDT", "DAI", "USD1", "USDE", "USDS"}


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temp_path, path)


def publish_fast_executor_config(cfg: dict[str, Any]) -> None:
    settings = cfg.get("copy_trading", {})
    _atomic_json(PROJECT_DIR / "data" / "fast-executor-config.json", {
        "enabled": bool(settings.get("enabled", False) and settings.get("mode") == "paper"),
        "networkIds": [int(value) for value in settings.get("network_ids", [])],
        "eventTypes": list(settings.get("event_types", [])),
        "activeBuyEventTypes": list(settings.get("active_buy_event_types", ["swap_buy", "single_user_buy"])),
        "fixedUsd": float(settings.get("fixed_usd", 0)),
        "minTargetBuyUsd": float(settings.get("min_target_buy_usd", 0)),
        "maxSignalAgeSeconds": float(settings.get("max_signal_age_seconds", 5)),
        "minMarketCapUsd": float(settings.get("min_market_cap_usd", 0)),
        "deferAssetChecks": bool(settings.get("defer_asset_checks", True)),
        "requireTradeIdInLive": bool(settings.get("require_trade_id_in_live", True)),
        "whitelistMaxAgeSeconds": int(settings.get("whitelist_max_age_seconds", 600)),
        "mode": "shadow",
        "updatedAt": time.time() * 1000,
    })


def publish_following_ids(following_ids: set[str]) -> None:
    _atomic_json(PROJECT_DIR / "data" / "following-ids.json", {
        "followingIds": sorted(following_ids),
        "updatedAt": time.time() * 1000,
    })


def _persist_session_tokens(access: str, refresh: str) -> None:
    """Persist Privy's rotated session without printing either secret."""
    env_path = SESSION_ENV_PATH
    env_path.parent.mkdir(parents=True, exist_ok=True)
    replacements = {
        "FOMO_ACCESS_TOKEN": access,
        "FOMO_REFRESH_TOKEN": refresh,
    }
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    seen: set[str] = set()
    output: list[str] = []
    for line in lines:
        key = line.split("=", 1)[0].strip() if "=" in line else ""
        if key in replacements:
            output.append(f"{key}={replacements[key]}")
            seen.add(key)
        else:
            output.append(line)
    for key, value in replacements.items():
        if key not in seen:
            output.append(f"{key}={value}")
    temp_path = env_path.with_name(".env.session.tmp")
    temp_path.write_text("\n".join(output) + "\n", encoding="utf-8")
    os.replace(temp_path, env_path)


def _jwt_payload(token: str) -> dict[str, Any]:
    part = token.split(".")[1]
    part += "=" * (-len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(part))


class FomoClient:
    def __init__(self) -> None:
        self.access = (os.getenv("FOMO_ACCESS_TOKEN") or keyring.get_password(KEYRING_SERVICE, "access_token") or "").strip()
        self.refresh = (os.getenv("FOMO_REFRESH_TOKEN") or keyring.get_password(KEYRING_SERVICE, "refresh_token") or "").strip()
        self._reload_newer_local_session()
        if not self.access:
            raise RuntimeError("缺少 FOMO_ACCESS_TOKEN")
        self.session = cf.Session(impersonate="chrome")

    def _reload_newer_local_session(self) -> None:
        """Use the newest JWT from .env or the browser-managed session file."""
        current_exp = 0
        try:
            current_exp = int(_jwt_payload(self.access).get("exp", 0))
        except Exception:
            pass
        for path in (DOTENV_PATH, SESSION_ENV_PATH):
            if not path.exists():
                continue
            shared = dotenv_values(path)
            candidate = str(shared.get("FOMO_ACCESS_TOKEN") or "").strip().strip('"')
            try:
                candidate_exp = int(_jwt_payload(candidate).get("exp", 0))
                if candidate and candidate_exp > current_exp:
                    self.access = candidate
                    self.refresh = str(shared.get("FOMO_REFRESH_TOKEN") or self.refresh).strip().strip('"')
                    current_exp = candidate_exp
            except Exception:
                pass

    def _ensure_token(self) -> None:
        # Token rotation belongs exclusively to the headless Privy browser. This
        # process only consumes the browser-managed access token.
        self._reload_newer_local_session()
        if self.access:
            try:
                if _jwt_payload(self.access).get("exp", 0) - time.time() >= 60:
                    return
            except Exception:
                pass
        raise RuntimeError("等待无窗口 Privy 客户端刷新 access token")

    def get(self, path: str, params: Any = None) -> Any:
        self._ensure_token()
        headers = {
            "Authorization": f"Bearer {self.access}", "X-Supported-Chains": CHAINS,
            "Content-Type": "application/json", "Origin": "https://fomo.family",
            "Referer": "https://fomo.family/",
        }
        response = self.session.get(f"{API_BASE}{path}", params=params, headers=headers, timeout=30)
        if response.status_code in (401, 430, 431):
            raise RuntimeError(f"Fomo 鉴权或风控失败（HTTP {response.status_code}）")
        response.raise_for_status()
        return response.json()["responseObject"]


class State:
    def __init__(self, path: str) -> None:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        existed = db_path.exists() and db_path.stat().st_size > 0
        self.path = db_path
        self.db = sqlite3.connect(db_path, timeout=5)
        self.db.row_factory = sqlite3.Row
        version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
        if existed and version < 3:
            backup_dir = db_path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            target = sqlite3.connect(backup_dir / f"{db_path.stem}.pre-v3.from-v{version}.{stamp}.sqlite3")
            try:
                self.db.backup(target)
            finally:
                target.close()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sent (id TEXT PRIMARY KEY, at INTEGER NOT NULL)")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS event_inbox (
          event_id TEXT PRIMARY KEY,
          source TEXT NOT NULL,
          source_path TEXT,
          start_offset INTEGER,
          end_offset INTEGER,
          event_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at REAL NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at REAL NOT NULL,
          completed_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_event_inbox_pending
          ON event_inbox(status,next_attempt_at,created_at);
        CREATE INDEX IF NOT EXISTS idx_event_inbox_offsets
          ON event_inbox(source_path,start_offset,end_offset);
        CREATE TABLE IF NOT EXISTS sidecar_records (
          source_path TEXT NOT NULL,
          start_offset INTEGER NOT NULL,
          end_offset INTEGER NOT NULL,
          event_id TEXT,
          status TEXT NOT NULL,
          PRIMARY KEY(source_path,start_offset)
        );
        CREATE TABLE IF NOT EXISTS notification_outbox (
          event_id TEXT NOT NULL,
          channel TEXT NOT NULL,
          event_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at REAL NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at REAL NOT NULL,
          sent_at REAL,
          PRIMARY KEY(event_id,channel)
        );
        CREATE INDEX IF NOT EXISTS idx_notification_outbox_pending
          ON notification_outbox(status,next_attempt_at,created_at);
        CREATE TABLE IF NOT EXISTS post_trade_outbox (
          event_id TEXT PRIMARY KEY,
          event_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at REAL NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at REAL NOT NULL,
          completed_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_post_trade_outbox_pending
          ON post_trade_outbox(status,next_attempt_at,created_at);
        """)
        self.db.execute("PRAGMA user_version=3")
        self.db.commit()

    def load(self, key: str, default: Any) -> Any:
        row = self.db.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def save(self, key: str, value: Any) -> None:
        self.db.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (key, json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def first_seen(self, event_id: str) -> bool:
        try:
            self.db.execute("INSERT INTO sent(id,at) VALUES(?,?)", (event_id, int(time.time())))
            self.db.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def was_sent(self, event_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM sent WHERE id=?", (event_id,)).fetchone() is not None

    def mark_sent(self, event_id: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO sent(id,at) VALUES(?,?)", (event_id, int(time.time())))
        self.db.commit()

    def enqueue_event(
        self,
        event: "Event",
        *,
        source: str,
        source_path: str | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> bool:
        payload = json.dumps(asdict(event), ensure_ascii=False, separators=(",", ":"))
        with self.db:
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO event_inbox
                   (event_id,source,source_path,start_offset,end_offset,event_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (event.id, source, source_path, start_offset, end_offset, payload, time.time()),
            )
        return cursor.rowcount == 1

    def enqueue_sidecar_line(self, path: str, start: int, end: int, events: list["Event"]) -> None:
        now = time.time()
        with self.db:
            if not events:
                self.db.execute(
                    "INSERT OR IGNORE INTO sidecar_records VALUES(?,?,?,?, 'done')",
                    (path, start, end, None),
                )
            for event in events:
                payload = json.dumps(asdict(event), ensure_ascii=False, separators=(",", ":"))
                self.db.execute(
                    """INSERT OR IGNORE INTO event_inbox
                       (event_id,source,source_path,start_offset,end_offset,event_json,created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (event.id, "sidecar", path, start, end, payload, now),
                )
                existing = self.db.execute(
                    "SELECT status FROM event_inbox WHERE event_id=?", (event.id,)
                ).fetchone()
                line_status = "done" if existing and existing[0] == "done" else "pending"
                self.db.execute(
                    "INSERT OR IGNORE INTO sidecar_records VALUES(?,?,?,?,?)",
                    (path, start, end, event.id, line_status),
                )
            self.db.execute(
                "INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)",
                (f"sidecar_scan_offset:{path}", json.dumps(end)),
            )
        self._advance_sidecar_offset(path)

    def _advance_sidecar_offset(self, path: str) -> None:
        committed = int(self.load(f"sidecar_offset:{path}", 0))
        while True:
            row = self.db.execute(
                """SELECT end_offset,status FROM sidecar_records
                   WHERE source_path=? AND start_offset=?""",
                (path, committed),
            ).fetchone()
            if not row or row[1] != "done":
                break
            committed = int(row[0])
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)",
                (f"sidecar_offset:{path}", json.dumps(committed)),
            )
            self.db.execute(
                "INSERT OR REPLACE INTO kv(k,v) VALUES('sidecar_offset',?)", (json.dumps(committed),)
            )

    def pending_events(self, limit: int = 100) -> list[sqlite3.Row]:
        return self.db.execute(
            """SELECT * FROM event_inbox
               WHERE status IN ('pending','retry') AND next_attempt_at<=?
               ORDER BY created_at,event_id LIMIT ?""",
            (time.time(), max(1, min(int(limit), 1000))),
        ).fetchall()

    def complete_event(self, event_id: str) -> None:
        now = time.time()
        with self.db:
            self.db.execute(
                "UPDATE event_inbox SET status='done',completed_at=?,last_error=NULL WHERE event_id=?",
                (now, event_id),
            )
            row = self.db.execute(
                "SELECT source_path FROM event_inbox WHERE event_id=?", (event_id,)
            ).fetchone()
            if row and row[0]:
                path = str(row[0])
                self.db.execute(
                    "UPDATE sidecar_records SET status='done' WHERE source_path=? AND event_id=?",
                    (path, event_id),
                )
        if row and row[0]:
            self._advance_sidecar_offset(str(row[0]))

    def fail_event(self, event_id: str, error: BaseException, max_retries: int = 20) -> None:
        row = self.db.execute("SELECT attempts FROM event_inbox WHERE event_id=?", (event_id,)).fetchone()
        attempts = int(row[0] if row else 0) + 1
        status = "dead" if attempts >= max_retries else "retry"
        delay = min(300.0, 2.0 ** min(attempts, 8))
        # Exception strings from HTTP/SDK layers can embed credential-bearing
        # URLs. Persist the class only; detailed traceback stays out of logs.
        message = type(error).__name__[:120]
        with self.db:
            self.db.execute(
                """UPDATE event_inbox SET status=?,attempts=?,next_attempt_at=?,last_error=?
                   WHERE event_id=?""",
                (status, attempts, time.time() + delay, message, event_id),
            )

    def enqueue_notifications(self, event: "Event", cfg: dict[str, Any]) -> None:
        settings = cfg.get("notifications", {})
        max_age = max(0.0, float(settings.get("max_event_age_seconds", 300)))
        if max_age and _event_age_seconds(event.created_at) > max_age:
            return
        channels = notification_channels(cfg)
        payload = json.dumps(asdict(event), ensure_ascii=False, separators=(",", ":"))
        now = time.time()
        with self.db:
            self.db.executemany(
                """INSERT OR IGNORE INTO notification_outbox
                   (event_id,channel,event_json,created_at) VALUES(?,?,?,?)""",
                [(event.id, channel, payload, now) for channel in channels],
            )

    def enqueue_post_trade(self, event: "Event") -> None:
        payload = json.dumps(asdict(event), ensure_ascii=False, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO post_trade_outbox(event_id,event_json,created_at) VALUES(?,?,?)",
                (event.id, payload, time.time()),
            )

    def complete_post_trade(self, event_id: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE post_trade_outbox SET status='done',completed_at=?,last_error=NULL WHERE event_id=?",
                (time.time(), event_id),
            )

    def prune(self, sent_ttl_days: int = 14, completed_ttl_days: int = 30) -> None:
        now = time.time()
        with self.db:
            self.db.execute("DELETE FROM sent WHERE at<?", (int(now - max(1, sent_ttl_days) * 86400),))
            self.db.execute(
                "DELETE FROM event_inbox WHERE status='done' AND completed_at<?",
                (now - max(1, completed_ttl_days) * 86400,),
            )
            self.db.execute(
                "DELETE FROM post_trade_outbox WHERE status='done' AND completed_at<?",
                (now - max(1, completed_ttl_days) * 86400,),
            )


@dataclass
class Event:
    id: str
    kind: str
    handle: str
    created_at: str
    symbol: str = "UNKNOWN"
    token_name: str = ""
    ca: str = ""
    network_id: int = 0
    amount_tokens: float = 0.0
    amount_usd: float = 0.0
    market_cap: float = 0.0
    price: float = 0.0
    current_position_usd: float = 0.0
    old_amount: float | None = None
    new_amount: float | None = None
    original_text: str = ""
    translated_text: str = ""
    trade_id: str = ""
    copy_note: str = ""
    source_type: str = ""
    user_id: str = ""


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _iso_local(value: str, timezone: str) -> str:
    if not value:
        return datetime.now(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M:%S %Z")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M:%S %Z")


def _event_age_seconds(value: str) -> float:
    if not value:
        return float("inf")
    try:
        created = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - created.astimezone(timezone.utc)).total_seconds())
    except ValueError:
        return float("inf")


def _balance_snapshot(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in data.get("balances", []):
        token_filter = item.get("tokenFilterResult") or {}
        token = token_filter.get("token") or {}
        user_token = item.get("userToken") or {}
        active = item.get("activeTrade") or {}
        ca = token.get("address") or (item.get("balance") or {}).get("tokenAddress") or user_token.get("tokenAddress")
        nid = int(token.get("networkId") or user_token.get("networkId") or 0)
        if not ca:
            continue
        amount = _num(user_token.get("humanAmountRemaining"))
        price = _num(token_filter.get("priceUSD"))
        result[f"{ca}:{nid}"] = {
            "ca": ca, "network_id": nid, "amount": amount, "price": price,
            "value": amount * price, "market_cap": _num(token_filter.get("marketCap")),
            "symbol": token.get("symbol") or "UNKNOWN", "name": token.get("name") or "",
            "trade_id": active.get("id") or "", "holding_since": user_token.get("holdingSince") or "",
        }
    return result


def position_events(handle: str, old: dict[str, Any], new: dict[str, Any], min_tokens: float, min_usd: float) -> list[Event]:
    events: list[Event] = []
    for key in sorted(set(old) | set(new)):
        before, after = old.get(key), new.get(key)
        old_amount = _num(before and before.get("amount"))
        new_amount = _num(after and after.get("amount"))
        delta = new_amount - old_amount
        meta = after or before or {}
        usd = abs(delta) * _num(meta.get("price"))
        if abs(delta) < min_tokens or usd < min_usd:
            continue
        kind = "buy" if delta > 0 else ("clear" if new_amount == 0 else "sell")
        ts = str(meta.get("holding_since") or "") if kind == "buy" else ""
        identity = f"position:{handle}:{key}:{old_amount:.12g}:{new_amount:.12g}"
        events.append(Event(
            id=identity, kind=kind, handle=handle, created_at=ts,
            symbol=meta.get("symbol", "UNKNOWN"), token_name=meta.get("name", ""),
            ca=meta.get("ca", ""), network_id=int(meta.get("network_id", 0)),
            amount_tokens=abs(delta), amount_usd=usd, market_cap=_num(meta.get("market_cap")),
            old_amount=old_amount, new_amount=new_amount, trade_id=meta.get("trade_id", ""),
        ))
    return events


def _walk_comments(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if isinstance(value.get("comment"), str) and value.get("id"):
            found.append(value)
        for child in value.values():
            found.extend(_walk_comments(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_comments(child))
    return found


def is_probably_english(text: str) -> bool:
    latin = sum(ch.isascii() and ch.isalpha() for ch in text)
    chinese = sum("\u4e00" <= ch <= "\u9fff" for ch in text)
    return latin >= 4 and latin > chinese * 2


def translate(text: str) -> str:
    url, key, model = (os.getenv(k, "").strip() for k in ("TRANSLATION_API_URL", "TRANSLATION_API_KEY", "TRANSLATION_MODEL"))
    if not (url and key and model):
        return ""
    response = cf.post(
        url, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "temperature": 0, "messages": [
            {"role": "system", "content": "将英文加密货币交易观点准确翻译为简体中文，只输出译文，不添加建议。"},
            {"role": "user", "content": text},
        ]}, impersonate="chrome", timeout=30,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def thesis_events(handle: str, spotlight: Any) -> list[Event]:
    output: list[Event] = []
    for comment in _walk_comments(spotlight):
        text = comment.get("comment", "").strip()
        if not text:
            continue
        output.append(Event(
            id=f"thesis:{comment['id']}", kind="thesis", handle=handle,
            created_at=comment.get("createdAt", ""), ca=comment.get("tokenAddress", ""),
            network_id=int(comment.get("networkId") or 0), original_text=text,
            translated_text="",
        ))
    return output


def feed_events(feed_data: Any, allowed_user_ids: set[str] | None = None) -> list[Event]:
    """把登录账号的个性化 Feed 转成统一事件；字段兼容不同 feed 类型。"""
    items = feed_data.get("feed", []) if isinstance(feed_data, dict) else (feed_data if isinstance(feed_data, list) else [])
    events: list[Event] = []
    for item in items:
        body = item.get("body") or {}
        trade = body.get("trade") or {}
        user = body.get("user") or body.get("trader") or {}
        actor_id = item.get("userId") or body.get("userId") or trade.get("userId") or user.get("id")
        # account Feed 会混入推荐/热门用户。启用白名单时，只接受明确属于
        # 当前账号关注用户的事件；缺失 actor id 的事件也不能绕过过滤。
        if allowed_user_ids is not None and str(actor_id or "") not in allowed_user_ids:
            continue
        comment_value = body.get("comment")
        comment = comment_value if isinstance(comment_value, dict) else {}
        trade_comment = item.get("tradeComment") if isinstance(item.get("tradeComment"), dict) else {}
        raw_segments = body.get("shortCommentSegments")
        segments: list[Any] = raw_segments if isinstance(raw_segments, list) else []
        segment_text = "\n".join(
            str(segment.get("text") or "").strip()
            for segment in segments if isinstance(segment, dict) and segment.get("text")
        ).strip()
        kind_raw = str(item.get("type") or body.get("type") or "feed")
        text = (
            comment.get("comment")
            or (comment_value if isinstance(comment_value, str) else "")
            or trade_comment.get("comment")
            or body.get("description")
            or body.get("text")
            or segment_text
            or ""
        )
        if kind_raw == "thesis_created" or text:
            kind = "thesis"
        elif "buy" in kind_raw:
            kind = "buy"
        elif "sell" in kind_raw:
            kind = "sell"
        else:
            # 转账、部署等仍作为 Feed 动态推送，使用“喊单/动态”文本承载原始摘要。
            kind = "thesis"
            text = text or f"Fomo Feed 动态：{kind_raw}"
        if kind == "thesis" and not text:
            text = f"Fomo Feed 动态：{kind_raw}（API 未返回正文）"
        handle = (
            item.get("userHandle") or body.get("userHandle") or trade.get("userHandle")
            or user.get("userHandle") or comment.get("userHandle") or "unknown"
        )
        ca = item.get("tokenAddress") or body.get("tokenAddress") or trade.get("tokenAddress") or comment.get("tokenAddress") or ""
        nid = int(item.get("networkId") or body.get("networkId") or trade.get("networkId") or comment.get("networkId") or 0)
        symbol = item.get("ticker") or item.get("symbol") or body.get("ticker") or body.get("symbol") or (body.get("token") or {}).get("symbol") or "UNKNOWN"
        if "buy" in kind_raw:
            usd = _num(item.get("usdAmount") or item.get("humanUsdAmountIn") or body.get("usdAmount") or body.get("inHumanAmount"))
        elif "sell" in kind_raw:
            usd = _num(item.get("usdAmount") or item.get("humanUsdAmountOut") or body.get("usdAmount") or body.get("outHumanAmount"))
        else:
            usd = _num(item.get("usdAmount") or body.get("humanUsdAmountIn") or body.get("humanUsdAmountOut") or body.get("usdAmount") or body.get("totalVolume"))
        price = _num(item.get("price") or body.get("price"))
        events.append(Event(
            id=f"feed:{item.get('id') or item.get('createdAt')}:{kind_raw}:{handle}", kind=kind,
            handle=str(handle), created_at=item.get("createdAt") or body.get("createdAt") or "",
            symbol=str(symbol), ca=str(ca), network_id=nid, amount_usd=usd,
            amount_tokens=usd / price if price > 0 else 0,
            market_cap=_num(item.get("marketCap") or item.get("fdv") or body.get("marketCap") or body.get("fdv")),
            price=price,
            current_position_usd=_num(item.get("currentSizeUsd") or body.get("currentSizeUsd") or body.get("positionNotionalUsd")), original_text=str(text),
            translated_text="",
            trade_id=str(item.get("tradeId") or trade.get("id") or body.get("tradeId") or ""),
            source_type=kind_raw,
            user_id=str(actor_id or ""),
        ))
    return events


def money(value: float) -> str:
    return f"${value:,.2f}"


def compact_money(value: float) -> str:
    absolute = abs(value)
    for divisor, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if absolute >= divisor:
            return f"${value / divisor:,.2f}{suffix}"
    return money(value)


def event_links(event: Event, cfg: dict[str, Any]) -> list[tuple[str, str]]:
    if not event.ca:
        return []
    chain = CHAIN_NAMES.get(event.network_id, str(event.network_id))
    params = {
        "ca": quote(event.ca, safe=""),
        "chain": chain,
        "fomo_chain": FOMO_CHAIN_NAMES.get(event.network_id, chain),
        "network_id": event.network_id,
        "trade_id": event.trade_id,
    }
    return [(name.upper(), template.format(**params)) for name, template in cfg.get("links", {}).items() if template]


def render(event: Event, cfg: dict[str, Any]) -> str:
    labels = {"buy": "🟢 买入", "sell": "🔴 减仓", "clear": "⚫ 清仓", "thesis": "📣 喊单"}
    lines = [f"{labels[event.kind]}｜@{event.handle}", f"时间：{_iso_local(event.created_at, cfg['timezone'])}"]
    if event.kind == "thesis":
        if event.translated_text:
            lines += [f"中文：{event.translated_text}", f"原文：{event.original_text}"]
        else:
            lines.append(f"内容：{event.original_text}")
    else:
        lines += [f"代币：{event.symbol}" + (f"（{event.token_name}）" if event.token_name else "")]
        if event.amount_usd:
            lines.append(f"金额：{money(event.amount_usd)}")
        if event.old_amount is not None and event.new_amount is not None:
            lines.append(f"数量变化：{event.old_amount:,.6g} → {event.new_amount:,.6g}")
        if event.market_cap:
            lines.append(f"当前市值：{money(event.market_cap)}")
        if event.current_position_usd:
            lines.append(f"当前仓位：{money(event.current_position_usd)}")
    if event.ca:
        lines.append(f"CA：{event.ca}")
        lines.extend(f"{name}: {url}" for name, url in event_links(event, cfg))
    if event.copy_note:
        lines.append(event.copy_note)
    return "\n".join(lines)


def feishu_card(event: Event, cfg: dict[str, Any]) -> dict[str, Any]:
    titles = {"buy": "🟢 买入", "sell": "🔴 减仓", "clear": "⚫ 清仓", "thesis": "📣 喊单"}
    colors = {"buy": "green", "sell": "red", "clear": "grey", "thesis": "blue"}
    when = _iso_local(event.created_at, cfg["timezone"]).replace(" CST", "")
    chain = CHAIN_LABELS.get(event.network_id, str(event.network_id) if event.network_id else "未知链")
    symbol = event.symbol if event.symbol and event.symbol != "UNKNOWN" else "未知币种"
    rows = [f"**@{event.handle}** · {when}"]
    metrics = []
    if event.amount_usd: metrics.append(f"成交 **{compact_money(event.amount_usd)}**")
    if event.market_cap: metrics.append(f"MC **{compact_money(event.market_cap)}**")
    if metrics: rows.append("  ·  ".join(metrics))
    detail = []
    if event.price: detail.append(f"价 ${event.price:,.8g}")
    if event.current_position_usd: detail.append(f"仓位 {compact_money(event.current_position_usd)}")
    if detail: rows.append("  ·  ".join(detail))
    if event.old_amount is not None and event.new_amount is not None:
        rows.append(f"数量 {event.old_amount:,.5g} → {event.new_amount:,.5g}")
    if event.copy_note:
        rows.append(event.copy_note)
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(rows)}}]
    if event.kind == "thesis" and event.original_text:
        content = f"**中文** {event.translated_text}\n**原文** {event.original_text}" if event.translated_text else f"**内容** {event.original_text}"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": content}})
    if event.ca:
        # Inline-code backticks are displayed literally by some Feishu clients.
        # Plain text keeps the address selectable/copyable on desktop and mobile.
        elements.append({"tag": "div", "text": {"tag": "plain_text", "content": f"CA  {event.ca}"}})
    links = event_links(event, cfg)
    if links:
        elements.append({"tag": "action", "actions": [
            {"tag": "button", "text": {"tag": "plain_text", "content": name}, "url": url, "type": "primary" if index == 0 else "default"}
            for index, (name, url) in enumerate(links)
        ]})
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": colors[event.kind], "title": {"tag": "plain_text", "content": f"{titles[event.kind]} · {symbol} · {chain}"}},
            "elements": elements,
        },
    }


def _post(url: str, **kwargs: Any) -> None:
    response = cf.post(url, impersonate="chrome", timeout=20, **kwargs)
    response.raise_for_status()


def notification_channels(cfg: dict[str, Any]) -> list[str]:
    enabled = cfg.get("notifications", {})
    channels = [name for name in ("telegram", "feishu", "generic_webhook")
                if enabled.get(name)]
    return channels or ["console"]


def notify_channel(channel: str, text: str, event: Event, cfg: dict[str, Any]) -> None:
    if channel == "telegram":
        token, chat_id = os.environ["TG_BOT_TOKEN"], os.environ["TG_CHAT_ID"]
        _post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
    elif channel == "feishu":
        _post(os.environ["FEISHU_WEBHOOK_URL"], json=feishu_card(event, cfg))
    elif channel == "generic_webhook":
        _post(os.environ["GENERIC_WEBHOOK_URL"], json={"text": text, "event": asdict(event)})
    elif channel == "console":
        print("\n" + text + "\n")


def notify(text: str, event: Event, cfg: dict[str, Any]) -> None:
    """Compatibility helper; durable delivery uses NotificationWorker."""
    for channel in notification_channels(cfg):
        notify_channel(channel, text, event, cfg)


def notify_once(state: State, event: Event, cfg: dict[str, Any]) -> None:
    """Atomically enqueue one independently retryable record per channel."""
    state.enqueue_notifications(event, cfg)


class NotificationWorker:
    """Deliver the persistent outbox without blocking risk or accounting."""

    def __init__(self, state_path: Path, cfg: dict[str, Any]):
        self.state_path = state_path
        self.cfg = cfg
        settings = cfg.get("notifications", {})
        self.max_retries = max(1, int(settings.get("max_retries", 8)))
        self.base_backoff = max(0.1, float(settings.get("retry_base_seconds", 2)))
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> "NotificationWorker":
        self.thread = threading.Thread(target=self._run, name="notification-outbox", daemon=True)
        self.thread.start()
        return self

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)

    def _run(self) -> None:
        db = sqlite3.connect(self.state_path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        try:
            while not self.stop_event.is_set():
                worked = self._drain(db)
                self.stop_event.wait(0.1 if worked else 1.0)
        finally:
            db.close()

    def _drain(self, db: sqlite3.Connection, limit: int = 20) -> int:
        rows = db.execute(
            """SELECT * FROM notification_outbox
               WHERE status IN ('pending','retry') AND next_attempt_at<=?
               ORDER BY created_at LIMIT ?""",
            (time.time(), limit),
        ).fetchall()
        for row in rows:
            event = Event(**json.loads(row["event_json"]))
            max_age = max(0.0, float(self.cfg.get("notifications", {}).get("max_event_age_seconds", 300)))
            if max_age and _event_age_seconds(event.created_at) > max_age:
                with db:
                    db.execute(
                        """UPDATE notification_outbox SET status='suppressed_stale',
                           attempts=attempts+1,last_error='stale_event'
                           WHERE event_id=? AND channel=?""",
                        (row["event_id"], row["channel"]),
                    )
                continue
            try:
                if event.original_text and not event.translated_text and is_probably_english(event.original_text):
                    event.translated_text = translate(event.original_text)
                notify_channel(str(row["channel"]), render(event, self.cfg), event, self.cfg)
            except Exception as exc:
                attempts = int(row["attempts"]) + 1
                status = "dead" if attempts >= self.max_retries else "retry"
                delay = self.base_backoff * (2 ** min(attempts - 1, 8))
                # Never persist exception strings: HTTP clients often embed credential-bearing URLs.
                error = type(exc).__name__[:120]
                with db:
                    db.execute(
                        """UPDATE notification_outbox SET status=?,attempts=?,next_attempt_at=?,last_error=?
                           WHERE event_id=? AND channel=?""",
                        (status, attempts, time.time() + delay, error, row["event_id"], row["channel"]),
                    )
                logging.warning("通知发送失败，将按退避策略重试：event=%s channel=%s error=%s",
                                row["event_id"], row["channel"], error)
            else:
                with db:
                    db.execute(
                        """UPDATE notification_outbox SET status='sent',attempts=attempts+1,
                           sent_at=?,last_error=NULL WHERE event_id=? AND channel=?""",
                        (time.time(), row["event_id"], row["channel"]),
                    )
        return len(rows)


class PostTradeWorker:
    """Drain durable analytical work on its own connections and thread."""

    def __init__(self, project_dir: Path, state_path: Path, cfg: dict[str, Any]):
        self.project_dir = project_dir
        self.state_path = state_path
        self.cfg = cfg
        settings = cfg.get("post_trade", {})
        self.max_retries = max(1, int(settings.get("max_retries", 20)))
        self.base_backoff = max(0.1, float(settings.get("retry_base_seconds", 1)))
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> "PostTradeWorker":
        self.thread = threading.Thread(target=self._run, name="post-trade-analysis", daemon=True)
        self.thread.start()
        return self

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)

    def _run(self) -> None:
        state = State(str(self.state_path))
        portfolio_settings = self.cfg.get("portfolio", {})
        portfolio_path = Path(str(portfolio_settings.get("database", "data/portfolio.sqlite3")))
        if not portfolio_path.is_absolute():
            portfolio_path = self.project_dir / portfolio_path
        portfolio = PortfolioLedger(
            portfolio_path,
            self.cfg["timezone"],
            str(portfolio_settings.get("account_id", "paper-main")),
        ) if portfolio_settings.get("enabled", True) else None
        risk_pipeline = RiskPipeline(self.project_dir, self.cfg.get("read_only_risk", {}))
        execution_settings = self.cfg.get("execution_journal", {})
        execution_path = Path(str(execution_settings.get("database", "data/execution.sqlite3")))
        if not execution_path.is_absolute():
            execution_path = self.project_dir / execution_path
        execution_journal = ExecutionJournal(
            execution_path, str(portfolio_settings.get("account_id", "paper-main"))
        ) if execution_settings.get("enabled", True) else None
        intelligence_settings = self.cfg.get("smart_money", {})
        intelligence_path = Path(str(intelligence_settings.get("database", "data/wallet-intelligence.sqlite3")))
        if not intelligence_path.is_absolute():
            intelligence_path = self.project_dir / intelligence_path
        intelligence = WalletIntelligenceStore(
            intelligence_path, intelligence_settings
        ) if intelligence_settings.get("enabled", True) else None
        try:
            while not self.stop_event.is_set():
                worked = self._drain(state, risk_pipeline, portfolio, execution_journal, intelligence)
                self.stop_event.wait(0.05 if worked else 0.5)
        finally:
            if intelligence is not None:
                intelligence.close()
            if execution_journal is not None:
                execution_journal.close()
            if portfolio is not None:
                portfolio.close()
            state.db.close()

    def _drain(
        self,
        state: State,
        risk_pipeline: RiskPipeline | None,
        portfolio: PortfolioLedger | None,
        execution_journal: ExecutionJournal | None,
        intelligence: WalletIntelligenceStore | None,
        limit: int = 50,
    ) -> int:
        rows = state.db.execute(
            """SELECT * FROM post_trade_outbox
               WHERE status IN ('pending','retry') AND next_attempt_at<=?
               ORDER BY created_at,event_id LIMIT ?""",
            (time.time(), limit),
        ).fetchall()
        for row in rows:
            event = Event(**json.loads(row["event_json"]))
            try:
                process_post_trade_event(
                    state, event, risk_pipeline, portfolio, execution_journal, intelligence
                )
            except Exception as exc:
                attempts = int(row["attempts"]) + 1
                status = "dead" if attempts >= self.max_retries else "retry"
                delay = self.base_backoff * (2 ** min(attempts - 1, 8))
                with state.db:
                    state.db.execute(
                        """UPDATE post_trade_outbox SET status=?,attempts=?,next_attempt_at=?,last_error=?
                           WHERE event_id=?""",
                        (status, attempts, time.time() + delay, type(exc).__name__[:120], row["event_id"]),
                    )
                logging.warning("买后分析失败，将异步重试：event=%s error=%s", row["event_id"], type(exc).__name__)
            else:
                state.complete_post_trade(str(row["event_id"]))
        return len(rows)


def paper_copy_trade(state: State, event: Event, cfg: dict[str, Any], portfolio: PortfolioLedger | None = None) -> dict[str, Any] | None:
    settings = cfg.get("copy_trading", {})
    if not settings.get("enabled") or settings.get("mode") != "paper" or event.kind != "buy":
        return None
    decision_id = f"paper:{event.id}"
    if state.was_sent(decision_id) or (portfolio is not None and portfolio.has_event(event.id)):
        return None

    gate = evaluate_copy_buy(event, settings)
    age = gate.signal_age_seconds
    reason = gate.status

    local_day = datetime.now(ZoneInfo(cfg["timezone"])).strftime("%Y-%m-%d")
    daily_key = f"paper_spend:{local_day}"
    token_key = f"paper_exposure:{event.network_id}:{event.ca.lower()}"
    # The portfolio is authoritative in normal operation. Legacy state counters
    # are only used when the ledger is explicitly disabled.
    daily_spend = float(state.load(daily_key, 0)) if portfolio is None else 0.0
    token_exposure = float(state.load(token_key, 0)) if portfolio is None else 0.0
    fixed_usd = float(settings.get("fixed_usd", 10))
    daily_remaining = max(0.0, float(settings.get("daily_limit_usd", 100)) - daily_spend)
    token_remaining = max(0.0, float(settings.get("per_token_limit_usd", 20)) - token_exposure)
    order_usd = min(fixed_usd, daily_remaining, token_remaining) if reason == "accepted" else 0.0
    if reason == "accepted" and order_usd <= 0:
        reason = "risk_limit_reached"
    if reason == "accepted" and portfolio is not None:
        durable_rejection = portfolio.pretrade_guard(event, order_usd, settings)
        if durable_rejection:
            reason = durable_rejection
            order_usd = 0.0

    record = {
        "recordedAt": datetime.now(timezone.utc).isoformat(),
        "eventId": event.id,
        "sourceType": event.source_type,
        "status": reason,
        "mode": "paper",
        "handle": event.handle,
        "symbol": event.symbol,
        "networkId": event.network_id,
        "ca": event.ca,
        "signalAgeSeconds": round(age, 3) if age != float("inf") else None,
        "targetBuyUsd": event.amount_usd,
        "marketCapUsd": event.market_cap,
        "paperBuyUsd": order_usd,
        "paperTokenAmount": order_usd / event.price if order_usd > 0 and event.price > 0 else None,
        "priceUsd": event.price or None,
        "userId": event.user_id or None,
        "maxSlippageBps": int(settings.get("max_slippage_bps", 200)),
        "stage": "fast_path_reserved" if reason == "accepted" else "fast_path_blocked",
        "deferredChecks": list(gate.deferred_checks),
        "decisionLatencyMs": gate.decision_latency_ms,
    }
    log_path = PROJECT_DIR / settings.get("log_path", "data/paper-orders.ndjson")
    append_ndjson(log_path, record, int(settings.get("audit_retention_days", 30)))
    if reason == "accepted" and portfolio is None:
        state.save(daily_key, daily_spend + order_usd)
        state.save(token_key, token_exposure + order_usd)
    if reason == "accepted":
        event.copy_note = f"🧪 模拟买入 {compact_money(order_usd)} · 滑点≤{record['maxSlippageBps'] / 100:g}%"
    return record


def process_fast_event(
    state: State,
    event: Event,
    cfg: dict[str, Any],
    portfolio: PortfolioLedger | None = None,
) -> dict[str, Any] | None:
    """Run the latency-sensitive copy decision before analytical work."""
    paper_decision = paper_copy_trade(state, event, cfg, portfolio)
    portfolio_result = None
    if portfolio is not None:
        portfolio_result = portfolio.apply_event(event, paper_decision)
    if paper_decision is not None:
        state.mark_sent(f"paper:{event.id}")
    notify_once(state, event, cfg)
    state.enqueue_post_trade(event)
    return portfolio_result


def process_post_trade_event(
    state: State,
    event: Event,
    risk_pipeline: RiskPipeline | None = None,
    portfolio: PortfolioLedger | None = None,
    execution_journal: ExecutionJournal | None = None,
    intelligence: WalletIntelligenceStore | None = None,
) -> None:
    """Run full analysis after the order hand-off; never precede the fast path."""
    if intelligence is not None:
        try:
            intelligence.record_event(event)
        except Exception:
            logging.exception("写入只读地址画像样本失败：%s", event.id)
    risk_id = f"risk:{event.id}"
    if risk_pipeline is not None and not state.was_sent(risk_id):
        context = RiskContext(exposure=portfolio.exposure_snapshot(event)) if portfolio is not None else None
        risk_decision = risk_pipeline.evaluate_event(event, context)
        if execution_journal is not None:
            execution_journal.record_risk_decision(event, risk_decision)
        state.mark_sent(risk_id)


def process_event(
    state: State,
    event: Event,
    cfg: dict[str, Any],
    risk_pipeline: RiskPipeline | None = None,
    portfolio: PortfolioLedger | None = None,
    execution_journal: ExecutionJournal | None = None,
    intelligence: WalletIntelligenceStore | None = None,
    phase: str = "full",
) -> dict[str, Any] | None:
    if phase not in {"full", "fast", "post"}:
        raise ValueError(f"unknown processing phase: {phase}")
    portfolio_result = process_fast_event(state, event, cfg, portfolio) if phase in {"full", "fast"} else None
    if phase in {"full", "post"}:
        process_post_trade_event(state, event, risk_pipeline, portfolio, execution_journal, intelligence)
        state.complete_post_trade(event.id)
    return portfolio_result


def validate_config(cfg: dict[str, Any]) -> None:
    if int(cfg.get("poll_seconds", 0)) < 30:
        raise ValueError("poll_seconds 必须 >= 30")
    if not cfg.get("targets"):
        raise ValueError("至少配置一个 targets.handle")


def sidecar_events(state: State, allowed_user_ids: set[str] | None, path: str = "data/ws-events.ndjson") -> list[Event]:
    event_path = Path(path)
    if not event_path.exists():
        return []
    canonical_path = str(event_path.resolve())
    committed = int(state.load(f"sidecar_offset:{canonical_path}", state.load("sidecar_offset", 0)))
    offset = int(state.load(f"sidecar_scan_offset:{canonical_path}", committed))
    size = event_path.stat().st_size
    if offset > size:
        # Rotation/truncation: only reset the scan cursor. The durable inbox
        # still protects already observed event ids from duplicate effects.
        offset = 0
    output: list[Event] = []
    with event_path.open("rb") as stream:
        stream.seek(offset)
        while True:
            start = stream.tell()
            raw = stream.readline()
            if not raw:
                break
            if not raw.endswith(b"\n"):
                # A writer is still appending this JSON object. Leave both
                # scan and committed offsets before the partial record.
                break
            end = stream.tell()
            candidates: list[Event] = []
            try:
                envelope = json.loads(raw.decode("utf-8"))
                payload = envelope.get("payload") or {}
                candidates = feed_events([payload], allowed_user_ids=allowed_user_ids)
                if not candidates and isinstance(payload, dict):
                    body = payload.get("body") or payload
                    synthetic = {
                        "id": payload.get("id") or hashlib.sha256(raw).hexdigest()[:24],
                        "type": payload.get("type") or body.get("type") or body.get("side") or "feed",
                        "createdAt": payload.get("createdAt") or envelope.get("receivedAt"),
                        "networkId": payload.get("networkId") or body.get("networkId"),
                        "tokenAddress": payload.get("tokenAddress") or body.get("tokenAddress"),
                        "userId": payload.get("userId") or body.get("userId"),
                        "body": body,
                    }
                    candidates = feed_events([synthetic], allowed_user_ids=allowed_user_ids)
                output.extend(candidates)
            except Exception:
                logging.exception("解析 Sidecar WebSocket 帧失败")
            # Persist the complete source span and parsed event before moving
            # the scan cursor. Filtered/malformed complete lines are durable
            # no-op records so they cannot block later valid events forever.
            state.enqueue_sidecar_line(canonical_path, start, end, candidates)
    return output


def process_pending_events(
    state: State,
    cfg: dict[str, Any],
    risk_pipeline: RiskPipeline | None,
    portfolio: PortfolioLedger | None,
    execution_journal: ExecutionJournal | None,
    intelligence: WalletIntelligenceStore | None,
    limit: int = 100,
    post_trade_async: bool = False,
) -> int:
    processed = 0
    fast_completed: list[tuple[str, Event]] = []
    for row in state.pending_events(limit):
        event_id = str(row["event_id"])
        try:
            event = Event(**json.loads(row["event_json"]))
            process_event(state, event, cfg, risk_pipeline, portfolio, execution_journal, intelligence, "fast")
            if post_trade_async:
                state.complete_event(event_id)
                processed += 1
            else:
                fast_completed.append((event_id, event))
        except Exception as exc:
            # One broken fast-path event is isolated; later signals still run.
            state.fail_event(event_id, exc)
            logging.error("极速路径失败，已保留在 durable inbox：event=%s error=%s",
                          event_id, type(exc).__name__)
    # Drain the whole burst through the order hand-off before any expensive
    # profile/risk analysis. This preserves arrival priority under load.
    for event_id, event in fast_completed:
        try:
            process_event(state, event, cfg, risk_pipeline, portfolio, execution_journal, intelligence, "post")
            state.complete_event(event_id)
            processed += 1
        except Exception as exc:
            state.fail_event(event_id, exc)
            logging.error("买后分析失败，已保留重试：event=%s error=%s", event_id, type(exc).__name__)
    return processed


def run(cfg: dict[str, Any], once: bool = False) -> None:
    apply_network_settings(PROJECT_DIR, cfg)
    validate_config(cfg)
    publish_fast_executor_config(cfg)
    client, state = FomoClient(), State(cfg["state_db"])
    state.prune(int(cfg.get("state_sent_ttl_days", 14)), int(cfg.get("inbox_retention_days", 30)))
    leaderboard_scheduler = LeaderboardScheduler(PROJECT_DIR, cfg, client).start() if not once else None
    if leaderboard_scheduler is not None:
        atexit.register(leaderboard_scheduler.stop)
    risk_pipeline = RiskPipeline(PROJECT_DIR, cfg.get("read_only_risk", {}))
    portfolio_settings = cfg.get("portfolio", {})
    portfolio_path = Path(str(portfolio_settings.get("database", "data/portfolio.sqlite3")))
    if not portfolio_path.is_absolute():
        portfolio_path = PROJECT_DIR / portfolio_path
    portfolio = PortfolioLedger(
        portfolio_path,
        cfg["timezone"],
        str(portfolio_settings.get("account_id", "paper-main")),
    ) if portfolio_settings.get("enabled", True) else None
    if portfolio is not None:
        atexit.register(portfolio.close)
    if not once:
        start_dashboard(PROJECT_DIR, cfg, on_network_change=lambda: publish_fast_executor_config(cfg))
        monitors = BackgroundMonitors(PROJECT_DIR, cfg, portfolio=portfolio).start()
        atexit.register(monitors.stop)
    notification_worker = NotificationWorker(state.path, cfg).start()
    atexit.register(notification_worker.stop)
    execution_settings = cfg.get("execution_journal", {})
    execution_path = Path(str(execution_settings.get("database", "data/execution.sqlite3")))
    if not execution_path.is_absolute():
        execution_path = PROJECT_DIR / execution_path
    execution_journal = ExecutionJournal(execution_path, str(portfolio_settings.get("account_id", "paper-main"))) if execution_settings.get("enabled", True) else None
    if execution_journal is not None:
        atexit.register(execution_journal.close)
    intelligence_settings = cfg.get("smart_money", {})
    intelligence_path = Path(str(intelligence_settings.get("database", "data/wallet-intelligence.sqlite3")))
    if not intelligence_path.is_absolute():
        intelligence_path = PROJECT_DIR / intelligence_path
    intelligence = WalletIntelligenceStore(intelligence_path, intelligence_settings) if intelligence_settings.get("enabled", True) else None
    if intelligence is not None:
        atexit.register(intelligence.close)
    post_trade_worker = (
        PostTradeWorker(PROJECT_DIR, state.path, cfg).start()
        if not once and cfg.get("post_trade", {}).get("enabled", True)
        else None
    )
    if post_trade_worker is not None:
        atexit.register(post_trade_worker.stop)
    backup_dir = Path(str(portfolio_settings.get("backup_dir", "data/backups")))
    if not backup_dir.is_absolute():
        backup_dir = PROJECT_DIR / backup_dir
    min_tokens = _num(cfg.get("filters", {}).get("min_token_change", 0.000001))
    min_usd = _num(cfg.get("filters", {}).get("min_usd_change", 0))
    following_ids: set[str] | None = None
    following_refreshed_at = 0.0
    following_retry_at = 0.0
    following_refresh_seconds = max(60, int(cfg.get("following_refresh_seconds", 300)))
    last_rest_poll = 0.0
    rest_poll_seconds = int(cfg["poll_seconds"])
    realtime_poll_seconds = max(0.2, float(cfg.get("realtime_poll_seconds", 1)))
    while True:
        now = time.time()
        if portfolio is not None and portfolio_settings.get("daily_backup", True):
            try:
                portfolio.maybe_daily_backup(backup_dir)
            except Exception:
                logging.exception("每日 PnL 账本备份失败")
        if cfg.get("browser_sidecar") or cfg.get("account_feed", True):
            try:
                if now >= following_retry_at and (following_ids is None or now - following_refreshed_at >= following_refresh_seconds):
                    following = client.get("/v2/users/current/followingIds")
                    following_ids = {str(value) for value in following.get("followingIds", [])}
                    publish_following_ids(following_ids)
                    following_refreshed_at = now
                    following_retry_at = 0.0
                    logging.info("已刷新关注用户白名单：%d 人", len(following_ids))
            except Exception:
                following_ids = None
                following_retry_at = now + 30
                logging.exception("刷新关注用户白名单失败")
        if cfg.get("browser_sidecar") and following_ids is not None:
            sidecar_events(state, allowed_user_ids=following_ids or set())
        rest_due = once or now - last_rest_poll >= rest_poll_seconds
        if rest_due and cfg.get("account_feed", True) and following_ids is not None:
            try:
                params = [("limit", "50"), *(("feedTypes", value) for value in FEED_TYPES)]
                events = feed_events(client.get("/feed", params=params), allowed_user_ids=following_ids or set())
                initialized = state.load("account_feed_initialized", False)
                for event in events:
                    if initialized:
                        state.enqueue_event(event, source="account_feed")
                    else:
                        state.mark_sent(event.id)
                state.save("account_feed_initialized", True)
            except Exception:
                logging.exception("个性化 Feed 监控失败")
        for target in cfg["targets"] if rest_due else []:
            if not target.get("enabled", True):
                continue
            handle = target["handle"]
            try:
                user = client.get(f"/v2/users/userHandle/{quote(handle, safe='')}")
                uid = user["id"]
                balances = _balance_snapshot(client.get(f"/v2/users/{uid}/balances"))
                key = f"balances:{uid}"
                previous = state.load(key, None)
                if previous is not None:
                    for event in position_events(handle, previous, balances, min_tokens, min_usd):
                        event.user_id = str(uid)
                        state.enqueue_event(event, source="position_poll")
                state.save(key, balances)

                spotlight = client.get(f"/v2/users/{uid}/spotlight")
                comments = thesis_events(handle, spotlight)
                initialized_key = f"thesis_initialized:{uid}"
                initialized = state.load(initialized_key, False)
                for event in comments:
                    if initialized:
                        event.user_id = str(uid)
                        state.enqueue_event(event, source="thesis_poll")
                    else:
                        state.mark_sent(event.id)
                state.save(initialized_key, True)
            except Exception:
                logging.exception("监控 @%s 失败", handle)
        process_pending_events(
            state, cfg, risk_pipeline, portfolio, execution_journal, intelligence,
            post_trade_async=post_trade_worker is not None,
        )
        if rest_due:
            last_rest_poll = now
        if once:
            return
        time.sleep(realtime_poll_seconds if cfg.get("browser_sidecar") else rest_poll_seconds)


def load_config(path: str) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def show_positions(handle: str) -> None:
    client = FomoClient()
    user = client.get(f"/v2/users/userHandle/{quote(handle, safe='')}")
    positions = list(_balance_snapshot(client.get(f"/v2/users/{user['id']}/balances")).values())
    positions.sort(key=lambda item: item["value"], reverse=True)
    nonzero = [item for item in positions if item["amount"] > 0]
    print(f"@{user.get('userHandle', handle)} | userId={user['id']}")
    print(f"接口返回 {len(positions)} 个代币记录；非零仓位 {len(nonzero)} 个")
    print("序号 | 代币 | 数量 | 价格(USD) | 仓位价值(USD) | 市值(USD) | 链 | CA")
    for index, item in enumerate(nonzero, 1):
        print(
            f"{index} | {item['symbol']} | {item['amount']:.8g} | ${item['price']:.10g} | "
            f"${item['value']:,.2f} | ${item['market_cap']:,.2f} | "
            f"{CHAIN_NAMES.get(item['network_id'], item['network_id'])} | {item['ca']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fomo 用户买卖、仓位和喊单多平台提醒")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--once", action="store_true", help="只轮询一次（首次仅建立基线，不推送历史事件）")
    parser.add_argument("--save-creds", action="store_true", help="交互式保存 Fomo token 到系统安全凭据库")
    parser.add_argument("--delete-creds", action="store_true", help="从系统安全凭据库删除 Fomo token")
    parser.add_argument("--show-positions", metavar="HANDLE", help="查询并打印指定用户的非零仓位")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.save_creds:
        from getpass import getpass
        access = getpass("FOMO access token（输入不可见）: ").strip()
        refresh = getpass("FOMO refresh token（输入不可见）: ").strip()
        if not access or not refresh:
            raise SystemExit("两个 token 均不能为空")
        # 先验证 access token 形状，避免误存明显错误的内容。
        _jwt_payload(access)
        keyring.set_password(KEYRING_SERVICE, "access_token", access)
        keyring.set_password(KEYRING_SERVICE, "refresh_token", refresh)
        print("已保存到系统安全凭据库；未写入项目文件。")
        return
    if args.delete_creds:
        for name in ("access_token", "refresh_token"):
            try:
                keyring.delete_password(KEYRING_SERVICE, name)
            except PasswordDeleteError:
                pass
        print("已从系统安全凭据库删除 Fomo token。")
        return
    if args.show_positions:
        show_positions(args.show_positions)
        return
    run(load_config(args.config), once=args.once)


if __name__ == "__main__":
    main()
