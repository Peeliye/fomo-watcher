from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from ..execution.journal import execution_snapshot, reconciliation_snapshot
from ..execution.readiness import execution_readiness, wallet_balance_snapshot
from ..execution.rpc_pool import rpc_health_snapshot
from ..execution.routing import route_readiness
from ..intelligence.profile import intelligence_snapshot
from ..intelligence.performance import performance_snapshot
from ..intelligence.leaderboard import LeaderboardArchive
from ..portfolio.ledger import portfolio_snapshot
from .wallet_management import WalletManagementError, WalletManagementStore


CHAIN_NAMES = {
    1: "Ethereum",
    56: "BNB Chain",
    137: "Polygon",
    4663: "Robinhood",
    5042: "ARC",
    8453: "Base",
    1399811149: "Solana",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _utc_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
    except ValueError:
        return None


def _trusted_wallet(entry: dict[str, Any], now: datetime) -> tuple[bool, str, bool, int]:
    evidence = entry.get("evidence", [])
    valid_evidence = [
        item for item in evidence if isinstance(item, dict)
        and str(item.get("type") or "").strip()
        and str(item.get("reference") or "").strip()
        and _utc_time(item.get("recordedAt")) is not None
    ] if isinstance(evidence, list) else []
    expires = _utc_time(entry.get("expiresAt"))
    expired = expires is None or expires <= now
    try:
        confidence = float(entry.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0
    if str(entry.get("status") or "shadow-only") == "revoked":
        return False, "revoked", expired, len(valid_evidence)
    if expired:
        return False, "expired", True, len(valid_evidence)
    if confidence < 0.8:
        return False, "low_confidence", False, len(valid_evidence)
    if not valid_evidence:
        return False, "missing_evidence", False, 0
    return True, "trusted", False, len(valid_evidence)


def build_status_payload(data_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    realtime = _read_json(data_dir / "realtime-status.json")
    privy = _read_json(data_dir / "privy-status.json")
    threshold = max(5, int(cfg.get("dashboard", {}).get("status_stale_seconds", 30)))
    now = datetime.now(timezone.utc)
    alerts: list[dict[str, str]] = []

    def health(name: str, payload: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
        updated = _utc_time(payload.get("updatedAt") or payload.get("lastFrameAt"))
        age = max(0, int((now - updated).total_seconds())) if updated else None
        connected = all(bool(payload.get(field)) for field in fields)
        fresh = age is not None and age <= threshold
        if not connected:
            alerts.append({"severity": "critical", "code": f"{name}_offline", "message": f"{name} 服务未就绪"})
        elif not fresh:
            alerts.append({"severity": "warning", "code": f"{name}_stale", "message": f"{name} 状态已过期"})
        return {"connected": connected, "fresh": fresh, "ageSeconds": age, "staleAfterSeconds": threshold}

    copy_settings, route_settings = cfg.get("copy_trading", {}), cfg.get("routing", {})
    return {
        "realtime": realtime,
        "privy": privy,
        "health": {
            "realtime": health("WebSocket", realtime, ("connected", "authenticated", "subscribed")),
            "privy": health("Privy", privy, ("authenticated", "sdkReady")),
        },
        "modes": {
            "paper": {"enabled": bool(copy_settings.get("enabled")) and str(copy_settings.get("mode", "paper")) == "paper"},
            "shadow": {"enabled": bool(route_settings.get("enabled")) and str(route_settings.get("mode", "shadow")) == "shadow"},
            "live": {"enabled": str(route_settings.get("mode", "shadow")) == "live"},
        },
        "alerts": alerts,
        "generatedAt": now.isoformat(),
    }


def build_dashboard_payload(log_path: Path, limit: int = 500) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
                except json.JSONDecodeError:
                    continue

    statuses = Counter(str(row.get("status") or "unknown") for row in rows)
    accepted = [row for row in rows if row.get("status") == "accepted"]
    accepted_chains = Counter(str(row.get("networkId") or "unknown") for row in accepted)
    recent = list(reversed(rows[-max(1, min(limit, 2000)) :]))

    return {
        "total": len(rows),
        "accepted": len(accepted),
        "acceptedUsd": round(sum(float(row.get("paperBuyUsd") or 0) for row in accepted), 6),
        "statuses": dict(statuses),
        "acceptedChains": dict(accepted_chains),
        "chainNames": {str(key): value for key, value in CHAIN_NAMES.items()},
        "latestAt": rows[-1].get("recordedAt") if rows else None,
        "orders": recent,
    }


def build_shadow_payload(log_path: Path, limit: int = 200) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
                except json.JSONDecodeError:
                    continue
    eligible = [row for row in rows if row.get("status") == "eligible"]
    latencies = sorted(float(row.get("decisionLatencyMs") or 0) for row in rows)
    p95_index = max(0, min(len(latencies) - 1, int(len(latencies) * 0.95))) if latencies else 0
    return {
        "total": len(rows),
        "eligible": len(eligible),
        "averageDecisionLatencyMs": round(sum(latencies) / len(latencies), 3) if latencies else None,
        "p95DecisionLatencyMs": round(latencies[p95_index], 3) if latencies else None,
        "statuses": dict(Counter(str(row.get("status") or "unknown") for row in rows)),
        "executions": list(reversed(rows[-max(1, min(limit, 1000)) :])),
    }


def build_risk_payload(log_path: Path, limit: int = 500) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
                except json.JSONDecodeError:
                    continue
    outcomes = Counter(str(row.get("outcome") or "unknown") for row in rows)
    blockers = Counter(
        str(reason)
        for row in rows
        for reason in (row.get("blockers") or [])
        if reason
    )
    recent = list(reversed(rows[-max(1, min(limit, 2000)) :]))
    return {
        "total": len(rows),
        "outcomes": dict(outcomes),
        "blockers": dict(blockers),
        "latestAt": rows[-1].get("recordedAt") if rows else None,
        "decisions": recent,
    }


def build_identity_payload(registry_path: Path, risk_log_path: Path) -> dict[str, Any]:
    registry = _read_json(registry_path)
    wallets = registry.get("wallets", []) if isinstance(registry.get("wallets", []), list) else []
    now = datetime.now(timezone.utc)
    candidate_counts: Counter[tuple[str, str]] = Counter()
    public_wallets: list[dict[str, Any]] = []
    for entry in wallets:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status") or "shadow-only")
        chain_ids = [str(value) for value in entry.get("chainIds", [])]
        trusted, trust_reason, expired, evidence_count = _trusted_wallet(entry, now)
        if trusted:
            for chain_id in chain_ids:
                candidate_counts[(str(entry.get("kolId") or ""), chain_id)] += 1
        public_wallets.append({
            "kolId": str(entry.get("kolId") or ""),
            "handle": str(entry.get("handle") or ""),
            "chainIds": chain_ids,
            "address": str(entry.get("address") or ""),
            "confidence": entry.get("confidence"),
            "status": status,
            "expired": expired,
            "trusted": trusted,
            "trustReason": trust_reason,
            "verifiedAt": entry.get("verifiedAt"),
            "expiresAt": entry.get("expiresAt"),
            "evidenceCount": evidence_count,
        })

    backlog: dict[tuple[str, str], dict[str, Any]] = {}
    if risk_log_path.exists():
        with risk_log_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict) or row.get("outcome") != "needs_identity":
                    continue
                kol_id = str(row.get("kolId") or "")
                chain_id = str(row.get("networkId") or "")
                key = (kol_id, chain_id)
                if candidate_counts[key] == 1:
                    continue
                item = backlog.setdefault(key, {
                    "kolId": kol_id,
                    "networkId": int(chain_id) if chain_id.isdigit() else chain_id,
                    "handle": str(row.get("handle") or "unknown"),
                    "events": 0,
                    "latestAt": row.get("recordedAt"),
                    "symbols": set(),
                    "reason": "ambiguous_wallet_mapping" if candidate_counts[key] > 1 else (row.get("blockers") or ["wallet_not_registered"])[0],
                })
                item["events"] += 1
                if str(row.get("recordedAt") or "") > str(item.get("latestAt") or ""):
                    item["latestAt"] = row.get("recordedAt")
                    item["handle"] = str(row.get("handle") or item["handle"])
                symbol = str(row.get("symbol") or "")
                if symbol and symbol != "UNKNOWN":
                    item["symbols"].add(symbol)
    backlog_rows = sorted(backlog.values(), key=lambda item: str(item.get("latestAt") or ""), reverse=True)
    for item in backlog_rows:
        item["symbols"] = sorted(item["symbols"])[:6]
    return {
        "registryVersion": int(registry.get("version", 1)),
        "registered": sum(1 for item in public_wallets if item["trusted"]),
        "revoked": sum(1 for item in public_wallets if item["status"] == "revoked"),
        "expired": sum(1 for item in public_wallets if item["expired"] and item["status"] != "revoked"),
        "pending": len(backlog_rows),
        "wallets": public_wallets,
        "backlog": backlog_rows,
    }


def build_intelligence_payload(
    database: Path,
    settings: dict[str, Any],
    registry_path: Path,
    limit: int = 200,
) -> dict[str, Any]:
    """Attach only public registry addresses to behavior profiles."""
    payload = intelligence_snapshot(database, settings, limit)
    registry = _read_json(registry_path)
    by_kol: dict[str, list[dict[str, Any]]] = {}
    now = datetime.now(timezone.utc)
    for entry in registry.get("wallets", []):
        if not isinstance(entry, dict):
            continue
        trusted, _, _, evidence_count = _trusted_wallet(entry, now)
        if not trusted:
            continue
        by_kol.setdefault(str(entry.get("kolId") or ""), []).append({
            "address": str(entry.get("address") or ""),
            "chainIds": [str(value) for value in entry.get("chainIds", [])],
            "confidence": entry.get("confidence"),
            "status": str(entry.get("status") or "shadow-only"),
            "evidenceCount": evidence_count,
            "trusted": True,
        })
    for profile in payload.get("profiles", []):
        profile_wallets: list[dict[str, Any]] = []
        seen_addresses: set[str] = set()
        for kol_id in profile.get("kolAliases", [profile.get("kolId")]):
            for wallet in by_kol.get(str(kol_id or ""), []):
                address = wallet["address"].casefold()
                if address not in seen_addresses:
                    seen_addresses.add(address)
                    profile_wallets.append(wallet)
        profile["wallets"] = profile_wallets
    return payload


def start_dashboard(project_dir: Path, cfg: dict[str, Any]) -> ThreadingHTTPServer | None:
    # The dashboard is also used as a standalone diagnostic entry point. Load
    # project-local configuration here so it reports the same RPC readiness as
    # the main application instead of silently treating every endpoint as empty.
    load_dotenv(project_dir / ".env", override=False)
    settings = cfg.get("dashboard", {})
    if not settings.get("enabled", True):
        return None

    host = str(settings.get("host", "127.0.0.1"))
    port = int(settings.get("port", 8765))
    html_path = Path(__file__).with_name("static") / "index.html"
    logo_path = html_path.parent / "assets" / "fomo-exec-logo-v1.png"
    copy_settings = cfg.get("copy_trading", {})
    configured_log = Path(str(copy_settings.get("log_path", "data/paper-orders.ndjson")))
    log_path = configured_log if configured_log.is_absolute() else project_dir / configured_log
    risk_settings = cfg.get("read_only_risk", {})
    configured_risk_log = Path(str(risk_settings.get("log_path", "data/risk-decisions.ndjson")))
    risk_log_path = configured_risk_log if configured_risk_log.is_absolute() else project_dir / configured_risk_log
    configured_registry = Path(str(risk_settings.get("registry_path", "wallet-registry.json")))
    registry_path = configured_registry if configured_registry.is_absolute() else project_dir / configured_registry
    portfolio_settings = cfg.get("portfolio", {})
    configured_portfolio_db = Path(str(portfolio_settings.get("database", "data/portfolio.sqlite3")))
    portfolio_db = configured_portfolio_db if configured_portfolio_db.is_absolute() else project_dir / configured_portfolio_db
    portfolio_account = str(portfolio_settings.get("account_id", "paper-main"))
    portfolio_mark_stale = int(portfolio_settings.get("mark_stale_seconds", 300))
    execution_settings = cfg.get("execution_journal", {})
    configured_execution_db = Path(str(execution_settings.get("database", "data/execution.sqlite3")))
    execution_db = configured_execution_db if configured_execution_db.is_absolute() else project_dir / configured_execution_db
    intelligence_settings = cfg.get("smart_money", {})
    configured_intelligence_db = Path(str(intelligence_settings.get("database", "data/wallet-intelligence.sqlite3")))
    intelligence_db = configured_intelligence_db if configured_intelligence_db.is_absolute() else project_dir / configured_intelligence_db
    intelligence_cache_seconds = max(3, int(intelligence_settings.get("dashboard_cache_seconds", 15)))
    intelligence_cache: dict[str, Any] = {"at": 0.0, "limit": 0, "payload": None}
    intelligence_cache_lock = threading.Lock()
    performance_settings = cfg.get("verified_performance", {})
    configured_performance_db = Path(str(performance_settings.get("database", "data/verified-performance.sqlite3")))
    performance_db = configured_performance_db if configured_performance_db.is_absolute() else project_dir / configured_performance_db
    wallet_management_settings = cfg.get("wallet_management", {})
    configured_watchlist = Path(str(wallet_management_settings.get("watchlist_path", "watch-wallets.json")))
    watchlist_path = configured_watchlist if configured_watchlist.is_absolute() else project_dir / configured_watchlist
    configured_wallet_audit = Path(str(wallet_management_settings.get("audit_log_path", "data/wallet-management-audit.ndjson")))
    wallet_audit_path = configured_wallet_audit if configured_wallet_audit.is_absolute() else project_dir / configured_wallet_audit
    wallet_store = WalletManagementStore(
        registry_path,
        watchlist_path,
        wallet_audit_path,
        int(wallet_management_settings.get("maximum_import_rows", 500)),
    )
    data_dir = project_dir / "data"
    leaderboard_settings = cfg.get("leaderboard_monitor", {})
    configured_leaderboard_db = Path(str(leaderboard_settings.get("database", "data/leaderboard.sqlite3")))
    leaderboard_db = configured_leaderboard_db if configured_leaderboard_db.is_absolute() else project_dir / configured_leaderboard_db
    configured_leaderboard_archive = Path(str(leaderboard_settings.get("archive_dir", "data/leaderboard")))
    leaderboard_archive = configured_leaderboard_archive if configured_leaderboard_archive.is_absolute() else project_dir / configured_leaderboard_archive

    def leaderboard_payload(action: str, query: dict[str, list[str]]) -> dict[str, Any] | list[dict[str, Any]]:
        store = LeaderboardArchive(leaderboard_db, leaderboard_archive, str(cfg.get("timezone", "Asia/Shanghai")))
        try:
            day = query.get("date", [None])[0]
            if action == "overview": return store.overview(day)
            if action == "current": return store.current_ranking(day)
            if action == "participants": return store.participants(day)
            if action == "history": return store.history(str(day or ""), int(query.get("hour", ["0"])[0]))
            if action == "participant": return store.participant_detail(str(day or ""), str(query.get("user_id", [""])[0]))
            return {"error": "unknown_action"}
        finally:
            store.close()

    class DashboardHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: Any, status: int = 200) -> None:
            content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(content)

        def _same_origin(self) -> bool:
            origin = str(self.headers.get("Origin") or "").rstrip("/")
            if not origin:
                return True
            return origin in {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}

        def _read_json_body(self) -> dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length") or "0")
            except ValueError as exc:
                raise WalletManagementError("请求长度无效") from exc
            if length <= 0:
                raise WalletManagementError("请求内容为空")
            if length > 1_048_576:
                raise OverflowError("请求内容超过 1 MB")
            try:
                value = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise WalletManagementError("请求 JSON 无法解析") from exc
            if not isinstance(value, dict):
                raise WalletManagementError("请求内容必须是 JSON 对象")
            return value

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path in {"/assets/fomo-exec-logo-v1.png", "/favicon.ico"}:
                try:
                    content = logo_path.read_bytes()
                except OSError:
                    self._send_json({"error": "logo_not_found"}, 404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(content)
                return
            if parsed.path == "/":
                try:
                    content = html_path.read_bytes()
                except OSError:
                    self._send_json({"error": "dashboard_not_found"}, 404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(content)
                return
            if parsed.path == "/api/paper-orders":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["500"])[0])
                except ValueError:
                    limit = 500
                self._send_json(build_dashboard_payload(log_path, limit))
                return
            if parsed.path == "/api/status":
                self._send_json(build_status_payload(data_dir, cfg))
                return
            if parsed.path.startswith("/api/50rank/"):
                action = parsed.path.rsplit("/", 1)[-1]
                try:
                    self._send_json(leaderboard_payload(action, parse_qs(parsed.query)))
                except (ValueError, sqlite3.Error) as exc:
                    self._send_json({"error": str(exc)}, 400)
                return
            if parsed.path == "/api/shadow-executions":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["200"])[0])
                except ValueError:
                    limit = 200
                self._send_json(build_shadow_payload(data_dir / "shadow-executions.ndjson", limit))
                return
            if parsed.path == "/api/risk-decisions":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["500"])[0])
                except ValueError:
                    limit = 500
                self._send_json(build_risk_payload(risk_log_path, limit))
                return
            if parsed.path == "/api/wallet-registry":
                self._send_json(build_identity_payload(registry_path, risk_log_path))
                return
            if parsed.path == "/api/wallet-management":
                try:
                    self._send_json(wallet_store.snapshot())
                except WalletManagementError as exc:
                    self._send_json({"error": str(exc)}, 500)
                return
            if parsed.path == "/api/wallet-intelligence":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["200"])[0])
                except ValueError:
                    limit = 200
                with intelligence_cache_lock:
                    now = time.monotonic()
                    if (
                        intelligence_cache["payload"] is None
                        or now - float(intelligence_cache["at"]) >= intelligence_cache_seconds
                        or int(intelligence_cache["limit"]) < limit
                    ):
                        intelligence_cache.update({
                            "at": now,
                            "limit": limit,
                            "payload": build_intelligence_payload(
                                intelligence_db, intelligence_settings, registry_path, limit
                            ),
                        })
                    payload = intelligence_cache["payload"]
                self._send_json(payload)
                return
            if parsed.path == "/api/wallet-performance":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["200"])[0])
                except ValueError:
                    limit = 200
                self._send_json(performance_snapshot(performance_db, limit, performance_settings))
                return
            if parsed.path == "/api/portfolio":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["200"])[0])
                except ValueError:
                    limit = 200
                self._send_json(portfolio_snapshot(portfolio_db, portfolio_account, limit, portfolio_mark_stale))
                return
            if parsed.path == "/api/execution-readiness":
                self._send_json(execution_readiness(project_dir, cfg))
                return
            if parsed.path == "/api/wallet-balances":
                self._send_json(wallet_balance_snapshot(project_dir, cfg))
                return
            if parsed.path == "/api/execution-intents":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["100"])[0])
                except ValueError:
                    limit = 100
                self._send_json(execution_snapshot(execution_db, limit))
                return
            if parsed.path == "/api/execution-reconciliation":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["100"])[0])
                except ValueError:
                    limit = 100
                self._send_json(reconciliation_snapshot(execution_db, limit))
                return
            if parsed.path == "/api/route-readiness":
                self._send_json(route_readiness(cfg))
                return
            if parsed.path == "/api/rpc-health":
                self._send_json(rpc_health_snapshot(project_dir, cfg))
                return
            if parsed.path == "/api/health":
                self._send_json({"ok": True})
                return
            self._send_json({"error": "not_found"}, 404)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path != "/api/wallet-management":
                self._send_json({"error": "not_found"}, 404)
                return
            if not self._same_origin():
                self._send_json({"error": "forbidden_origin"}, 403)
                return
            content_type = str(self.headers.get("Content-Type") or "").lower()
            if "application/json" not in content_type:
                self._send_json({"error": "content_type_must_be_application_json"}, 415)
                return
            try:
                payload = self._read_json_body()
                self._send_json(wallet_store.mutate(payload))
            except OverflowError as exc:
                self._send_json({"error": str(exc)}, 413)
            except WalletManagementError as exc:
                self._send_json({"error": str(exc)}, 400)
            except OSError:
                logging.exception("钱包管理写入失败")
                self._send_json({"error": "钱包数据写入失败"}, 500)

        def log_message(self, format: str, *args: Any) -> None:
            return

    try:
        server = ThreadingHTTPServer((host, port), DashboardHandler)
    except OSError:
        logging.exception("可视化面板启动失败：http://%s:%d", host, port)
        return None
    thread = threading.Thread(target=server.serve_forever, name="fomo-dashboard", daemon=True)
    thread.start()
    logging.info("可视化面板已启动：http://%s:%d", host, port)
    return server
