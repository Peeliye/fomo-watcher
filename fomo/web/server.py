from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import sqlite3
import struct
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from ..execution.journal import execution_snapshot, reconciliation_snapshot
from ..execution.networks import apply_network_settings, network_settings_path
from ..execution.readiness import execution_readiness
from ..execution.rpc_pool import rpc_health_snapshot
from ..execution.routing import route_readiness
from ..intelligence.profile import intelligence_snapshot
from ..intelligence.performance import performance_snapshot
from ..intelligence.leaderboard import LeaderboardArchive
from ..portfolio.ledger import portfolio_snapshot
from ..portfolio.exit_policy import ExitPolicyError, ExitPolicyStore
from .wallet_management import WalletManagementError, WalletManagementStore
from .rpc_management import RpcManagementError, RpcManagementStore
from .audit_index import AuditLogIndex


CHAIN_NAMES = {
    1: "Ethereum",
    56: "BNB Chain",
    137: "Polygon",
    4663: "Robinhood",
    5042: "ARC",
    8453: "Base",
    1399811149: "Solana",
}


def public_execution_readiness(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    """Return operational readiness without exposing execution-wallet metadata."""
    payload = execution_readiness(project_dir, cfg)
    payload.pop("walletId", None)
    payload.pop("accounts", None)
    for chain in payload.get("chains", []):
        chain.pop("accountId", None)
        chain.pop("address", None)
    payload["note"] = "Execution-wallet identifiers, addresses, and balances are hidden from the dashboard."
    return payload


def _websocket_text_frame(payload: dict[str, Any]) -> bytes:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    length = len(data)
    if length < 126:
        header = bytes((0x81, length))
    elif length <= 65_535:
        header = bytes((0x81, 126)) + struct.pack("!H", length)
    else:
        header = bytes((0x81, 127)) + struct.pack("!Q", length)
    return header + data


def _paths_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for path in paths:
        candidates = [path]
        if path.suffix in {".sqlite", ".sqlite3", ".db"}:
            candidates.extend((Path(str(path) + "-wal"), Path(str(path) + "-shm")))
        for candidate in candidates:
            try:
                stat = candidate.stat()
                signature.append((str(candidate), stat.st_mtime_ns, stat.st_size))
            except OSError:
                signature.append((str(candidate), 0, 0))
    return tuple(signature)


class DashboardChangeBus:
    """One file watcher shared by every dashboard WebSocket client."""

    def __init__(self, sources: dict[str, list[Path]], interval: float = 0.75, maximum_clients: int = 32):
        self.sources = sources
        self.interval = max(0.2, interval)
        self.maximum_clients = max(1, maximum_clients)
        self.condition = threading.Condition()
        self.revision = 0
        self.changed: list[str] = []
        self.clients = 0
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="dashboard-file-watcher", daemon=True)
        self.thread.start()

    def register(self) -> bool:
        with self.condition:
            if self.clients >= self.maximum_clients:
                return False
            self.clients += 1
            return True

    def unregister(self) -> None:
        with self.condition:
            self.clients = max(0, self.clients - 1)

    def wait(self, after: int, timeout: float = 15.0) -> tuple[int, list[str]]:
        with self.condition:
            self.condition.wait_for(lambda: self.revision > after or self.stop_event.is_set(), timeout)
            return self.revision, list(self.changed) if self.revision > after else []

    def stop(self) -> None:
        self.stop_event.set()
        with self.condition:
            self.condition.notify_all()
        self.thread.join(timeout=5)

    def _run(self) -> None:
        signatures = {name: _paths_signature(paths) for name, paths in self.sources.items()}
        while not self.stop_event.wait(self.interval):
            changed = []
            for name, paths in self.sources.items():
                current = _paths_signature(paths)
                if current != signatures[name]:
                    signatures[name] = current
                    changed.append(name)
            if changed:
                with self.condition:
                    self.changed = changed
                    self.revision += 1
                    self.condition.notify_all()


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


def build_dashboard_payload(log_path: Path, limit: int = 500, retention_days: int = 30) -> dict[str, Any]:
    index=AuditLogIndex(log_path, retention_days);db=index.sync()
    try:
        metrics=index.metrics(db)
        total=int(metrics.get("total",(0,None))[0]);accepted=int(metrics.get("accepted",(0,None))[0]);accepted_usd=metrics.get("accepted_usd",(0,None))[0]
        statuses={key.removeprefix("status:"):int(value[0]) for key,value in metrics.items() if key.startswith("status:")}
        accepted_chains={key.removeprefix("accepted_chain:"):int(value[0]) for key,value in metrics.items() if key.startswith("accepted_chain:")}
        recent=index.recent(db,limit)
        latest_at=metrics.get("latest_at",(0,None))[1]
    finally:index.close(db)
    return {
        "total": int(total),
        "accepted": int(accepted or 0),
        "acceptedUsd": round(float(accepted_usd), 6),
        "statuses": statuses,
        "acceptedChains": accepted_chains,
        "chainNames": {str(key): value for key, value in CHAIN_NAMES.items()},
        "latestAt": latest_at,
        "orders": recent,
    }


def build_shadow_payload(log_path: Path, limit: int = 200, retention_days: int = 30) -> dict[str, Any]:
    index=AuditLogIndex(log_path, retention_days);db=index.sync()
    try:
        metrics=index.metrics(db);total=int(metrics.get("total",(0,None))[0]);eligible=int(metrics.get("eligible",(0,None))[0]);latency_count=metrics.get("latency_count",(0,None))[0]
        average=metrics.get("latency_sum",(0,None))[0]/latency_count if latency_count else None
        statuses={key.removeprefix("status:"):int(value[0]) for key,value in metrics.items() if key.startswith("status:")}
        p95_row=db.execute("SELECT latency FROM audit_rows ORDER BY latency LIMIT 1 OFFSET ?",
                           (max(0,int(int(total)*0.95)-1),)).fetchone() if total else None
        recent=index.recent(db,limit)
    finally:index.close(db)
    return {
        "total": int(total),
        "eligible": int(eligible or 0),
        "averageDecisionLatencyMs": round(float(average), 3) if average is not None else None,
        "p95DecisionLatencyMs": round(float(p95_row[0]), 3) if p95_row else None,
        "statuses": statuses,
        "executions": recent,
    }


def build_risk_payload(log_path: Path, limit: int = 500, retention_days: int = 30) -> dict[str, Any]:
    index=AuditLogIndex(log_path, retention_days);db=index.sync()
    try:
        metrics=index.metrics(db);total=int(metrics.get("total",(0,None))[0])
        outcomes={key.removeprefix("outcome:"):int(value[0]) for key,value in metrics.items() if key.startswith("outcome:")}
        blockers={key.removeprefix("blocker:"):int(value[0]) for key,value in metrics.items() if key.startswith("blocker:")}
        recent=index.recent(db,limit);latest=metrics.get("latest_at",(0,None))[1]
    finally:index.close(db)
    return {
        "total": int(total),
        "outcomes": outcomes,
        "blockers": blockers,
        "latestAt": latest,
        "decisions": recent,
    }


def build_identity_payload(
    registry_path: Path,
    risk_log_path: Path,
    retention_days: int = 30,
    following_path: Path | None = None,
    intelligence_database: Path | None = None,
) -> dict[str, Any]:
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

    backlog_rows=[]
    index=AuditLogIndex(risk_log_path, retention_days);db=index.sync()
    try:
        grouped=db.execute("""SELECT COALESCE(json_extract(payload_json,'$.kolId'),''),network_id,
          COUNT(*),MAX(recorded_at),MAX(json_extract(payload_json,'$.handle')),
          GROUP_CONCAT(DISTINCT json_extract(payload_json,'$.symbol'))
          FROM audit_rows WHERE outcome='needs_identity' GROUP BY 1,2""").fetchall()
    finally:index.close(db)
    for kol_id,chain_id,events,latest,handle,symbols in grouped:
        key=(str(kol_id),str(chain_id))
        if candidate_counts[key]==1:continue
        backlog_rows.append({"kolId":str(kol_id),"networkId":int(chain_id) if str(chain_id).isdigit() else chain_id,
          "handle":str(handle or "unknown"),"events":int(events),"latestAt":latest,
          "symbols":sorted({x for x in str(symbols or "").split(",") if x and x!="UNKNOWN"})[:6],
          "reason":"ambiguous_wallet_mapping" if candidate_counts[key]>1 else "wallet_not_registered"})
    backlog_rows.sort(key=lambda item:str(item.get("latestAt") or ""),reverse=True)
    followed_document = _read_json(following_path) if following_path is not None else {}
    followed_kols = len({str(value) for value in followed_document.get("followingIds", []) if str(value)})
    profiled_kols = 0
    if intelligence_database is not None and intelligence_database.exists():
        profile_db = sqlite3.connect(f"file:{intelligence_database.as_posix()}?mode=ro", uri=True, timeout=5)
        try:
            profiled_kols = int(profile_db.execute("SELECT COUNT(DISTINCT kol_id) FROM intelligence_events").fetchone()[0])
        except sqlite3.Error:
            profiled_kols = 0
        finally:
            profile_db.close()
    registered_entries = sum(1 for item in public_wallets if item["trusted"])
    registered_unique = len({item["kolId"] for item in public_wallets if item["trusted"] and item["kolId"]})
    expired_entries = sum(1 for item in public_wallets if item["expired"] and item["status"] != "revoked")
    revoked_entries = sum(1 for item in public_wallets if item["status"] == "revoked")
    pending_unique = len({item["kolId"] for item in backlog_rows if item["kolId"]})
    return {
        "registryVersion": int(registry.get("version", 1)),
        "followedKols": followed_kols,
        "profiledKols": profiled_kols,
        "pendingUniqueKols": pending_unique,
        "pendingKolChainPairs": len(backlog_rows),
        "registeredWalletEntries": registered_entries,
        "registeredUniqueKols": registered_unique,
        "expiredWalletEntries": expired_entries,
        "revokedWalletEntries": revoked_entries,
        # Compatibility aliases have explicit units above and are not used by the UI.
        "registered": registered_entries,
        "revoked": revoked_entries,
        "expired": expired_entries,
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


def start_dashboard(project_dir: Path, cfg: dict[str, Any],
                    on_network_change: Callable[[], None] | None = None) -> ThreadingHTTPServer | None:
    # The dashboard is also used as a standalone diagnostic entry point. Load
    # project-local configuration here so it reports the same RPC readiness as
    # the main application instead of silently treating every endpoint as empty.
    load_dotenv(project_dir / ".env", override=False)
    apply_network_settings(project_dir, cfg)
    settings = cfg.get("dashboard", {})
    if not settings.get("enabled", True):
        return None

    host = str(settings.get("host", "127.0.0.1"))
    port = int(settings.get("port", 8765))
    try:
        loopback_bind = host.casefold() == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback_bind = False
    admin_token = os.getenv(str(settings.get("admin_token_env", "FOMO_DASHBOARD_ADMIN_TOKEN")), "")
    csrf_token = os.getenv(str(settings.get("csrf_token_env", "FOMO_DASHBOARD_CSRF_TOKEN")), "")
    if not loopback_bind and (not admin_token or not csrf_token):
        logging.error("Dashboard 拒绝远程绑定：必须配置管理认证和 CSRF 环境变量")
        return None
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
    configured_exit_policy = Path(str(portfolio_settings.get("exit_policy_path", "data/exit-policy.json")))
    exit_policy_path = configured_exit_policy if configured_exit_policy.is_absolute() else project_dir / configured_exit_policy
    exit_policy_store = ExitPolicyStore(exit_policy_path, portfolio_settings.get("exit_strategy"))
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
    rpc_management_settings = cfg.get("rpc_management", {})
    configured_rpc_audit = Path(str(rpc_management_settings.get("audit_log_path", "data/rpc-management-audit.ndjson")))
    rpc_audit_path = configured_rpc_audit if configured_rpc_audit.is_absolute() else project_dir / configured_rpc_audit
    rpc_store = RpcManagementStore(project_dir, cfg, rpc_audit_path, on_network_change)
    leaderboard_settings = cfg.get("leaderboard_monitor", {})
    configured_leaderboard_db = Path(str(leaderboard_settings.get("database", "data/leaderboard.sqlite3")))
    leaderboard_db = configured_leaderboard_db if configured_leaderboard_db.is_absolute() else project_dir / configured_leaderboard_db
    configured_leaderboard_archive = Path(str(leaderboard_settings.get("archive_dir", "data/leaderboard")))
    leaderboard_archive = configured_leaderboard_archive if configured_leaderboard_archive.is_absolute() else project_dir / configured_leaderboard_archive
    rpc_health_setting = Path(str(cfg.get("rpc_pool", {}).get("database", "data/rpc-health.sqlite3")))
    rpc_health_db = rpc_health_setting if rpc_health_setting.is_absolute() else project_dir / rpc_health_setting
    wallet_profile_setting = Path(str(cfg.get("execution", {}).get("wallet_profile", "execution-wallet.json")))
    wallet_profile_path = wallet_profile_setting if wallet_profile_setting.is_absolute() else project_dir / wallet_profile_setting
    env_path = project_dir / ".env"
    config_path = project_dir / "config.yaml"
    network_state_path = network_settings_path(project_dir, cfg)
    change_sources: dict[str, list[Path]] = {
        "orders": [log_path],
        "status": [data_dir / "realtime-status.json", data_dir / "privy-status.json"],
        "shadow": [data_dir / "shadow-executions.ndjson"],
        "risk": [risk_log_path],
        "identity": [registry_path, risk_log_path],
        "management": [registry_path, watchlist_path, wallet_audit_path],
        "rpcManagement": [env_path, rpc_health_db, rpc_audit_path, network_state_path],
        "intelligence": [intelligence_db],
        "performance": [performance_db],
        "portfolio": [portfolio_db, exit_policy_path],
        "readiness": [env_path, wallet_profile_path, rpc_health_db, config_path],
        "wallet": [env_path, wallet_profile_path, rpc_health_db],
        "journal": [execution_db],
        "route": [env_path, config_path, rpc_health_db],
        "rpc": [env_path, rpc_health_db],
        "rank50": [leaderboard_db],
    }
    change_bus = DashboardChangeBus(
        change_sources,
        float(settings.get("watch_interval_seconds", 0.75)),
        int(settings.get("maximum_websocket_clients", 32)),
    )
    service_instance_id = uuid.uuid4().hex
    service_started_at = datetime.now(timezone.utc).isoformat()

    def leaderboard_payload(action: str, query: dict[str, list[str]]) -> dict[str, Any] | list[dict[str, Any]]:
        store = LeaderboardArchive(leaderboard_db, leaderboard_archive,
                                   str(cfg.get("timezone", "Asia/Shanghai")), readonly=True)
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
        protocol_version = "HTTP/1.1"

        def _send_json(self, payload: Any, status: int = 200) -> None:
            generated_at = datetime.now(timezone.utc).isoformat()
            if isinstance(payload, dict):
                payload = {
                    **payload,
                    "serviceInstanceId": service_instance_id,
                    "startupId": service_instance_id,
                    "serviceStartedAt": service_started_at,
                    "generatedAt": generated_at,
                    "dataRevision": change_bus.revision,
                }
            content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Fomo-Service-Instance", service_instance_id)
                self.send_header("X-Fomo-Generated-At", generated_at)
                self.send_header("X-Fomo-Data-Revision", str(change_bus.revision))
                self.end_headers()
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                self.close_connection = True

        def _write_content(self, content: bytes) -> None:
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                self.close_connection = True

        def _same_origin(self) -> bool:
            origin = str(self.headers.get("Origin") or "").rstrip("/")
            if not origin:
                return True
            allowed = {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}
            allowed.update(str(value).rstrip("/") for value in settings.get("allowed_origins", []))
            return origin in allowed

        def _admin_authorized(self) -> bool:
            supplied_csrf = str(self.headers.get("X-Fomo-CSRF") or "")
            if loopback_bind:
                return self._same_origin() and hmac.compare_digest(supplied_csrf, "1")
            authorization = str(self.headers.get("Authorization") or "")
            supplied_token = authorization[7:] if authorization.startswith("Bearer ") else ""
            return (self._same_origin() and hmac.compare_digest(supplied_token, admin_token)
                    and hmac.compare_digest(supplied_csrf, csrf_token))

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
            if parsed.path == "/ws/dashboard":
                if not self._same_origin():
                    self._send_json({"error": "forbidden_origin"}, 403)
                    return
                key = str(self.headers.get("Sec-WebSocket-Key") or "")
                if str(self.headers.get("Upgrade") or "").casefold() != "websocket" or not key:
                    self._send_json({"error": "websocket_upgrade_required"}, 426)
                    return
                if not change_bus.register():
                    self._send_json({"error": "websocket_capacity_reached"}, 503)
                    return
                accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()).decode("ascii")
                self.send_response(101, "Switching Protocols")
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                self.send_header("Sec-WebSocket-Accept", accept)
                self.end_headers()
                try:
                    self.connection.sendall(_websocket_text_frame({
                        "type": "ready", "keys": list(change_sources),
                        "serviceInstanceId": service_instance_id,
                        "generatedAt": datetime.now(timezone.utc).isoformat(),
                        "dataRevision": change_bus.revision,
                    }))
                    revision = change_bus.revision
                    while True:
                        next_revision, changed = change_bus.wait(revision, 15)
                        if changed:
                            self.connection.sendall(_websocket_text_frame({"type": "invalidate", "keys": changed}))
                            revision = next_revision
                        else:
                            self.connection.sendall(_websocket_text_frame({"type": "heartbeat"}))
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                finally:
                    change_bus.unregister()
                return
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
                self._write_content(content)
                return
            if parsed.path == "/assets/dashboard-consistency.mjs":
                module_path = html_path.parent / "dashboard-consistency.mjs"
                try:
                    content = module_path.read_bytes()
                except OSError:
                    self._send_json({"error": "dashboard_module_not_found"}, 404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self._write_content(content)
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
                self._write_content(content)
                return
            if parsed.path == "/api/paper-orders":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["500"])[0])
                except ValueError:
                    limit = 500
                self._send_json(build_dashboard_payload(
                    log_path, limit, int(copy_settings.get("audit_retention_days", 30))
                ))
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
                self._send_json(build_shadow_payload(
                    data_dir / "shadow-executions.ndjson",
                    limit,
                    int(copy_settings.get("audit_retention_days", 30)),
                ))
                return
            if parsed.path == "/api/risk-decisions":
                query = parse_qs(parsed.query)
                try:
                    limit = int(query.get("limit", ["500"])[0])
                except ValueError:
                    limit = 500
                self._send_json(build_risk_payload(
                    risk_log_path, limit, int(risk_settings.get("audit_retention_days", 30))
                ))
                return
            if parsed.path == "/api/wallet-registry":
                self._send_json(build_identity_payload(
                    registry_path, risk_log_path, int(risk_settings.get("audit_retention_days", 30)),
                    data_dir / "following-ids.json", intelligence_db,
                ))
                return
            if parsed.path == "/api/wallet-management":
                try:
                    self._send_json(wallet_store.snapshot())
                except WalletManagementError as exc:
                    self._send_json({"error": str(exc)}, 500)
                return
            if parsed.path == "/api/rpc-management":
                try:
                    self._send_json(rpc_store.snapshot())
                except (RpcManagementError, OSError, sqlite3.Error) as exc:
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
                payload = portfolio_snapshot(portfolio_db, portfolio_account, limit, portfolio_mark_stale)
                payload["exitPolicy"] = exit_policy_store.read()
                self._send_json(payload)
                return
            if parsed.path == "/api/portfolio-exit-policy":
                self._send_json(exit_policy_store.read())
                return
            if parsed.path == "/api/execution-readiness":
                self._send_json(public_execution_readiness(project_dir, cfg))
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
            if parsed.path not in {"/api/wallet-management", "/api/rpc-management", "/api/portfolio-exit-policy"}:
                self._send_json({"error": "not_found"}, 404)
                return
            if not self._admin_authorized():
                self._send_json({"error": "admin_authentication_required"}, 403)
                return
            content_type = str(self.headers.get("Content-Type") or "").lower()
            if "application/json" not in content_type:
                self._send_json({"error": "content_type_must_be_application_json"}, 415)
                return
            try:
                payload = self._read_json_body()
                if parsed.path == "/api/wallet-management":
                    result = wallet_store.mutate(payload)
                elif parsed.path == "/api/rpc-management":
                    result = rpc_store.mutate(payload)
                else:
                    result = exit_policy_store.write(payload)
                self._send_json(result)
            except OverflowError as exc:
                self._send_json({"error": str(exc)}, 413)
            except WalletManagementError as exc:
                self._send_json({"error": str(exc)}, 400)
            except RpcManagementError as exc:
                self._send_json({"error": str(exc)}, 400)
            except ExitPolicyError as exc:
                self._send_json({"error": str(exc)}, 400)
            except OSError:
                logging.exception("本地管理配置写入失败")
                self._send_json({"error": "本地配置写入失败"}, 500)

        def log_message(self, format: str, *args: Any) -> None:
            return

    try:
        server = ThreadingHTTPServer((host, port), DashboardHandler)
        server.daemon_threads = True
    except OSError:
        change_bus.stop()
        logging.exception("可视化面板启动失败：http://%s:%d", host, port)
        return None
    thread = threading.Thread(target=server.serve_forever, name="fomo-dashboard", daemon=True)
    original_server_close = server.server_close
    def close_with_watcher() -> None:
        change_bus.stop()
        original_server_close()
    server.server_close = close_with_watcher  # type: ignore[method-assign]
    server.change_bus = change_bus  # type: ignore[attr-defined]
    thread.start()
    logging.info("可视化面板已启动：http://%s:%d", host, port)
    return server
