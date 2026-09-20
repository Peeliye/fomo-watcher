"""Secret-safe RPC pool configuration and persistent health telemetry.

Endpoint URLs remain in environment variables. This module stores only logical
endpoint identities and measurements, so dashboard/API output cannot leak keys.
"""

from __future__ import annotations

import os
import sqlite3
import json
import time
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import urlparse

from curl_cffi import requests as cf
from curl_cffi.const import CurlOpt

from .networks import enabled_chain_ids
from .url_safety import UnsafeEndpointError, validate_endpoint_url


@dataclass(frozen=True)
class RpcEndpoint:
    chain_id: str
    provider: str
    role: str
    http_env: str = ""
    ws_env: str | None = None
    region: str = "auto"
    priority: int = 100
    public_http_url: str | None = None
    public_ws_url: str | None = None

    @property
    def endpoint_id(self) -> str:
        return f"{self.chain_id}:{self.provider}:{self.role}"

    @property
    def http_configured(self) -> bool:
        return bool((self.http_env and os.getenv(self.http_env, "").strip()) or self.public_http_url)

    @property
    def ws_configured(self) -> bool:
        return bool((self.ws_env and os.getenv(self.ws_env, "").strip()) or self.public_ws_url)

    @property
    def resolved_http_url(self) -> str:
        return (os.getenv(self.http_env, "").strip() if self.http_env else "") or str(self.public_http_url or "")

    def public_dict(self) -> dict[str, Any]:
        return {
            "endpointId": self.endpoint_id,
            "chainId": int(self.chain_id) if self.chain_id.isdigit() else self.chain_id,
            "provider": self.provider,
            "role": self.role,
            "region": self.region,
            "priority": self.priority,
            "httpEnv": self.http_env,
            "wsEnv": self.ws_env,
            "httpConfigured": self.http_configured,
            "wsConfigured": self.ws_configured,
        }


def load_rpc_endpoints(cfg: dict[str, Any]) -> list[RpcEndpoint]:
    pool = cfg.get("rpc_pool", {})
    raw_chains = pool.get("endpoints", {}) if isinstance(pool, dict) else {}
    endpoints: list[RpcEndpoint] = []
    if isinstance(raw_chains, dict):
        for chain_id, items in raw_chains.items():
            if not isinstance(items, list):
                continue
            for index, item in enumerate(items):
                if not isinstance(item, dict) or not (item.get("http_env") or item.get("public_http_url")):
                    continue
                endpoints.append(RpcEndpoint(
                    chain_id=str(chain_id),
                    provider=str(item.get("provider") or f"rpc-{index + 1}"),
                    role=str(item.get("role") or ("primary" if index == 0 else "backup")),
                    http_env=str(item.get("http_env") or ""),
                    ws_env=str(item["ws_env"]) if item.get("ws_env") else None,
                    region=str(item.get("region") or "auto"),
                    priority=int(item.get("priority", (index + 1) * 10)),
                    public_http_url=str(item["public_http_url"]) if item.get("public_http_url") else None,
                    public_ws_url=str(item["public_ws_url"]) if item.get("public_ws_url") else None,
                ))
    return sorted(endpoints, key=lambda item: (item.chain_id, item.priority, item.provider))


def rpc_pool_readiness(cfg: dict[str, Any]) -> dict[str, Any]:
    endpoints = load_rpc_endpoints(cfg)
    by_chain: dict[str, list[dict[str, Any]]] = {}
    for endpoint in endpoints:
        by_chain.setdefault(endpoint.chain_id, []).append(endpoint.public_dict())
    return {
        "configured": sum(1 for endpoint in endpoints if endpoint.http_configured),
        "total": len(endpoints),
        "websocketConfigured": sum(1 for endpoint in endpoints if endpoint.ws_configured),
        "chains": by_chain,
    }


def probe_rpc_endpoint(endpoint: RpcEndpoint, timeout_seconds: float = 2.0) -> dict[str, Any]:
    """Probe one endpoint without ever returning or persisting its URL."""
    url = endpoint.resolved_http_url
    if not url:
        return {"success": False, "latency_ms": None, "block_height": None, "error_code": "not_configured"}
    method = "getSlot" if endpoint.chain_id == "1399811149" else "eth_blockNumber"
    started = time.perf_counter()
    try:
        url, first_addresses = validate_endpoint_url(url)
        _, second_addresses = validate_endpoint_url(url)
        if first_addresses != second_addresses:
            return {"success": False, "latency_ms": None, "block_height": None,
                    "error_code": "dns_rebinding_rejected"}
        parsed = urlparse(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        host = str(parsed.hostname)
        resolve = [f"{host}:{port}:{'[' + address + ']' if ':' in address else address}"
                   for address in sorted(second_addresses)]
        response = cf.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": []},
                           headers={"Accept": "application/json"}, timeout=timeout_seconds,
                           impersonate="chrome", allow_redirects=False, proxy="",
                           curl_options={CurlOpt.RESOLVE: resolve, CurlOpt.PROXY: ""})
        latency_ms = (time.perf_counter() - started) * 1000
        if response.primary_ip and response.primary_ip not in second_addresses:
            return {"success": False, "latency_ms": latency_ms, "block_height": None,
                    "error_code": "connected_address_mismatch"}
        if 300 <= response.status_code < 400:
            return {"success": False, "latency_ms": latency_ms, "block_height": None,
                    "error_code": "redirect_rejected"}
        if response.status_code >= 400:
            return {"success": False, "latency_ms": latency_ms, "block_height": None,
                    "error_code": f"http_{response.status_code}"}
        result = response.json().get("result")
        height = int(result, 16) if isinstance(result, str) and result.startswith("0x") else int(result)
        return {"success": True, "latency_ms": latency_ms, "block_height": height, "error_code": None}
    except UnsafeEndpointError as error:
        return {"success": False, "latency_ms": (time.perf_counter() - started) * 1000,
                "block_height": None, "error_code": str(error)}
    except (OSError, ValueError, TypeError, json.JSONDecodeError, cf.RequestsError) as error:
        return {"success": False, "latency_ms": (time.perf_counter() - started) * 1000,
                "block_height": None, "error_code": type(error).__name__}


def run_rpc_probe_cycle(project_dir: Path, cfg: dict[str, Any], samples: int = 3, timeout_seconds: float = 2.0) -> dict[str, Any]:
    return run_rpc_probe_endpoints(project_dir, cfg, None, samples, timeout_seconds)


def run_rpc_probe_endpoints(
    project_dir: Path,
    cfg: dict[str, Any],
    endpoint_ids: set[str] | None = None,
    samples: int = 3,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Probe selected logical endpoints and persist results without exposing URLs."""
    enabled = set(enabled_chain_ids(cfg))
    endpoints = [
        endpoint for endpoint in load_rpc_endpoints(cfg)
        if endpoint.http_configured and endpoint.chain_id in enabled
        and (endpoint_ids is None or endpoint.endpoint_id in endpoint_ids)
    ]
    if endpoint_ids is not None:
        known = {endpoint.endpoint_id for endpoint in load_rpc_endpoints(cfg)}
        missing = endpoint_ids - known
        if missing:
            raise ValueError("unknown RPC endpoint: " + ", ".join(sorted(missing)))
    settings = cfg.get("rpc_pool", {})
    database = Path(str(settings.get("database", "data/rpc-health.sqlite3")))
    if not database.is_absolute():
        database = project_dir / database
    max_samples = max(100, int(settings.get("max_samples_per_endpoint", 5000)))
    store = RpcHealthStore(database, max_samples_per_endpoint=max_samples)
    try:
        if endpoints:
            batch: list[tuple[RpcEndpoint, dict[str, Any]]] = []
            with ThreadPoolExecutor(max_workers=min(12, len(endpoints))) as executor:
                for _ in range(max(1, min(int(samples), 20))):
                    jobs = [(endpoint, executor.submit(probe_rpc_endpoint, endpoint, max(0.2, timeout_seconds))) for endpoint in endpoints]
                    for endpoint, future in jobs:
                        method = "getSlot" if endpoint.chain_id == "1399811149" else "eth_blockNumber"
                        batch.append((endpoint, {"method": method, **future.result()}))
            store.record_many(batch)
        return store.snapshot(cfg)
    finally:
        store.close()


class RpcHealthStore:
    def __init__(self, database: str | Path, max_samples_per_endpoint: int = 5000) -> None:
        self.path = Path(database)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self.connection = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if existed and version < 2:
            backup_dir = self.path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            target = sqlite3.connect(backup_dir / f"{self.path.stem}.pre-v2.from-v{version}.{stamp}.sqlite3")
            try:
                self.connection.backup(target)
            finally:
                target.close()
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS rpc_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint_id TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                role TEXT NOT NULL,
                sampled_at TEXT NOT NULL,
                method TEXT NOT NULL,
                latency_ms REAL,
                success INTEGER NOT NULL,
                block_height INTEGER,
                error_code TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_rpc_samples_endpoint_time
                ON rpc_samples(endpoint_id, sampled_at DESC);
            CREATE INDEX IF NOT EXISTS idx_rpc_samples_endpoint_id
                ON rpc_samples(endpoint_id, id DESC);
        """)
        self.connection.execute("PRAGMA user_version=2")
        self.connection.commit()
        self.max_samples_per_endpoint = max(100, int(max_samples_per_endpoint))

    def close(self) -> None:
        self.connection.close()

    def record(
        self,
        endpoint: RpcEndpoint,
        *,
        method: str,
        latency_ms: float | None,
        success: bool,
        block_height: int | None = None,
        error_code: str | None = None,
        sampled_at: str | None = None,
    ) -> None:
        self.record_many([(
            endpoint,
            {
                "method": method,
                "latency_ms": latency_ms,
                "success": success,
                "block_height": block_height,
                "error_code": error_code,
                "sampled_at": sampled_at,
            },
        )])

    def record_many(self, samples: list[tuple[RpcEndpoint, dict[str, Any]]]) -> None:
        if not samples:
            return
        endpoint_ids = sorted({endpoint.endpoint_id for endpoint, _ in samples})
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.executemany(
                """INSERT INTO rpc_samples
                   (endpoint_id, chain_id, provider, role, sampled_at, method,
                    latency_ms, success, block_height, error_code)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        endpoint.endpoint_id,
                        endpoint.chain_id,
                        endpoint.provider,
                        endpoint.role,
                        values.get("sampled_at") or datetime.now(timezone.utc).isoformat(),
                        values["method"],
                        values.get("latency_ms"),
                        int(bool(values.get("success"))),
                        values.get("block_height"),
                        values.get("error_code"),
                    )
                    for endpoint, values in samples
                ],
            )
            self._prune_locked(endpoint_ids, self.max_samples_per_endpoint)
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def _prune_locked(self, endpoint_ids: list[str] | None, maximum: int) -> int:
        if endpoint_ids:
            placeholders = ",".join("?" for _ in endpoint_ids)
            sql = f"""DELETE FROM rpc_samples WHERE id IN (
                SELECT id FROM (
                  SELECT id,ROW_NUMBER() OVER(PARTITION BY endpoint_id ORDER BY id DESC) AS row_num
                  FROM rpc_samples WHERE endpoint_id IN ({placeholders})
                ) WHERE row_num>?
            )"""
            cursor = self.connection.execute(sql, (*endpoint_ids, maximum))
        else:
            cursor = self.connection.execute(
                """DELETE FROM rpc_samples WHERE id IN (
                     SELECT id FROM (
                       SELECT id,ROW_NUMBER() OVER(PARTITION BY endpoint_id ORDER BY id DESC) AS row_num
                       FROM rpc_samples
                     ) WHERE row_num>?
                   )""",
                (maximum,),
            )
        return cursor.rowcount

    def prune(self, maximum: int | None = None) -> int:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            deleted = self._prune_locked(None, max(100, int(maximum or self.max_samples_per_endpoint)))
            self.connection.commit()
            self.connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            return deleted
        except BaseException:
            self.connection.rollback()
            raise

    def snapshot(self, cfg: dict[str, Any]) -> dict[str, Any]:
        pool_cfg = cfg.get("rpc_pool", {}) if isinstance(cfg.get("rpc_pool", {}), dict) else {}
        sample_window = max(3, min(int(pool_cfg.get("sample_window", 100)), 1000))
        max_p95 = float(pool_cfg.get("maximum_p95_latency_ms", 750))
        max_lag = int(pool_cfg.get("maximum_block_lag", 3))
        max_age = max(30, int(pool_cfg.get("maximum_sample_age_seconds", 300)))
        evaluation_seconds = max(max_age, int(pool_cfg.get("evaluation_window_seconds", 900)))
        now = datetime.now(timezone.utc)
        evaluation_cutoff = now - timedelta(seconds=evaluation_seconds)
        endpoints = load_rpc_endpoints(cfg)
        rows_by_endpoint: dict[str, list[sqlite3.Row]] = {}
        for endpoint in endpoints:
            available = list(self.connection.execute(
                "SELECT * FROM rpc_samples WHERE endpoint_id = ? ORDER BY id DESC LIMIT ?",
                (endpoint.endpoint_id, sample_window),
            ))
            recent = []
            for row in available:
                try:
                    sampled = datetime.fromisoformat(str(row["sampled_at"]).replace("Z", "+00:00"))
                    if sampled.astimezone(timezone.utc) >= evaluation_cutoff:
                        recent.append(row)
                except ValueError:
                    continue
            # Keep the newest stale row solely so status can say "stale"
            # instead of erasing the fact that the endpoint was tested.
            rows_by_endpoint[endpoint.endpoint_id] = recent or available[:1]

        latest_heights: dict[str, int] = {}
        for endpoint in endpoints:
            heights = [int(row["block_height"]) for row in rows_by_endpoint[endpoint.endpoint_id]
                       if row["success"] and row["block_height"] is not None]
            if heights:
                latest_heights[endpoint.chain_id] = max(latest_heights.get(endpoint.chain_id, 0), heights[0])

        enabled_chains = set(enabled_chain_ids(cfg))
        public: list[dict[str, Any]] = []
        for endpoint in endpoints:
            rows = rows_by_endpoint[endpoint.endpoint_id]
            successes = [row for row in rows if row["success"]]
            latencies = sorted(float(row["latency_ms"]) for row in successes if row["latency_ms"] is not None)
            latest_height = next((int(row["block_height"]) for row in rows if row["success"] and row["block_height"] is not None), None)
            lag = latest_heights.get(endpoint.chain_id, latest_height or 0) - latest_height if latest_height is not None else None
            p95_index = max(0, min(len(latencies) - 1, ceil(len(latencies) * 0.95) - 1)) if latencies else 0
            success_rate = len(successes) / len(rows) if rows else None
            last_sample_at = rows[0]["sampled_at"] if rows else None
            sample_age = None
            if last_sample_at:
                try:
                    sampled = datetime.fromisoformat(str(last_sample_at).replace("Z", "+00:00"))
                    sample_age = max(0.0, (now - sampled.astimezone(timezone.utc)).total_seconds())
                except ValueError:
                    sample_age = None
            telemetry_current = sample_age is not None and sample_age <= max_age
            reachable = bool(endpoint.http_configured and successes and telemetry_current)
            reliable = bool(len(rows) >= 3 and success_rate is not None and success_rate >= 0.95)
            current = bool(lag is None or lag <= max_lag)
            fast = bool(latencies and latencies[p95_index] <= max_p95)
            healthy = bool(reachable and reliable and current and fast)
            chain_enabled = endpoint.chain_id in enabled_chains
            status = (
                "disabled" if not chain_enabled else
                "unconfigured" if not endpoint.http_configured else
                "stale" if rows and not telemetry_current else
                "healthy" if healthy else
                "slow" if reachable and reliable and current else
                "degraded" if rows else "untested"
            )
            item = endpoint.public_dict()
            item.update({
                "status": status,
                "samples": len(rows),
                "successRate": round(success_rate, 4) if success_rate is not None else None,
                "medianLatencyMs": round(median(latencies), 3) if latencies else None,
                "p95LatencyMs": round(latencies[p95_index], 3) if latencies else None,
                "latestBlockHeight": latest_height,
                "blockLag": lag,
                "lastSampleAt": last_sample_at,
                "sampleAgeSeconds": round(sample_age, 1) if sample_age is not None else None,
                "telemetryCurrent": telemetry_current,
                "chainEnabled": chain_enabled,
            })
            public.append(item)

        selected: dict[str, str] = {}
        for chain_id in {endpoint.chain_id for endpoint in endpoints}:
            candidates = [item for item in public if str(item["chainId"]) == chain_id and item["status"] == "healthy"]
            if candidates:
                best = min(candidates, key=lambda item: (item["p95LatencyMs"], item["priority"]))
                selected[chain_id] = str(best["endpointId"])
        return {
            **rpc_pool_readiness(cfg),
            "reachable": sum(1 for item in public if item["chainEnabled"] and item["httpConfigured"] and item["telemetryCurrent"] and item["successRate"] is not None and item["successRate"] > 0),
            "healthy": sum(1 for item in public if item["status"] == "healthy"),
            "selected": selected,
            "endpoints": public,
            "policy": {"sampleWindow": sample_window, "evaluationWindowSeconds": evaluation_seconds,
                       "maximumP95LatencyMs": max_p95, "maximumBlockLag": max_lag, "maximumSampleAgeSeconds": max_age},
        }


def rpc_health_snapshot(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    settings = cfg.get("rpc_pool", {}) if isinstance(cfg.get("rpc_pool", {}), dict) else {}
    configured = Path(str(settings.get("database", "data/rpc-health.sqlite3")))
    database = configured if configured.is_absolute() else project_dir / configured
    store = RpcHealthStore(database)
    try:
        return store.snapshot(cfg)
    finally:
        store.close()
