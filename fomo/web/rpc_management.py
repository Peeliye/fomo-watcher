from __future__ import annotations

import json
import os
import ipaddress
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from ..execution.rpc_pool import load_rpc_endpoints, rpc_health_snapshot, run_rpc_probe_endpoints
from ..execution.networks import enabled_chain_ids, network_snapshot, save_enabled_chain_ids


class RpcManagementError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_url(value: Any, websocket: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(character in text for character in ("\r", "\n", "\x00")):
        raise RpcManagementError("RPC URL 含有非法控制字符")
    parsed = urlparse(text)
    allowed = {"ws", "wss"} if websocket else {"http", "https"}
    if parsed.scheme.lower() not in allowed or not parsed.hostname:
        kind = "WebSocket" if websocket else "HTTP"
        raise RpcManagementError(f"{kind} RPC URL 格式无效")
    if parsed.username or parsed.password:
        raise RpcManagementError("RPC URL 不能包含 username:password 形式的凭据")
    if len(text) > 2048:
        raise RpcManagementError("RPC URL 过长")
    hostname = str(parsed.hostname or "").rstrip(".").casefold()
    if hostname in {"localhost", "metadata", "metadata.google.internal"} or hostname.endswith((".localhost", ".local", ".internal")):
        raise RpcManagementError("RPC URL 不得访问本机、内网或云 metadata")
    addresses: set[str] = set()
    try:
        addresses.add(str(ipaddress.ip_address(hostname)))
    except ValueError:
        try:
            addresses.update(str(item[4][0]) for item in socket.getaddrinfo(
                hostname, parsed.port or 443, type=socket.SOCK_STREAM
            ))
        except socket.gaierror:
            pass
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global or ip.is_loopback or ip.is_private or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            raise RpcManagementError("RPC URL 不得访问本机、内网或云 metadata")
    return text


def _masked_host(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}/…"


class RpcManagementStore:
    def __init__(self, project_dir: Path, cfg: dict[str, Any], audit_path: Path,
                 on_network_change: Callable[[], None] | None = None):
        self.project_dir = project_dir
        self.cfg = cfg
        self.env_path = project_dir / ".env"
        self.audit_path = audit_path
        self.on_network_change = on_network_change
        self._lock = threading.RLock()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            health = rpc_health_snapshot(self.project_dir, self.cfg)
            endpoint_map = {item["endpointId"]: item for item in health.get("endpoints", [])}
            managed = []
            for endpoint in load_rpc_endpoints(self.cfg):
                row = dict(endpoint_map.get(endpoint.endpoint_id, endpoint.public_dict()))
                resolved_http = endpoint.resolved_http_url
                resolved_ws = (os.getenv(endpoint.ws_env or "", "").strip() if endpoint.ws_env else "") or str(endpoint.public_ws_url or "")
                row.update({
                    "httpSource": "environment" if endpoint.http_env and os.getenv(endpoint.http_env, "").strip() else "public" if endpoint.public_http_url else "none",
                    "wsSource": "environment" if endpoint.ws_env and os.getenv(endpoint.ws_env, "").strip() else "public" if endpoint.public_ws_url else "none",
                    "maskedHttpUrl": _masked_host(resolved_http),
                    "maskedWsUrl": _masked_host(resolved_ws),
                    "httpEditable": bool(endpoint.http_env),
                    "wsEditable": bool(endpoint.ws_env),
                })
                managed.append(row)
            health["endpoints"] = managed
            health["networks"] = network_snapshot(self.cfg)
            health["enabledNetworks"] = sum(1 for item in health["networks"] if item["enabled"])
            health["secretsExposed"] = False
            return health

    def _write_env(self, changes: Mapping[str, str]) -> None:
        try:
            original = self.env_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            original = ""
        lines = original.splitlines()
        remaining = dict(changes)
        output: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in line:
                output.append(line)
                continue
            key = line.split("=", 1)[0].strip()
            if key in remaining:
                output.append(f"{key}={remaining.pop(key)}")
            else:
                output.append(line)
        if remaining and output and output[-1] != "":
            output.append("")
        output.extend(f"{key}={value}" for key, value in remaining.items())
        temporary = self.env_path.with_name(self.env_path.name + ".tmp")
        temporary.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")
        os.replace(temporary, self.env_path)
        for key, value in changes.items():
            if value:
                os.environ[key] = value
            else:
                os.environ.pop(key, None)

    def _audit(self, action: str, endpoint_id: str, fields: list[str] | None = None) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"at": _now(), "action": action, "endpointId": endpoint_id, "fields": fields or []}, ensure_ascii=False) + "\n")

    def mutate(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action") or "")
        endpoint_id = str(payload.get("endpointId") or "")
        with self._lock:
            endpoint = next((item for item in load_rpc_endpoints(self.cfg) if item.endpoint_id == endpoint_id), None) if endpoint_id else None
            if action == "set_chain_enabled":
                chain_id = str(payload.get("chainId") or "").strip()
                networks = {str(item["chainId"]): item for item in network_snapshot(self.cfg)}
                if chain_id not in networks:
                    raise RpcManagementError("未找到该网络")
                if not isinstance(payload.get("enabled"), bool):
                    raise RpcManagementError("网络开关值无效")
                current = enabled_chain_ids(self.cfg)
                enabled = payload["enabled"]
                next_ids = [value for value in current if value != chain_id]
                if enabled and chain_id not in next_ids:
                    next_ids.append(chain_id)
                try:
                    save_enabled_chain_ids(self.project_dir, self.cfg, next_ids)
                except ValueError as exc:
                    raise RpcManagementError(str(exc)) from exc
                if self.on_network_change is not None:
                    self.on_network_change()
                self._audit("enable_network" if enabled else "disable_network", chain_id)
                return {"ok": True, "snapshot": self.snapshot()}
            if action == "save_endpoint":
                if endpoint is None:
                    raise RpcManagementError("未找到该 RPC 节点")
                changes: dict[str, str] = {}
                if "httpUrl" in payload:
                    if not endpoint.http_env:
                        raise RpcManagementError("这个内置公共 HTTP 节点不可修改")
                    changes[endpoint.http_env] = _safe_url(payload.get("httpUrl"))
                if "wsUrl" in payload:
                    if not endpoint.ws_env:
                        raise RpcManagementError("这个节点没有可编辑的 WebSocket 插槽")
                    changes[endpoint.ws_env] = _safe_url(payload.get("wsUrl"), websocket=True)
                if not changes:
                    raise RpcManagementError("没有需要保存的修改")
                self._write_env(changes)
                self._audit("save", endpoint_id, sorted(changes))
                return {"ok": True, "snapshot": self.snapshot()}
            if action in {"test_endpoint", "test_all"}:
                selected = {endpoint_id} if action == "test_endpoint" else None
                if action == "test_endpoint" and endpoint is None:
                    raise RpcManagementError("未找到该 RPC 节点")
                if action == "test_endpoint" and endpoint is not None and not endpoint.http_configured:
                    raise RpcManagementError("该节点尚未配置 HTTP RPC URL")
                if action == "test_endpoint" and endpoint is not None and endpoint.chain_id not in set(enabled_chain_ids(self.cfg)):
                    raise RpcManagementError("该网络已禁用，请先启用后再测试")
                samples = max(1, min(int(payload.get("samples", 3)), 5))
                timeout = max(0.5, min(float(payload.get("timeoutSeconds", 3)), 10.0))
                try:
                    result = run_rpc_probe_endpoints(self.project_dir, self.cfg, selected, samples, timeout)
                except ValueError as exc:
                    raise RpcManagementError(str(exc)) from exc
                self._audit("test", endpoint_id or "all")
                return {"ok": True, "snapshot": self.snapshot(), "probe": result}
        raise RpcManagementError("未知的 RPC 管理操作")
