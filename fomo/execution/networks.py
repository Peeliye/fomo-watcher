"""Persistent runtime control for copy-trading networks."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


CHAIN_NAMES = {
    "1": "Ethereum",
    "56": "BNB Chain",
    "137": "Polygon",
    "4663": "Robinhood",
    "5042": "ARC",
    "8453": "Base",
    "1399811149": "Solana",
}


def _chain_ids(values: Iterable[Any]) -> list[str]:
    output: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in output:
            output.append(text)
    return output


def configured_chain_ids(cfg: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for section, key in (("copy_trading", "network_ids"), ("execution", "enabled_chain_ids")):
        settings = cfg.get(section, {})
        if isinstance(settings, dict) and isinstance(settings.get(key), list):
            values.extend(settings[key])
    endpoints = cfg.get("rpc_pool", {}).get("endpoints", {})
    if isinstance(endpoints, dict):
        values.extend(endpoints)
    routes = cfg.get("routing", {}).get("providers", {})
    if isinstance(routes, dict):
        values.extend(routes)
    discovered = _chain_ids(values)
    canonical = [chain_id for chain_id in CHAIN_NAMES if chain_id in discovered]
    return canonical + [chain_id for chain_id in discovered if chain_id not in CHAIN_NAMES]


def network_settings_path(project_dir: Path, cfg: dict[str, Any]) -> Path:
    settings = cfg.get("rpc_management", {})
    value = Path(str(settings.get("network_settings_path", "data/network-settings.json")))
    return value if value.is_absolute() else project_dir / value


def enabled_chain_ids(cfg: dict[str, Any]) -> list[str]:
    runtime = cfg.get("_runtime_enabled_chain_ids")
    if isinstance(runtime, list):
        return _chain_ids(runtime)
    copy_settings = cfg.get("copy_trading", {})
    if isinstance(copy_settings, dict) and isinstance(copy_settings.get("network_ids"), list):
        return _chain_ids(copy_settings["network_ids"])
    execution = cfg.get("execution", {})
    if isinstance(execution, dict) and isinstance(execution.get("enabled_chain_ids"), list):
        return _chain_ids(execution["enabled_chain_ids"])
    return configured_chain_ids(cfg)


def _apply(cfg: dict[str, Any], chain_ids: Iterable[Any]) -> list[str]:
    enabled = _chain_ids(chain_ids)
    numeric = [int(value) if value.isdigit() else value for value in enabled]
    cfg.setdefault("copy_trading", {})["network_ids"] = list(numeric)
    cfg.setdefault("execution", {})["enabled_chain_ids"] = list(numeric)
    cfg["_runtime_enabled_chain_ids"] = list(enabled)
    return enabled


def apply_network_settings(project_dir: Path, cfg: dict[str, Any]) -> list[str]:
    """Load the persisted override, falling back to the checked-in defaults."""
    path = network_settings_path(project_dir, cfg)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        values = payload.get("enabledChainIds")
        if not isinstance(values, list):
            raise ValueError("enabledChainIds must be an array")
        configured = set(configured_chain_ids(cfg))
        return _apply(cfg, (value for value in values if str(value) in configured))
    except FileNotFoundError:
        return _apply(cfg, enabled_chain_ids(cfg))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return _apply(cfg, enabled_chain_ids(cfg))


def save_enabled_chain_ids(project_dir: Path, cfg: dict[str, Any], chain_ids: Iterable[Any]) -> list[str]:
    configured = configured_chain_ids(cfg)
    requested = _chain_ids(chain_ids)
    unknown = sorted(set(requested) - set(configured))
    if unknown:
        raise ValueError("unknown network: " + ", ".join(unknown))
    requested_set = set(requested)
    enabled = _apply(cfg, (chain_id for chain_id in configured if chain_id in requested_set))
    path = network_settings_path(project_dir, cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps({
        "version": 1,
        "enabledChainIds": [int(value) if value.isdigit() else value for value in enabled],
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)
    return enabled


def network_snapshot(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    enabled = set(enabled_chain_ids(cfg))
    endpoints = cfg.get("rpc_pool", {}).get("endpoints", {})
    endpoints = endpoints if isinstance(endpoints, dict) else {}
    return [{
        "chainId": int(chain_id) if chain_id.isdigit() else chain_id,
        "name": CHAIN_NAMES.get(chain_id, chain_id),
        "enabled": chain_id in enabled,
        "endpointCount": len(endpoints.get(chain_id, [])) if isinstance(endpoints.get(chain_id, []), list) else 0,
    } for chain_id in configured_chain_ids(cfg)]
