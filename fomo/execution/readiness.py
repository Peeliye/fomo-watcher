"""Multi-chain execution readiness; configuration strings are never proof.

The default registry is inert. An explicitly supplied capability registry may
perform RPC and OS credential-store self-checks, but never exposes secrets.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..risk.engine import chain_family, normalize_wallet
from ..watching.rpc_transport import FailoverJsonRpc, RpcTransport
from .capabilities import DEFAULT_CAPABILITY_REGISTRY, CapabilityRegistry
from .journal import execution_snapshot
from .rpc_pool import RpcEndpoint, load_rpc_endpoints, rpc_pool_readiness
from .routing import route_readiness


CHAIN_NAMES = {
    "1": "Ethereum", "56": "BNB Chain",
    "4663": "Robinhood", "5042": "ARC", "8453": "Base", "1399811149": "Solana",
}
NATIVE_SYMBOLS = {"1": "ETH", "56": "BNB", "4663": "ETH", "5042": "USDC", "8453": "ETH", "1399811149": "SOL"}


def _native_balance(chain_id: str, address: str, rpc: RpcTransport) -> dict[str, Any]:
    try:
        if chain_id == "1399811149":
            result = rpc.call("getBalance", [address, {"commitment": "confirmed"}])
            value = int(result["value"])
            amount = Decimal(value) / Decimal(10**9)
        else:
            result = rpc.call("eth_getBalance", [address, "latest"])
            value = int(result, 16) if isinstance(result, str) and result.startswith("0x") else int(result)
            amount = Decimal(value) / Decimal(10**18)
        if value < 0:
            raise ValueError("negative_native_balance")
        return {"chainId": int(chain_id), "symbol": NATIVE_SYMBOLS[chain_id], "balance": format(amount, "f"),
                "available": True, "rpcIdentityVerified": True, "executionReady": False}
    except Exception as error:
        return {"chainId": int(chain_id), "symbol": NATIVE_SYMBOLS.get(chain_id), "balance": None,
                "available": False, "rpcIdentityVerified": False, "executionReady": False,
                "error": type(error).__name__}


def wallet_balance_snapshot(project_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    readiness = execution_readiness(project_dir, cfg)
    pool_endpoints = load_rpc_endpoints(cfg)
    jobs = []
    for chain in readiness["chains"]:
        chain_id, address = str(chain["chainId"]), chain.get("address")
        endpoints = [item for item in pool_endpoints if item.chain_id == chain_id and item.http_configured]
        env_key = str(chain.get("rpcEnv") or "")
        if not endpoints and env_key and os.getenv(env_key, "").strip():
            endpoints = [RpcEndpoint(chain_id, "legacy-config", "primary", http_env=env_key)]
        if address and endpoints:
            jobs.append((chain_id, address, FailoverJsonRpc(chain_id, endpoints)))
    with ThreadPoolExecutor(max_workers=min(6, max(1, len(jobs)))) as pool:
        balances = list(pool.map(lambda item: _native_balance(*item), jobs)) if jobs else []
    return {"walletId": readiness["walletId"], "accounts": readiness["accounts"], "balances": balances,
            "nativeOnly": True, "executionReady": False}


def load_wallet_profile(path: str | Path) -> dict[str, Any]:
    profile = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(profile, dict) or not isinstance(profile.get("accounts", []), list):
        raise ValueError("execution wallet profile must contain an accounts array")
    return profile


def execution_readiness(project_dir: Path, cfg: dict[str, Any],
                        capabilities: CapabilityRegistry | None = None) -> dict[str, Any]:
    settings = cfg.get("execution", {})
    profile_value = Path(str(settings.get("wallet_profile", "execution-wallet.json")))
    profile_path = profile_value if profile_value.is_absolute() else project_dir / profile_value
    enabled_chains = [str(value) for value in settings.get("enabled_chain_ids", CHAIN_NAMES)]
    blockers: list[str] = []
    try:
        profile = load_wallet_profile(profile_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        profile = {"walletId": "unconfigured", "accounts": [], "signer": {"backend": "disabled"}}
        blockers.append(f"wallet_profile_invalid:{type(error).__name__}")

    accounts = []
    chain_accounts: dict[str, list[dict[str, Any]]] = {chain_id: [] for chain_id in enabled_chains}
    for raw in profile.get("accounts", []):
        if not isinstance(raw, dict):
            continue
        family = str(raw.get("family") or "")
        address = str(raw.get("address") or "").strip()
        chain_ids = [str(value) for value in raw.get("chainIds", [])]
        valid = False
        error = None
        if address and chain_ids:
            try:
                normalized = normalize_wallet(chain_ids[0], address)
                if any(chain_family(chain_id) != family for chain_id in chain_ids):
                    raise ValueError("account family does not match chainIds")
                address = normalized
                valid = True
            except ValueError as exc:
                error = str(exc)
        account = {
            "accountId": str(raw.get("accountId") or ""), "family": family,
            "address": address or None, "chainIds": chain_ids, "valid": valid, "error": error,
        }
        accounts.append(account)
        if valid:
            for chain_id in chain_ids:
                if chain_id in chain_accounts:
                    chain_accounts[chain_id].append(account)

    evm_addresses = {account["address"] for account in accounts if account["valid"] and account["family"] == "evm"}
    if len(evm_addresses) > 1:
        blockers.append("multiple_evm_execution_addresses")

    rpc_settings = settings.get("rpc", {}) if isinstance(settings.get("rpc", {}), dict) else {}
    pool = rpc_pool_readiness(cfg)
    routes = route_readiness(cfg)
    chains = []
    for chain_id in enabled_chains:
        candidates = chain_accounts.get(chain_id, [])
        if len(candidates) != 1:
            blockers.append(f"wallet_address_required:{chain_id}" if not candidates else f"ambiguous_execution_account:{chain_id}")
        rpc_item = rpc_settings.get(chain_id, {}) if isinstance(rpc_settings.get(chain_id, {}), dict) else {}
        env_key = str(rpc_item.get("env") or "")
        pool_items = pool["chains"].get(chain_id, [])
        rpc_configured = any(item["httpConfigured"] for item in pool_items) if pool_items else bool(env_key and os.getenv(env_key, "").strip())
        if not rpc_configured:
            blockers.append(f"rpc_required:{chain_id}")
        route = next((item for item in routes["chains"] if str(item["chainId"]) == chain_id), None)
        ready_routes = [item for item in (route or {}).get("providers", []) if item.get("ready")]
        if len(ready_routes) < int(routes.get("minimumIndependentRoutes", 2)):
            blockers.append(f"independent_route_adapters_required:{chain_id}")
        chains.append({
            "chainId": int(chain_id) if chain_id.isdigit() else chain_id,
            "name": CHAIN_NAMES.get(chain_id, chain_id), "family": chain_family(chain_id),
            "accountId": candidates[0]["accountId"] if len(candidates) == 1 else None,
            "address": candidates[0]["address"] if len(candidates) == 1 else None,
            "rpcEnv": env_key or None, "rpcConfigured": rpc_configured,
            "rpcProbeReachable": None, "rpcExecutionMethodsVerified": False,
            "rpcConfiguredEndpoints": sum(1 for item in pool_items if item["httpConfigured"]),
            "rpcTotalEndpoints": len(pool_items),
        })

    signer = profile.get("signer", {}) if isinstance(profile.get("signer", {}), dict) else {}
    execution_mode = str(profile.get("mode") or "disabled")
    signer_backend = str(signer.get("backend") or "disabled")
    capability_status = (capabilities or DEFAULT_CAPABILITY_REGISTRY).status()
    signer_status = capability_status["capabilities"]["signer"]
    broadcaster_status = capability_status["capabilities"]["broadcaster"]
    signer_configured = bool(signer_status["implemented"] and signer_status["ready"])
    broadcaster_configured = bool(broadcaster_status["implemented"] and broadcaster_status["ready"])
    if not signer_configured:
        blockers.append("signer_required")
    if not broadcaster_configured:
        blockers.append("broadcaster_required")
    if not capability_status["ready"]:
        blockers.append("execution_capabilities_required")
    if execution_mode != "live":
        blockers.append("execution_mode_not_live")
    if routes.get("mode") != "live":
        blockers.append("routing_mode_not_live")
    control = execution_snapshot(project_dir / "data" / "execution.sqlite3", limit=1)["control"]
    live_armed = bool(control.get("liveArmed")) and not bool(control.get("circuitBreakerTripped", True))
    if not live_armed:
        blockers.append("live_execution_lock_disabled")
    address_blocked = any(reason.startswith(("wallet_", "ambiguous_", "multiple_evm")) for reason in blockers)
    rpc_blocked = any(reason.startswith("rpc_required") for reason in blockers)
    route_blocked = any(reason.startswith("independent_route_adapters_required") for reason in blockers)
    stage = (
        "wallet_addresses" if address_blocked else
        "rpc" if rpc_blocked else
        "routing" if route_blocked else
        "signer" if not signer_configured else
        "broadcaster" if not broadcaster_configured else
        "capabilities" if not capability_status["ready"] else
        "mode" if execution_mode != "live" or routes.get("mode") != "live" else
        "live_lock" if not live_armed else
        "live_execution_ready"
    )
    return {
        "walletId": str(profile.get("walletId") or "unconfigured"),
        "mode": execution_mode,
        "liveArmed": live_armed,
        "stage": stage, "ready": stage == "live_execution_ready",
        "readOnly": stage != "live_execution_ready", "signerBackend": signer_backend, "signerConfigured": signer_configured,
        "broadcasterConfigured": broadcaster_configured,
        "capabilityRegistry": capability_status,
        "accounts": accounts, "chains": chains, "rpcPool": pool, "routeReadiness": routes,
        "blockers": sorted(set(blockers)),
        "note": "One logical wallet profile uses one EVM address across EVM chains and one Solana address. No secret is loaded here.",
    }
