from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ..risk.engine import WalletRegistry, chain_family, normalize_wallet


SUPPORTED_CHAINS = {
    "1": "Ethereum",
    "56": "BNB Chain",
    "4663": "Robinhood",
    "5042": "ARC",
    "8453": "Base",
    "1399811149": "Solana",
}
VALID_KOL_STATUSES = {"active", "shadow-only", "revoked"}
VALID_WATCH_STATUSES = {"active", "paused"}
SECRET_MARKERS = ("private", "secret", "mnemonic", "seed", "keystore")


class WalletManagementError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _entry_id(entry: Mapping[str, Any]) -> str:
    existing = str(entry.get("entryId") or "").strip()
    if existing:
        return existing
    identity = "|".join(
        [str(entry.get("kolId") or ""), str(entry.get("address") or "").lower(), ",".join(map(str, entry.get("chainIds") or []))]
    )
    return "kol_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read_document(path: Path, default: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else dict(default)
    except FileNotFoundError:
        return dict(default)
    except (OSError, json.JSONDecodeError) as exc:
        raise WalletManagementError(f"无法读取 {path.name}: {exc}") from exc


def _reject_secrets(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower().replace("_", "").replace("-", "")
            if any(marker in lowered for marker in SECRET_MARKERS):
                raise WalletManagementError("这里只能登记公开地址，不能提交私钥、助记词、种子词或密钥库")
            _reject_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secrets(child, f"{path}[{index}]")


def _chain_ids(value: Any) -> list[str]:
    raw = value if isinstance(value, list) else str(value or "").replace("|", ",").split(",")
    chain_ids = list(dict.fromkeys(str(item).strip() for item in raw if str(item).strip()))
    if not chain_ids:
        raise WalletManagementError("至少选择一条适用链")
    unsupported = [item for item in chain_ids if item not in SUPPORTED_CHAINS]
    if unsupported:
        raise WalletManagementError("不支持的链: " + ", ".join(unsupported))
    if len({chain_family(item) for item in chain_ids}) != 1:
        raise WalletManagementError("同一地址不能混合 EVM 与 Solana 网络")
    return chain_ids


def _number(value: Any, field: str, *, minimum: float = 0) -> float:
    try:
        result = float(value or 0)
    except (TypeError, ValueError) as exc:
        raise WalletManagementError(f"{field} 必须是数字") from exc
    if result < minimum or result != result or result in (float("inf"), float("-inf")):
        raise WalletManagementError(f"{field} 必须不小于 {minimum:g}")
    return result


def _boolean(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "是", "启用"}


def _iso_time(value: Any, field: str, *, required: bool = True) -> str:
    text = str(value or "").strip()
    if not text and not required:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WalletManagementError(f"{field} 不是有效时间") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalize_kol(raw: Mapping[str, Any], existing_id: str = "") -> dict[str, Any]:
    chain_ids = _chain_ids(raw.get("chainIds"))
    kol_id = str(raw.get("kolId") or "").strip()
    handle = str(raw.get("handle") or "").strip().lstrip("@")
    if not kol_id or not handle:
        raise WalletManagementError("KOL ID 和 Fomo 用户名都不能为空")
    try:
        address = normalize_wallet(chain_ids[0], str(raw.get("address") or ""))
    except ValueError as exc:
        raise WalletManagementError("钱包地址格式与所选网络不匹配") from exc
    confidence = _number(raw.get("confidence", 0.8), "可信度")
    if confidence > 1:
        raise WalletManagementError("可信度必须在 0 到 1 之间")
    status = str(raw.get("status") or "shadow-only")
    if status not in VALID_KOL_STATUSES:
        raise WalletManagementError("无效的关联状态")
    evidence = raw.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        evidence = [{
            "type": str(raw.get("evidenceType") or "user-attestation").strip(),
            "reference": str(raw.get("evidenceReference") or "").strip(),
            "recordedAt": _now(),
        }]
    normalized_evidence = []
    for item in evidence:
        if not isinstance(item, Mapping):
            raise WalletManagementError("证据格式无效")
        evidence_type = str(item.get("type") or "").strip()
        reference = str(item.get("reference") or "").strip()
        if not evidence_type or not reference:
            raise WalletManagementError("证据类型和来源都不能为空")
        normalized_evidence.append({
            "type": evidence_type,
            "reference": reference,
            "recordedAt": _iso_time(item.get("recordedAt") or _now(), "证据时间"),
        })
    verified_at = _iso_time(raw.get("verifiedAt") or _now(), "验证时间")
    expires_at = _iso_time(raw.get("expiresAt"), "有效期")
    if datetime.fromisoformat(expires_at) <= datetime.fromisoformat(verified_at):
        raise WalletManagementError("有效期必须晚于验证时间")
    result = {
        "entryId": existing_id or str(raw.get("entryId") or "").strip() or "kol_" + uuid.uuid4().hex,
        "kolId": kol_id,
        "handle": handle,
        "chainIds": chain_ids,
        "address": address,
        "confidence": confidence,
        "evidence": normalized_evidence,
        "verifiedAt": verified_at,
        "expiresAt": expires_at,
        "status": status,
    }
    return result


def _normalize_watch(raw: Mapping[str, Any], existing: Mapping[str, Any] | None = None) -> dict[str, Any]:
    chain_ids = _chain_ids(raw.get("chainIds"))
    try:
        address = normalize_wallet(chain_ids[0], str(raw.get("address") or ""))
    except ValueError as exc:
        raise WalletManagementError("钱包地址格式与所选网络不匹配") from exc
    name = str(raw.get("name") or "").strip()
    if not name:
        raise WalletManagementError("自定义名称不能为空")
    status = str(raw.get("status") or "active")
    if status not in VALID_WATCH_STATUSES:
        raise WalletManagementError("无效的观察状态")
    tags_value = raw.get("tags") or []
    tags = tags_value if isinstance(tags_value, list) else str(tags_value).replace("|", ",").split(",")
    tags = list(dict.fromkeys(str(item).strip() for item in tags if str(item).strip()))[:20]
    notifications = raw.get("notifications") if isinstance(raw.get("notifications"), Mapping) else {}
    filters = raw.get("filters") if isinstance(raw.get("filters"), Mapping) else {}
    minimum = _number(filters.get("minAmountUsd", raw.get("minAmountUsd", 0)), "最低金额")
    min_cap = _number(filters.get("minMarketCapUsd", raw.get("minMarketCapUsd", 0)), "最低市值")
    max_cap = _number(filters.get("maxMarketCapUsd", raw.get("maxMarketCapUsd", 0)), "最高市值")
    if max_cap and max_cap < min_cap:
        raise WalletManagementError("最高市值不能低于最低市值")
    now = _now()
    return {
        "id": str((existing or {}).get("id") or raw.get("id") or "watch_" + uuid.uuid4().hex),
        "name": name,
        "address": address,
        "chainIds": chain_ids,
        "tags": tags,
        "linkedKolId": str(raw.get("linkedKolId") or "").strip(),
        "linkedHandle": str(raw.get("linkedHandle") or "").strip().lstrip("@"),
        "status": status,
        "notifications": {
            "buy": _boolean(notifications.get("buy", raw.get("notifyBuy")), True),
            "sell": _boolean(notifications.get("sell", raw.get("notifySell")), True),
            "transfer": _boolean(notifications.get("transfer", raw.get("notifyTransfer")), False),
            "largeTrade": _boolean(notifications.get("largeTrade", raw.get("notifyLargeTrade")), True),
        },
        "filters": {"minAmountUsd": minimum, "minMarketCapUsd": min_cap, "maxMarketCapUsd": max_cap},
        "createdAt": str((existing or {}).get("createdAt") or raw.get("createdAt") or now),
        "updatedAt": now,
    }


class WalletManagementStore:
    def __init__(self, registry_path: Path, watchlist_path: Path, audit_path: Path, maximum_import_rows: int = 500):
        self.registry_path = registry_path
        self.watchlist_path = watchlist_path
        self.audit_path = audit_path
        self.maximum_import_rows = max(1, min(int(maximum_import_rows), 5000))
        self._lock = threading.RLock()

    def _registry(self) -> dict[str, Any]:
        document = _read_document(self.registry_path, {"version": 1, "wallets": []})
        wallets = document.get("wallets")
        if not isinstance(wallets, list):
            raise WalletManagementError("钱包身份登记表结构无效")
        for entry in wallets:
            if isinstance(entry, dict):
                entry["entryId"] = _entry_id(entry)
        return document

    def _watchlist(self) -> dict[str, Any]:
        document = _read_document(self.watchlist_path, {"version": 1, "wallets": []})
        if not isinstance(document.get("wallets"), list):
            raise WalletManagementError("观察钱包文件结构无效")
        return document

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            registry, watchlist = self._registry(), self._watchlist()
            return {
                "registryVersion": int(registry.get("version", 1)),
                "watchlistVersion": int(watchlist.get("version", 1)),
                "kolWallets": registry["wallets"],
                "watchWallets": watchlist["wallets"],
                "chainNames": SUPPORTED_CHAINS,
                "trackingStatus": "awaiting_rpc_stream_adapter",
            }

    def _audit(self, action: str, entity: str, record: Mapping[str, Any]) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "at": _now(), "action": action, "entity": entity,
            "id": record.get("entryId") or record.get("id"),
            "address": record.get("address"), "chainIds": record.get("chainIds", []),
        }
        with self.audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _save_registry(self, document: dict[str, Any]) -> None:
        document["version"] = int(document.get("version", 1)) + 1
        WalletRegistry.from_dict(document)
        _atomic_json(self.registry_path, document)

    def _save_watchlist(self, document: dict[str, Any]) -> None:
        document["version"] = int(document.get("version", 1)) + 1
        _atomic_json(self.watchlist_path, document)

    def mutate(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        _reject_secrets(payload)
        action = str(payload.get("action") or "")
        with self._lock:
            if action == "upsert_kol":
                return self._upsert_kol(payload.get("entry"))
            if action == "set_kol_status":
                return self._set_kol_status(payload)
            if action == "upsert_watch":
                return self._upsert_watch(payload.get("wallet"))
            if action == "set_watch_status":
                return self._set_watch_status(payload)
            if action == "delete_watch":
                return self._delete_watch(payload)
            if action == "import_watch":
                return self._import_watch(payload)
        raise WalletManagementError("未知的钱包管理操作")

    def _upsert_kol(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise WalletManagementError("缺少 KOL 钱包资料")
        document = self._registry()
        target_id = str(raw.get("entryId") or "")
        index = next((i for i, item in enumerate(document["wallets"]) if _entry_id(item) == target_id), None) if target_id else None
        entry = _normalize_kol(raw, target_id if index is not None else "")
        if index is None:
            document["wallets"].append(entry)
            action = "create"
        else:
            document["wallets"][index] = entry
            action = "update"
        self._save_registry(document)
        self._audit(action, "kol_wallet", entry)
        return {"ok": True, "record": entry, "snapshot": self.snapshot()}

    def _set_kol_status(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        entry_id, status = str(payload.get("entryId") or ""), str(payload.get("status") or "")
        if status not in VALID_KOL_STATUSES:
            raise WalletManagementError("无效的关联状态")
        document = self._registry()
        entry = next((item for item in document["wallets"] if _entry_id(item) == entry_id), None)
        if entry is None:
            raise WalletManagementError("未找到这条 KOL 关联")
        entry["entryId"], entry["status"] = _entry_id(entry), status
        self._save_registry(document)
        self._audit("status:" + status, "kol_wallet", entry)
        return {"ok": True, "record": entry, "snapshot": self.snapshot()}

    def _upsert_watch(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise WalletManagementError("缺少观察钱包资料")
        document = self._watchlist()
        target_id = str(raw.get("id") or "")
        index = next((i for i, item in enumerate(document["wallets"]) if str(item.get("id")) == target_id), None) if target_id else None
        existing = document["wallets"][index] if index is not None else None
        wallet = _normalize_watch(raw, existing)
        duplicate = next((item for item in document["wallets"] if item.get("id") != wallet["id"] and item.get("address") == wallet["address"] and set(item.get("chainIds", [])) & set(wallet["chainIds"])), None)
        if duplicate:
            raise WalletManagementError("该地址已在相同网络中观察")
        if index is None:
            document["wallets"].append(wallet)
            action = "create"
        else:
            document["wallets"][index] = wallet
            action = "update"
        self._save_watchlist(document)
        self._audit(action, "watch_wallet", wallet)
        return {"ok": True, "record": wallet, "snapshot": self.snapshot()}

    def _set_watch_status(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        wallet_id, status = str(payload.get("id") or ""), str(payload.get("status") or "")
        if status not in VALID_WATCH_STATUSES:
            raise WalletManagementError("无效的观察状态")
        document = self._watchlist()
        wallet = next((item for item in document["wallets"] if str(item.get("id")) == wallet_id), None)
        if wallet is None:
            raise WalletManagementError("未找到这个观察钱包")
        wallet["status"], wallet["updatedAt"] = status, _now()
        self._save_watchlist(document)
        self._audit("status:" + status, "watch_wallet", wallet)
        return {"ok": True, "record": wallet, "snapshot": self.snapshot()}

    def _delete_watch(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        wallet_id = str(payload.get("id") or "")
        document = self._watchlist()
        wallet = next((item for item in document["wallets"] if str(item.get("id")) == wallet_id), None)
        if wallet is None:
            raise WalletManagementError("未找到这个观察钱包")
        document["wallets"] = [item for item in document["wallets"] if str(item.get("id")) != wallet_id]
        self._save_watchlist(document)
        self._audit("delete", "watch_wallet", wallet)
        return {"ok": True, "snapshot": self.snapshot()}

    def _import_watch(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        import_format = str(payload.get("format") or "json").lower()
        data = payload.get("data")
        if import_format == "csv":
            if not isinstance(data, str):
                raise WalletManagementError("CSV 内容必须是文本")
            rows = list(csv.DictReader(io.StringIO(data.lstrip("\ufeff"))))
        elif import_format == "json":
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as exc:
                    raise WalletManagementError("JSON 内容无法解析") from exc
            rows = data.get("wallets", []) if isinstance(data, Mapping) else data
        else:
            raise WalletManagementError("只支持 CSV 或 JSON")
        if not isinstance(rows, list) or not rows:
            raise WalletManagementError("没有可导入的钱包")
        if len(rows) > self.maximum_import_rows:
            raise WalletManagementError(f"一次最多导入 {self.maximum_import_rows} 条")
        document = self._watchlist()
        normalized = [_normalize_watch(item) for item in rows if isinstance(item, Mapping)]
        if len(normalized) != len(rows):
            raise WalletManagementError("导入列表中存在无效行")
        existing_keys = {(item.get("address"), chain) for item in document["wallets"] for chain in item.get("chainIds", [])}
        batch_keys: set[tuple[Any, str]] = set()
        for wallet in normalized:
            for chain in wallet["chainIds"]:
                key = (wallet["address"], chain)
                if key in existing_keys or key in batch_keys:
                    raise WalletManagementError(f"重复地址: {wallet['address']} / {SUPPORTED_CHAINS[chain]}")
                batch_keys.add(key)
        document["wallets"].extend(normalized)
        self._save_watchlist(document)
        for wallet in normalized:
            self._audit("import", "watch_wallet", wallet)
        return {"ok": True, "imported": len(normalized), "snapshot": self.snapshot()}
