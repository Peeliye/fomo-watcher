"""Read-only, fail-closed watch-wallet selection for wallet RPC adapters."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fomo.risk.engine import normalize_wallet


def enabled_wallets(path: str | Path, chain_id: str) -> tuple[dict[str, Any], ...]:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("wallets"), list):
        raise ValueError("watch_wallets_invalid_document")
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in document["wallets"]:
        if not isinstance(item, dict):
            raise ValueError("watch_wallets_invalid_entry")
        if item.get("enabled") is not True or item.get("status", "active") != "active":
            continue
        chains = item.get("chains")
        if not isinstance(chains, list) or str(chain_id) not in set(map(str, chains)):
            continue
        address = normalize_wallet(str(chain_id), str(item.get("address") or ""))
        key = address.casefold() if str(chain_id) != "1399811149" else address
        if key in seen:
            raise ValueError("duplicate_enabled_watch_wallet")
        seen.add(key)
        selected.append({**item, "address": address})
    return tuple(selected)
