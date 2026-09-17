"""Safely maintain the public KOL wallet registry.

Only public addresses and evidence references belong here. Never store a
private key, seed phrase, signing token, or RPC credential in this registry.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fomo.risk.engine import WalletEntry, WalletRegistry, normalize_wallet


DEFAULT_REGISTRY = Path(__file__).resolve().parents[1] / "wallet-registry.json"


def load_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "wallets": []}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("wallets", []), list):
        raise ValueError("wallet registry must be a JSON object with a wallets array")
    WalletRegistry.from_dict(value)
    return value


def save_document(path: Path, document: dict[str, Any]) -> None:
    WalletRegistry.from_dict(document)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def register_wallet(path: Path, entry_data: dict[str, Any]) -> dict[str, Any]:
    entry = WalletEntry.from_dict(entry_data)
    if not entry.kol_id or not entry.handle:
        raise ValueError("kolId and handle are required")
    if not entry.evidence or any(not item.reference.strip() for item in entry.evidence):
        raise ValueError("at least one non-empty evidence reference is required")
    document = load_document(path)
    replacement = {
        "kolId": entry.kol_id,
        "handle": entry.handle,
        "chainIds": list(entry.chain_ids),
        "address": entry.address,
        "confidence": float(entry.confidence),
        "evidence": [
            {"type": item.evidence_type, "reference": item.reference, "recordedAt": item.recorded_at.isoformat()}
            for item in entry.evidence
        ],
        "verifiedAt": entry.verified_at.isoformat(),
        "expiresAt": entry.expires_at.isoformat(),
        "status": entry.status,
    }
    wallets = list(document.get("wallets", []))
    matched = False
    for index, existing in enumerate(wallets):
        existing_chains = [str(value) for value in existing.get("chainIds", [])]
        same_family_address = any(
            normalize_wallet(chain_id, str(existing.get("address", ""))) == entry.address
            for chain_id in existing_chains
            if (chain_id == "1399811149") == (entry.chain_ids[0] == "1399811149")
        )
        if str(existing.get("kolId", "")) == entry.kol_id and same_family_address:
            wallets[index] = replacement
            matched = True
            break
    if not matched:
        wallets.append(replacement)
    updated = {**document, "version": int(document.get("version", 1)) + 1, "wallets": wallets}
    save_document(path, updated)
    return replacement


def revoke_wallet(path: Path, kol_id: str, chain_id: str, address: str) -> dict[str, Any]:
    document = load_document(path)
    wanted = normalize_wallet(chain_id, address)
    revoked = None
    for entry in document.get("wallets", []):
        chains = [str(value) for value in entry.get("chainIds", [])]
        if str(entry.get("kolId", "")) != kol_id or str(chain_id) not in chains:
            continue
        if normalize_wallet(chain_id, str(entry.get("address", ""))) == wanted:
            entry["status"] = "revoked"
            revoked = entry
            break
    if revoked is None:
        raise ValueError("matching wallet entry not found")
    document["version"] = int(document.get("version", 1)) + 1
    save_document(path, document)
    return revoked


def _now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Maintain the public KOL wallet identity registry")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="print non-secret registry entries")
    add = commands.add_parser("add", help="add or replace a public wallet mapping")
    add.add_argument("--kol-id", required=True)
    add.add_argument("--handle", required=True)
    add.add_argument("--chain-id", action="append", required=True)
    add.add_argument("--address", required=True)
    add.add_argument("--confidence", type=float, required=True)
    add.add_argument("--evidence-type", required=True)
    add.add_argument("--evidence-ref", required=True)
    add.add_argument("--expires-days", type=int, default=30)
    add.add_argument("--status", choices=("active", "shadow-only"), default="shadow-only")
    revoke = commands.add_parser("revoke", help="revoke one public wallet mapping")
    revoke.add_argument("--kol-id", required=True)
    revoke.add_argument("--chain-id", required=True)
    revoke.add_argument("--address", required=True)
    args = parser.parse_args()

    if args.command == "list":
        document = load_document(args.registry)
        print(json.dumps(document, ensure_ascii=False, indent=2))
        return 0
    if args.command == "add":
        if args.expires_days < 1:
            raise ValueError("expires-days must be at least 1")
        now = _now()
        result = register_wallet(args.registry, {
            "kolId": args.kol_id,
            "handle": args.handle,
            "chainIds": args.chain_id,
            "address": args.address,
            "confidence": args.confidence,
            "evidence": [{"type": args.evidence_type, "reference": args.evidence_ref, "recordedAt": now.isoformat()}],
            "verifiedAt": now.isoformat(),
            "expiresAt": (now + timedelta(days=args.expires_days)).isoformat(),
            "status": args.status,
        })
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    result = revoke_wallet(args.registry, args.kol_id, args.chain_id, args.address)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
