"""Manage the private wallet recovery phrase in the OS credential vault."""

from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path

from fomo.execution.wallet_vault import WalletVault, derive_public_accounts

PROJECT_DIR = Path(__file__).resolve().parents[1]


def apply_public_accounts(path: Path, addresses: dict[str, str]) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    for account in document.get("accounts", []):
        family = str(account.get("family") or "")
        if family in addresses:
            account["address"] = addresses[family]
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read_confirmed(prompt: str, confirmation: str) -> str:
    first = getpass.getpass(prompt)
    second = getpass.getpass(confirmation)
    if first != second:
        raise ValueError("the two hidden entries did not match")
    return first


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Store a wallet recovery phrase in the current user's OS credential vault"
    )
    parser.add_argument("--name", default="live-wallet", help="credential name (default: live-wallet)")
    commands = parser.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("init", help="securely import a recovery phrase")
    initialize.add_argument("--replace", action="store_true", help="replace an existing credential")
    initialize.add_argument(
        "--with-bip39-passphrase",
        action="store_true",
        help="also prompt for the optional BIP-39 passphrase (not the recovery phrase)",
    )
    commands.add_parser("status", help="check whether the credential exists")
    commands.add_parser("verify", help="decrypt and validate the credential without displaying it")
    derive = commands.add_parser("derive", help="show account-zero public addresses")
    derive.add_argument("--apply-profile", action="store_true", help="write public addresses to execution-wallet.json")
    remove = commands.add_parser("delete", help="delete the credential from the OS vault")
    remove.add_argument("--confirm-name", required=True, help="repeat the credential name to authorize deletion")
    args = parser.parse_args()

    vault = WalletVault()
    if args.command == "init":
        mnemonic = _read_confirmed("Recovery phrase (hidden): ", "Repeat recovery phrase: ")
        bip39_passphrase = ""
        if args.with_bip39_passphrase:
            bip39_passphrase = _read_confirmed("BIP-39 passphrase (hidden): ", "Repeat BIP-39 passphrase: ")
        vault.store(args.name, mnemonic, bip39_passphrase, replace=args.replace)
        print(json.dumps(vault.verify(args.name), ensure_ascii=False, indent=2))
        print("Stored in the OS credential vault. No trading or signing has been enabled.")
        return 0
    if args.command == "status":
        print(json.dumps({"name": args.name, "stored": vault.exists(args.name)}, ensure_ascii=False, indent=2))
        return 0
    if args.command == "verify":
        print(json.dumps(vault.verify(args.name), ensure_ascii=False, indent=2))
        return 0
    if args.command == "derive":
        addresses = derive_public_accounts(vault.load(args.name))
        output = {"credential": args.name, "evm": {"path": "m/44'/60'/0'/0/0", "address": addresses["evm"]}, "solana": {"path": "m/44'/501'/0'/0'", "address": addresses["solana"]}}
        if args.apply_profile:
            apply_public_accounts(PROJECT_DIR / "execution-wallet.json", addresses)
            output["profileUpdated"] = True
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    if args.confirm_name != args.name:
        raise ValueError("--confirm-name must exactly match --name")
    vault.delete(args.name)
    print(json.dumps({"name": args.name, "stored": False}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
