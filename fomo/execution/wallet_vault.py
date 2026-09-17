"""OS credential-vault storage for wallet recovery phrases.

This module deliberately does not derive accounts or sign transactions.  It
only gives a future signer a narrow, testable way to retrieve a secret from the
current operating-system user's credential store.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

import keyring
from bip_utils import Bip32Slip10Ed25519, Bip39SeedGenerator, Bip44, Bip44Changes, Bip44Coins, SolAddrEncoder


SERVICE_NAME = "fomo-watcher.wallet-vault"
ALLOWED_WORD_COUNTS = frozenset({12, 15, 18, 21, 24})
_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$")


class CredentialBackend(Protocol):
    def get_password(self, service: str, username: str) -> str | None: ...

    def set_password(self, service: str, username: str, password: str) -> None: ...

    def delete_password(self, service: str, username: str) -> None: ...


@dataclass(frozen=True)
class WalletSecret:
    mnemonic: str
    bip39_passphrase: str
    created_at: str


def derive_public_accounts(secret: WalletSecret) -> dict[str, str]:
    seed = Bip39SeedGenerator(secret.mnemonic).Generate(secret.bip39_passphrase)
    evm = Bip44.FromSeed(seed, Bip44Coins.ETHEREUM).Purpose().Coin().Account(0).Change(Bip44Changes.CHAIN_EXT).AddressIndex(0)
    solana = Bip32Slip10Ed25519.FromSeed(seed).DerivePath("m/44'/501'/0'/0'")
    return {"evm": evm.PublicKey().ToAddress(), "solana": SolAddrEncoder.EncodeKey(solana.PublicKey().KeyObject())}


def validate_name(name: str) -> str:
    value = name.strip()
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError("name must be 1-64 letters, numbers, dots, dashes, or underscores")
    return value


def normalize_mnemonic(mnemonic: str) -> str:
    # BIP-39 requires UTF-8 NFKD. Collapsing whitespace also prevents accidental
    # newlines copied from password managers from changing the stored phrase.
    value = " ".join(unicodedata.normalize("NFKD", mnemonic).split())
    word_count = len(value.split())
    if word_count not in ALLOWED_WORD_COUNTS:
        expected = ", ".join(str(item) for item in sorted(ALLOWED_WORD_COUNTS))
        raise ValueError(f"recovery phrase must contain {expected} words; got {word_count}")
    return value


class WalletVault:
    def __init__(self, backend: CredentialBackend = keyring, service: str = SERVICE_NAME):
        self.backend = backend
        self.service = service

    def exists(self, name: str) -> bool:
        return self.backend.get_password(self.service, validate_name(name)) is not None

    def store(
        self,
        name: str,
        mnemonic: str,
        bip39_passphrase: str = "",
        *,
        replace: bool = False,
    ) -> None:
        credential_name = validate_name(name)
        normalized = normalize_mnemonic(mnemonic)
        if self.backend.get_password(self.service, credential_name) is not None and not replace:
            raise FileExistsError(f"wallet credential already exists: {credential_name}")
        payload = {
            "version": 1,
            "mnemonic": normalized,
            "bip39Passphrase": unicodedata.normalize("NFKD", bip39_passphrase),
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        self.backend.set_password(self.service, credential_name, json.dumps(payload, separators=(",", ":")))

    def load(self, name: str) -> WalletSecret:
        credential_name = validate_name(name)
        raw = self.backend.get_password(self.service, credential_name)
        if raw is None:
            raise KeyError(f"wallet credential not found: {credential_name}")
        try:
            payload = json.loads(raw)
            if payload.get("version") != 1:
                raise ValueError("unsupported wallet credential version")
            mnemonic = normalize_mnemonic(str(payload["mnemonic"]))
            passphrase = unicodedata.normalize("NFKD", str(payload.get("bip39Passphrase", "")))
            created_at = str(payload["createdAt"])
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("wallet credential is malformed") from error
        return WalletSecret(mnemonic=mnemonic, bip39_passphrase=passphrase, created_at=created_at)

    def verify(self, name: str) -> dict[str, object]:
        secret = self.load(name)
        return {
            "name": validate_name(name),
            "stored": True,
            "wordCount": len(secret.mnemonic.split()),
            "hasBip39Passphrase": bool(secret.bip39_passphrase),
            "createdAt": secret.created_at,
        }

    def delete(self, name: str) -> None:
        credential_name = validate_name(name)
        if self.backend.get_password(self.service, credential_name) is None:
            raise KeyError(f"wallet credential not found: {credential_name}")
        self.backend.delete_password(self.service, credential_name)
