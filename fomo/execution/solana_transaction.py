"""Strict legacy Solana transaction codec and OS-vault signer.

Versioned transactions and arbitrary Jupiter instructions remain unsupported
until address-table and swap-instruction scope decoders are independently audited.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Mapping

from bip_utils import Base58Encoder, Bip32Slip10Ed25519, Bip39SeedGenerator
from nacl.signing import SigningKey

from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityStatus
from .interfaces import BuiltTransaction, ExecutableQuote
from .wallet_vault import WalletVault


def compact_length(data: bytes, position: int = 0) -> tuple[int, int]:
    value, shift = 0, 0
    for _ in range(3):
        if position >= len(data):
            raise ValueError("truncated Solana compact length")
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            if value > 256:
                raise ValueError("Solana compact length exceeds bound")
            return value, position
        shift += 7
    raise ValueError("Solana compact length exceeds bound")


@dataclass(frozen=True, slots=True)
class SolanaLegacyTransaction:
    signature_count: int
    signatures: tuple[bytes, ...]
    message: bytes
    signer: str
    recent_blockhash: str
    program_ids: tuple[str, ...]


def parse_legacy_transaction(data: bytes) -> SolanaLegacyTransaction:
    count, position = compact_length(data)
    if count != 1 or position != 1 or position + 64 >= len(data):
        raise ValueError("single-signer Solana legacy transaction required")
    signatures = (data[position:position + 64],)
    position += 64
    message = data[position:]
    if len(message) < 3 or message[0] & 0x80 or message[0] != 1:
        raise ValueError("unsupported Solana transaction version or signer count")
    cursor = 3
    account_count, cursor = compact_length(message, cursor)
    if not 1 <= account_count <= 64 or cursor + account_count * 32 + 32 > len(message):
        raise ValueError("invalid Solana account list")
    keys = [message[cursor + index * 32:cursor + (index + 1) * 32]
            for index in range(account_count)]
    cursor += account_count * 32
    blockhash = message[cursor:cursor + 32]
    cursor += 32
    instruction_count, cursor = compact_length(message, cursor)
    if not 1 <= instruction_count <= 16:
        raise ValueError("invalid Solana instruction count")
    programs = []
    for _ in range(instruction_count):
        if cursor >= len(message):
            raise ValueError("truncated Solana instruction")
        program_index = message[cursor]
        cursor += 1
        if program_index >= account_count:
            raise ValueError("invalid Solana program index")
        programs.append(Base58Encoder.Encode(keys[program_index]))
        account_indices, cursor = compact_length(message, cursor)
        if cursor + account_indices > len(message) or any(index >= account_count for index in message[cursor:cursor + account_indices]):
            raise ValueError("invalid Solana instruction accounts")
        cursor += account_indices
        payload_length, cursor = compact_length(message, cursor)
        if cursor + payload_length > len(message):
            raise ValueError("truncated Solana instruction data")
        cursor += payload_length
    if cursor != len(message):
        raise ValueError("unexpected Solana transaction bytes")
    return SolanaLegacyTransaction(count, signatures, message, Base58Encoder.Encode(keys[0]),
                                   Base58Encoder.Encode(blockhash), tuple(programs))


def sign_legacy_transaction(unsigned: bytes, private_seed: bytes) -> bytes:
    parsed = parse_legacy_transaction(unsigned)
    if any(parsed.signatures[0]):
        raise ValueError("Solana transaction is already signed")
    key = SigningKey(private_seed)
    if Base58Encoder.Encode(bytes(key.verify_key)) != parsed.signer:
        raise ValueError("Solana signer does not match message")
    signature = key.sign(parsed.message).signature
    return unsigned[:1] + signature + parsed.message


class SolanaLegacyBuilder:
    def __init__(self, wallet: str, allowed_program_ids: set[str]) -> None:
        self.wallet = wallet
        self.allowed_program_ids = allowed_program_ids

    def self_check(self) -> CapabilityStatus:
        return CapabilityStatus("transaction_builder", True, False,
                                "audited_swap_instruction_scope_decoder_required", {})

    def build(self, intent: ExecutionIntent, quote: ExecutableQuote,
              nonce_or_blockhash: str) -> BuiltTransaction:
        payload = quote.execution_payload
        if not quote.firm or not isinstance(payload, Mapping) or str(intent.chain_id) != "1399811149":
            raise ValueError("firm_solana_quote_required")
        unsigned = base64.b64decode(str(payload.get("base64UnsignedTransaction") or ""), validate=True)
        parsed = parse_legacy_transaction(unsigned)
        if (parsed.signer != self.wallet or parsed.recent_blockhash != nonce_or_blockhash
                or not set(parsed.program_ids).issubset(self.allowed_program_ids)):
            raise ValueError("solana_transaction_scope_unverified")
        return BuiltTransaction(unsigned, quote.provider, nonce_or_blockhash)


class VaultSolanaSigner:
    def __init__(self, credential_name: str, expected_wallet: str, vault: WalletVault | None = None) -> None:
        self.credential_name = credential_name
        self.expected_wallet = expected_wallet
        self.vault = vault or WalletVault()

    def _seed(self) -> bytes:
        secret = self.vault.load(self.credential_name)
        seed = Bip39SeedGenerator(secret.mnemonic).Generate(secret.bip39_passphrase)
        key = Bip32Slip10Ed25519.FromSeed(seed).DerivePath("m/44'/501'/0'/0'")
        private_seed = key.PrivateKey().Raw().ToBytes()
        if Base58Encoder.Encode(bytes(SigningKey(private_seed).verify_key)) != self.expected_wallet:
            raise ValueError("vault_account_does_not_match_execution_wallet")
        return private_seed

    def self_check(self) -> CapabilityStatus:
        try:
            self._seed()
        except Exception as error:
            return CapabilityStatus("signer", True, False, "vault_signer_unavailable",
                                    {"errorType": type(error).__name__})
        return CapabilityStatus("signer", True, True, "ok", {"backend": "os_credential_store"})

    def sign(self, transaction: BuiltTransaction) -> bytes:
        parsed = parse_legacy_transaction(transaction.serialized)
        if parsed.signer != self.expected_wallet:
            raise ValueError("Solana execution wallet mismatch")
        return sign_legacy_transaction(transaction.serialized, self._seed())


class SolanaLegacyParser:
    def self_check(self) -> CapabilityStatus:
        return CapabilityStatus("transaction_parser", True, False,
                                "audited_swap_instruction_scope_decoder_required", {})

    def parse(self, serialized_transaction: bytes) -> Mapping[str, Any]:
        parsed = parse_legacy_transaction(serialized_transaction)
        # No token/minOut claim can be made from unknown instruction bytes.
        return {"chainId": "1399811149", "wallet": parsed.signer,
                "targets": list(parsed.program_ids), "operations": [],
                "minimumOutputAmount": "0", "sellAmount": "0", "tokenOut": ""}
