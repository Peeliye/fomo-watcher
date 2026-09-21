"""Strict EIP-1559 / Uniswap-V2 exact-input codec and vault-backed signer.

Only the audited swapExactTokensForTokens selector is accepted. Arbitrary 0x
or other aggregator calldata is not considered safely parseable by this codec.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from bip_utils import Bip39SeedGenerator, Bip44, Bip44Changes, Bip44Coins
from coincurve import PrivateKey, PublicKey
from Crypto.Hash import keccak

from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityStatus
from .interfaces import BuiltTransaction, ExecutableQuote
from .wallet_vault import WalletVault


SWAP_EXACT_TOKENS_FOR_TOKENS = bytes.fromhex("38ed1739")
SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141


def keccak256(data: bytes) -> bytes:
    digest = keccak.new(digest_bits=256)
    digest.update(data)
    return digest.digest()


def _integer(value: Any) -> int:
    result = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    if result < 0:
        raise ValueError("negative transaction quantity")
    return result


def _address(value: Any) -> bytes:
    text = str(value or "")
    if len(text) != 42 or not text.startswith("0x"):
        raise ValueError("invalid EVM address")
    data = bytes.fromhex(text[2:])
    if len(data) != 20:
        raise ValueError("invalid EVM address")
    return data


def _length_prefix(length: int, offset: int) -> bytes:
    if length <= 55:
        return bytes([offset + length])
    encoded = length.to_bytes((length.bit_length() + 7) // 8, "big")
    return bytes([offset + 55 + len(encoded)]) + encoded


def rlp_encode(value: int | bytes | Sequence[Any]) -> bytes:
    if isinstance(value, int):
        if value < 0:
            raise ValueError("negative RLP integer")
        value = value.to_bytes((value.bit_length() + 7) // 8, "big") if value else b""
    if isinstance(value, bytes):
        if len(value) == 1 and value[0] < 0x80:
            return value
        return _length_prefix(len(value), 0x80) + value
    payload = b"".join(rlp_encode(item) for item in value)
    return _length_prefix(len(payload), 0xC0) + payload


def _read_length(data: bytes, position: int, width: int) -> tuple[int, int]:
    if width < 1 or position + width > len(data) or data[position] == 0:
        raise ValueError("invalid RLP length")
    return int.from_bytes(data[position:position + width], "big"), position + width


def _rlp_one(data: bytes, position: int) -> tuple[Any, int]:
    if position >= len(data):
        raise ValueError("truncated RLP")
    prefix = data[position]
    if prefix < 0x80:
        return bytes([prefix]), position + 1
    if prefix <= 0xB7:
        length, start = prefix - 0x80, position + 1
        end = start + length
        if end > len(data) or (length == 1 and data[start] < 0x80):
            raise ValueError("noncanonical RLP bytes")
        return data[start:end], end
    if prefix <= 0xBF:
        length, start = _read_length(data, position + 1, prefix - 0xB7)
        if length <= 55 or start + length > len(data):
            raise ValueError("noncanonical RLP length")
        return data[start:start + length], start + length
    if prefix <= 0xF7:
        length, start = prefix - 0xC0, position + 1
    else:
        length, start = _read_length(data, position + 1, prefix - 0xF7)
        if length <= 55:
            raise ValueError("noncanonical RLP list length")
    end = start + length
    if end > len(data):
        raise ValueError("truncated RLP list")
    output: list[Any] = []
    while start < end:
        child, start = _rlp_one(data, start)
        output.append(child)
    if start != end:
        raise ValueError("invalid RLP list")
    return output, end


def rlp_decode(data: bytes) -> Any:
    value, end = _rlp_one(data, 0)
    if end != len(data) or rlp_encode(value) != data:
        raise ValueError("noncanonical RLP transaction")
    return value


def _word(data: bytes, index: int) -> int:
    start = 4 + index * 32
    if start + 32 > len(data):
        raise ValueError("truncated swap calldata")
    return int.from_bytes(data[start:start + 32], "big")


def decode_v2_swap(data: bytes) -> dict[str, Any]:
    if len(data) < 4 + 6 * 32 or data[:4] != SWAP_EXACT_TOKENS_FOR_TOKENS:
        raise ValueError("unsupported_swap_calldata")
    amount_in, minimum_out, offset, recipient, deadline = (_word(data, index) for index in range(5))
    if offset != 160 or recipient >> 160 or amount_in <= 0 or minimum_out <= 0:
        raise ValueError("invalid_swap_scope")
    path_length = _word(data, 5)
    if path_length < 2 or path_length > 4 or len(data) != 4 + (6 + path_length) * 32:
        raise ValueError("invalid_swap_path")
    path = []
    for index in range(path_length):
        value = _word(data, 6 + index)
        if value >> 160:
            raise ValueError("invalid_swap_token_address")
        path.append("0x" + value.to_bytes(20, "big").hex())
    return {"amountIn": amount_in, "minimumOut": minimum_out,
            "recipient": "0x" + recipient.to_bytes(20, "big").hex(),
            "deadline": deadline, "path": path}


@dataclass(frozen=True, slots=True)
class Eip1559Fields:
    chain_id: int
    nonce: int
    priority_fee: int
    maximum_fee: int
    gas_limit: int
    to: bytes
    value: int
    data: bytes

    def unsigned_items(self) -> list[Any]:
        return [self.chain_id, self.nonce, self.priority_fee, self.maximum_fee,
                self.gas_limit, self.to, self.value, self.data, []]

    def unsigned_bytes(self) -> bytes:
        return b"\x02" + rlp_encode(self.unsigned_items())


def _as_int(value: bytes) -> int:
    if len(value) > 1 and value[0] == 0:
        raise ValueError("noncanonical transaction integer")
    return int.from_bytes(value, "big")


def decode_eip1559(data: bytes, *, signed: bool) -> tuple[Eip1559Fields, list[Any]]:
    if not data or data[0] != 2:
        raise ValueError("EIP-1559 transaction required")
    items = rlp_decode(data[1:])
    if not isinstance(items, list) or len(items) != (12 if signed else 9):
        raise ValueError("invalid EIP-1559 field count")
    if items[8] != [] or any(not isinstance(item, bytes) for item in items[:8]):
        raise ValueError("access list or transaction field unsupported")
    fields = Eip1559Fields(
        _as_int(items[0]), _as_int(items[1]), _as_int(items[2]), _as_int(items[3]),
        _as_int(items[4]), items[5], _as_int(items[6]), items[7],
    )
    if len(fields.to) != 20 or fields.chain_id <= 0 or fields.gas_limit < 21_000:
        raise ValueError("invalid EIP-1559 transaction scope")
    return fields, items


def sign_eip1559(unsigned: bytes, private_key: bytes) -> bytes:
    fields, items = decode_eip1559(unsigned, signed=False)
    del fields
    signature = PrivateKey(private_key).sign_recoverable(keccak256(unsigned), hasher=None)
    parity = signature[64]
    if parity not in {0, 1}:
        raise ValueError("unsupported EVM recovery parity")
    r = int.from_bytes(signature[:32], "big")
    s = int.from_bytes(signature[32:64], "big")
    if not 0 < r < SECP256K1_N or not 0 < s <= SECP256K1_N // 2:
        raise ValueError("invalid EVM signature")
    return b"\x02" + rlp_encode(items + [parity, r, s])


class EvmEip1559Builder:
    def __init__(self, chain_id: int, wallet: str, allowed_routers: set[str], *, maximum_gas: int = 1_000_000) -> None:
        self.chain_id = int(chain_id)
        self.wallet = "0x" + _address(wallet).hex()
        self.allowed_routers = {"0x" + _address(value).hex() for value in allowed_routers}
        self.maximum_gas = maximum_gas

    def self_check(self) -> CapabilityStatus:
        ready = bool(self.allowed_routers and self.maximum_gas >= 21_000)
        return CapabilityStatus("transaction_builder", True, ready,
                                "ok" if ready else "audited_router_allowlist_required", {})

    def build(self, intent: ExecutionIntent, quote: ExecutableQuote,
              nonce_or_blockhash: str) -> BuiltTransaction:
        payload = quote.execution_payload
        if not quote.firm or not isinstance(payload, Mapping) or str(intent.chain_id) != str(self.chain_id):
            raise ValueError("firm_quote_and_matching_chain_required")
        target = "0x" + _address(payload.get("to")).hex()
        if target not in self.allowed_routers or target not in {value.lower() for value in quote.route_targets}:
            raise ValueError("route_not_allowlisted")
        if str(payload.get("from") or "").lower() != self.wallet:
            raise ValueError("quote_wallet_mismatch")
        calldata = bytes.fromhex(str(payload.get("data") or "").removeprefix("0x"))
        swap = decode_v2_swap(calldata)
        if (swap["recipient"] != self.wallet or swap["path"][0] != intent.token_in.lower()
                or swap["path"][-1] != intent.token_out.lower()
                or swap["minimumOut"] < _integer(quote.minimum_output_amount)):
            raise ValueError("quote_swap_scope_mismatch")
        now = int(time.time())
        if not now < swap["deadline"] <= now + 300:
            raise ValueError("swap_deadline_out_of_range")
        gas = _integer(payload.get("gas"))
        priority_fee = _integer(payload.get("maxPriorityFeePerGas"))
        maximum_fee = _integer(payload.get("maxFeePerGas"))
        if gas > self.maximum_gas or priority_fee <= 0 or maximum_fee < priority_fee:
            raise ValueError("gas_or_fee_out_of_range")
        if _integer(payload.get("value", 0)) != 0:
            raise ValueError("native_value_forbidden")
        fields = Eip1559Fields(self.chain_id, _integer(nonce_or_blockhash), priority_fee,
                              maximum_fee, gas, _address(target), 0, calldata)
        return BuiltTransaction(fields.unsigned_bytes(), quote.provider, nonce_or_blockhash)


class VaultEvmSigner:
    def __init__(self, credential_name: str, expected_wallet: str, vault: WalletVault | None = None) -> None:
        self.credential_name = credential_name
        self.expected_wallet = "0x" + _address(expected_wallet).hex()
        self.vault = vault or WalletVault()

    def _key(self) -> bytes:
        secret = self.vault.load(self.credential_name)
        seed = Bip39SeedGenerator(secret.mnemonic).Generate(secret.bip39_passphrase)
        account = Bip44.FromSeed(seed, Bip44Coins.ETHEREUM).Purpose().Coin().Account(0).Change(
            Bip44Changes.CHAIN_EXT).AddressIndex(0)
        if account.PublicKey().ToAddress().lower() != self.expected_wallet:
            raise ValueError("vault_account_does_not_match_execution_wallet")
        return account.PrivateKey().Raw().ToBytes()

    def self_check(self) -> CapabilityStatus:
        try:
            self._key()
        except Exception as error:
            return CapabilityStatus("signer", True, False, "vault_signer_unavailable",
                                    {"errorType": type(error).__name__})
        return CapabilityStatus("signer", True, True, "ok", {"backend": "os_credential_store"})

    def sign(self, transaction: BuiltTransaction) -> bytes:
        return sign_eip1559(transaction.serialized, self._key())


class EvmTransactionParser:
    def __init__(self, allowed_routers: set[str]) -> None:
        self.allowed_routers = {"0x" + _address(value).hex() for value in allowed_routers}

    def self_check(self) -> CapabilityStatus:
        ready = bool(self.allowed_routers)
        return CapabilityStatus("transaction_parser", True, ready,
                                "ok" if ready else "audited_router_allowlist_required", {})

    def parse(self, serialized_transaction: bytes) -> Mapping[str, Any]:
        fields, items = decode_eip1559(serialized_transaction, signed=True)
        parity, r, s = (_as_int(items[index]) for index in (9, 10, 11))
        if parity not in {0, 1} or not 0 < r < SECP256K1_N or not 0 < s <= SECP256K1_N // 2:
            raise ValueError("invalid EVM signature")
        digest = keccak256(fields.unsigned_bytes())
        recoverable = r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([parity])
        key = PublicKey.from_signature_and_message(recoverable, digest, hasher=None)
        wallet = "0x" + keccak256(key.format(compressed=False)[1:])[-20:].hex()
        target = "0x" + fields.to.hex()
        if target not in self.allowed_routers or fields.value != 0:
            raise ValueError("untrusted EVM target or native value")
        swap = decode_v2_swap(fields.data)
        if swap["recipient"] != wallet:
            raise ValueError("swap recipient differs from recovered signer")
        return {
            "chainId": str(fields.chain_id), "wallet": wallet, "tokenOut": swap["path"][-1],
            "sellAmount": str(swap["amountIn"]), "minimumOutputAmount": str(swap["minimumOut"]),
            "targets": [target], "operations": ["swap"], "approvals": [],
            "nonce": fields.nonce, "deadline": swap["deadline"], "tokenIn": swap["path"][0],
        }
