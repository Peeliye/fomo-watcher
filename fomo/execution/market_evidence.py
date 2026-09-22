"""Independent USD price and chain-pinned native-balance evidence.

These objects are observations, not an authorization to trade. In particular,
Hermes' parsed REST response is transport-authenticated but is not verified as
an on-chain Pyth proof; execution readiness still requires the separate risk
evidence and transaction-scope gates.
"""

from __future__ import annotations

import hashlib
import base64
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Mapping

from bip_utils import Base58Decoder

from fomo.watching.rpc_transport import FailoverJsonRpc, RpcTransport, rpc_view
from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityStatus
from .interfaces import ExecutableQuote
from .quote_adapters import ApiTransport, PinnedApiTransport
from .solana_transaction import compact_length

if TYPE_CHECKING:
    from .pool_prices import EvmV2PoolPriceCache


PYTH_HERMES_URL = "https://pyth.dourolabs.app/hermes/v2/updates/price/latest"
_FEED_ID = re.compile(r"^(?:0x)?[0-9a-fA-F]{64}$")
_BLOCK_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_EVM_WORD = re.compile(r"^0x[0-9a-fA-F]{64}$")
_SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_NATIVE_ASSETS = {
    "1": ("ETH", 18), "56": ("BNB", 18), "8453": ("ETH", 18),
    "4663": ("ETH", 18), "1399811149": ("SOL", 9),
}
# Reviewed against Pyth's published ETH/USD price ID. Other pairs stay
# unavailable until their pair-to-ID binding is independently reviewed.
_APPROVED_FEEDS = {
    "ETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "SOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
}
_APPROVED_QUOTE_INPUTS = {
    ("1", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"): ("ETH", 18),
    ("1399811149", "So11111111111111111111111111111111111111112"): ("SOL", 9),
}


def approved_feed_id(asset: str) -> str:
    feed = _APPROVED_FEEDS.get(asset)
    if feed is None:
        raise ValueError("approved_pyth_feed_binding_required")
    return feed


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("market_evidence_invalid_number") from error
    if not result.is_finite():
        raise ValueError("market_evidence_invalid_number")
    return result


def _quantity(value: Any) -> int:
    try:
        result = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("balance_rpc_invalid_quantity") from error
    if result < 0:
        raise ValueError("balance_rpc_invalid_quantity")
    return result


@dataclass(frozen=True, slots=True)
class PriceObservation:
    chain_id: str
    asset: str
    price_usd: Decimal
    confidence_usd: Decimal
    feed_id: str
    published_at_ms: int
    fetched_at_ms: int
    payload_sha256: str
    source: str = "pyth_core_hermes"
    pool_id: str = ""
    block_hash: str = ""
    block_height: int = 0
    liquidity_usd: Decimal = Decimal(0)

    def require_fresh(self, *, now_ms: int | None = None, maximum_age_ms: int = 5_000,
                      maximum_confidence_ratio: Decimal = Decimal("0.01")) -> None:
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        identity_ok = (self.feed_id == _APPROVED_FEEDS.get(self.asset)
                       if self.source == "pyth_core_hermes" else
                       self.source == "evm_v2_verified_pool" and self.feed_id == self.pool_id
                       and bool(_EVM_ADDRESS.fullmatch(self.pool_id))
                       and bool(_BLOCK_HASH.fullmatch(self.block_hash))
                       and self.block_height > 0 and self.liquidity_usd > 0)
        publication_age_limit = 60_000 if self.source == "evm_v2_verified_pool" else maximum_age_ms
        if (maximum_age_ms <= 0 or not 0 < maximum_confidence_ratio <= 1
                or not identity_ok
                or self.price_usd <= 0 or self.confidence_usd < 0
                or self.confidence_usd / self.price_usd > maximum_confidence_ratio
                or not 0 <= current - self.published_at_ms <= publication_age_limit
                or not 0 <= current - self.fetched_at_ms <= maximum_age_ms):
            raise ValueError("independent_price_stale_or_unreliable")

    def independent_of(self, quote_provider: str, route_targets: tuple[str, ...] = ()) -> bool:
        if not quote_provider:
            return False
        if self.source == "evm_v2_verified_pool":
            # Router/Settler addresses do not disclose the route's underlying
            # pools. Until final route bytes prove venue separation, this is a
            # sizing reference only, not an independent sanity quote.
            return False
        return not quote_provider.lower().startswith("pyth")


class PythHermesPriceAdapter:
    """Fetch one explicitly bound USD feed; unknown assets have no fallback."""

    def __init__(self, *, chain_id: str | int, asset: str, feed_id: str,
                 api_key_env: str = "PYTH_API_KEY", transport: ApiTransport | None = None,
                 maximum_age_ms: int = 5_000,
                 maximum_confidence_ratio: Decimal = Decimal("0.01")) -> None:
        normalized = feed_id.lower().removeprefix("0x")
        if (not _FEED_ID.fullmatch(feed_id) or normalized != _APPROVED_FEEDS.get(asset)
                or not 0 < maximum_age_ms <= 30_000
                or not 0 < _decimal(maximum_confidence_ratio) <= 1):
            raise ValueError("approved_pyth_feed_binding_required")
        self.chain_id = str(chain_id)
        self.asset = asset
        self.feed_id = normalized
        self.api_key_env = api_key_env
        self.transport = transport or PinnedApiTransport()
        self.maximum_age_ms = maximum_age_ms
        self.maximum_confidence_ratio = _decimal(maximum_confidence_ratio)

    def latest(self, *, now_ms: int | None = None) -> PriceObservation:
        key = os.getenv(self.api_key_env, "")
        if not key:
            raise ValueError("pyth_api_credential_unavailable")
        body = self.transport.request(
            "GET", PYTH_HERMES_URL, params={"ids[]": "0x" + self.feed_id, "parsed": "true"},
            payload=None, headers={"Authorization": "Bearer " + key},
        )
        fetched = int(time.time() * 1000) if now_ms is None else int(now_ms)
        parsed = body.get("parsed")
        if not isinstance(parsed, list) or len(parsed) != 1 or not isinstance(parsed[0], Mapping):
            raise ValueError("pyth_price_response_invalid")
        item = parsed[0]
        price_data = item.get("price")
        if str(item.get("id") or "").lower().removeprefix("0x") != self.feed_id or not isinstance(price_data, Mapping):
            raise ValueError("pyth_feed_identity_mismatch")
        try:
            exponent = int(str(price_data.get("expo")))
        except ValueError as error:
            raise ValueError("pyth_price_exponent_invalid") from error
        if not -18 <= exponent <= 0:
            raise ValueError("pyth_price_exponent_invalid")
        factor = Decimal(10) ** exponent
        raw_price = _decimal(price_data.get("price"))
        raw_confidence = _decimal(price_data.get("conf"))
        published = _quantity(price_data.get("publish_time")) * 1000
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()
        observation = PriceObservation(
            self.chain_id, self.asset, raw_price * factor, raw_confidence * factor,
            self.feed_id, published, fetched, hashlib.sha256(canonical).hexdigest(),
        )
        observation.require_fresh(now_ms=fetched, maximum_age_ms=self.maximum_age_ms,
                                  maximum_confidence_ratio=self.maximum_confidence_ratio)
        return observation

    def self_check(self) -> CapabilityStatus:
        if type(self.transport) is not PinnedApiTransport:
            return CapabilityStatus("independent_price", True, False, "unaudited_price_transport", {})
        try:
            observation = self.latest()
            ready = bool(observation.payload_sha256)
        except Exception as error:
            return CapabilityStatus("independent_price", True, False, "price_probe_failed",
                                    {"errorType": type(error).__name__})
        return CapabilityStatus("independent_price", True, ready, "ok", {"source": "pyth_core_hermes"})


def quote_input_price(adapter: PythHermesPriceAdapter | EvmV2PoolPriceCache, *, chain_id: str | int,
                      token_in: str, decimals: int, now_ms: int | None = None) -> PriceObservation:
    """Only reviewed wrapped-native token/price-feed bindings can size a quote."""
    chain = str(chain_id)
    token = token_in if chain == "1399811149" else token_in.lower()
    approved = _APPROVED_QUOTE_INPUTS.get((chain, token))
    from .pool_prices import EvmV2PoolPriceCache
    if (type(adapter) not in (PythHermesPriceAdapter, EvmV2PoolPriceCache) or approved is None
            or adapter.chain_id != chain or (adapter.asset, decimals) != approved):
        raise ValueError("quote_input_price_binding_unapproved")
    observation: PriceObservation = adapter.latest(now_ms=now_ms)
    observation.require_fresh(now_ms=now_ms)
    return observation


def verify_quote_input_price(quote: ExecutableQuote, *, latest: PriceObservation,
                             chain_id: str | int, token_in: str, decimals: int,
                             now_ms: int | None = None,
                             maximum_deviation: Decimal = Decimal("0.02")) -> None:
    """Tie quote sizing proof to a fresh, independent same-feed observation."""
    chain = str(chain_id)
    token = token_in if chain == "1399811149" else token_in.lower()
    deviation = _decimal(maximum_deviation)
    if (_APPROVED_QUOTE_INPUTS.get((chain, token)) != (latest.asset, decimals)
            or latest.chain_id != chain or not 0 <= deviation <= Decimal("0.05")):
        raise ValueError("quote_input_price_binding_unapproved")
    latest.require_fresh(now_ms=now_ms)
    payload = quote.execution_payload
    proof = payload.get("inputPriceProof") if isinstance(payload, Mapping) else None
    if not isinstance(proof, Mapping):
        raise ValueError("quote_independent_price_proof_missing")
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    try:
        observed_at = int(str(proof.get("publishedAtMs")))
    except (TypeError, ValueError) as error:
        raise ValueError("quote_independent_price_proof_invalid") from error
    proof_price = _decimal(proof.get("priceUsd"))
    if (proof.get("source") != latest.source or proof.get("feedId") != latest.feed_id
            or not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("payloadSha256") or ""))
            or proof_price <= 0 or not 0 <= current - observed_at <= 5_000
            or abs(proof_price - latest.price_usd) / latest.price_usd > deviation):
        raise ValueError("quote_independent_price_proof_invalid")
    if latest.source == "evm_v2_verified_pool" and (
        proof.get("blockHash") != latest.block_hash
        or proof.get("poolId") != latest.pool_id
        or proof.get("payloadSha256") != latest.payload_sha256
        or observed_at != latest.fetched_at_ms
        or not latest.independent_of(quote.provider, quote.route_targets)
    ):
        raise ValueError("quote_independent_pool_price_invalid")


@dataclass(frozen=True, slots=True)
class NativeBalanceSnapshot:
    chain_id: str
    wallet: str
    native_asset: str
    native_units: int
    native_decimals: int
    block_height: int
    block_hash: str
    rpc_provider: str
    captured_at_ms: int
    price: PriceObservation
    reserve_usd: Decimal

    def require_fresh(self, *, now_ms: int | None = None, maximum_age_ms: int = 5_000) -> None:
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        self.price.require_fresh(now_ms=current, maximum_age_ms=maximum_age_ms)
        if (self.reserve_usd < 0 or not self.block_hash or not self.rpc_provider
                or not 0 <= current - self.captured_at_ms <= maximum_age_ms):
            raise ValueError("native_balance_snapshot_stale_or_untrusted")


@dataclass(frozen=True, slots=True)
class TokenBalanceSnapshot:
    chain_id: str
    wallet: str
    token: str
    units: int
    decimals: int
    block_height: int
    block_hash: str
    rpc_provider: str
    captured_at_ms: int

    def require_fresh(self, *, now_ms: int | None = None, maximum_age_ms: int = 5_000) -> None:
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        if (not 0 <= self.decimals <= 30 or self.units < 0 or not self.block_hash
                or not self.rpc_provider or not 0 <= current - self.captured_at_ms <= maximum_age_ms):
            raise ValueError("token_balance_snapshot_stale_or_untrusted")


def require_spend_balance(snapshot: TokenBalanceSnapshot, *, chain_id: str | int,
                          wallet: str, token: str, required_units: int,
                          now_ms: int | None = None) -> None:
    snapshot.require_fresh(now_ms=now_ms)
    if (not isinstance(required_units, int) or required_units <= 0
            or snapshot.chain_id != str(chain_id)
            or snapshot.wallet.casefold() != wallet.casefold()
            or snapshot.token.casefold() != token.casefold()
            or snapshot.units < required_units):
        raise ValueError("spend_token_balance_insufficient_or_mismatched")


def minimum_evm_gas_reserve_usd(parsed_signed_scope: Mapping[str, Any],
                                native_price: PriceObservation, *,
                                safety_multiplier: Decimal = Decimal("1.2"),
                                now_ms: int | None = None) -> Decimal:
    """Use the parsed final transaction's maximum fee, not a caller gas guess."""
    native_price.require_fresh(now_ms=now_ms)
    if (str(parsed_signed_scope.get("chainId")) != native_price.chain_id
            or native_price.asset != _NATIVE_ASSETS.get(native_price.chain_id, (None, 0))[0]
            or not 1 <= _decimal(safety_multiplier) <= 3):
        raise ValueError("evm_gas_price_or_chain_invalid")
    gas_limit = _quantity(parsed_signed_scope.get("gasLimit"))
    maximum_fee = _quantity(parsed_signed_scope.get("maxFeePerGasWei"))
    if not 21_000 <= gas_limit <= 5_000_000 or maximum_fee <= 0:
        raise ValueError("evm_gas_scope_invalid")
    return (Decimal(gas_limit) * Decimal(maximum_fee) / Decimal(10**18)
            * native_price.price_usd * _decimal(safety_multiplier))


def capture_token_balance(rpc: RpcTransport, *, chain_id: str | int, wallet: str,
                          token: str, expected_decimals: int,
                          expected_block_height: int | None = None,
                          expected_block_hash: str | None = None,
                          now_ms: int | None = None) -> TokenBalanceSnapshot:
    """Read a fungible-token balance from one verified block/slot, fail closed."""
    chain = str(chain_id)
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if not 0 <= expected_decimals <= 30:
        raise ValueError("token_decimals_not_approved")
    with rpc_view(rpc):
        if chain == "1399811149":
            if expected_block_height is not None or expected_block_hash is not None:
                raise ValueError("solana_exact_slot_balance_unsupported")
            slot = _quantity(rpc.call("getSlot", [{"commitment": "confirmed"}]))
            response = rpc.call("getTokenAccountsByOwner", [
                wallet, {"mint": token},
                {"encoding": "jsonParsed", "commitment": "confirmed", "minContextSlot": slot},
            ])
            if (not isinstance(response, Mapping) or not isinstance(response.get("context"), Mapping)
                    or not isinstance(response.get("value"), list)):
                raise ValueError("solana_token_balance_response_invalid")
            height = _quantity(response["context"].get("slot"))
            if height < slot:
                raise ValueError("solana_token_balance_context_stale")
            units = 0
            for account in response["value"]:
                if not isinstance(account, Mapping):
                    raise ValueError("solana_token_account_invalid")
                data = account.get("account")
                parsed = data.get("data") if isinstance(data, Mapping) else None
                info = parsed.get("parsed") if isinstance(parsed, Mapping) else None
                details = info.get("info") if isinstance(info, Mapping) else None
                token_amount = details.get("tokenAmount") if isinstance(details, Mapping) else None
                if (not isinstance(data, Mapping) or data.get("owner") != _SPL_TOKEN_PROGRAM
                        or not isinstance(info, Mapping) or info.get("type") != "account"
                        or not isinstance(details, Mapping) or not isinstance(token_amount, Mapping)
                        or details.get("mint") != token or details.get("owner") != wallet
                        or _quantity(token_amount.get("decimals")) != expected_decimals):
                    raise ValueError("solana_token_account_scope_invalid")
                units += _quantity(token_amount.get("amount"))
            block_args: list[Any] = [height, {"commitment": "confirmed", "transactionDetails": "none",
                                               "rewards": False, "maxSupportedTransactionVersion": 0}]
            block = rpc.call("getBlock", block_args)
            check = rpc.call("getBlock", block_args)
            block_hash = str(block.get("blockhash") or "") if isinstance(block, Mapping) else ""
            if not block_hash or not isinstance(check, Mapping) or check.get("blockhash") != block_hash:
                raise ValueError("solana_token_balance_slot_unstable")
        elif chain in _NATIVE_ASSETS and chain != "5042":
            if not _EVM_ADDRESS.fullmatch(wallet) or not _EVM_ADDRESS.fullmatch(token):
                raise ValueError("evm_token_or_wallet_invalid")
            block_tag = hex(expected_block_height) if expected_block_height is not None else "latest"
            block = rpc.call("eth_getBlockByNumber", [block_tag, False])
            if not isinstance(block, Mapping):
                raise ValueError("evm_token_balance_block_missing")
            height = _quantity(block.get("number"))
            block_hash = str(block.get("hash") or "")
            if height <= 0 or not _BLOCK_HASH.fullmatch(block_hash):
                raise ValueError("evm_token_balance_block_invalid")
            if ((expected_block_height is not None and height != expected_block_height)
                    or (expected_block_hash is not None and block_hash != expected_block_hash)):
                raise ValueError("evm_token_balance_block_mismatch")
            balance_data = "0x70a08231" + wallet[2:].lower().rjust(64, "0")
            raw = rpc.call("eth_call", [{"to": token, "data": balance_data}, hex(height)])
            raw_decimals = rpc.call("eth_call", [{"to": token, "data": "0x313ce567"}, hex(height)])
            if not isinstance(raw, str) or not _EVM_WORD.fullmatch(raw):
                raise ValueError("evm_token_balance_response_invalid")
            if not isinstance(raw_decimals, str) or not _EVM_WORD.fullmatch(raw_decimals):
                raise ValueError("evm_token_decimals_response_invalid")
            if int(raw_decimals, 16) != expected_decimals:
                raise ValueError("evm_token_decimals_mismatch")
            units = int(raw, 16)
            check = rpc.call("eth_getBlockByNumber", [hex(height), False])
            if not isinstance(check, Mapping) or check.get("hash") != block_hash:
                raise ValueError("evm_token_balance_block_reorged")
        else:
            raise ValueError("token_balance_chain_not_audited")
        provider = str(getattr(rpc, "last_provider", "") or "")
        if not provider:
            raise ValueError("token_balance_rpc_provider_unknown")
    snapshot = TokenBalanceSnapshot(chain, wallet, token, units, expected_decimals, height,
                                    block_hash, provider, current)
    snapshot.require_fresh(now_ms=current)
    return snapshot


@dataclass(frozen=True, slots=True)
class MarketEvidenceFacts:
    quote_fresh: bool
    balance_fresh: bool
    independent_sanity_price_count: int
    native_gas_reserve_usd: Decimal
    price_feed_id: str
    price_payload_sha256: str
    balance_block_hash: str
    balance_rpc_provider: str


@dataclass(frozen=True, slots=True)
class EvmFundingEvidence:
    market: MarketEvidenceFacts
    spend_token: str
    spend_balance_units: int
    required_spend_units: int
    minimum_gas_reserve_usd: Decimal
    snapshot_block_hash: str


@dataclass(frozen=True, slots=True)
class SolanaFeeEvidence:
    message_sha256: str
    fee_lamports: int
    context_slot: int
    rpc_provider: str
    minimum_gas_reserve_usd: Decimal


@dataclass(frozen=True, slots=True)
class SolanaFundingEvidence:
    market: MarketEvidenceFacts
    spend_token: str
    spend_balance_units: int
    required_spend_units: int
    minimum_gas_reserve_usd: Decimal
    fee: SolanaFeeEvidence


def solana_fee_for_signed_transaction(rpc: RpcTransport, *, signed_transaction: bytes,
                                      balance: NativeBalanceSnapshot,
                                      safety_multiplier: Decimal = Decimal("1.2"),
                                      now_ms: int | None = None) -> SolanaFeeEvidence:
    """Ask RPC for the fee of the exact final message; never infer from a config constant."""
    balance.require_fresh(now_ms=now_ms)
    if (balance.chain_id != "1399811149" or not 1 <= _decimal(safety_multiplier) <= 3
            or not signed_transaction or len(signed_transaction) > 1232):
        raise ValueError("solana_fee_scope_invalid")
    signatures, cursor = compact_length(signed_transaction)
    if not 1 <= signatures <= 16 or cursor + signatures * 64 + 3 >= len(signed_transaction):
        raise ValueError("solana_fee_signature_layout_invalid")
    signature_bytes = signed_transaction[cursor:cursor + signatures * 64]
    if any(not any(signature_bytes[index * 64:(index + 1) * 64]) for index in range(signatures)):
        raise ValueError("solana_fee_unsigned_transaction")
    message = signed_transaction[cursor + signatures * 64:]
    if message[0] & 0x80 and message[0] != 0x80:
        raise ValueError("solana_fee_message_version_unsupported")
    required_signatures = message[1] if message[0] == 0x80 else message[0]
    if required_signatures != signatures:
        raise ValueError("solana_fee_signer_count_mismatch")
    with rpc_view(rpc):
        response = rpc.call("getFeeForMessage", [
            base64.b64encode(message).decode("ascii"),
            {"commitment": "confirmed", "minContextSlot": balance.block_height},
        ])
        provider = str(getattr(rpc, "last_provider", "") or "")
        block = rpc.call("getBlock", [balance.block_height, {
            "commitment": "confirmed", "transactionDetails": "none", "rewards": False,
            "maxSupportedTransactionVersion": 0,
        }])
    if (not isinstance(response, Mapping) or not isinstance(response.get("context"), Mapping)
            or response.get("value") is None or not isinstance(block, Mapping)
            or block.get("blockhash") != balance.block_hash
            or provider != balance.rpc_provider):
        raise ValueError("solana_fee_rpc_context_invalid")
    context_slot = _quantity(response["context"].get("slot"))
    fee = _quantity(response.get("value"))
    if context_slot < balance.block_height or fee <= 0:
        raise ValueError("solana_fee_stale_or_zero")
    minimum = Decimal(fee) / Decimal(10**9) * balance.price.price_usd * _decimal(safety_multiplier)
    return SolanaFeeEvidence(hashlib.sha256(message).hexdigest(), fee, context_slot,
                             provider, minimum)


def evaluate_evm_funding_evidence(quote: ExecutableQuote, *, price: PriceObservation,
                                  native_balance: NativeBalanceSnapshot,
                                  token_balance: TokenBalanceSnapshot,
                                  parsed_signed_scope: Mapping[str, Any],
                                  expected_chain_id: str | int, expected_wallet: str,
                                  expected_token_in: str, required_spend_units: int,
                                  now_ms: int | None = None) -> EvmFundingEvidence:
    """Require spend token, gas and signed fee cap to share a chain view."""
    chain = str(expected_chain_id)
    if chain == "1399811149" or chain not in _NATIVE_ASSETS:
        raise ValueError("evm_funding_chain_not_audited")
    if (native_balance.chain_id != token_balance.chain_id
            or native_balance.block_height != token_balance.block_height
            or native_balance.block_hash != token_balance.block_hash
            or native_balance.rpc_provider != token_balance.rpc_provider):
        raise ValueError("funding_snapshots_inconsistent_chain_view")
    require_spend_balance(token_balance, chain_id=chain, wallet=expected_wallet,
                          token=expected_token_in, required_units=required_spend_units,
                          now_ms=now_ms)
    if (str(parsed_signed_scope.get("tokenIn") or "").casefold() != expected_token_in.casefold()
            or _quantity(parsed_signed_scope.get("sellAmount")) != required_spend_units):
        raise ValueError("funding_signed_spend_scope_mismatch")
    gas_floor = minimum_evm_gas_reserve_usd(parsed_signed_scope, price, now_ms=now_ms)
    market = evaluate_market_evidence(
        quote, price=price, balance=native_balance, expected_chain_id=chain,
        expected_wallet=expected_wallet, minimum_native_gas_reserve_usd=gas_floor,
        planned_native_spend_units=0, now_ms=now_ms,
    )
    return EvmFundingEvidence(market, token_balance.token, token_balance.units,
                              required_spend_units, gas_floor, native_balance.block_hash)


def evaluate_market_evidence(quote: ExecutableQuote, *, price: PriceObservation,
                             balance: NativeBalanceSnapshot, expected_chain_id: str | int,
                             expected_wallet: str, minimum_native_gas_reserve_usd: Decimal,
                             planned_native_spend_units: int,
                             now_ms: int | None = None,
                             maximum_age_ms: int = 5_000) -> MarketEvidenceFacts:
    """Derive preflight market facts from observations, never caller booleans."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    price.require_fresh(now_ms=current, maximum_age_ms=maximum_age_ms)
    balance.require_fresh(now_ms=current, maximum_age_ms=maximum_age_ms)
    minimum = _decimal(minimum_native_gas_reserve_usd)
    if (not isinstance(planned_native_spend_units, int) or planned_native_spend_units < 0
            or planned_native_spend_units > balance.native_units):
        raise ValueError("native_spend_exceeds_snapshot")
    remaining_usd = (Decimal(balance.native_units - planned_native_spend_units)
                     / Decimal(10**balance.native_decimals) * price.price_usd)
    if (minimum <= 0 or remaining_usd < minimum
            or balance.chain_id != str(expected_chain_id)
            or balance.wallet.casefold() != expected_wallet.casefold()
            or balance.price != price or not price.independent_of(quote.provider, quote.route_targets)
            or not quote.firm):
        raise ValueError("market_evidence_scope_or_reserve_invalid")
    try:
        captured = datetime.fromisoformat(quote.captured_at.replace("Z", "+00:00"))
        if captured.tzinfo is None:
            raise ValueError("quote_timestamp_timezone_required")
        quote_age_ms = current - int(captured.astimezone(timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("quote_timestamp_invalid") from error
    if not 0 <= quote_age_ms <= maximum_age_ms:
        raise ValueError("quote_stale")
    return MarketEvidenceFacts(True, True, 1, remaining_usd, price.feed_id,
                               price.payload_sha256, balance.block_hash, balance.rpc_provider)


def capture_native_balance(rpc: RpcTransport, *, chain_id: str | int, wallet: str,
                           price: PriceObservation, now_ms: int | None = None) -> NativeBalanceSnapshot:
    chain = str(chain_id)
    if chain not in _NATIVE_ASSETS:
        # ARC has no audited native unit/price binding or exit route yet.
        raise ValueError("native_asset_chain_not_audited")
    symbol, decimals = _NATIVE_ASSETS[chain]
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if price.chain_id != chain or price.asset != symbol:
        raise ValueError("native_price_asset_mismatch")
    price.require_fresh(now_ms=current)
    with rpc_view(rpc):
        if chain == "1399811149":
            slot = _quantity(rpc.call("getSlot", [{"commitment": "confirmed"}]))
            result = rpc.call("getBalance", [wallet, {
                "commitment": "confirmed", "minContextSlot": slot,
            }])
            if not isinstance(result, Mapping) or not isinstance(result.get("context"), Mapping):
                raise ValueError("solana_balance_context_missing")
            height = _quantity(result["context"].get("slot"))
            if height < slot:
                raise ValueError("solana_balance_context_stale")
            units = _quantity(result.get("value"))
            block_args: list[Any] = [height, {"commitment": "confirmed", "transactionDetails": "none",
                                               "rewards": False, "maxSupportedTransactionVersion": 0}]
            block = rpc.call("getBlock", block_args)
            check = rpc.call("getBlock", block_args)
            block_hash = str(block.get("blockhash") or "") if isinstance(block, Mapping) else ""
            if not block_hash or not isinstance(check, Mapping) or check.get("blockhash") != block_hash:
                raise ValueError("solana_balance_slot_unstable")
        else:
            block = rpc.call("eth_getBlockByNumber", ["latest", False])
            if not isinstance(block, Mapping):
                raise ValueError("evm_balance_block_missing")
            height = _quantity(block.get("number"))
            block_hash = str(block.get("hash") or "")
            if height <= 0 or not _BLOCK_HASH.fullmatch(block_hash):
                raise ValueError("evm_balance_block_invalid")
            units = _quantity(rpc.call("eth_getBalance", [wallet, hex(height)]))
            check = rpc.call("eth_getBlockByNumber", [hex(height), False])
            if not isinstance(check, Mapping) or check.get("hash") != block_hash:
                raise ValueError("evm_balance_block_reorged")
        provider = str(getattr(rpc, "last_provider", "") or "")
        if not provider:
            raise ValueError("balance_rpc_provider_unknown")
    reserve = Decimal(units) / Decimal(10**decimals) * price.price_usd
    snapshot = NativeBalanceSnapshot(chain, wallet, symbol, units, decimals, height, block_hash,
                                     provider, current, price, reserve)
    snapshot.require_fresh(now_ms=current)
    return snapshot


def collect_market_evidence(quote: ExecutableQuote, *, price_adapter: PythHermesPriceAdapter,
                            rpc: RpcTransport, wallet: str,
                            minimum_native_gas_reserve_usd: Decimal,
                            planned_native_spend_units: int,
                            now_ms: int | None = None) -> MarketEvidenceFacts:
    """Fetch a fresh independent price and chain-pinned gas balance together."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    observed = price_adapter.latest(now_ms=current)
    balance = capture_native_balance(rpc, chain_id=price_adapter.chain_id,
                                     wallet=wallet, price=observed, now_ms=current)
    return evaluate_market_evidence(quote, price=observed, balance=balance,
                                    expected_chain_id=price_adapter.chain_id,
                                    expected_wallet=wallet,
                                    minimum_native_gas_reserve_usd=minimum_native_gas_reserve_usd,
                                    planned_native_spend_units=planned_native_spend_units,
                                    now_ms=current)


class EvmMarketEvidenceProvider:
    """Real-probe EVM market gate; supports only reviewed input/feed bindings."""

    def __init__(self, *, chain_id: str | int, wallet: str, token_in: str,
                 token_decimals: int, price_adapter: PythHermesPriceAdapter | EvmV2PoolPriceCache,
                 rpc: RpcTransport) -> None:
        self.chain_id = str(chain_id)
        self.wallet = wallet
        self.token_in = token_in
        self.token_decimals = token_decimals
        self.price_adapter = price_adapter
        self.rpc = rpc

    def _binding_ok(self) -> bool:
        return (self.chain_id != "1399811149" and
                _APPROVED_QUOTE_INPUTS.get((self.chain_id, self.token_in.lower())) ==
                (self.price_adapter.asset, self.token_decimals) and
                self.price_adapter.chain_id == self.chain_id and
                _EVM_ADDRESS.fullmatch(self.wallet) is not None)

    def _snapshots(self, price: PriceObservation) -> tuple[NativeBalanceSnapshot, TokenBalanceSnapshot]:
        with rpc_view(self.rpc):
            native = capture_native_balance(self.rpc, chain_id=self.chain_id,
                                            wallet=self.wallet, price=price)
            token = capture_token_balance(
                self.rpc, chain_id=self.chain_id, wallet=self.wallet,
                token=self.token_in, expected_decimals=self.token_decimals,
                expected_block_height=native.block_height,
                expected_block_hash=native.block_hash,
            )
        return native, token

    def self_check(self) -> CapabilityStatus:
        from .pool_prices import EvmV2PoolPriceCache
        price_ready = (type(self.price_adapter) is PythHermesPriceAdapter
                       and type(self.price_adapter.transport) is PinnedApiTransport
                       or type(self.price_adapter) is EvmV2PoolPriceCache)
        if (not self._binding_ok() or type(self.rpc) is not FailoverJsonRpc
                or not self.rpc.uses_network_transport
                or not price_ready or not self.price_adapter.self_check().ready):
            return CapabilityStatus("market_evidence", True, False, "market_binding_or_transport_unapproved", {})
        try:
            price = self.price_adapter.latest()
            if price.source == "evm_v2_verified_pool":
                raise ValueError("pool_route_independence_unproven")
            native, token = self._snapshots(price)
            if (native.native_units <= 0 or token.units <= 0
                    or native.block_hash != token.block_hash
                    or native.rpc_provider != token.rpc_provider):
                raise ValueError("market_balance_probe_insufficient")
        except Exception as error:
            return CapabilityStatus("market_evidence", True, False, "market_probe_failed",
                                    {"errorType": type(error).__name__})
        return CapabilityStatus("market_evidence", True, True, "ok",
                                {"priceSource": price.source, "chainId": self.chain_id})

    def assess(self, intent: ExecutionIntent, quote: ExecutableQuote,
               parsed_signed_scope: Mapping[str, Any],
               signed_transaction: bytes) -> EvmFundingEvidence:
        if not signed_transaction:
            raise ValueError("market_signed_transaction_required")
        if (not self._binding_ok() or str(intent.chain_id) != self.chain_id
                or intent.token_in.casefold() != self.token_in.casefold()
                or str(parsed_signed_scope.get("wallet") or "").casefold() != self.wallet.casefold()):
            raise ValueError("market_evidence_intent_scope_mismatch")
        price = self.price_adapter.latest()
        verify_quote_input_price(quote, latest=price, chain_id=self.chain_id,
                                 token_in=self.token_in, decimals=self.token_decimals)
        native, token = self._snapshots(price)
        required_units = _quantity(parsed_signed_scope.get("sellAmount"))
        return evaluate_evm_funding_evidence(
            quote, price=price, native_balance=native, token_balance=token,
            parsed_signed_scope=parsed_signed_scope, expected_chain_id=self.chain_id,
            expected_wallet=self.wallet, expected_token_in=self.token_in,
            required_spend_units=required_units,
        )


class SolanaMarketEvidenceProvider:
    """Market/fee gate for reviewed WSOL input; scope parser remains a separate P02 blocker."""

    CHAIN_ID = "1399811149"

    def __init__(self, *, wallet: str, token_in: str, price_adapter: PythHermesPriceAdapter,
                 rpc: RpcTransport) -> None:
        self.wallet = wallet
        self.token_in = token_in
        self.price_adapter = price_adapter
        self.rpc = rpc

    def _binding_ok(self) -> bool:
        try:
            wallet_bytes = Base58Decoder.Decode(self.wallet)
        except ValueError:
            return False
        return (len(wallet_bytes) == 32 and
                _APPROVED_QUOTE_INPUTS.get((self.CHAIN_ID, self.token_in)) == ("SOL", 9)
                and self.price_adapter.chain_id == self.CHAIN_ID
                and self.price_adapter.asset == "SOL")

    def _snapshots(self, price: PriceObservation) -> tuple[NativeBalanceSnapshot, TokenBalanceSnapshot]:
        with rpc_view(self.rpc):
            for _ in range(3):
                native = capture_native_balance(self.rpc, chain_id=self.CHAIN_ID,
                                                wallet=self.wallet, price=price)
                token = capture_token_balance(self.rpc, chain_id=self.CHAIN_ID,
                                              wallet=self.wallet, token=self.token_in,
                                              expected_decimals=9)
                if (native.block_height == token.block_height and
                        native.block_hash == token.block_hash and
                        native.rpc_provider == token.rpc_provider):
                    return native, token
        raise ValueError("solana_funding_snapshots_inconsistent_slot")

    def self_check(self) -> CapabilityStatus:
        if (not self._binding_ok() or type(self.rpc) is not FailoverJsonRpc
                or not self.rpc.uses_network_transport
                or type(self.price_adapter) is not PythHermesPriceAdapter
                or type(self.price_adapter.transport) is not PinnedApiTransport):
            return CapabilityStatus("market_evidence", True, False, "market_binding_or_transport_unapproved", {})
        try:
            price = self.price_adapter.latest()
            native, token = self._snapshots(price)
            if native.native_units <= 0 or token.units <= 0:
                raise ValueError("market_balance_probe_insufficient")
            latest = self.rpc.call("getLatestBlockhash", [{"commitment": "confirmed"}])
            value = latest.get("value") if isinstance(latest, Mapping) else None
            blockhash = str(value.get("blockhash") or "") if isinstance(value, Mapping) else ""
            blockhash_bytes = Base58Decoder.Decode(blockhash)
            if len(blockhash_bytes) != 32:
                raise ValueError("solana_blockhash_probe_invalid")
            # A fee-only method probe: this message is never signed or broadcast.
            message = bytes([1, 0, 0, 1]) + Base58Decoder.Decode(self.wallet) + blockhash_bytes + b"\0"
            response = self.rpc.call("getFeeForMessage", [base64.b64encode(message).decode("ascii"), {
                "commitment": "confirmed", "minContextSlot": native.block_height,
            }])
            if (not isinstance(response, Mapping) or not isinstance(response.get("context"), Mapping)
                    or _quantity(response.get("value")) <= 0
                    or _quantity(response["context"].get("slot")) < native.block_height):
                raise ValueError("solana_fee_probe_invalid")
        except Exception as error:
            return CapabilityStatus("market_evidence", True, False, "market_probe_failed",
                                    {"errorType": type(error).__name__})
        return CapabilityStatus("market_evidence", True, True, "ok",
                                {"priceSource": "pyth_core_hermes", "chainId": self.CHAIN_ID})

    def assess(self, intent: ExecutionIntent, quote: ExecutableQuote,
               parsed_signed_scope: Mapping[str, Any],
               signed_transaction: bytes) -> SolanaFundingEvidence:
        if (not self._binding_ok() or str(intent.chain_id) != self.CHAIN_ID
                or intent.token_in != self.token_in
                or str(parsed_signed_scope.get("wallet") or "") != self.wallet
                or str(parsed_signed_scope.get("tokenIn") or "") != self.token_in):
            raise ValueError("market_evidence_intent_scope_mismatch")
        price = self.price_adapter.latest()
        verify_quote_input_price(quote, latest=price, chain_id=self.CHAIN_ID,
                                 token_in=self.token_in, decimals=9)
        native, token = self._snapshots(price)
        spend = _quantity(parsed_signed_scope.get("sellAmount"))
        native_spend = _quantity(parsed_signed_scope.get("nativeSpendLamports"))
        require_spend_balance(token, chain_id=self.CHAIN_ID, wallet=self.wallet,
                              token=self.token_in, required_units=spend)
        fee = solana_fee_for_signed_transaction(self.rpc, signed_transaction=signed_transaction,
                                                balance=native)
        market = evaluate_market_evidence(
            quote, price=price, balance=native, expected_chain_id=self.CHAIN_ID,
            expected_wallet=self.wallet, minimum_native_gas_reserve_usd=fee.minimum_gas_reserve_usd,
            planned_native_spend_units=native_spend,
        )
        return SolanaFundingEvidence(market, token.token, token.units, spend,
                                     fee.minimum_gas_reserve_usd, fee)
