"""Executable 0x and Jupiter quote requests with strict response validation.

The route's serialized bytes must still pass chain-specific scope parsing and an
independent sanity price before broadcast; a quote alone never arms live mode.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Any, Literal, Mapping, Protocol
from urllib.parse import urlparse

from curl_cffi import requests as cf
from curl_cffi.const import CurlOpt

from fomo.execution.url_safety import validate_endpoint_url
from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityStatus
from .interfaces import ExecutableQuote
from .zero_x_calldata import inspect_allowance_holder_settler

if TYPE_CHECKING:
    from .market_evidence import PythHermesPriceAdapter
    from .pool_prices import EvmV2PoolPriceCache


def _price_source_ready(adapter: object | None) -> bool:
    if adapter is None:
        return False
    from .market_evidence import PythHermesPriceAdapter
    from .pool_prices import EvmV2PoolPriceCache
    if type(adapter) is PythHermesPriceAdapter:
        return type(adapter.transport) is PinnedApiTransport and adapter.self_check().ready
    if type(adapter) is EvmV2PoolPriceCache:
        return adapter.self_check().ready
    return False


ALLOWANCE_HOLDER_CANCUN = "0x0000000000001ff3684f28c67538d4d072c22734"


class ApiTransport(Protocol):
    def request(self, method: Literal["GET", "POST"], url: str, *, params: Mapping[str, str] | None,
                payload: Mapping[str, Any] | None, headers: Mapping[str, str]) -> Mapping[str, Any]: ...


class PinnedApiTransport:
    def __init__(self, timeout_seconds: float = 3.0) -> None:
        self.timeout_seconds = timeout_seconds

    def request(self, method: Literal["GET", "POST"], url: str, *, params: Mapping[str, str] | None,
                payload: Mapping[str, Any] | None, headers: Mapping[str, str]) -> Mapping[str, Any]:
        safe_url, first = validate_endpoint_url(url)
        _, second = validate_endpoint_url(url)
        if first != second:
            raise ValueError("api_dns_rebinding_rejected")
        parsed = urlparse(safe_url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = sorted(address for address in second if ":" not in address)
        address = (addresses or sorted(second))[0]
        pinned = f"[{address}]" if ":" in address else address
        response = cf.request(
            method, safe_url, params=dict(params or {}), json=dict(payload) if payload is not None else None,
            headers=dict(headers), timeout=self.timeout_seconds, allow_redirects=False, proxy="",
            curl_options={CurlOpt.RESOLVE: [f"{parsed.hostname}:{port}:{pinned}"], CurlOpt.PROXY: ""},
        )
        if response.primary_ip and response.primary_ip not in second:
            raise ValueError("api_connected_address_mismatch")
        if response.status_code != 200:
            raise ValueError("route_api_unavailable")
        body = response.json()
        if not isinstance(body, Mapping):
            raise ValueError("route_api_invalid_response")
        return body


class _RecentQuote:
    def __init__(self) -> None:
        self.last_verified_at_ms = 0
        self.market_price_verified = False

    def _status(self, name: str, configured: bool) -> CapabilityStatus:
        ready = (configured and self.market_price_verified and self.last_verified_at_ms > 0
                 and 0 <= int(time.time() * 1000) - self.last_verified_at_ms <= 5_000)
        return CapabilityStatus(name, True, ready, "ok" if ready else "recent_verified_quote_required", {})


class ZeroXQuoteAdapter(_RecentQuote):
    URL = "https://api.0x.org/swap/allowance-holder/quote"

    def __init__(self, *, chain_id: int, wallet: str, input_decimals: int,
                 input_usd_price: Decimal | None = None, price_observed_at_ms: int = 0,
                 price_adapter: PythHermesPriceAdapter | EvmV2PoolPriceCache | None = None,
                 api_key_env: str = "ZEROX_API_KEY", slippage_bps: int = 100,
                 transport: ApiTransport | None = None) -> None:
        super().__init__()
        self.chain_id = int(chain_id)
        self.wallet = wallet
        self.input_decimals = input_decimals
        self.input_usd_price = Decimal(input_usd_price) if input_usd_price is not None else Decimal(0)
        self.price_observed_at_ms = price_observed_at_ms
        self.price_adapter = price_adapter
        self.api_key_env = api_key_env
        self.slippage_bps = slippage_bps
        self.transport = transport or PinnedApiTransport()

    def self_check(self) -> CapabilityStatus:
        return self._status("quote_adapter", bool(os.getenv(self.api_key_env, ""))
                            and type(self.transport) is PinnedApiTransport)

    def quote(self, intent: ExecutionIntent) -> tuple[ExecutableQuote, ...]:
        now_ms = int(time.time() * 1000)
        if intent.side != "buy" or str(intent.chain_id) != str(self.chain_id):
            raise ValueError("quote_input_or_sanity_price_invalid")
        input_price = self.input_usd_price
        price_observed_at_ms = self.price_observed_at_ms
        observed = None
        if self.price_adapter is not None:
            from .market_evidence import quote_input_price
            observed = quote_input_price(self.price_adapter, chain_id=self.chain_id,
                                         token_in=intent.token_in, decimals=self.input_decimals)
            input_price = observed.price_usd
            price_observed_at_ms = observed.fetched_at_ms if observed.pool_id else observed.published_at_ms
            now_ms = int(time.time() * 1000)
        if (not 0 <= now_ms - price_observed_at_ms <= 5000
                or input_price <= 0 or not 0 <= self.input_decimals <= 30
                or not 0 <= self.slippage_bps <= 500):
            raise ValueError("quote_input_or_sanity_price_invalid")
        key = os.getenv(self.api_key_env, "")
        if not key:
            raise ValueError("route_api_credential_unavailable")
        amount = int((intent.requested_usd / input_price * Decimal(10**self.input_decimals))
                     .to_integral_value(rounding=ROUND_DOWN))
        if amount <= 0:
            raise ValueError("quote_input_amount_zero")
        response = self.transport.request("GET", self.URL, params={
            "chainId": str(self.chain_id), "sellToken": intent.token_in,
            "buyToken": intent.token_out, "sellAmount": str(amount),
            "taker": self.wallet, "slippageBps": str(self.slippage_bps),
        }, payload=None, headers={"0x-api-key": key, "0x-version": "v2"})
        issues = response.get("issues")
        transaction = response.get("transaction")
        if (response.get("liquidityAvailable") is not True or not isinstance(transaction, Mapping)
                or not isinstance(issues, Mapping) or any(bool(value) for value in issues.values())
                or str(response.get("sellToken") or "").lower() != intent.token_in.lower()
                or str(response.get("buyToken") or "").lower() != intent.token_out.lower()
                or int(response.get("sellAmount") or 0) != amount):
            raise ValueError("zero_x_quote_not_executable")
        buy_amount = int(response.get("buyAmount") or 0)
        minimum = int(response.get("minBuyAmount") or 0)
        impact = Decimal(str(response.get("estimatedPriceImpact")
                             if response.get("estimatedPriceImpact") is not None else "NaN"))
        if buy_amount <= 0 or minimum <= 0 or minimum > buy_amount or not impact.is_finite() or impact < 0:
            raise ValueError("zero_x_quote_protection_missing")
        target = str(transaction.get("to") or "")
        if not target or not transaction.get("data") or not transaction.get("gas"):
            raise ValueError("zero_x_transaction_incomplete")
        # The v2 AllowanceHolder response is not a legacy V2-router call. This
        # structural check is deliberately narrower than a full action audit;
        # the EVM builder continues to reject this route until P01 is complete.
        try:
            call = inspect_allowance_holder_settler(bytes.fromhex(str(transaction["data"]).removeprefix("0x")))
        except (TypeError, ValueError) as error:
            raise ValueError("zero_x_transaction_format_unverified") from error
        if (target.lower() != ALLOWANCE_HOLDER_CANCUN or call.operator != call.settler
                or call.sell_token != intent.token_in.lower() or call.sell_amount != amount
                or call.buy_token != intent.token_out.lower() or call.recipient != self.wallet.lower()
                or call.minimum_buy_amount < minimum or int(transaction.get("value") or 0) != 0):
            raise ValueError("zero_x_transaction_scope_mismatch")
        self.last_verified_at_ms = now_ms
        self.market_price_verified = _price_source_ready(self.price_adapter)
        execution_payload = {**transaction, "from": self.wallet}
        if observed is not None:
            execution_payload["inputPriceProof"] = {
                "source": observed.source, "feedId": observed.feed_id,
                "priceUsd": str(observed.price_usd),
                "publishedAtMs": price_observed_at_ms,
                "payloadSha256": observed.payload_sha256,
                "poolId": observed.pool_id, "blockHash": observed.block_hash,
            }
        return (ExecutableQuote("0x_allowance_holder", str(buy_amount), str(minimum),
                                datetime.now(timezone.utc).isoformat(), str(impact * 100), True,
                                (target, call.settler), execution_payload),)


class JupiterQuoteAdapter(_RecentQuote):
    QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
    SWAP_URL = "https://api.jup.ag/swap/v1/swap"

    def __init__(self, *, wallet: str, input_decimals: int, input_usd_price: Decimal | None = None,
                 price_observed_at_ms: int = 0,
                 price_adapter: PythHermesPriceAdapter | EvmV2PoolPriceCache | None = None,
                 api_key_env: str = "JUPITER_API_KEY",
                 slippage_bps: int = 100, transport: ApiTransport | None = None) -> None:
        super().__init__()
        self.wallet = wallet
        self.input_decimals = input_decimals
        self.input_usd_price = Decimal(input_usd_price) if input_usd_price is not None else Decimal(0)
        self.price_observed_at_ms = price_observed_at_ms
        self.price_adapter = price_adapter
        self.api_key_env = api_key_env
        self.slippage_bps = slippage_bps
        self.transport = transport or PinnedApiTransport()

    def self_check(self) -> CapabilityStatus:
        return self._status("quote_adapter", bool(os.getenv(self.api_key_env, ""))
                            and type(self.transport) is PinnedApiTransport)

    def quote(self, intent: ExecutionIntent) -> tuple[ExecutableQuote, ...]:
        now_ms = int(time.time() * 1000)
        if intent.side != "buy" or str(intent.chain_id) != "1399811149":
            raise ValueError("quote_input_or_sanity_price_invalid")
        input_price = self.input_usd_price
        price_observed_at_ms = self.price_observed_at_ms
        observed = None
        if self.price_adapter is not None:
            from .market_evidence import quote_input_price
            observed = quote_input_price(self.price_adapter, chain_id="1399811149",
                                         token_in=intent.token_in, decimals=self.input_decimals)
            input_price = observed.price_usd
            price_observed_at_ms = observed.fetched_at_ms if observed.pool_id else observed.published_at_ms
            now_ms = int(time.time() * 1000)
        if (not 0 <= now_ms - price_observed_at_ms <= 5000
                or input_price <= 0 or not 0 <= self.input_decimals <= 30
                or not 0 <= self.slippage_bps <= 500):
            raise ValueError("quote_input_or_sanity_price_invalid")
        key = os.getenv(self.api_key_env, "")
        if not key:
            raise ValueError("route_api_credential_unavailable")
        amount = int((intent.requested_usd / input_price * Decimal(10**self.input_decimals))
                     .to_integral_value(rounding=ROUND_DOWN))
        if amount <= 0:
            raise ValueError("quote_input_amount_zero")
        headers = {"x-api-key": key, "Content-Type": "application/json"}
        quote = self.transport.request("GET", self.QUOTE_URL, params={
            "inputMint": intent.token_in, "outputMint": intent.token_out,
            "amount": str(amount), "slippageBps": str(self.slippage_bps),
        }, payload=None, headers=headers)
        if (quote.get("inputMint") != intent.token_in or quote.get("outputMint") != intent.token_out
                or int(quote.get("inAmount") or 0) != amount):
            raise ValueError("jupiter_quote_identity_mismatch")
        output = int(quote.get("outAmount") or 0)
        minimum = int(quote.get("otherAmountThreshold") or 0)
        impact = Decimal(str(quote.get("priceImpactPct")
                             if quote.get("priceImpactPct") is not None else "NaN"))
        if output <= 0 or minimum <= 0 or minimum > output or not impact.is_finite() or impact < 0:
            raise ValueError("jupiter_quote_protection_missing")
        swap = self.transport.request("POST", self.SWAP_URL, params=None, payload={
            "quoteResponse": dict(quote), "userPublicKey": self.wallet,
            "wrapAndUnwrapSol": False, "dynamicComputeUnitLimit": True,
        }, headers=headers)
        serialized = str(swap.get("swapTransaction") or "")
        if not serialized or int(swap.get("lastValidBlockHeight") or 0) <= 0:
            raise ValueError("jupiter_swap_transaction_missing")
        self.last_verified_at_ms = now_ms
        self.market_price_verified = _price_source_ready(self.price_adapter)
        execution_payload = {"base64UnsignedTransaction": serialized,
                             "lastValidBlockHeight": swap["lastValidBlockHeight"]}
        if observed is not None:
            execution_payload["inputPriceProof"] = {
                "source": observed.source, "feedId": observed.feed_id,
                "priceUsd": str(observed.price_usd),
                "publishedAtMs": price_observed_at_ms,
                "payloadSha256": observed.payload_sha256,
                "poolId": observed.pool_id, "blockHash": observed.block_hash,
            }
        return (ExecutableQuote("jupiter_metis", str(output), str(minimum),
                                datetime.now(timezone.utc).isoformat(), str(impact * 100), True,
                                tuple(str(step.get("swapInfo", {}).get("ammKey")) for step in quote.get("routePlan") or []),
                                execution_payload),)
