"""Fixed Ethereum Uniswap V3 USDC/WETH pool, read-only state and simulation.

No signer, approval, production assembly, or broadcaster is reachable here.
"""

from __future__ import annotations

import re
import time
import json
from dataclasses import dataclass, replace
from typing import Any, Mapping
from urllib.parse import urlparse

from curl_cffi import requests as cf
from curl_cffi.const import CurlOpt

from fomo.watching.rpc_transport import FailoverJsonRpc, RpcTransport, RpcUnavailable, rpc_view
from .url_safety import validate_endpoint_url

from .evm_transaction import decode_eip1559, keccak256
from .interfaces import BuiltTransaction
from .v3_math import (MAX_SQRT_RATIO, MIN_SQRT_RATIO, V3Quote, quote_exact_input,
                      required_input_for_output, spot_quote_per_base)
from .v3_transaction import decode_exact_input_single


@dataclass(frozen=True, slots=True)
class V3ChainConfig:
    chain_id: int
    factory: str
    router: str
    allowed_tokens: frozenset[str]
    router_variant: str = "swap_router"


@dataclass(frozen=True, slots=True)
class V3PoolTarget:
    pool: str
    token0: str
    token1: str
    fee: int
    tick_spacing: int
    decimals0: int
    decimals1: int


# Additional networks require independently reviewed addresses, not copied math.
CHAIN_CONFIGS: Mapping[int, V3ChainConfig] = {
    1: V3ChainConfig(
        1, "0x1f98431c8ad98523631ae4a59f267346ea31f984",
        "0xe592427a0aece92de3edee1f18e0157c05861564",
        frozenset({"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                   "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}),
    ),
    8453: V3ChainConfig(
        8453, "0x33128a8fc17869897dce68ed026d694621f6fdfd",
        "0x2626664c2603336e57b271c5c0b26f421741e481",
        frozenset({"0x4200000000000000000000000000000000000006",
                   "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"}),
        "swap_router_02",
    ),
}

MAINNET_PROBE_POOL = V3PoolTarget(
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    500, 10, 6, 18,
)
BASE_PROBE_POOL = V3PoolTarget(
    "0xd0b53d9277642d899df5c87a3966a349a798f224",
    "0x4200000000000000000000000000000000000006",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    500, 10, 18, 6,
)
_HEX_WORD = re.compile(r"^0x[0-9a-fA-F]{64}$")
_BLOCK_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _selector(signature: str) -> str:
    return "0x" + keccak256(signature.encode())[:4].hex()


def _word(value: Any) -> int:
    if not isinstance(value, str) or not _HEX_WORD.fullmatch(value):
        raise ValueError("v3_rpc_word_invalid")
    return int(value, 16)


def _address(value: Any) -> str:
    number = _word(value)
    if number >> 160 or number == 0:
        raise ValueError("v3_rpc_address_invalid")
    return "0x" + f"{number:040x}"


def _signed(value: int, width: int) -> int:
    low = value & ((1 << width) - 1)
    upper = value >> width
    negative = bool(low & (1 << (width - 1)))
    if upper != ((1 << (256 - width)) - 1 if negative else 0):
        raise ValueError("v3_signed_word_invalid")
    return low - (1 << width) if negative else low


def _call(rpc: RpcTransport, address: str, signature: str, block_tag: str,
          *arguments: int) -> Any:
    data = _selector(signature) + "".join(f"{argument % (1 << 256):064x}" for argument in arguments)
    return rpc.call("eth_call", [{"to": address, "data": data}, block_tag])


def _request(address: str, signature: str, block_tag: str,
             *arguments: int) -> tuple[str, list[Any]]:
    data = _selector(signature) + "".join(f"{argument % (1 << 256):064x}" for argument in arguments)
    return "eth_call", [{"to": address, "data": data}, block_tag]


def _batch(rpc: RpcTransport, requests: list[tuple[str, list[Any]]],
           *, call_offset: int) -> list[Any]:
    """One HTTP batch at a pinned height; fixture transports use identical requests."""
    for index, (method, params) in enumerate(requests, call_offset + 1):
        if (method != "eth_call" or len(params) != 2 or not isinstance(params[1], str)
                or not re.fullmatch(r"0x[0-9a-f]+", params[1])):
            raise ValueError(f"v3_rpc_request_unpinned:call={index}:method={method}:block_tag=false")
    if not isinstance(rpc, FailoverJsonRpc) or not rpc.uses_network_transport:
        results = []
        for index, (method, params) in enumerate(requests, call_offset + 1):
            try:
                results.append(rpc.call(method, params))
            except (RpcUnavailable, OSError) as error:
                raise ValueError(f"v3_rpc_failure:call={index}:method={method}:block_tag=true") from error
        return results
    if len(rpc.endpoints) != 1 or not rpc.endpoints[0].http_configured:
        raise ValueError("v3_rpc_batch_endpoint_scope_invalid")
    endpoint = rpc.endpoints[0]
    url, first = validate_endpoint_url(endpoint.resolved_http_url)
    _, second = validate_endpoint_url(endpoint.resolved_http_url)
    if first != second:
        raise ValueError("v3_rpc_dns_rebinding_rejected")
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    addresses = sorted(address for address in second if ":" not in address)
    address = (addresses or sorted(second))[0]
    pinned = f"[{address}]" if ":" in address else address
    payload = [{"jsonrpc": "2.0", "id": i + 1, "method": method, "params": params}
               for i, (method, params) in enumerate(requests)]
    rpc.last_request_method = "eth_call"
    rpc.last_block_tagged = True
    rpc.last_http_status = None
    rpc.last_provider_error_code = None
    rpc.last_transport_error_type = None
    for attempt in range(2):
        rpc.last_http_status = None
        rpc.last_provider_error_code = None
        rpc.last_transport_error_type = None
        try:
            response = cf.post(
                url, json=payload, headers={"Accept": "application/json"},
                timeout=rpc.timeout_seconds, allow_redirects=False, proxy="",
                curl_options={CurlOpt.RESOLVE: [f"{parsed.hostname}:{port}:{pinned}"], CurlOpt.PROXY: ""},
            )
            rpc.last_http_status = int(response.status_code)
            if (response.primary_ip and response.primary_ip not in second
                    or response.status_code != 200):
                raise ValueError("v3_rpc_batch_transport_invalid")
            body = response.json()
            if not isinstance(body, list) or len(body) != len(requests):
                raise ValueError("v3_rpc_batch_response_invalid")
            by_id = {item.get("id"): item for item in body if isinstance(item, dict)}
            if len(by_id) != len(requests):
                raise ValueError("v3_rpc_batch_response_invalid")
            results = []
            for i in range(len(requests)):
                item = by_id.get(i + 1)
                if (not isinstance(item, dict) or item.get("jsonrpc") != "2.0"
                        or item.get("error") is not None or "result" not in item):
                    rpc_error = item.get("error") if isinstance(item, dict) else None
                    if isinstance(rpc_error, dict) and type(rpc_error.get("code")) is int:
                        rpc.last_provider_error_code = rpc_error["code"]
                    raise ValueError(f"v3_rpc_failure:call={call_offset+i+1}:method=eth_call:block_tag=true")
                results.append(item["result"])
            rpc.last_provider = endpoint.provider
            rpc.last_diagnostic = "ok"
            return results
        except (OSError, ValueError, cf.RequestsError, json.JSONDecodeError) as error:
            if isinstance(error, cf.RequestsError):
                rpc.last_transport_error_type = type(error).__name__
            rpc.last_diagnostic = (str(error) if isinstance(error, ValueError)
                                   and str(error).startswith("v3_rpc_failure:")
                                   else "rpc_transport_failure")
            if isinstance(error, ValueError) and str(error).startswith("v3_rpc_failure:"):
                raise
            if attempt:
                raise ValueError(f"v3_rpc_failure:call={call_offset+1}:method=eth_call:block_tag=true") from error
    raise AssertionError("unreachable")


@dataclass(frozen=True, slots=True)
class V3PoolSnapshot:
    chain_id: str
    factory: str
    router: str
    pool: str
    token0: str
    token1: str
    fee: int
    tick_spacing: int
    decimals0: int
    decimals1: int
    sqrt_price_x96: int
    tick: int
    liquidity: int
    bitmaps: Mapping[int, int]
    liquidity_nets: Mapping[int, int]
    block_height: int
    block_hash: str
    observed_at_ms: int
    provider: str

    @property
    def spot_quote_per_base(self):
        return spot_quote_per_base(self.sqrt_price_x96, self.decimals0, self.decimals1)

    def quote(self, *, token_in: str, amount_in: int) -> V3Quote:
        source = token_in.lower()
        config = CHAIN_CONFIGS.get(int(self.chain_id))
        if (config is None or self.factory != config.factory or self.router != config.router
                or self.token0 not in config.allowed_tokens or self.token1 not in config.allowed_tokens
                or source not in {self.token0, self.token1}):
            raise ValueError("v3_quote_pool_scope_invalid")
        quote = quote_exact_input(
            amount_in=amount_in, zero_for_one=source == self.token0,
            sqrt_price_x96=self.sqrt_price_x96, tick=self.tick, liquidity=self.liquidity,
            fee_pips=self.fee, tick_spacing=self.tick_spacing,
            bitmaps=self.bitmaps, liquidity_nets=self.liquidity_nets,
        )
        return replace(quote, block_hash=self.block_hash, pool=self.pool)

    def input_for_output(self, *, token_in: str, desired_output: int,
                         maximum_input: int) -> int:
        source = token_in.lower()
        self.quote(token_in=source, amount_in=maximum_input)
        return required_input_for_output(
            desired_output=desired_output, maximum_input=maximum_input,
            zero_for_one=source == self.token0,
            sqrt_price_x96=self.sqrt_price_x96, tick=self.tick, liquidity=self.liquidity,
            fee_pips=self.fee, tick_spacing=self.tick_spacing,
            bitmaps=self.bitmaps, liquidity_nets=self.liquidity_nets,
        )


class UniswapV3PoolReader:
    """Read fixed-pool immutables and bounded tick coverage at one pinned block."""

    def __init__(self, *, rpc: RpcTransport, chain_id: int = 1,
                 target: V3PoolTarget | None = None) -> None:
        config = CHAIN_CONFIGS.get(chain_id)
        if target is None:
            target = {1: MAINNET_PROBE_POOL, 8453: BASE_PROBE_POOL}.get(chain_id)
        if (config is None or target is None
                or any(not re.fullmatch(r"0x[0-9a-f]{40}", value)
                       for value in (target.pool, target.token0, target.token1))
                or target.token0 >= target.token1
                or target.token0.lower() not in config.allowed_tokens
                or target.token1.lower() not in config.allowed_tokens
                or not 0 < target.fee < 1_000_000 or not 0 < target.tick_spacing < 32768):
            raise ValueError("v3_chain_unapproved")
        self.rpc = rpc
        self.config = config
        self.target = target
        self.last_block_diagnostic: dict[str, Any] = {}

    def snapshot(self, *, now_ms: int | None = None) -> V3PoolSnapshot:
        attempts = 3 if self.config.chain_id == 8453 else 2
        for attempt in range(attempts):
            try:
                return self._snapshot_once(now_ms=now_ms)
            except ValueError as error:
                retryable = ("v3_head_advanced_retry", "v3_block_reorged_or_provider_unknown")
                if (attempt + 1 == attempts or str(error) not in retryable
                        or (self.config.chain_id != 8453 and not isinstance(self.rpc, FailoverJsonRpc))):
                    raise
        raise AssertionError("unreachable")

    def _snapshot_once(self, *, now_ms: int | None = None) -> V3PoolSnapshot:
        started = int(time.time() * 1000) if now_ms is None else int(now_ms)
        diagnostic: dict[str, Any] = {
            "rule": "height>0;hash=32bytes;0<=ageAtReceiveMs<=60000;fixedBlockHashMustMatch",
            "thresholdSeconds": 60, "thresholdBlocks": None,
            "startMs": started, "headBefore": None, "blockBefore": None,
            "headAfter": None, "blockAfter": None, "headerAttempts": [],
        }
        self.last_block_diagnostic = diagnostic
        def base_header(head: int) -> tuple[Mapping[str, Any], int]:
            """Read only headers; one previous height is the sole fallback."""
            for candidate in (head, head - 1) if head > 1 else (head,):
                diagnostic["headerAttempts"].append(candidate)
                try:
                    header = self.rpc.call("eth_getBlockByNumber", [hex(candidate), False])
                except RpcUnavailable:
                    if candidate == head and head > 1:
                        continue
                    raise
                if isinstance(header, Mapping) and header.get("hash"):
                    return header, candidate
                if candidate != head or head <= 1:
                    raise ValueError("v3_block_missing")
            raise ValueError("v3_block_missing")
        with rpc_view(self.rpc):
            try:
                if self.config.chain_id == 8453:
                    head_before = int(str(self.rpc.call("eth_blockNumber", [])), 16)
                    diagnostic["headBefore"] = head_before
                    block, selected_height = base_header(head_before)
                    diagnostic["headerFallback"] = selected_height != head_before
                else:
                    block = self.rpc.call("eth_getBlockByNumber", ["latest", False])
            except RpcUnavailable as error:
                method = "eth_blockNumber" if diagnostic["headBefore"] is None and self.config.chain_id == 8453 else "eth_getBlockByNumber"
                tagged = method == "eth_getBlockByNumber" and self.config.chain_id == 8453
                call_index = 2 if tagged else 1
                raise ValueError(f"v3_rpc_failure:call={call_index}:method={method}:block_tag={str(tagged).lower()}") from error
            if not isinstance(block, Mapping):
                raise ValueError("v3_block_missing")
            try:
                height = int(str(block["number"]), 16)
                timestamp_ms = int(str(block["timestamp"]), 16) * 1000
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("v3_block_invalid") from error
            block_hash = str(block.get("hash") or "")
            received_ms = started if now_ms is not None else int(time.time() * 1000)
            age_ms = received_ms - timestamp_ms
            diagnostic["blockBefore"] = {"number": height, "hash": block_hash}
            diagnostic["ageAtReceiveMs"] = age_ms
            invalid_identity = height <= 0 or not _BLOCK_HASH.fullmatch(block_hash)
            invalid_age = not 0 <= age_ms <= 60_000
            diagnostic["checks"] = {"heightPositive": height > 0,
                                    "hash32Bytes": bool(_BLOCK_HASH.fullmatch(block_hash)),
                                    "ageWithin60Seconds": not invalid_age}
            if invalid_identity or invalid_age:
                if self.config.chain_id == 8453:
                    try:
                        head_after = int(str(self.rpc.call("eth_blockNumber", [])), 16)
                        diagnostic["headAfter"] = head_after
                        later, _ = base_header(head_after)
                        diagnostic["blockAfter"] = {
                            "number": int(str(later.get("number") or "0x0"), 16),
                            "hash": str(later.get("hash") or ""),
                        } if isinstance(later, Mapping) else None
                    except RpcUnavailable as error:
                        raise ValueError("v3_rpc_failure:method=head_after:block_tag=true") from error
                    if not invalid_identity and head_after > height and age_ms < 0:
                        raise ValueError("v3_head_advanced_retry")
                raise ValueError("v3_block_stale_or_invalid")
            if self.config.chain_id == 8453 and height != selected_height:
                raise ValueError("v3_block_stale_or_invalid")
            tag = hex(height)
            config, target = self.config, self.target
            identity = _batch(self.rpc, [
                _request(config.factory, "getPool(address,address,uint24)", tag,
                         int(target.token0, 16), int(target.token1, 16), target.fee),
                _request(target.pool, "factory()", tag),
                _request(target.pool, "token0()", tag),
                _request(target.pool, "token1()", tag),
                _request(target.pool, "fee()", tag),
                _request(target.pool, "tickSpacing()", tag),
                _request(config.factory, "feeAmountTickSpacing(uint24)", tag, target.fee),
                _request(target.token0, "decimals()", tag),
                _request(target.token1, "decimals()", tag),
                _request(target.pool, "slot0()", tag),
                _request(target.pool, "liquidity()", tag),
            ], call_offset=1)
            factory_pool, factory, token0, token1 = map(_address, identity[:4])
            fee = _word(identity[4])
            spacing = _signed(_word(identity[5]), 24)
            enabled_spacing = _signed(_word(identity[6]), 24)
            decimals0, decimals1 = map(_word, identity[7:9])
            if (factory_pool != target.pool or factory != config.factory
                    or token0 != target.token0 or token1 != target.token1
                    or fee != target.fee or spacing != target.tick_spacing
                    or enabled_spacing != target.tick_spacing
                    or decimals0 != target.decimals0 or decimals1 != target.decimals1):
                raise ValueError("v3_pool_identity_mismatch")
            slot0_raw = identity[9]
            if not isinstance(slot0_raw, str) or not re.fullmatch(r"0x[0-9a-fA-F]{448}", slot0_raw):
                raise ValueError("v3_slot0_response_invalid")
            values = [int(slot0_raw[2 + index * 64:2 + (index + 1) * 64], 16)
                      for index in range(7)]
            sqrt_price, tick = values[0], _signed(values[1], 24)
            liquidity = _word(identity[10])
            if (not MIN_SQRT_RATIO < sqrt_price < MAX_SQRT_RATIO or not -887272 <= tick <= 887272
                    or not 0 < liquidity < 2**128 or any(value >= 2**16 for value in values[2:5])
                    or values[5] >= 2**8 or values[6] != 1):
                raise ValueError("v3_pool_state_invalid")
            current_word = (tick // target.tick_spacing) >> 8
            bitmaps: dict[int, int] = {}
            liquidity_nets: dict[int, int] = {}
            bits = _word(_batch(self.rpc, [
                _request(target.pool, "tickBitmap(int16)", tag, current_word),
            ], call_offset=12)[0])
            bitmaps[current_word] = bits
            compressed = tick // target.tick_spacing
            position = compressed & 255
            below = bits & ((1 << (position + 1)) - 1)
            above = bits >> (position + 1)
            nearest_bits = []
            if below:
                nearest_bits.append(below.bit_length() - 1)
            if above:
                nearest_bits.append(position + 1 + (above & -above).bit_length() - 1)
            tick_indexes = [((current_word << 8) + bit) * target.tick_spacing
                            for bit in nearest_bits]
            if any(not -887272 <= index <= 887272 for index in tick_indexes):
                raise ValueError("v3_bitmap_tick_out_of_range")
            raws = _batch(self.rpc, [
                _request(target.pool, "ticks(int24)", tag, index) for index in tick_indexes
            ], call_offset=13) if tick_indexes else []
            for tick_index, raw in zip(tick_indexes, raws):
                    if not isinstance(raw, str) or not re.fullmatch(r"0x[0-9a-fA-F]{512}", raw):
                        raise ValueError("v3_tick_response_invalid")
                    words = [int(raw[2 + index * 64:2 + (index + 1) * 64], 16)
                             for index in range(8)]
                    if not 0 < words[0] < 2**128 or words[7] != 1:
                        raise ValueError("v3_tick_uninitialized")
                    liquidity_nets[tick_index] = _signed(words[1], 128)
            try:
                check = self.rpc.call("eth_getBlockByNumber", [tag, False])
            except RpcUnavailable as error:
                raise ValueError(f"v3_rpc_failure:call={14+len(tick_indexes)}:method=eth_getBlockByNumber:block_tag=true") from error
            provider = str(getattr(self.rpc, "last_provider", "") or "")
            diagnostic["pinnedRecheck"] = {
                "number": int(str(check.get("number") or "0x0"), 16),
                "hash": str(check.get("hash") or ""),
            } if isinstance(check, Mapping) else None
            if (not isinstance(check, Mapping) or check.get("hash") != block_hash
                    or not provider):
                raise ValueError("v3_block_reorged_or_provider_unknown")
            if self.config.chain_id == 8453:
                try:
                    head_after = int(str(self.rpc.call("eth_blockNumber", [])), 16)
                    diagnostic["headAfter"] = head_after
                    latest, _ = base_header(head_after)
                except RpcUnavailable as error:
                    raise ValueError("v3_rpc_failure:method=head_after:block_tag=true") from error
                diagnostic["blockAfter"] = {
                    "number": int(str(latest.get("number") or "0x0"), 16),
                    "hash": str(latest.get("hash") or ""),
                } if isinstance(latest, Mapping) else None
                if head_after != diagnostic["headBefore"] or head_after < height:
                    raise ValueError("v3_head_advanced_retry")
        observed = started if now_ms is not None else int(time.time() * 1000)
        if observed - timestamp_ms > 60_000:
            raise ValueError("v3_block_stale_after_read")
        return V3PoolSnapshot(str(config.chain_id), config.factory, config.router,
                              target.pool, target.token0, target.token1, fee, spacing,
                              decimals0, decimals1, sqrt_price, tick, liquidity,
                              bitmaps, liquidity_nets, height, block_hash, observed, provider)


def simulate_same_block(rpc: RpcTransport, *, wallet: str, transaction: BuiltTransaction,
                        snapshot: V3PoolSnapshot, quote: V3Quote,
                        state_override: Mapping[str, Any] | None = None) -> int:
    """eth_call only; exact output equality rejects a partial/different-block result."""
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    swap = decode_exact_input_single(fields.data, chain_id=int(snapshot.chain_id))
    owner = wallet.lower()
    config = CHAIN_CONFIGS.get(int(snapshot.chain_id))
    expected_quote = snapshot.quote(token_in=swap["tokenIn"], amount_in=swap["amountIn"])
    age_ms = int(time.time() * 1000) - snapshot.observed_at_ms
    if (transaction.provider != "uniswap_v3_l0" or fields.chain_id != int(snapshot.chain_id)
            or "0x" + fields.to.hex() != snapshot.router or fields.value != 0
            or config is None or snapshot.factory != config.factory or snapshot.router != config.router
            or snapshot.block_height <= 0
            or not 0 <= age_ms <= 60_000
            or quote.block_hash != snapshot.block_hash or quote.pool != snapshot.pool
            or quote != expected_quote
            or {swap["tokenIn"], swap["tokenOut"]} != {snapshot.token0, snapshot.token1}
            or swap["fee"] != snapshot.fee
            or swap["sqrtPriceLimitX96"] != (MIN_SQRT_RATIO + 1 if swap["tokenIn"] == snapshot.token0
                                                  else MAX_SQRT_RATIO - 1)
            or swap["recipient"] != owner or swap["amountIn"] != quote.amount_in
            or swap["amountOutMinimum"] > quote.amount_out):
        raise ValueError("v3_simulation_scope_invalid")
    call = {"from": owner, "to": snapshot.router, "data": "0x" + fields.data.hex(),
            "value": "0x0", "gas": hex(fields.gas_limit)}
    params: list[Any] = [call, hex(snapshot.block_height)]
    if state_override is not None:
        params.append(dict(state_override))
    with rpc_view(rpc):
        result = rpc.call("eth_call", params)
        block = rpc.call("eth_getBlockByNumber", [hex(snapshot.block_height), False])
    if not isinstance(block, Mapping) or block.get("hash") != snapshot.block_hash:
        raise ValueError("v3_simulation_block_reorged")
    if _word(result) != quote.amount_out:
        raise ValueError("v3_simulation_quote_mismatch_or_partial")
    return quote.amount_out


def mainnet_probe_override(*, wallet: str, token: str, amount_units: int) -> dict[str, Any]:
    """Synthetic balance/allowance for known mainnet token storage; eth_call only."""
    owner = wallet.lower()
    asset = token.lower()
    if (not re.fullmatch(r"0x[0-9a-fA-F]{40}", owner)
            or asset not in {MAINNET_PROBE_POOL.token0, MAINNET_PROBE_POOL.token1}
            or not 0 < amount_units < 2**112):
        raise ValueError("v3_override_scope_invalid")
    balance_slot_number, allowance_slot_number = ((9, 10) if asset == MAINNET_PROBE_POOL.token0
                                                  else (3, 4))
    owner_word = int(owner, 16).to_bytes(32, "big")
    spender_word = int(CHAIN_CONFIGS[1].router, 16).to_bytes(32, "big")
    balance_slot = keccak256(owner_word + balance_slot_number.to_bytes(32, "big"))
    allowance_outer = keccak256(owner_word + allowance_slot_number.to_bytes(32, "big"))
    allowance_slot = keccak256(spender_word + allowance_outer)
    amount = "0x" + amount_units.to_bytes(32, "big").hex()
    return {asset: {"stateDiff": {"0x" + balance_slot.hex(): amount,
                                   "0x" + allowance_slot.hex(): amount}},
            owner: {"balance": hex(10**18)}}


def verify_mainnet_probe_override(rpc: RpcTransport, *, wallet: str, token: str,
                                  amount_units: int, snapshot: V3PoolSnapshot,
                                  override: Mapping[str, Any]) -> None:
    if (snapshot.chain_id != "1" or snapshot.pool != MAINNET_PROBE_POOL.pool
            or token.lower() not in {snapshot.token0, snapshot.token1}):
        raise ValueError("v3_override_snapshot_invalid")
    owner, asset = wallet.lower(), token.lower()
    balance_data = "0x70a08231" + owner[2:].rjust(64, "0")
    allowance_data = ("0xdd62ed3e" + owner[2:].rjust(64, "0")
                      + snapshot.router[2:].rjust(64, "0"))
    tag = hex(snapshot.block_height)
    with rpc_view(rpc):
        balance = rpc.call("eth_call", [{"to": asset, "data": balance_data}, tag, override])
        allowance = rpc.call("eth_call", [{"to": asset, "data": allowance_data}, tag, override])
        block = rpc.call("eth_getBlockByNumber", [tag, False])
    if (not isinstance(block, Mapping) or block.get("hash") != snapshot.block_hash
            or _word(balance) != amount_units or _word(allowance) != amount_units):
        raise ValueError("v3_override_not_applied_or_block_changed")
