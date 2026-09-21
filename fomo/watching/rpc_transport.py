"""Secret-safe, chain-verified JSON-RPC transport with bounded failover."""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Protocol, Sequence
from urllib.parse import urlparse

from curl_cffi import requests as cf
from curl_cffi.const import CurlOpt

from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.url_safety import validate_endpoint_url


class RpcUnavailable(RuntimeError):
    """No endpoint returned a valid RPC response; never embeds endpoint URLs."""


class RpcTransport(Protocol):
    def call(self, method: str, params: Sequence[Any]) -> Any: ...


SOLANA_MAINNET_GENESIS = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
ONE_SHOT_METHODS = frozenset({"eth_sendRawTransaction", "sendTransaction"})
RpcRequest = Callable[[RpcEndpoint, str, Sequence[Any]], Any]


@dataclass(frozen=True)
class _Health:
    checked_at: float
    height: int
    block_hash: str


def _quantity(value: Any) -> int:
    result = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    if result < 0:
        raise ValueError("rpc_invalid_quantity")
    return result


class FailoverJsonRpc:
    def __init__(self, chain_id: str, endpoints: Sequence[RpcEndpoint], *, timeout_seconds: float = 3.0,
                 identity_ttl_seconds: float = 2.0, maximum_lag_blocks: int = 4,
                 requester: RpcRequest | None = None) -> None:
        self.chain_id = str(chain_id)
        self.endpoints = sorted((endpoint for endpoint in endpoints if endpoint.chain_id == self.chain_id),
                                key=lambda endpoint: (endpoint.priority, endpoint.provider))
        self.timeout_seconds = timeout_seconds
        self.identity_ttl_seconds = max(0.1, identity_ttl_seconds)
        self.maximum_lag_blocks = max(0, maximum_lag_blocks)
        self._requester = requester or self._http_request
        self._health: dict[str, _Health] = {}
        self._highest_height = -1
        self._pending_nonces: dict[str, int] = {}
        self._last_endpoint: str | None = None
        self._thread = threading.local()
        self.last_provider: str | None = None
        self.last_diagnostic = "not_probed"

    def _http_request(self, endpoint: RpcEndpoint, method: str, params: Sequence[Any]) -> Any:
        url, first = validate_endpoint_url(endpoint.resolved_http_url)
        _, second = validate_endpoint_url(endpoint.resolved_http_url)
        if first != second:
            raise ValueError("rpc_dns_rebinding_rejected")
        parsed = urlparse(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = sorted(address for address in second if ":" not in address)
        address = (addresses or sorted(second))[0]
        pinned = f"[{address}]" if ":" in address else address
        response = cf.post(
            url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
            headers={"Accept": "application/json"}, timeout=self.timeout_seconds,
            allow_redirects=False, proxy="",
            curl_options={CurlOpt.RESOLVE: [f"{parsed.hostname}:{port}:{pinned}"], CurlOpt.PROXY: ""},
        )
        if response.primary_ip and response.primary_ip not in second:
            raise ValueError("rpc_connected_address_mismatch")
        if 300 <= response.status_code < 400:
            raise ValueError("rpc_redirect_rejected")
        if response.status_code != 200:
            raise ValueError("rpc_http_status_invalid")
        body = response.json()
        if not isinstance(body, dict) or body.get("id") != 1 or body.get("jsonrpc") != "2.0":
            raise ValueError("rpc_response_invalid")
        if body.get("error") is not None or "result" not in body:
            raise ValueError("rpc_method_unavailable")
        return body["result"]

    def _request(self, endpoint: RpcEndpoint, method: str, params: Sequence[Any]) -> Any:
        return self._requester(endpoint, method, params)

    def _block_hash(self, endpoint: RpcEndpoint, height: int) -> str:
        if self.chain_id == "1399811149":
            block = self._request(endpoint, "getBlock", [height, {
                "commitment": "confirmed", "transactionDetails": "none", "rewards": False,
                "maxSupportedTransactionVersion": 0,
            }])
            return str(block.get("blockhash") or "") if isinstance(block, dict) else ""
        block = self._request(endpoint, "eth_getBlockByNumber", [hex(height), False])
        return str(block.get("hash") or "") if isinstance(block, dict) else ""

    def _probe(self, endpoint: RpcEndpoint, *, force: bool = False) -> _Health:
        cached = self._health.get(endpoint.endpoint_id)
        now = time.monotonic()
        if not force and cached and now - cached.checked_at <= self.identity_ttl_seconds:
            return cached
        if self.chain_id == "1399811149":
            if str(self._request(endpoint, "getGenesisHash", [])) != SOLANA_MAINNET_GENESIS:
                raise ValueError("rpc_wrong_chain")
            height = _quantity(self._request(endpoint, "getSlot", [{"commitment": "confirmed"}]))
            block_hash = ""
            for slot in range(height, max(0, height - 32), -1):
                block_hash = self._block_hash(endpoint, slot)
                if block_hash:
                    height = slot
                    break
        else:
            if str(_quantity(self._request(endpoint, "eth_chainId", []))) != self.chain_id:
                raise ValueError("rpc_wrong_chain")
            height = _quantity(self._request(endpoint, "eth_blockNumber", []))
            block_hash = self._block_hash(endpoint, height)
        if height <= 0 or not block_hash:
            raise ValueError("rpc_head_invalid")
        health = _Health(now, height, block_hash)
        self._health[endpoint.endpoint_id] = health
        return health

    @contextmanager
    def consistent_view(self) -> Iterator[None]:
        """Pin a multi-call receipt/block read to one endpoint; fail on mid-view loss."""
        if getattr(self._thread, "view_open", False):
            yield
            return
        self._thread.view_open = True
        self._thread.pinned_endpoint = None
        try:
            yield
        finally:
            self._thread.view_open = False
            self._thread.pinned_endpoint = None

    def call(self, method: str, params: Sequence[Any]) -> Any:
        pinned = getattr(self._thread, "pinned_endpoint", None)
        candidates = [item for item in self.endpoints if item.endpoint_id == pinned] if pinned else self.endpoints
        last_code = "rpc_no_configured_endpoint"
        for index, endpoint in enumerate(candidates):
            if not endpoint.http_configured:
                continue
            try:
                switched = self._last_endpoint is not None and endpoint.endpoint_id != self._last_endpoint
                health = self._probe(endpoint, force=switched)
                if self._highest_height >= 0 and health.height + self.maximum_lag_blocks < self._highest_height:
                    raise ValueError("rpc_head_stale")
                if switched and self._last_endpoint in self._health:
                    old = self._health[self._last_endpoint]
                    if old.height <= health.height and self._block_hash(endpoint, old.height) != old.block_hash:
                        raise ValueError("rpc_chain_view_conflict")
                    if health.height < old.height:
                        raise ValueError("rpc_head_regressed_on_failover")
                if (method == "eth_getTransactionCount" and len(params) >= 2 and params[1] == "pending"
                        and index > 0 and str(params[0]).casefold() not in self._pending_nonces):
                    raise ValueError("rpc_pending_nonce_unanchored_failover")
                value = self._request(endpoint, method, params)
                if method == "eth_chainId" and str(_quantity(value)) != self.chain_id:
                    raise ValueError("rpc_wrong_chain")
                if method == "getGenesisHash" and str(value) != SOLANA_MAINNET_GENESIS:
                    raise ValueError("rpc_wrong_chain")
                if method == "eth_getTransactionCount" and len(params) >= 2 and params[1] == "pending":
                    wallet = str(params[0]).casefold()
                    nonce = _quantity(value)
                    if nonce < self._pending_nonces.get(wallet, 0):
                        raise ValueError("rpc_pending_nonce_regressed")
                    self._pending_nonces[wallet] = nonce
                self._highest_height = max(self._highest_height, health.height)
                self._last_endpoint = endpoint.endpoint_id
                self.last_provider = endpoint.provider
                self.last_diagnostic = "ok"
                if getattr(self._thread, "view_open", False):
                    self._thread.pinned_endpoint = endpoint.endpoint_id
                return value
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, cf.RequestsError) as error:
                last_code = (str(error) if isinstance(error, ValueError) and str(error).startswith("rpc_")
                             else "rpc_transport_failure")
                self.last_diagnostic = last_code
                if pinned or method in ONE_SHOT_METHODS:
                    break
        raise RpcUnavailable(f"rpc_unavailable:{self.chain_id}:{method}:{last_code}")


def rpc_view(rpc: RpcTransport):
    """Use a pinned endpoint when supported; fixture transports remain usable."""
    return rpc.consistent_view() if isinstance(rpc, FailoverJsonRpc) else nullcontext()
