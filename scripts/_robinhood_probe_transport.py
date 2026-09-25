"""Proxy-aware transport strictly for local Robinhood L0 read-only probes.

The production RPC transport retains its DNS-pinned direct path. Windows
hosts with a configured outbound proxy cannot reach that pinned address;
these probes use the existing HTTPS proxy without enabling write RPC methods.
"""

from __future__ import annotations

import os
import re
from typing import Any, Sequence

from curl_cffi import requests as cf

from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.url_safety import validate_endpoint_url

READ_METHODS = frozenset({"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber",
                          "eth_call", "eth_getLogs", "eth_getCode"})


def proxy_configured() -> bool:
    return any(os.getenv(name) for name in ("HTTPS_PROXY", "https_proxy",
                                             "ALL_PROXY", "all_proxy"))


def read_only_proxy_request(endpoint: RpcEndpoint, method: str,
                            params: Sequence[Any], *, timeout_seconds: float = 10.0) -> Any:
    if endpoint.chain_id != "4663" or method not in READ_METHODS:
        raise ValueError("robinhood_probe_rpc_scope_invalid")
    url, first = validate_endpoint_url(endpoint.resolved_http_url)
    _, second = validate_endpoint_url(endpoint.resolved_http_url)
    if first != second or not url.startswith("https://"):
        raise ValueError("robinhood_probe_endpoint_invalid")
    response = cf.post(url, json={"jsonrpc": "2.0", "id": 1,
                                  "method": method, "params": list(params)},
                       headers={"Accept": "application/json"},
                       timeout=timeout_seconds, allow_redirects=False)
    if response.status_code != 200:
        raise ValueError(f"robinhood_probe_http_status_{response.status_code}")
    body = response.json()
    if (not isinstance(body, dict) or body.get("jsonrpc") != "2.0"
            or body.get("id") != 1 or body.get("error") is not None
            or "result" not in body):
        raise ValueError("robinhood_probe_rpc_response_invalid")
    return body["result"]


class RobinhoodDiscoveryRpc:
    """One-endpoint read-only view; discovery rechecks the event block hash."""

    supports_multicall3 = True

    def __init__(self, endpoint: RpcEndpoint, *, timeout_seconds: float = 10.0) -> None:
        if endpoint.chain_id != "4663":
            raise ValueError("robinhood_probe_rpc_scope_invalid")
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.last_request_method: str | None = None
        self.last_provider: str | None = None
        self.request_count = 0
        self.http_round_trip_count = 0
        self.json_rpc_method_count = 0
        self.multicall_subcall_count = 0
        self.eth_get_logs_count = 0
        self.rate_limit_count = 0
        self.timeout_count = 0

    def call(self, method: str, params: Sequence[Any]) -> Any:
        self.last_request_method = method
        self.request_count += 1
        self.http_round_trip_count += 1
        self.json_rpc_method_count += 1
        if method == "eth_getLogs":
            self.eth_get_logs_count += 1
        try:
            result = read_only_proxy_request(self.endpoint, method, params,
                                             timeout_seconds=self.timeout_seconds)
        except Exception as error:
            if str(error) == "robinhood_probe_http_status_429":
                self.rate_limit_count += 1
            if "timeout" in type(error).__name__.lower() or "timeout" in str(error).lower():
                self.timeout_count += 1
            raise
        self.last_provider = self.endpoint.provider
        return result

    def record_multicall_subcalls(self, count: int) -> None:
        if not 0 < count <= 64:
            raise ValueError("robinhood_probe_multicall_scope_invalid")
        self.multicall_subcall_count += count

    def call_batch(self, requests: Sequence[tuple[str, list[Any]]]) -> list[Any]:
        """Bounded, same-block JSON-RPC batch for read-only tick calls."""
        if not 0 < len(requests) <= 3:
            raise ValueError("robinhood_probe_batch_scope_invalid")
        self.request_count += len(requests)
        self.http_round_trip_count += 1
        self.json_rpc_method_count += len(requests)
        tags = set()
        for method, params in requests:
            if (method != "eth_call" or len(params) != 2
                    or not isinstance(params[1], str)
                    or not re.fullmatch(r"0x[0-9a-f]+", params[1])):
                raise ValueError("robinhood_probe_batch_unpinned")
            tags.add(params[1])
        if len(tags) != 1:
            raise ValueError("robinhood_probe_batch_cross_block")
        self.last_request_method = "eth_call"
        url, first = validate_endpoint_url(self.endpoint.resolved_http_url)
        _, second = validate_endpoint_url(self.endpoint.resolved_http_url)
        if first != second or not url.startswith("https://"):
            raise ValueError("robinhood_probe_endpoint_invalid")
        payload = [{"jsonrpc": "2.0", "id": index + 1,
                    "method": method, "params": params}
                   for index, (method, params) in enumerate(requests)]
        response = cf.post(url, json=payload, headers={"Accept": "application/json"},
                           timeout=self.timeout_seconds, allow_redirects=False)
        if response.status_code != 200:
            raise ValueError("robinhood_probe_http_status_invalid")
        body = response.json()
        if not isinstance(body, list) or len(body) != len(requests):
            raise ValueError("robinhood_probe_batch_response_invalid")
        by_id = {item.get("id"): item for item in body if isinstance(item, dict)}
        if len(by_id) != len(requests):
            raise ValueError("robinhood_probe_batch_response_invalid")
        results = []
        for index in range(len(requests)):
            item = by_id.get(index + 1)
            if (not isinstance(item, dict) or item.get("jsonrpc") != "2.0"
                    or item.get("error") is not None or "result" not in item):
                raise ValueError("robinhood_probe_batch_call_failed")
            results.append(item["result"])
        self.last_provider = self.endpoint.provider
        return results
