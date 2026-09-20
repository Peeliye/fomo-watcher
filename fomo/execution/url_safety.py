"""Shared DNS-aware outbound endpoint validation for HTTP and WebSocket RPCs."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse


class UnsafeEndpointError(ValueError):
    pass


Resolver = Callable[..., list[tuple[Any, ...]]]


def validate_endpoint_url(
    value: Any,
    *,
    websocket: bool = False,
    resolver: Resolver | None = None,
) -> tuple[str, frozenset[str]]:
    text = str(value or "").strip()
    if not text:
        raise UnsafeEndpointError("endpoint_not_configured")
    if any(character in text for character in ("\r", "\n", "\x00")) or len(text) > 2048:
        raise UnsafeEndpointError("invalid_endpoint")
    parsed = urlparse(text)
    allowed = {"ws", "wss"} if websocket else {"http", "https"}
    if parsed.scheme.lower() not in allowed or not parsed.hostname or parsed.username or parsed.password:
        raise UnsafeEndpointError("invalid_endpoint")
    hostname = str(parsed.hostname).rstrip(".").casefold()
    if hostname in {"localhost", "metadata", "metadata.google.internal"} or hostname.endswith(
        (".localhost", ".local", ".internal")
    ):
        raise UnsafeEndpointError("unsafe_endpoint")
    port = parsed.port or (443 if parsed.scheme.lower() in {"https", "wss"} else 80)
    resolver = resolver or socket.getaddrinfo
    try:
        literal = ipaddress.ip_address(hostname)
        addresses = {str(literal)}
    except ValueError:
        try:
            addresses = {str(item[4][0]) for item in resolver(hostname, port, type=socket.SOCK_STREAM)}
        except (OSError, socket.gaierror) as exc:
            raise UnsafeEndpointError("dns_resolution_failed") from exc
    if not addresses:
        raise UnsafeEndpointError("dns_resolution_failed")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if (
            not ip.is_global
            or ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise UnsafeEndpointError("unsafe_endpoint")
    return text, frozenset(addresses)
