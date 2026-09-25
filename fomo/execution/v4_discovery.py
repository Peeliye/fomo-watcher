"""Read-only V4 Initialize discovery for an explicitly supplied PoolId."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from fomo.watching.rpc_transport import RpcTransport, rpc_view

from .direct_v4 import CHAIN_CONFIGS, V4PoolKey, ZERO
from .evm_transaction import keccak256
INITIALIZE_TOPIC = "0x" + keccak256(
    b"Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
).hex()
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_LOG_WORDS = re.compile(r"^0x[0-9a-fA-F]{320}$")


@dataclass(frozen=True, slots=True)
class V4PoolDiscovery:
    chain_id: int
    key: V4PoolKey
    pool_id: str
    block_height: int
    block_hash: str
    identity_verified: bool
    quote_allowed: bool
    reject_reason: str | None


def _topic_address(topic: Any) -> str:
    if not isinstance(topic, str) or not _HASH.fullmatch(topic) or int(topic[2:26], 16):
        raise ValueError("v4_initialize_topic_invalid")
    return "0x" + topic[-40:].lower()


def _signed24(word: int) -> int:
    low = word & ((1 << 24) - 1)
    negative = bool(low & (1 << 23))
    if word >> 24 != (((1 << 232) - 1) if negative else 0):
        raise ValueError("v4_initialize_int24_invalid")
    return low - (1 << 24) if negative else low


def decode_initialize(log: Mapping[str, Any], *, chain_id: int,
                      expected_pool_id: str, expected_token: str) -> V4PoolDiscovery:
    """Decode all five PoolKey fields and compare its hash with the indexed id."""
    config = CHAIN_CONFIGS.get(chain_id)
    if config is None or not _HASH.fullmatch(expected_pool_id):
        raise ValueError("v4_discovery_scope_invalid")
    if str(log.get("address", "")).lower() != config.pool_manager:
        raise ValueError("v4_initialize_manager_mismatch")
    topics = log.get("topics")
    data = log.get("data")
    if (not isinstance(topics, list) or len(topics) != 4
            or topics[0].lower() != INITIALIZE_TOPIC
            or not isinstance(topics[1], str) or topics[1].lower() != expected_pool_id.lower()
            or not isinstance(data, str) or not _LOG_WORDS.fullmatch(data)):
        raise ValueError("v4_initialize_event_invalid")
    words = [int(data[2 + i * 64:2 + (i + 1) * 64], 16) for i in range(5)]
    if (words[0] >= 1 << 24 or words[2] >= 1 << 160
            or not 0 < words[3] < 1 << 160):
        raise ValueError("v4_initialize_fields_invalid")
    _signed24(words[4])  # Initial tick may be negative; require ABI sign extension.
    fee = words[0]
    spacing = _signed24(words[1])
    hooks = f"0x{words[2]:040x}"
    key = V4PoolKey(_topic_address(topics[2]), _topic_address(topics[3]),
                    fee, spacing, hooks)
    block_hash = str(log.get("blockHash") or "")
    block_number = log.get("blockNumber")
    if (not _HASH.fullmatch(block_hash) or not isinstance(block_number, str)
            or not re.fullmatch(r"0x[0-9a-fA-F]+", block_number)
            or log.get("removed") is True):
        raise ValueError("v4_initialize_block_invalid")
    identity = (key.currency0 < key.currency1 and spacing > 0
                and expected_token.lower() in {key.currency0, key.currency1}
                and key.pool_id == expected_pool_id.lower())
    reason = ("v4_pool_identity_mismatch" if not identity else
              "v4_nonzero_hook_rejected" if hooks != ZERO else None)
    if reason is None:
        try:
            key.validate(config)
        except ValueError:
            reason = "v4_pool_key_unapproved"
    return V4PoolDiscovery(chain_id, key, expected_pool_id.lower(),
                           int(block_number, 16), block_hash.lower(),
                           identity, reason is None, reason)


def discover_pool(rpc: RpcTransport, *, chain_id: int,
                  pool_id: str,
                  token: str,
                  timestamp_hint: int | None = None) -> V4PoolDiscovery:
    """Bound log discovery to one head; then bind the event to its block hash."""
    config = CHAIN_CONFIGS.get(chain_id)
    if config is None or not _HASH.fullmatch(pool_id):
        raise ValueError("v4_discovery_scope_invalid")
    with rpc_view(rpc):
        head = rpc.call("eth_getBlockByNumber", ["latest", False])
        if not isinstance(head, Mapping) or not _HASH.fullmatch(str(head.get("hash") or "")):
            raise ValueError("v4_discovery_head_invalid")
        upper = str(head.get("number") or "")
        if not re.fullmatch(r"0x[0-9a-fA-F]+", upper):
            raise ValueError("v4_discovery_head_invalid")
        if timestamp_hint is None:
            matches = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                                 "fromBlock": "0x0", "toBlock": upper,
                                                 "topics": [INITIALIZE_TOPIC, pool_id]}])
        else:
            top = int(upper, 16)
            def first_at_or_after(timestamp: int) -> int:
                left, right = 0, top + 1
                while left < right:
                    middle = (left + right) // 2
                    block = rpc.call("eth_getBlockByNumber", [hex(middle), False])
                    if (not isinstance(block, Mapping)
                            or int(str(block.get("number") or "0x0"), 16) != middle
                            or not _HASH.fullmatch(str(block.get("hash") or ""))):
                        raise ValueError("v4_discovery_header_invalid")
                    if int(str(block.get("timestamp") or "0x0"), 16) < timestamp:
                        left = middle + 1
                    else:
                        right = middle
                return left
            lower = first_at_or_after(timestamp_hint - 2)
            end = min(top, first_at_or_after(timestamp_hint + 3) - 1)
            if lower > end or end - lower > 128:
                raise ValueError("v4_initialize_hint_window_invalid")
            matches = []
            for start in range(lower, end + 1, 3):
                result = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                                    "fromBlock": hex(start),
                                                    "toBlock": hex(min(start + 2, end)),
                                                    "topics": [INITIALIZE_TOPIC, pool_id]}])
                if not isinstance(result, list):
                    raise ValueError("v4_initialize_logs_invalid")
                matches.extend(result)
        if not isinstance(matches, list) or len(matches) != 1:
            raise ValueError("v4_initialize_not_unique_or_missing")
        candidate = decode_initialize(matches[0], chain_id=chain_id,
                                      expected_pool_id=pool_id, expected_token=token)
        header = rpc.call("eth_getBlockByNumber", [hex(candidate.block_height), False])
        if (not isinstance(header, Mapping)
                or str(header.get("hash") or "").lower() != candidate.block_hash):
            raise ValueError("v4_initialize_block_reorged")
        pinned = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                            "blockHash": candidate.block_hash,
                                            "topics": [INITIALIZE_TOPIC, pool_id]}])
        if (not isinstance(pinned, list) or len(pinned) != 1 or pinned[0] != matches[0]):
            raise ValueError("v4_initialize_pinned_log_mismatch")
    return candidate
