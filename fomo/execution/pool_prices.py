"""Read-only in-process price cache fed by verified EVM V2 Sync logs.

The caller supplies logs received over WS. A log is never trusted merely because
it arrived on a subscription: the same block/log must be found through the
chain-identity-checked RPC view before it can enter the cache. No subscription
is started here and no transaction is signed or broadcast.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from fomo.watching.rpc_transport import FailoverJsonRpc, RpcTransport, rpc_view

from .capabilities import CapabilityStatus
from .evm_transaction import keccak256
from .market_evidence import PriceObservation


_SYNC_TOPIC = "0x" + keccak256(b"Sync(uint112,uint112)").hex()
_MAINNET_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
_MAINNET_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"


def _quantity(value: Any) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError("pool_rpc_quantity_invalid")
    return int(value, 16)


@dataclass(frozen=True, slots=True)
class EvmV2PoolBinding:
    chain_id: str
    pool_id: str
    native_token: str
    stable_token: str
    native_is_token0: bool
    native_decimals: int
    stable_decimals: int
    minimum_liquidity_usd: Decimal = Decimal("100000")
    confirmation_depth: int = 2

    def __post_init__(self) -> None:
        addresses = (self.pool_id, self.native_token, self.stable_token)
        if (self.chain_id != "1" or any(len(x) != 42 or not x.startswith("0x")
                                         or any(c not in "0123456789abcdefABCDEF" for c in x[2:])
                                         for x in addresses)
                or self.native_token.casefold() != _MAINNET_WETH
                or self.stable_token.casefold() != _MAINNET_USDC
                or self.native_decimals != 18 or self.stable_decimals != 6
                or not 0 <= self.native_decimals <= 30 or not 0 <= self.stable_decimals <= 30
                or self.minimum_liquidity_usd <= 0 or self.confirmation_depth < 1):
            raise ValueError("pool_binding_not_audited")


class EvmV2PoolPriceCache:
    """One approved WETH/stable pool; unknown chains/AMMs remain unavailable."""

    chain_id = "1"
    asset = "ETH"

    def __init__(self, *, binding: EvmV2PoolBinding, rpc: RpcTransport) -> None:
        if binding.chain_id != self.chain_id:
            raise ValueError("pool_binding_not_audited")
        self.binding = binding
        self.rpc = rpc
        self._lock = threading.RLock()
        self._latest: PriceObservation | None = None

    def ingest_sync_log(self, log: Mapping[str, Any], *, now_ms: int | None = None) -> PriceObservation:
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        pool = self.binding.pool_id.lower()
        if log.get("removed") is True:
            with self._lock:
                if (self._latest is not None and
                        str(log.get("blockHash") or "").lower() == self._latest.block_hash.lower()):
                    self._latest = None
            raise ValueError("pool_sync_reorged")
        if (str(log.get("address") or "").lower() != pool
                or log.get("topics") != [_SYNC_TOPIC]):
            raise ValueError("pool_sync_identity_invalid")
        raw = str(log.get("data") or "")
        if len(raw) != 130 or not raw.startswith("0x"):
            raise ValueError("pool_sync_reserves_invalid")
        try:
            reserves = (int(raw[2:66], 16), int(raw[66:130], 16))
            height = _quantity(log.get("blockNumber"))
            log_index = _quantity(log.get("logIndex"))
        except (TypeError, ValueError) as error:
            raise ValueError("pool_sync_reserves_invalid") from error
        if any(value <= 0 or value >= 2**112 for value in reserves):
            raise ValueError("pool_sync_reserves_invalid")
        block_hash = str(log.get("blockHash") or "")
        tx_hash = str(log.get("transactionHash") or "")
        if (len(block_hash) != 66 or len(tx_hash) != 66
                or any(c not in "0123456789abcdefABCDEF" for c in block_hash[2:] + tx_hash[2:])):
            raise ValueError("pool_sync_identity_invalid")
        with rpc_view(self.rpc):
            block = self.rpc.call("eth_getBlockByNumber", [hex(height), False])
            head = self.rpc.call("eth_blockNumber", [])
            matches = self.rpc.call("eth_getLogs", [{"blockHash": block_hash,
                                                      "address": self.binding.pool_id,
                                                      "topics": [_SYNC_TOPIC]}])
            token0 = self.rpc.call("eth_call", [{"to": self.binding.pool_id,
                                                  "data": "0x0dfe1681"}, hex(height)])
            token1 = self.rpc.call("eth_call", [{"to": self.binding.pool_id,
                                                  "data": "0xd21220a7"}, hex(height)])
            get_pair = ("0xe6a43905" + self.binding.native_token[2:].lower().rjust(64, "0")
                        + self.binding.stable_token[2:].lower().rjust(64, "0"))
            factory_pair = self.rpc.call("eth_call", [{"to": _UNISWAP_V2_FACTORY,
                                                        "data": get_pair}, hex(height)])
            onchain_reserves = self.rpc.call("eth_call", [{"to": self.binding.pool_id,
                                                            "data": "0x0902f1ac"}, hex(height)])
            check = self.rpc.call("eth_getBlockByNumber", [hex(height), False])
        expected0 = self.binding.native_token if self.binding.native_is_token0 else self.binding.stable_token
        expected1 = self.binding.stable_token if self.binding.native_is_token0 else self.binding.native_token
        if (not isinstance(block, Mapping) or not isinstance(check, Mapping)
                or str(block.get("hash") or "").lower() != block_hash.lower()
                or str(check.get("hash") or "").lower() != block_hash.lower()
                or not isinstance(token0, str) or token0.lower() != "0x" + expected0[2:].lower().rjust(64, "0")
                or not isinstance(token1, str) or token1.lower() != "0x" + expected1[2:].lower().rjust(64, "0")
                or not isinstance(factory_pair, str)
                or factory_pair.lower() != "0x" + self.binding.pool_id[2:].lower().rjust(64, "0")
                or not isinstance(onchain_reserves, str) or len(onchain_reserves) != 194
                or not onchain_reserves.startswith("0x")
                or int(onchain_reserves[2:66], 16) != reserves[0]
                or int(onchain_reserves[66:130], 16) != reserves[1]
                or _quantity(head) - height < self.binding.confirmation_depth
                or not isinstance(matches, list)
                or not matches or not isinstance(matches[-1], Mapping)
                or matches[-1] != log or _quantity(matches[-1].get("logIndex")) != log_index
                or str(matches[-1].get("transactionHash") or "").lower() != tx_hash.lower()):
            raise ValueError("pool_sync_chain_proof_invalid")
        published = _quantity(block.get("timestamp")) * 1000
        native = reserves[0 if self.binding.native_is_token0 else 1]
        stable = reserves[1 if self.binding.native_is_token0 else 0]
        price = (Decimal(stable) / Decimal(10**self.binding.stable_decimals)
                 / (Decimal(native) / Decimal(10**self.binding.native_decimals)))
        liquidity = Decimal(stable) / Decimal(10**self.binding.stable_decimals) * 2
        if liquidity < self.binding.minimum_liquidity_usd:
            raise ValueError("pool_liquidity_below_floor")
        canonical = json.dumps(dict(log), sort_keys=True, separators=(",", ":")).encode()
        observation = PriceObservation(
            self.chain_id, self.asset, price, Decimal(0), pool, published, current,
            hashlib.sha256(canonical).hexdigest(), "evm_v2_verified_pool", pool,
            block_hash, height, liquidity,
        )
        observation.require_fresh(now_ms=current)
        with self._lock:
            if self._latest is None or height >= self._latest.block_height:
                self._latest = observation
        return observation

    def latest(self, *, now_ms: int | None = None) -> PriceObservation:
        with self._lock:
            observation = self._latest
        if observation is None:
            raise ValueError("pool_price_stream_empty")
        observation.require_fresh(now_ms=now_ms)
        return observation

    def self_check(self) -> CapabilityStatus:
        if type(self.rpc) is not FailoverJsonRpc or not self.rpc.uses_network_transport:
            return CapabilityStatus("independent_price", True, False, "pool_rpc_not_audited", {})
        try:
            observation = self.latest()
        except ValueError:
            return CapabilityStatus("independent_price", True, False, "pool_price_stale_or_missing", {})
        return CapabilityStatus("independent_price", True, True, "ok", {
            "source": observation.source, "chainId": observation.chain_id,
        })
