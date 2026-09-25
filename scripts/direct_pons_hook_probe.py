"""Robinhood WORM/PLTR hook evidence only: no quote, codec, or simulation."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv

from fomo.execution.direct_v4 import (CHAIN_CONFIGS, V4PoolKey, _call, _word)
from fomo.execution.evm_transaction import keccak256
from fomo.execution.pons_v4_hook import (CHAIN_ID, HOOK, PLTR, POOL_ID, WORM,
                                         PonsFeePolicy)
from fomo.execution import pons_v4_hook
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.execution.v4_discovery import INITIALIZE_TOPIC, decode_initialize
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcTransport, rpc_view
from scripts._robinhood_probe_transport import (RobinhoodDiscoveryRpc,
                                                proxy_configured)

_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_CODE = re.compile(r"^0x(?:[0-9a-fA-F]{2})+$")


def collect_evidence(rpc: RpcTransport) -> dict[str, Any]:
    """Pin all current-state calls to one numbered block and recheck its hash."""
    if int(str(rpc.call("eth_chainId", [])), 16) != CHAIN_ID:
        raise ValueError("pons_chain_id_mismatch")
    config = CHAIN_CONFIGS[CHAIN_ID]
    with rpc_view(rpc):
        height = int(str(rpc.call("eth_blockNumber", [])), 16)
        tag = hex(height)
        header = rpc.call("eth_getBlockByNumber", [tag, False])
        if (not isinstance(header, Mapping)
                or int(str(header.get("number") or "0x0"), 16) != height
                or not _HASH.fullmatch(str(header.get("hash") or ""))):
            raise ValueError("pons_header_invalid")
        block_hash = str(header["hash"]).lower()
        if pons_v4_hook.INITIALIZE_BLOCK_HINT > height:
            raise ValueError("pons_initialize_after_snapshot")
        matches = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                             "fromBlock": hex(pons_v4_hook.INITIALIZE_BLOCK_HINT),
                                             "toBlock": hex(pons_v4_hook.INITIALIZE_BLOCK_HINT),
                                             "topics": [INITIALIZE_TOPIC, POOL_ID]}])
        if not isinstance(matches, list) or len(matches) != 1:
            raise ValueError("pons_initialize_missing_or_ambiguous")
        found = decode_initialize(matches[0], chain_id=CHAIN_ID,
                                  expected_pool_id=POOL_ID, expected_token=WORM)
        expected = V4PoolKey(WORM, PLTR, 0, 200, HOOK)
        if (not found.identity_verified or found.key != expected
                or found.key.pool_id != POOL_ID or found.block_height > height):
            raise ValueError("pons_initialize_identity_mismatch")
        event_header = rpc.call("eth_getBlockByNumber", [hex(found.block_height), False])
        if (not isinstance(event_header, Mapping)
                or str(event_header.get("hash") or "").lower() != found.block_hash):
            raise ValueError("pons_initialize_block_reorged")
        pinned = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                           "blockHash": found.block_hash,
                                           "topics": [INITIALIZE_TOPIC, POOL_ID]}])
        if not isinstance(pinned, list) or len(pinned) != 1 or pinned[0] != matches[0]:
            raise ValueError("pons_initialize_log_mismatch")
        code = rpc.call("eth_getCode", [HOOK, tag])
        if not isinstance(code, str) or not _CODE.fullmatch(code):
            raise ValueError("pons_hook_bytecode_missing")
        code_hash = "0x" + keccak256(bytes.fromhex(code[2:])).hex()
        manager = _word(_call(rpc, HOOK, "poolManager()", tag))
        if manager != int(config.pool_manager, 16):
            raise ValueError("pons_hook_manager_mismatch")
        policy = PonsFeePolicy.from_launches_result(
            _call(rpc, HOOK, "launches(bytes32)", tag, int(POOL_ID, 16)),
            key=found.key, expected_pool_id=POOL_ID)
        check = rpc.call("eth_getBlockByNumber", [tag, False])
        if not isinstance(check, Mapping) or str(check.get("hash") or "").lower() != block_hash:
            raise ValueError("pons_block_reorged")
    return {"chainId": CHAIN_ID, "block": height, "blockHash": block_hash,
            "poolKey": {"currency0": found.key.currency0,
                        "currency1": found.key.currency1, "fee": found.key.fee,
                        "tickSpacing": found.key.tick_spacing,
                        "hooks": found.key.hooks},
            "poolId": found.key.pool_id, "hook": HOOK,
            "hookCodeKeccak256": code_hash,
            "hookFeeBps": {"read": True, "value": policy.hook_fee_bps},
            "creatorTaxBps": {"read": True, "value": policy.creator_tax_bps},
            "quoteRejected": True,
            "rejectReason": "pons_evidence_only_no_quote",
            "encoding": False, "simulationVerified": False,
            "tradingReady": False}


def main(argv: list[str] | None = None) -> int:
    if argv:
        raise ValueError("pons_probe_arguments_disabled")
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    if not os.getenv("RPC_ROBINHOOD_URL", "").strip():
        print(json.dumps({"status": "未注入", "tradingReady": False}, ensure_ascii=False))
        return 2
    started = time.monotonic()
    rpc = None
    try:
        endpoint = RpcEndpoint("4663", "pons-hook-l0", "primary",
                               http_env="RPC_ROBINHOOD_URL")
        rpc = (RobinhoodDiscoveryRpc(endpoint, timeout_seconds=20.0) if proxy_configured() else
               FailoverJsonRpc("4663", [endpoint]))
        result = collect_evidence(rpc)
        result["elapsedMs"] = round((time.monotonic() - started) * 1000)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as error:
        reason = str(error)
        print(json.dumps({"chainId": CHAIN_ID, "quoteRejected": True,
                          "rejectReason": (reason if reason.startswith(("pons_", "v4_")) else
                                           "pons_read_unavailable"),
                          "errorType": type(error).__name__,
                          "rpcErrorClass": (reason if reason.startswith("robinhood_probe_")
                                            else None),
                          "method": getattr(rpc, "last_request_method", None),
                          "hookFeeBps": {"read": False},
                          "creatorTaxBps": {"read": False},
                          "encoding": False, "simulationVerified": False,
                          "tradingReady": False,
                          "elapsedMs": round((time.monotonic() - started) * 1000)},
                         ensure_ascii=False))
        return 1


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
