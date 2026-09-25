from __future__ import annotations

from typing import Any

import pytest

from fomo.execution.pons_v4_dynamic import candidate_key, has_reviewed_launch, simulate_buy

TOKEN = "0x83a49b808f8d5e02cb2931cd2352988f498e5ba3"


class MissingLaunchRpc:
    def __init__(self) -> None:
        self.methods: list[str] = []

    def call(self, method: str, params: list[Any]) -> Any:
        self.methods.append(method)
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_getBlockByNumber":
            return {"number": "0xf", "hash": "0x" + "1" * 64}
        if method == "eth_call":
            return "0x" + "0" * 832
        raise AssertionError(method)


def test_missing_launch_never_reaches_wallet_or_simulation() -> None:
    rpc = MissingLaunchRpc()
    assert simulate_buy(rpc, token_out=TOKEN, amount_in_wei=10**12, slippage_bps=100) is None
    assert "eth_estimateGas" not in rpc.methods
    assert "eth_sendRawTransaction" not in rpc.methods
    assert "eth_getTransactionCount" not in rpc.methods


def test_unreviewed_or_malformed_launch_fails_closed() -> None:
    key = candidate_key(TOKEN)
    class BadLaunchRpc:
        def call(self, _method: str, _params: list[Any]) -> str:
            return "0x" + "2".rjust(64, "0") + "0" * 768
    with pytest.raises(ValueError, match="pons_dynamic_launch_response_invalid"):
        has_reviewed_launch(BadLaunchRpc(), key=key, block_tag="0xf")
    with pytest.raises(ValueError, match="pons_dynamic_token_invalid"):
        candidate_key("not-an-address")
