from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from fomo.execution.rh_auto_routes import simulate_zero_hook

TOKEN = "0x314ad0f11422842d28b4f950a64cd40fafb029fd"
BLOCK_HASH = "0x" + "2" * 64


class Rpc:
    wallet_profile = ("0x4ccb77f12801ee8853a9cc3782828678c8f5584b", {})

    def call(self, method: str, params: object) -> object:
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getTransactionCount":
            return "0x2"
        if method == "eth_getBalance":
            return hex(10**18)
        if method == "eth_getBlockByNumber":
            return {"hash": BLOCK_HASH}
        raise AssertionError((method, params))


def _snapshot(key: object) -> SimpleNamespace:
    return SimpleNamespace(key=key, block_height=100, block_hash=BLOCK_HASH,
                           pool_id="0x" + "3" * 64,
                           quote=lambda **_: SimpleNamespace(amount_out=10**12))


def test_zero_hook_missing_and_ambiguous_fail_closed() -> None:
    class Missing:
        def __init__(self, *, key: object, **_: object) -> None:
            self.key = key

        def snapshot(self) -> SimpleNamespace:
            raise ValueError("v4_pool_uninitialized_or_fee_mismatch")

    with patch("fomo.execution.rh_auto_routes.UniswapV4PoolReader", Missing):
        assert simulate_zero_hook(Rpc(), token_out=TOKEN, amount_in_wei=10**12,
                                  slippage_bps=100) is None

    class TwoPools(Missing):
        def snapshot(self) -> SimpleNamespace:
            if self.key.fee in (100, 500):
                return _snapshot(self.key)
            return super().snapshot()

    with patch("fomo.execution.rh_auto_routes.UniswapV4PoolReader", TwoPools):
        with pytest.raises(ValueError, match="rh_auto_ambiguous_zero_hook_pools"):
            simulate_zero_hook(Rpc(), token_out=TOKEN, amount_in_wei=10**12,
                               slippage_bps=100)


def test_zero_hook_final_transaction_roundtrip_and_simulation() -> None:
    class OnePool:
        def __init__(self, *, key: object, **_: object) -> None:
            self.key = key

        def snapshot(self) -> SimpleNamespace:
            if self.key.fee == 3000:
                return _snapshot(self.key)
            raise ValueError("v4_pool_uninitialized_or_fee_mismatch")

    with (patch("fomo.execution.rh_auto_routes.UniswapV4PoolReader", OnePool),
          patch("scripts.uniswap_v3_swap_once._fees", return_value=(1, 2)),
          patch("scripts.uniswap_swap_once._simulate", return_value=150000) as simulate):
        result = simulate_zero_hook(Rpc(), token_out=TOKEN, amount_in_wei=10**12,
                                    slippage_bps=100)
    assert result is not None
    assert result["protocol"] == "uniswap_v4_zero_hook"
    assert result["minOut"] == str(10**12 * 99 // 100)
    assert result["simulation_success"] is True
    assert simulate.call_count == 2
