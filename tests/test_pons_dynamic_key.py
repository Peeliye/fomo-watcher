from __future__ import annotations

import time

import pytest

from fomo.execution.direct_v4 import V4PoolKey, ZERO
from fomo.execution.pons_v4_hook import HOOK
from fomo.execution.pons_v4_once import build_unsigned, decode_calldata, decode_unsigned
from fomo.execution.evm_transaction import decode_eip1559


def test_reviewed_pons_codec_binds_new_pool_key() -> None:
    token = "0x83a49b808f8d5e02cb2931cd2352988f498e5ba3"
    key = V4PoolKey(ZERO, token, 0, 200, HOOK)
    built = build_unsigned(amount_in=10**12, minimum_out=12345,
                           deadline=int(time.time()) + 120, nonce=3,
                           gas_limit=300000, priority_fee=1, maximum_fee=100,
                           key=key)
    parsed = decode_unsigned(built, key=key)
    assert parsed["poolId"] == key.pool_id
    assert parsed["tokenOut"] == token
    assert parsed["msgValue"] == 10**12
    fields, _ = decode_eip1559(built.serialized, signed=False)
    assert decode_calldata(fields.data, key=key)["minOut"] == 12345
    with pytest.raises(ValueError, match="pons_pool_key_mismatch"):
        decode_unsigned(built)


def test_unreviewed_hook_stays_rejected() -> None:
    key = V4PoolKey(ZERO, "0x83a49b808f8d5e02cb2931cd2352988f498e5ba3",
                    0, 200, "0x1111111111111111111111111111111111111111")
    with pytest.raises(ValueError, match="pons_pool_key_mismatch"):
        build_unsigned(amount_in=10**12, minimum_out=1,
                       deadline=int(time.time()) + 120, nonce=0,
                       gas_limit=300000, priority_fee=1, maximum_fee=100,
                       key=key)
