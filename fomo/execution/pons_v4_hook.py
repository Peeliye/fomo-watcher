"""Pool-bound evidence and afterSwap fee math for the reviewed Pons V2 hook."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .evm_transaction import keccak256

CHAIN_ID = 4663
WORM = "0x3ba500f1ababbcf0f0247d06d1c56fd7e6c4c09d"
PLTR = "0x894e1ec2d74ffe5aef8dc8a9e84686accb964f2a"
HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
POOL_ID = "0xf49ee5b66c4e7163026e5d25e6a1f93cff46dca4d3752f712d181d9392c1d567"
# Discovery hint only. The Initialize log, canonical header and PoolId are
# independently checked; a wrong hint yields no quote.
INITIALIZE_BLOCK_HINT = 57921072

# Operator-approved, pool-specific runtime pin. Never learn this from eth_getCode.
REVIEWED_RUNTIME_CODE_HASH = "0xc21b1e6c1b45403e81a581f22ed6d9c747997af1cfdac1b1dc9f4b1d346a10db"
EXPECTED_HOOK_FEE_BPS = 100
EXPECTED_CREATOR_TAX_BPS = 90

_HEX_CODE = re.compile(r"^0x(?:[0-9a-fA-F]{2})+$")


def is_target_key(chain_id: int, key: object) -> bool:
    return (chain_id == CHAIN_ID
            and getattr(key, "currency0", None) == WORM
            and getattr(key, "currency1", None) == PLTR
            and getattr(key, "fee", None) == 0
            and getattr(key, "tick_spacing", None) == 200
            and getattr(key, "hooks", None) == HOOK
            and getattr(key, "pool_id", None) == POOL_ID)


def verify_pool_key(chain_id: int, key: object, expected_pool_id: str) -> None:
    """Bind a supplied PoolKey to one caller-approved PoolId and reviewed hook."""
    if (chain_id != CHAIN_ID or getattr(key, "pool_id", None) != expected_pool_id
            or getattr(key, "hooks", None) != HOOK
            or getattr(key, "fee", None) != 0
            or getattr(key, "tick_spacing", None) != 200
            or not isinstance(getattr(key, "currency0", None), str)
            or not isinstance(getattr(key, "currency1", None), str)
            or not getattr(key, "currency0") < getattr(key, "currency1")):
        raise ValueError("pons_pool_key_mismatch")


def verify_runtime_code(code: object, expected_hash: str | None) -> str:
    if not isinstance(expected_hash, str) or not re.fullmatch(r"0x[0-9a-f]{64}", expected_hash):
        raise ValueError("pons_reviewed_bytecode_unpinned")
    if not isinstance(code, str) or not _HEX_CODE.fullmatch(code):
        raise ValueError("pons_hook_bytecode_missing")
    actual = "0x" + keccak256(bytes.fromhex(code[2:])).hex()
    if actual != expected_hash:
        raise ValueError("pons_hook_bytecode_mismatch")
    return actual


@dataclass(frozen=True, slots=True)
class PonsFeePolicy:
    hook_fee_bps: int
    creator_tax_bps: int
    registered: bool
    memecoin_is_currency0: bool
    memecoin: str
    quote_token: str
    creator: str
    buyback_creator_recipient: str
    protocol_fee_recipient: str
    protocol_fee_share_bps: int
    buyback_burn_bps: int
    max_internal_price_impact_bps: int
    buyback_enabled: bool

    @classmethod
    def from_launches_result(cls, raw: object, *, key: object,
                             expected_pool_id: str) -> "PonsFeePolicy":
        # Solidity's public mapping getter returns the 13 static LaunchInfo
        # fields in their declaration order. Require a full, canonical ABI.
        if not isinstance(raw, str) or not re.fullmatch(r"0x[0-9a-fA-F]{832}", raw):
            raise ValueError("pons_launch_info_missing")
        fields = [int(raw[2 + 64*i:2 + 64*(i+1)], 16) for i in range(13)]
        verify_pool_key(CHAIN_ID, key, expected_pool_id)
        currency0 = str(getattr(key, "currency0"))
        currency1 = str(getattr(key, "currency1"))
        # LaunchInfo stores memecoin and quote, not sorted PoolKey order.
        memecoin = currency0 if fields[1] == 1 else currency1
        quote = currency1 if fields[1] == 1 else currency0
        if (fields[0] != 1 or fields[1] not in (0, 1)
                or fields[2] != int(memecoin, 16) or fields[3] != int(quote, 16)
                or any(fields[index] >= 1 << 160 for index in range(2, 7))
                or any(fields[index] == 0 for index in (4, 5, 6))
                or any(fields[index] >= 1 << 16 for index in range(7, 12))
                or fields[12] not in (0, 1)
                or fields[7] > 1000 or fields[8] > 5000
                or fields[9] > 10_000 or fields[10] > 1000
                or not 0 < fields[11] < 10_000
                or fields[7] + fields[10] > 2000):
            raise ValueError("pons_launch_info_mismatch")
        def address(value: int) -> str:
            return f"0x{value:040x}"
        return cls(fields[10], fields[7], True, bool(fields[1]),
                   address(fields[2]), address(fields[3]), address(fields[4]),
                   address(fields[5]), address(fields[6]), fields[8], fields[9],
                   fields[11], bool(fields[12]))

    def fee_components(self, gross_output: int) -> tuple[int, int, int]:
        if not 0 < gross_output < 1 << 128:
            raise ValueError("pons_gross_output_invalid")
        hook_fee = gross_output * self.hook_fee_bps // 10_000
        creator_tax = gross_output * self.creator_tax_bps // 10_000
        net = gross_output - hook_fee - creator_tax
        if net <= 0:
            raise ValueError("pons_net_output_zero")
        return hook_fee, creator_tax, net

    def net_exact_input_output(self, gross_output: int) -> int:
        return self.fee_components(gross_output)[2]
