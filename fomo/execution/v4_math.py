"""Bounded, exact-input Uniswap v4 no-hook quote for a single tick interval.

The integer swap step follows v4-core SwapMath. Crossing a tick is deliberately
out of scope for this first L0 probe: there is no quote, never a partial quote.
"""

from __future__ import annotations

from dataclasses import dataclass

from .v3_math import (MAX_SQRT_RATIO, MIN_SQRT_RATIO, Q96, amount0_delta,
                      amount1_delta, ceil_div, sqrt_ratio_at_tick,
                      tick_at_sqrt_ratio)

PIPS = 1_000_000


@dataclass(frozen=True, slots=True)
class V4Quote:
    amount_in: int
    amount_out: int
    swap_fee_pips: int
    final_sqrt_price_x96: int
    block_hash: str = ""
    pool_id: str = ""


def directional_swap_fee(*, lp_fee: int, packed_protocol_fee: int,
                         zero_for_one: bool) -> int:
    if not 0 <= lp_fee < PIPS or not 0 <= packed_protocol_fee < 1 << 24:
        raise ValueError("v4_fee_invalid")
    low, high = packed_protocol_fee & 0xfff, packed_protocol_fee >> 12
    if low > 1000 or high > 1000:
        raise ValueError("v4_protocol_fee_invalid")
    protocol = low if zero_for_one else high
    return protocol + lp_fee - protocol * lp_fee // PIPS


def quote_single_interval(*, amount_in: int, zero_for_one: bool,
                          sqrt_price_x96: int, tick: int, liquidity: int,
                          tick_spacing: int, lp_fee: int, protocol_fee: int,
                          nearest_initialized_tick: int | None) -> V4Quote:
    if (not 0 < amount_in < 1 << 128 or not MIN_SQRT_RATIO < sqrt_price_x96 < MAX_SQRT_RATIO
            or not 0 < liquidity < 1 << 128 or tick_spacing <= 0
            or abs(tick_at_sqrt_ratio(sqrt_price_x96) - tick) > 1):
        raise ValueError("v4_quote_state_invalid")
    if nearest_initialized_tick is None:
        raise ValueError("v4_tick_coverage_missing")
    if (nearest_initialized_tick % tick_spacing != 0
            or (zero_for_one and nearest_initialized_tick > tick)
            or (not zero_for_one and nearest_initialized_tick <= tick)):
        raise ValueError("v4_tick_boundary_invalid")
    fee = directional_swap_fee(lp_fee=lp_fee, packed_protocol_fee=protocol_fee,
                               zero_for_one=zero_for_one)
    available = amount_in * (PIPS - fee) // PIPS
    if available <= 0:
        raise ValueError("v4_quote_zero_output")
    boundary = sqrt_ratio_at_tick(nearest_initialized_tick)
    if zero_for_one:
        # v4 SqrtPriceMath.getNextSqrtPriceFromAmount0RoundingUp
        numerator = liquidity * Q96 * sqrt_price_x96
        denominator = liquidity * Q96 + available * sqrt_price_x96
        end = ceil_div(numerator, denominator)
        if end <= boundary or end <= MIN_SQRT_RATIO:
            raise ValueError("v4_quote_tick_crossing_or_partial")
        used = amount0_delta(end, sqrt_price_x96, liquidity, round_up=True)
        output = amount1_delta(end, sqrt_price_x96, liquidity, round_up=False)
    else:
        end = sqrt_price_x96 + available * Q96 // liquidity
        if end >= boundary or end >= MAX_SQRT_RATIO:
            raise ValueError("v4_quote_tick_crossing_or_partial")
        used = amount1_delta(sqrt_price_x96, end, liquidity, round_up=True)
        output = amount0_delta(sqrt_price_x96, end, liquidity, round_up=False)
    if used > available or output <= 0:
        raise ValueError("v4_quote_rounding_or_zero_output")
    return V4Quote(amount_in, output, fee, end)
