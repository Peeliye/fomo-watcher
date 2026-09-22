"""Exact-input Uniswap V3 single-pool integer math over a proven tick window.

The caller must supply every bitmap word traversed and every initialized tick
referenced by those words. No RPC, fallback quote, or partial fill is allowed.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

Q96 = 1 << 96
MAX_UINT256 = (1 << 256) - 1
MIN_TICK = -887272
MAX_TICK = 887272
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
FEE_DENOMINATOR = 1_000_000
_TICK_FACTORS = (
    0xfffcb933bd6fad37aa2d162d1a594001,
    0xfff97272373d413259a46990580e213a,
    0xfff2e50f5f656932ef12357cf3c7fdcc,
    0xffe5caca7e10e4e61c3624eaa0941cd0,
    0xffcb9843d60f6159c9db58835c926644,
    0xff973b41fa98c081472e6896dfb254c0,
    0xff2ea16466c96a3843ec78b326b52861,
    0xfe5dee046a99a2a811c461f1969c3053,
    0xfcbe86c7900a88aedcffc83b479aa3a4,
    0xf987a7253ac413176f2b074cf7815e54,
    0xf3392b0822b70005940c7a398e4b70f3,
    0xe7159475a2c29b7443b29c7fa6e889d9,
    0xd097f3bdfd2022b8845ad8f792aa5825,
    0xa9f746462d870fdf8a65dc1f90e061e5,
    0x70d869a156d2a1b890bb3df62baf32f7,
    0x31be135f97d08fd981231505542fcfa6,
    0x9aa508b5b7a84e1c677de54f3e99bc9,
    0x5d6af8dedb81196699c329225ee604,
    0x2216e584f5fa1ea926041bedfe98,
    0x48a170391f7dc42444e8fa2,
)


def ceil_div(numerator: int, denominator: int) -> int:
    if numerator < 0 or denominator <= 0:
        raise ValueError("v3_division_invalid")
    return (numerator + denominator - 1) // denominator


def sqrt_ratio_at_tick(tick: int) -> int:
    """Port of Uniswap V3 TickMath.getSqrtRatioAtTick, including rounding."""
    if not MIN_TICK <= tick <= MAX_TICK:
        raise ValueError("v3_tick_out_of_range")
    absolute = abs(tick)
    ratio = 1 << 128
    for bit, factor in enumerate(_TICK_FACTORS):
        if absolute & (1 << bit):
            ratio = ratio * factor >> 128
    if tick > 0:
        ratio = MAX_UINT256 // ratio
    return (ratio >> 32) + (ratio % (1 << 32) != 0)


def tick_at_sqrt_ratio(sqrt_price_x96: int) -> int:
    if not MIN_SQRT_RATIO <= sqrt_price_x96 < MAX_SQRT_RATIO:
        raise ValueError("v3_sqrt_price_out_of_range")
    lower, upper = MIN_TICK, MAX_TICK
    while lower < upper:
        middle = (lower + upper + 1) // 2
        if sqrt_ratio_at_tick(middle) <= sqrt_price_x96:
            lower = middle
        else:
            upper = middle - 1
    return lower


def amount0_delta(sqrt_a: int, sqrt_b: int, liquidity: int, *, round_up: bool) -> int:
    low, high = sorted((sqrt_a, sqrt_b))
    if low <= 0 or liquidity <= 0:
        raise ValueError("v3_liquidity_or_price_invalid")
    numerator = (liquidity << 96) * (high - low)
    denominator = high * low
    return ceil_div(numerator, denominator) if round_up else numerator // denominator


def amount1_delta(sqrt_a: int, sqrt_b: int, liquidity: int, *, round_up: bool) -> int:
    numerator = liquidity * abs(sqrt_b - sqrt_a)
    return ceil_div(numerator, Q96) if round_up else numerator // Q96


def _next_sqrt_from_input(sqrt_price: int, liquidity: int, amount_in: int, zero_for_one: bool) -> int:
    if zero_for_one:
        numerator = liquidity << 96
        return ceil_div(numerator * sqrt_price, numerator + amount_in * sqrt_price)
    return sqrt_price + amount_in * Q96 // liquidity


def _next_boundary(tick: int, zero_for_one: bool, spacing: int,
                   bitmaps: Mapping[int, int]) -> tuple[int, bool]:
    compressed = tick // spacing
    word = compressed >> 8
    bits = bitmaps.get(word)
    if bits is None or not 0 <= bits <= MAX_UINT256:
        raise ValueError("v3_bitmap_word_missing")
    position = compressed & 255
    if zero_for_one:
        masked = bits & ((1 << (position + 1)) - 1)
        next_compressed = (word << 8) + (masked.bit_length() - 1 if masked else 0)
        initialized = bool(masked)
    else:
        masked = bits >> (position + 1)
        if not masked and position == 255:
            word += 1
            bits = bitmaps.get(word)
            if bits is None or not 0 <= bits <= MAX_UINT256:
                raise ValueError("v3_bitmap_word_missing")
            masked = bits
            position = -1
        next_compressed = (word << 8) + (position + 1 + (masked & -masked).bit_length() - 1
                                         if masked else 255)
        initialized = bool(masked)
    return max(MIN_TICK, min(MAX_TICK, next_compressed * spacing)), initialized


@dataclass(frozen=True, slots=True)
class V3Quote:
    amount_in: int
    amount_out: int
    fee_amount: int
    final_sqrt_price_x96: int
    crossed_ticks: tuple[int, ...]
    block_hash: str = ""
    pool: str = ""


def quote_exact_input(*, amount_in: int, zero_for_one: bool, sqrt_price_x96: int,
                      tick: int, liquidity: int, fee_pips: int, tick_spacing: int,
                      bitmaps: Mapping[int, int], liquidity_nets: Mapping[int, int],
                      max_steps: int = 512) -> V3Quote:
    if (not 0 < amount_in < 2**256 or not MIN_SQRT_RATIO < sqrt_price_x96 < MAX_SQRT_RATIO
            or not MIN_TICK <= tick <= MAX_TICK or liquidity <= 0 or liquidity >= 2**128
            or not 0 < fee_pips < FEE_DENOMINATOR or tick_spacing <= 0
            or not 1 <= max_steps <= 4096):
        raise ValueError("v3_quote_inputs_invalid")
    if abs(tick_at_sqrt_ratio(sqrt_price_x96) - tick) > 1:
        raise ValueError("v3_slot0_tick_mismatch")
    remaining, output, fees = amount_in, 0, 0
    price, current_tick, active_liquidity = sqrt_price_x96, tick, liquidity
    crossed: list[int] = []
    limit = MIN_SQRT_RATIO + 1 if zero_for_one else MAX_SQRT_RATIO - 1
    for _ in range(max_steps):
        if remaining == 0:
            if output <= 0:
                raise ValueError("v3_quote_zero_output")
            return V3Quote(amount_in, output, fees, price, tuple(crossed))
        if price == limit or active_liquidity <= 0:
            raise ValueError("v3_quote_partial_or_no_liquidity")
        next_tick, initialized = _next_boundary(current_tick, zero_for_one, tick_spacing, bitmaps)
        boundary = sqrt_ratio_at_tick(next_tick)
        target = max(limit, boundary) if zero_for_one else min(limit, boundary)
        if (zero_for_one and target > price) or (not zero_for_one and target < price):
            raise ValueError("v3_quote_tick_direction_invalid")
        available = remaining * (FEE_DENOMINATOR - fee_pips) // FEE_DENOMINATOR
        to_target = (amount0_delta(target, price, active_liquidity, round_up=True) if zero_for_one
                     else amount1_delta(price, target, active_liquidity, round_up=True))
        reached = available >= to_target
        next_price = target if reached else _next_sqrt_from_input(price, active_liquidity, available, zero_for_one)
        if (zero_for_one and not target <= next_price <= price) or (not zero_for_one and not price <= next_price <= target):
            raise ValueError("v3_quote_price_step_invalid")
        used = (amount0_delta(next_price, price, active_liquidity, round_up=True) if zero_for_one
                else amount1_delta(price, next_price, active_liquidity, round_up=True))
        gained = (amount1_delta(next_price, price, active_liquidity, round_up=False) if zero_for_one
                  else amount0_delta(price, next_price, active_liquidity, round_up=False))
        fee = ceil_div(used * fee_pips, FEE_DENOMINATOR - fee_pips) if reached else remaining - used
        if used + fee > remaining:
            raise ValueError("v3_quote_rounding_overflow")
        remaining -= used + fee
        output += gained
        fees += fee
        if next_price == price and remaining > 0 and not (reached and next_price == boundary):
            raise ValueError("v3_quote_no_progress")
        price = next_price
        if price == boundary:
            if initialized:
                net = liquidity_nets.get(next_tick)
                if net is None or not -(2**127) <= net < 2**127:
                    raise ValueError("v3_initialized_tick_missing")
                active_liquidity += -net if zero_for_one else net
                if not 0 < active_liquidity < 2**128:
                    raise ValueError("v3_quote_partial_or_no_liquidity")
                crossed.append(next_tick)
            current_tick = next_tick - 1 if zero_for_one else next_tick + (0 if initialized else 1)
        else:
            current_tick = tick_at_sqrt_ratio(price)
    raise ValueError("v3_quote_step_limit")


def required_input_for_output(*, desired_output: int, maximum_input: int,
                              zero_for_one: bool, sqrt_price_x96: int, tick: int,
                              liquidity: int, fee_pips: int, tick_spacing: int,
                              bitmaps: Mapping[int, int], liquidity_nets: Mapping[int, int]) -> int:
    """Find minimum exact-input size within a fully covered caller-supplied cap."""
    if desired_output <= 0 or maximum_input <= 0:
        raise ValueError("v3_inverse_quote_inputs_invalid")
    def output_at(amount: int) -> int:
        return quote_exact_input(
            amount_in=amount, zero_for_one=zero_for_one,
            sqrt_price_x96=sqrt_price_x96, tick=tick, liquidity=liquidity,
            fee_pips=fee_pips, tick_spacing=tick_spacing,
            bitmaps=bitmaps, liquidity_nets=liquidity_nets,
        ).amount_out
    ceiling = output_at(maximum_input)
    if ceiling < desired_output:
        raise ValueError("v3_inverse_quote_uncovered")
    low, high = 1, maximum_input
    while low < high:
        middle = (low + high) // 2
        try:
            output = output_at(middle)
        except ValueError as error:
            if str(error) != "v3_quote_zero_output":
                raise
            output = 0
        if output >= desired_output:
            high = middle
        else:
            low = middle + 1
    return low


def spot_quote_per_base(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    """Token0 units per token1 unit, adjusted for verified token decimals."""
    if (not MIN_SQRT_RATIO < sqrt_price_x96 < MAX_SQRT_RATIO
            or not 0 <= decimals0 <= 30 or not 0 <= decimals1 <= 30):
        raise ValueError("v3_sqrt_price_out_of_range")
    return (Decimal(Q96 * Q96) / Decimal(sqrt_price_x96 * sqrt_price_x96)
            * Decimal(10**decimals1) / Decimal(10**decimals0))
