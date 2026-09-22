import unittest

from fomo.execution.v3_math import (MAX_SQRT_RATIO, MIN_SQRT_RATIO, Q96,
                                    quote_exact_input, required_input_for_output,
                                    spot_quote_per_base,
                                    sqrt_ratio_at_tick, tick_at_sqrt_ratio)


class V3MathTests(unittest.TestCase):
    def test_tick_math_official_boundaries_and_roundtrip(self):
        self.assertEqual(sqrt_ratio_at_tick(0), Q96)
        self.assertEqual(sqrt_ratio_at_tick(-887272), MIN_SQRT_RATIO)
        self.assertEqual(sqrt_ratio_at_tick(-887271), 4295343490)
        self.assertEqual(sqrt_ratio_at_tick(887271),
                         1461373636630004318706518188784493106690254656249)
        self.assertEqual(sqrt_ratio_at_tick(887272), MAX_SQRT_RATIO)
        for tick in (-200000, -1, 0, 1, 200000):
            self.assertEqual(tick_at_sqrt_ratio(sqrt_ratio_at_tick(tick)), tick)
        self.assertEqual(spot_quote_per_base(Q96, 6, 18), 10**12)

    def test_both_directions_consume_full_input_with_integer_fee(self):
        price = sqrt_ratio_at_tick(200000)
        for zero_for_one, amount in ((True, 10**6), (False, 10**15)):
            quote = quote_exact_input(
                amount_in=amount, zero_for_one=zero_for_one,
                sqrt_price_x96=price, tick=200000, liquidity=10**18,
                fee_pips=500, tick_spacing=10, bitmaps={77: 0, 78: 0, 79: 0},
                liquidity_nets={},
            )
            self.assertEqual(quote.amount_in, amount)
            self.assertGreater(quote.amount_out, 0)
            self.assertGreater(quote.fee_amount, 0)
            self.assertLess(quote.fee_amount, amount)

    def test_inverse_input_is_minimal_inside_known_coverage(self):
        arguments = dict(zero_for_one=True, sqrt_price_x96=sqrt_ratio_at_tick(200000),
                         tick=200000, liquidity=10**18, fee_pips=500,
                         tick_spacing=10, bitmaps={77: 0, 78: 0, 79: 0},
                         liquidity_nets={})
        desired = quote_exact_input(amount_in=10**6, **arguments).amount_out
        needed = required_input_for_output(desired_output=desired,
                                           maximum_input=2 * 10**6, **arguments)
        self.assertLessEqual(needed, 10**6)
        self.assertGreaterEqual(quote_exact_input(amount_in=needed, **arguments).amount_out, desired)
        if needed > 1:
            try:
                prior = quote_exact_input(amount_in=needed - 1, **arguments).amount_out
            except ValueError as error:
                self.assertEqual(str(error), "v3_quote_zero_output")
                prior = 0
            self.assertLess(prior, desired)

    def test_missing_bitmap_or_initialized_tick_fails_closed(self):
        price = sqrt_ratio_at_tick(5)
        with self.assertRaisesRegex(ValueError, "v3_bitmap_word_missing"):
            quote_exact_input(amount_in=10**6, zero_for_one=True, sqrt_price_x96=price,
                              tick=5, liquidity=10**18, fee_pips=500, tick_spacing=10,
                              bitmaps={}, liquidity_nets={})
        with self.assertRaisesRegex(ValueError, "v3_initialized_tick_missing"):
            quote_exact_input(amount_in=10**16, zero_for_one=True, sqrt_price_x96=price,
                              tick=5, liquidity=10**18, fee_pips=500, tick_spacing=10,
                              bitmaps={0: 1}, liquidity_nets={})

    def test_initialized_tick_crossing_changes_active_liquidity(self):
        quote = quote_exact_input(
            amount_in=10**16, zero_for_one=False,
            sqrt_price_x96=sqrt_ratio_at_tick(5), tick=5,
            liquidity=10**18, fee_pips=500, tick_spacing=10,
            bitmaps={0: 2}, liquidity_nets={10: 10**17},
        )
        self.assertEqual(quote.crossed_ticks, (10,))
        self.assertGreater(quote.amount_out, 0)

    def test_uncovered_size_and_direction_limit_cannot_return_partial_quote(self):
        price = sqrt_ratio_at_tick(5)
        with self.assertRaisesRegex(ValueError, "v3_bitmap_word_missing|v3_quote_partial"):
            quote_exact_input(amount_in=10**19, zero_for_one=True, sqrt_price_x96=price,
                              tick=5, liquidity=10**18, fee_pips=500, tick_spacing=10,
                              bitmaps={0: 0}, liquidity_nets={})
        with self.assertRaisesRegex(ValueError, "v3_quote_partial_or_no_liquidity"):
            quote_exact_input(amount_in=1, zero_for_one=True,
                              sqrt_price_x96=MIN_SQRT_RATIO + 1, tick=-887272,
                              liquidity=10**18, fee_pips=500, tick_spacing=10,
                              bitmaps={0: 0}, liquidity_nets={})


if __name__ == "__main__":
    unittest.main()
