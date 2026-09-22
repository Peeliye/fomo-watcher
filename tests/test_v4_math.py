import unittest

from fomo.execution.v3_math import sqrt_ratio_at_tick
from fomo.execution.v4_math import directional_swap_fee, quote_single_interval


class V4MathTests(unittest.TestCase):
    def test_directional_protocol_fee_and_full_single_interval_quote(self):
        packed = 100 | (200 << 12)
        self.assertEqual(directional_swap_fee(lp_fee=500, packed_protocol_fee=packed,
                                              zero_for_one=True), 600)
        self.assertEqual(directional_swap_fee(lp_fee=500, packed_protocol_fee=packed,
                                              zero_for_one=False), 700)
        for direction, boundary in ((True, 0), (False, 10)):
            quote = quote_single_interval(amount_in=10**12, zero_for_one=direction,
                                          sqrt_price_x96=sqrt_ratio_at_tick(5), tick=5,
                                          liquidity=10**24, tick_spacing=10, lp_fee=500,
                                          protocol_fee=packed,
                                          nearest_initialized_tick=boundary)
            self.assertEqual(quote.amount_in, 10**12)
            self.assertGreater(quote.amount_out, 0)

    def test_missing_tick_and_oversized_input_fail_closed(self):
        common = dict(zero_for_one=True, sqrt_price_x96=sqrt_ratio_at_tick(5),
                      tick=5, liquidity=10**24, tick_spacing=10,
                      lp_fee=500, protocol_fee=0)
        with self.assertRaisesRegex(ValueError, "v4_tick_coverage_missing"):
            quote_single_interval(amount_in=1, nearest_initialized_tick=None, **common)
        with self.assertRaisesRegex(ValueError, "v4_quote_tick_crossing_or_partial"):
            quote_single_interval(amount_in=10**22, nearest_initialized_tick=0, **common)


if __name__ == "__main__":
    unittest.main()
