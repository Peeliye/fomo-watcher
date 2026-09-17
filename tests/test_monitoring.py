import unittest
from unittest.mock import Mock, patch

from fomo.monitoring import fetch_dexscreener_marks


class MonitoringTests(unittest.TestCase):
    @patch("fomo.monitoring.cf.get")
    def test_market_marks_require_matching_chain_and_choose_liquidity(self, get: Mock):
        response = Mock()
        response.json.return_value = {"pairs": [
            {"chainId": "base", "baseToken": {"address": "0xabc"}, "quoteToken": {},
             "priceUsd": "3", "liquidity": {"usd": 100}},
            {"chainId": "ethereum", "baseToken": {"address": "0xabc"}, "quoteToken": {},
             "priceUsd": "2", "liquidity": {"usd": 1000}},
            {"chainId": "ethereum", "baseToken": {"address": "0xabc"}, "quoteToken": {},
             "priceUsd": "1", "liquidity": {"usd": 10}},
        ]}
        response.raise_for_status.return_value = None
        get.return_value = response
        marks = fetch_dexscreener_marks([{"chainId": 1, "tokenAddress": "0xAbC"}], 2)
        self.assertEqual(len(marks), 1)
        self.assertEqual(marks[0]["priceUsd"], "2")
        self.assertEqual(marks[0]["source"], "dexscreener")


if __name__ == "__main__":
    unittest.main()
