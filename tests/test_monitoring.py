import unittest
from unittest.mock import Mock, patch

from fomo.monitoring import fetch_dexscreener_marks


class MonitoringTests(unittest.TestCase):
    @patch("fomo.monitoring.cf.get")
    def test_market_marks_require_matching_chain_and_choose_liquidity(self, get: Mock):
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        response = Mock()
        response.json.return_value = {"pairs": [
            {"chainId": "base", "baseToken": {"address": "0xabc"},
             "quoteToken": {"address": weth},
             "priceUsd": "3", "liquidity": {"usd": 100}},
            {"chainId": "ethereum", "baseToken": {"address": "0xabc"},
             "quoteToken": {"address": weth},
             "priceUsd": "2", "liquidity": {"usd": 1000}},
            {"chainId": "ethereum", "baseToken": {"address": "0xabc"},
             "quoteToken": {"address": weth},
             "priceUsd": "1", "liquidity": {"usd": 10}},
        ]}
        response.raise_for_status.return_value = None
        get.return_value = response
        marks = fetch_dexscreener_marks([{"chainId": 1, "tokenAddress": "0xAbC"}], 2)
        self.assertEqual(len(marks), 1)
        self.assertEqual(marks[0]["priceUsd"], "2")
        self.assertEqual(marks[0]["source"], "dexscreener")

    @patch("fomo.monitoring.cf.get")
    def test_market_marks_reject_manipulated_untrusted_quote(self, get: Mock):
        token = "JE3HT7SbCgXDQWV6xp3oiiAisDzq4HyZ8wyEVBDCs45Z"
        wsol = "So11111111111111111111111111111111111111112"
        response = Mock()
        response.json.return_value = {"pairs": [
            {"chainId": "solana", "baseToken": {"address": token},
             "quoteToken": {"address": "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"},
             "priceUsd": "5.33", "liquidity": {"usd": 549_201_059}},
            {"chainId": "solana", "baseToken": {"address": token},
             "quoteToken": {"address": wsol}, "pairAddress": "trusted-pair",
             "priceUsd": "0.001134", "liquidity": {"usd": 2_989}},
        ]}
        response.raise_for_status.return_value = None
        get.return_value = response
        marks = fetch_dexscreener_marks([{
            "chainId": 1399811149, "tokenAddress": token,
            "referencePriceUsd": 0.000837959,
        }], 2)
        self.assertEqual(len(marks), 1)
        self.assertEqual(marks[0]["priceUsd"], "0.001134")
        self.assertEqual(marks[0]["pairAddress"], "trusted-pair")
        self.assertEqual(marks[0]["quoteTokenAddress"], wsol)

    @patch("fomo.monitoring.cf.get")
    def test_market_marks_fail_closed_on_implausible_jump(self, get: Mock):
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        response = Mock()
        response.json.return_value = {"pairs": [{
            "chainId": "ethereum", "baseToken": {"address": "0xabc"},
            "quoteToken": {"address": weth}, "priceUsd": "100",
            "liquidity": {"usd": 1_000_000},
        }]}
        response.raise_for_status.return_value = None
        get.return_value = response
        marks = fetch_dexscreener_marks([{
            "chainId": 1, "tokenAddress": "0xabc", "referencePriceUsd": 1,
        }], 2)
        self.assertEqual(marks, [])

    @patch("fomo.monitoring.cf.get")
    def test_robinhood_usdg_is_a_trusted_quote(self, get: Mock):
        usdg = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
        response = Mock()
        response.json.return_value = {"pairs": [{
            "chainId": "robinhood", "baseToken": {"address": "0xabc"},
            "quoteToken": {"address": usdg}, "priceUsd": "0.00009613",
            "liquidity": {"usd": 448_087},
        }]}
        response.raise_for_status.return_value = None
        get.return_value = response
        marks = fetch_dexscreener_marks([{
            "chainId": 4663, "tokenAddress": "0xabc", "referencePriceUsd": 0.00009736,
        }], 2)
        self.assertEqual(len(marks), 1)
        self.assertEqual(marks[0]["priceUsd"], "0.00009613")
        self.assertEqual(marks[0]["quoteTokenAddress"], usdg)


if __name__ == "__main__":
    unittest.main()
