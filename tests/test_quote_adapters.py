from __future__ import annotations

import time
import unittest
from decimal import Decimal
from unittest.mock import patch

from fomo.execution.quote_adapters import JupiterQuoteAdapter, ZeroXQuoteAdapter
from fomo.signals.strategy import ExecutionIntent


class RouteFixture:
    def __init__(self, *, bad: bool = False):
        self.bad = bad
        self.calls: list[str] = []

    def request(self, method, url, *, params, payload, headers):
        self.calls.append(url)
        if "0x.org" in url:
            return {"liquidityAvailable": True, "issues": {"allowance": {"spender": "x"} if self.bad else None},
                    "sellToken": "0xinput", "buyToken": "0xoutput", "sellAmount": "10000000",
                    "buyAmount": "5000000", "minBuyAmount": "4500000", "estimatedPriceImpact": "0.5",
                    "transaction": {"to": "0xrouter", "data": "0x1234", "gas": "200000", "value": "0"}}
        if method == "GET":
            return {"inputMint": "Input", "outputMint": "Output", "inAmount": "10000000",
                    "outAmount": "5000000", "otherAmountThreshold": "4500000", "priceImpactPct": "0",
                    "routePlan": [{"swapInfo": {"ammKey": "program"}}]}
        return {"swapTransaction": "AQID", "lastValidBlockHeight": 123}


class QuoteAdapterTests(unittest.TestCase):
    def test_zero_x_firm_quote_and_allowance_fail_closed(self):
        intent = ExecutionIntent("i", "s", "fomo_push", "test", "x", "1", "buy",
                                 "0xinput", "0xoutput", Decimal("10"))
        fixture = RouteFixture()
        adapter = ZeroXQuoteAdapter(chain_id=1, wallet="0xwallet", input_decimals=6,
                                    input_usd_price=Decimal("1"), price_observed_at_ms=int(time.time() * 1000),
                                    transport=fixture)
        with patch.dict("os.environ", {"ZEROX_API_KEY": "test-only"}):
            self.assertFalse(adapter.self_check().ready)
            quote = adapter.quote(intent)[0]
            self.assertTrue(quote.firm)
            self.assertEqual(quote.minimum_output_amount, "4500000")
            self.assertTrue(adapter.self_check().ready)
            bad = ZeroXQuoteAdapter(chain_id=1, wallet="0xwallet", input_decimals=6,
                                    input_usd_price=Decimal("1"), price_observed_at_ms=int(time.time() * 1000),
                                    transport=RouteFixture(bad=True))
            with self.assertRaisesRegex(ValueError, "not_executable"):
                bad.quote(intent)

    def test_jupiter_quote_build_response_is_not_a_scope_proof(self):
        intent = ExecutionIntent("i", "s", "wallet_rpc_solana", "test", "x", "1399811149",
                                 "buy", "Input", "Output", Decimal("10"))
        fixture = RouteFixture()
        adapter = JupiterQuoteAdapter(wallet="wallet", input_decimals=6, input_usd_price=Decimal("1"),
                                      price_observed_at_ms=int(time.time() * 1000), transport=fixture)
        with patch.dict("os.environ", {"JUPITER_API_KEY": "test-only"}):
            quote = adapter.quote(intent)[0]
        self.assertTrue(quote.firm)
        assert quote.execution_payload is not None
        self.assertEqual(quote.execution_payload["lastValidBlockHeight"], 123)
        self.assertEqual(len(fixture.calls), 2)


if __name__ == "__main__":
    unittest.main()
