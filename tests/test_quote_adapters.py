from __future__ import annotations

import time
import unittest
from decimal import Decimal
from unittest.mock import patch

from fomo.execution.evm_transaction import EvmEip1559Builder
from fomo.execution.market_evidence import PythHermesPriceAdapter
from fomo.execution.quote_adapters import JupiterQuoteAdapter, ZeroXQuoteAdapter
from fomo.signals.strategy import ExecutionIntent
from tests.test_zero_x_calldata import BUY, SELL, SETTLER, WALLET, allowance_holder_fixture


class RouteFixture:
    def __init__(self, *, bad: bool = False, recipient: str = WALLET,
                 expected_amount: str = "10000000"):
        self.bad = bad
        self.recipient = recipient
        self.expected_amount = expected_amount
        self.calls: list[str] = []
        self.amounts: list[str] = []

    def request(self, method, url, *, params, payload, headers):
        self.calls.append(url)
        if params and "amount" in params:
            self.amounts.append(params["amount"])
        if "0x.org" in url:
            return {"liquidityAvailable": True, "issues": {"allowance": {"spender": "x"} if self.bad else None},
                    "sellToken": SELL, "buyToken": BUY, "sellAmount": "10000000",
                    "buyAmount": "5000000", "minBuyAmount": "4500000", "estimatedPriceImpact": "0.5",
                    "transaction": {"to": "0x0000000000001ff3684f28c67538d4d072c22734",
                                    "data": "0x" + allowance_holder_fixture(recipient=self.recipient).hex(),
                                    "gas": "200000", "value": "0"}}
        if method == "GET":
            return {"inputMint": params["inputMint"], "outputMint": params["outputMint"],
                    "inAmount": self.expected_amount,
                    "outAmount": "5000000", "otherAmountThreshold": "4500000", "priceImpactPct": "0",
                    "routePlan": [{"swapInfo": {"ammKey": "program"}}]}
        return {"swapTransaction": "AQID", "lastValidBlockHeight": 123}


class SolPriceFixture:
    def request(self, method, url, *, params, payload, headers):
        assert params["ids[]"] == "0x" + "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"
        return {"parsed": [{"id": params["ids[]"], "price": {
            "price": "10000000000", "conf": "100000", "expo": -8,
            "publish_time": int(time.time()),
        }}]}


class QuoteAdapterTests(unittest.TestCase):
    def test_zero_x_firm_quote_and_allowance_fail_closed(self):
        intent = ExecutionIntent("i", "s", "fomo_push", "test", "x", "1", "buy",
                                 SELL, BUY, Decimal("10"))
        fixture = RouteFixture()
        adapter = ZeroXQuoteAdapter(chain_id=1, wallet=WALLET, input_decimals=6,
                                    input_usd_price=Decimal("1"), price_observed_at_ms=int(time.time() * 1000),
                                    transport=fixture)
        with patch.dict("os.environ", {"ZEROX_API_KEY": "test-only"}):
            self.assertFalse(adapter.self_check().ready)
            quote = adapter.quote(intent)[0]
            self.assertTrue(quote.firm)
            self.assertEqual(quote.minimum_output_amount, "4500000")
            self.assertIn(SETTLER, quote.route_targets)
            self.assertFalse(adapter.self_check().ready)
            builder = EvmEip1559Builder(1, WALLET,
                                        {"0x0000000000001ff3684f28c67538d4d072c22734"})
            with self.assertRaisesRegex(ValueError, "unsupported_swap_calldata"):
                builder.build(intent, quote, "1")
            bad = ZeroXQuoteAdapter(chain_id=1, wallet=WALLET, input_decimals=6,
                                    input_usd_price=Decimal("1"), price_observed_at_ms=int(time.time() * 1000),
                                    transport=RouteFixture(bad=True))
            with self.assertRaisesRegex(ValueError, "not_executable"):
                bad.quote(intent)
            wrong_recipient = ZeroXQuoteAdapter(chain_id=1, wallet=WALLET, input_decimals=6,
                                                input_usd_price=Decimal("1"),
                                                price_observed_at_ms=int(time.time() * 1000),
                                                transport=RouteFixture(recipient="0x" + "55" * 20))
            with self.assertRaisesRegex(ValueError, "scope_mismatch"):
                wrong_recipient.quote(intent)

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
        self.assertFalse(adapter.self_check().ready)

    def test_jupiter_quote_uses_bound_independent_sol_price(self):
        mint = "So11111111111111111111111111111111111111112"
        intent = ExecutionIntent("i", "s", "wallet_rpc_solana", "test", "x", "1399811149",
                                 "buy", mint, "Output", Decimal("10"))
        price_adapter = PythHermesPriceAdapter(
            chain_id=1399811149, asset="SOL",
            feed_id="ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
            transport=SolPriceFixture(),
        )
        route = RouteFixture(expected_amount="100000000")
        adapter = JupiterQuoteAdapter(wallet="wallet", input_decimals=9,
                                      price_adapter=price_adapter, transport=route)
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only", "JUPITER_API_KEY": "test-only"}):
            quoted = adapter.quote(intent)[0]
        self.assertEqual(route.amounts, ["100000000"])
        assert quoted.execution_payload is not None
        self.assertEqual(quoted.execution_payload["inputPriceProof"]["feedId"],
                         "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d")
        self.assertFalse(adapter.self_check().ready)  # Fixture transport cannot prove production readiness.
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only", "JUPITER_API_KEY": "test-only"}):
            with self.assertRaisesRegex(ValueError, "binding_unapproved"):
                adapter.quote(ExecutionIntent("j", "s", "wallet_rpc_solana", "test", "x", "1399811149",
                                              "buy", "unknown-mint", "Output", Decimal("10")))


if __name__ == "__main__":
    unittest.main()
