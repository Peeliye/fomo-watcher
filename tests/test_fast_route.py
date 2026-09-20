import unittest
import os
from unittest.mock import patch
from decimal import Decimal

from fomo.execution.routing import FastRoutePlanner, RouteQuote, route_readiness


POLICY = {"quote_deadline_ms": {"evm": 200, "solana": 160}, "maximum_quote_age_ms": 500, "maximum_price_impact_bps": 100, "maximum_output_sacrifice_bps": 30, "minimum_independent_routes": 2}


def quote(provider, output, latency, submit=20, **changes):
    values = {"provider": provider, "chain_id": "1", "output_amount": Decimal(output), "quote_latency_ms": Decimal(latency), "submit_p95_ms": Decimal(submit), "quote_age_ms": Decimal("20"), "price_impact_bps": Decimal("20"), "simulation_passed": True}
    values.update(changes)
    return RouteQuote(**values)


class FastRoutePlannerTests(unittest.TestCase):
    def test_fastest_route_wins_only_inside_price_guard(self):
        planner = FastRoutePlanner(POLICY)
        decision = planner.select(1, [quote("fast_bad_price", "99", 20), quote("fast_safe", "99.8", 35), quote("slow_best", "100", 120)])
        self.assertEqual(decision.selected_provider, "fast_safe")
        self.assertEqual(decision.reason, "fastest_within_price_guard")

    def test_cross_chain_stale_and_unsimulated_routes_fail_closed(self):
        planner = FastRoutePlanner(POLICY)
        decision = planner.select(1, [quote("wrong_chain", "100", 20, chain_id="56"), quote("stale", "100", 20, quote_age_ms=Decimal("900")), quote("unsafe", "100", 20, simulation_passed=False)])
        self.assertEqual(decision.status, "blocked")
        self.assertEqual(decision.reason, "independent_routes_unavailable")

    def test_requires_two_independent_routes(self):
        decision = FastRoutePlanner(POLICY).select(1, [quote("only", "100", 20)])
        self.assertEqual(decision.status, "blocked")

    def test_speed_mode_accepts_unsimulated_but_scoped_allowlisted_route(self):
        policy = {**POLICY, "minimum_independent_routes": 1, "require_simulation": False,
                  "require_allowlisted_route": True, "require_scope_validation": True,
                  "require_minimum_output": True}
        decision = FastRoutePlanner(policy).select(1, [
            quote("primary", "100", 20, simulation_passed=False,
                  route_allowlisted=True, scope_validated=True,
                  minimum_output_amount=Decimal("98"))
        ])
        self.assertEqual(decision.status, "selected_for_shadow")

    def test_speed_mode_still_rejects_unsafe_transaction_scope(self):
        policy = {**POLICY, "minimum_independent_routes": 1, "require_simulation": False,
                  "require_scope_validation": True}
        decision = FastRoutePlanner(policy).select(1, [
            quote("primary", "100", 20, simulation_passed=False, scope_validated=False)
        ])
        self.assertEqual(decision.status, "blocked")
        self.assertIn("transaction_scope_invalid", decision.rejected["primary"])

    def test_provider_configuration_does_not_imply_adapter_is_implemented(self):
        cfg = {"routing": {"mode": "live", "providers": {"1": [
            {"name": "aggregator", "api_key_env": "QUOTE_KEY", "adapter_env": "ROUTE_ENABLED"}
        ]}}}
        with patch.dict(os.environ, {"QUOTE_KEY": "secret", "ROUTE_ENABLED": "false"}, clear=False):
            provider = route_readiness(cfg)["chains"][0]["providers"][0]
        self.assertTrue(provider["apiKeyConfigured"])
        self.assertFalse(provider["implemented"])
        self.assertFalse(provider["ready"])


if __name__ == "__main__":
    unittest.main()
