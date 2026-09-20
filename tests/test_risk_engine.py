import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fomo.risk.engine import ReadOnlyRiskEngine, RiskContext, UnifiedSignal, WalletRegistry


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 10, 0, 0, 1, tzinfo=timezone.utc)
SOL_WALLET = "11111111111111111111111111111111"


def policy():
    with (ROOT / "risk-policy.example.json").open("r", encoding="utf-8") as stream:
        return json.load(stream)


def registry(confidence=0.95, status="active", kol_id="kol-1"):
    return WalletRegistry.from_dict(
        {
            "version": 7,
            "wallets": [
                {
                    "kolId": kol_id,
                    "handle": "example",
                    "chainIds": ["1399811149"],
                    "address": SOL_WALLET,
                    "confidence": confidence,
                    "evidence": [],
                    "verifiedAt": "2026-09-01T00:00:00+00:00",
                    "expiresAt": "2026-10-01T00:00:00+00:00",
                    "status": status,
                }
            ],
        }
    )


def signal(**changes):
    data = {
        "signalId": "signal-1",
        "source": "solana_processed",
        "observedAt": "2026-09-10T00:00:00.900000+00:00",
        "sourceTimestamp": "2026-09-10T00:00:00.900000+00:00",
        "chainId": "1399811149",
        "kolId": "kol-1",
        "wallet": SOL_WALLET,
        "walletConfidence": 0.95,
        "originalTx": "signature-1",
        "side": "buy",
        "tokenIn": "So11111111111111111111111111111111111111112",
        "tokenOut": "token-mint",
        "estimatedUsd": 500,
        "decoder": "jupiter-swap@1",
        "rawPayloadHash": "hash",
    }
    data.update(changes)
    return UnifiedSignal.from_dict(data)


def complete_context():
    return RiskContext.from_dict(
        {
            "asset": {
                "liquidityUsd": 250000,
                "marketCapUsd": 1000000,
                "buyTaxBps": 0,
                "sellTaxBps": 0,
                "topHolderPercent": 8,
                "privilegesKnownSafe": True,
                "sellSimulationPassed": True,
                "routeAllowlisted": True,
            },
            "quotes": [
                {
                    "provider": "a",
                    "outputAmount": "1000000",
                    "priceImpactBps": 30,
                    "capturedAt": "2026-09-10T00:00:00.900000+00:00",
                },
                {
                    "provider": "b",
                    "outputAmount": "995000",
                    "priceImpactBps": 32,
                    "capturedAt": "2026-09-10T00:00:00.900000+00:00",
                },
            ],
            "exposure": {
                "perTokenUsd": 0,
                "perKolDailyUsd": 0,
                "globalDailyUsd": 0,
                "openPositions": 0,
                "chainExposurePercent": 0,
                "nativeGasReserveUsd": 100,
            },
            "health": {
                "clockOffsetMs": 5,
                "primaryStreamConnected": True,
                "whitelistFresh": True,
                "evmRpcBlockLag": 0,
                "solanaRpcSlotLag": 0,
                "circuitBreakerTripped": False,
            },
            "exitPlanReady": True,
            "transactionSimulationPassed": True,
        }
    )


class RiskEngineTests(unittest.TestCase):
    def engine(self, wallet_registry=None):
        return ReadOnlyRiskEngine(
            policy(), wallet_registry or registry(), now=lambda: NOW
        )

    def test_complete_safe_context_is_approved_for_shadow_only(self):
        decision = self.engine().evaluate(signal(), complete_context())
        self.assertEqual("approved_for_shadow", decision.outcome)
        self.assertTrue(decision.read_only)
        self.assertNotIn("live", decision.outcome)

    def test_missing_context_needs_data(self):
        decision = self.engine().evaluate(signal())
        self.assertEqual("needs_data", decision.outcome)
        reasons = {check.reason for check in decision.checks}
        self.assertIn("asset_snapshot_required", reasons)
        self.assertIn("independent_quotes_required", reasons)
        self.assertIn("exit_plan_required", reasons)

    def test_unknown_wallet_is_rejected(self):
        decision = self.engine(WalletRegistry(())).evaluate(signal(), complete_context())
        self.assertEqual("rejected", decision.outcome)
        self.assertIn("wallet_not_registered", {c.reason for c in decision.checks})

    def test_low_confidence_wallet_is_shadow_rejected(self):
        decision = self.engine(registry(confidence=0.5)).evaluate(signal(), complete_context())
        self.assertEqual("rejected", decision.outcome)
        self.assertIn("wallet_confidence_too_low", {c.reason for c in decision.checks})

    def test_stale_signal_is_rejected(self):
        old = (NOW - timedelta(seconds=2)).isoformat()
        decision = self.engine().evaluate(
            signal(observedAt=old, sourceTimestamp=old), complete_context()
        )
        self.assertEqual("rejected", decision.outcome)
        self.assertIn("signal_too_old", {c.reason for c in decision.checks})

    def test_duplicate_signal_is_rejected(self):
        engine = self.engine()
        first = engine.evaluate(signal(), complete_context())
        second = engine.evaluate(signal(), complete_context())
        self.assertEqual("approved_for_shadow", first.outcome)
        self.assertEqual("rejected", second.outcome)
        self.assertIn("duplicate_signal", {c.reason for c in second.checks})

    def test_quote_divergence_is_rejected(self):
        bad = RiskContext.from_dict(
            {
                "asset": {
                    "liquidityUsd": 250000,
                    "marketCapUsd": 1000000,
                    "buyTaxBps": 0,
                    "sellTaxBps": 0,
                    "topHolderPercent": 8,
                    "privilegesKnownSafe": True,
                    "sellSimulationPassed": True,
                    "routeAllowlisted": True,
                },
                "quotes": [
                    {"provider": "a", "outputAmount": 100, "priceImpactBps": 20, "capturedAt": NOW.isoformat()},
                    {"provider": "b", "outputAmount": 90, "priceImpactBps": 20, "capturedAt": NOW.isoformat()},
                ],
                "exposure": {
                    "nativeGasReserveUsd": 100,
                    "openPositions": 0,
                    "chainExposurePercent": 0,
                },
                "health": {
                    "primaryStreamConnected": True,
                    "whitelistFresh": True,
                },
                "exitPlanReady": True,
                "transactionSimulationPassed": True,
            }
        )
        self.assertEqual("rejected", self.engine().evaluate(signal(), bad).outcome)

    def test_ambiguous_wallet_ownership_fails_at_load(self):
        item = {
            "handle": "one",
            "chainIds": ["1399811149"],
            "address": SOL_WALLET,
            "confidence": 1,
            "evidence": [],
            "verifiedAt": "2026-09-01T00:00:00+00:00",
            "expiresAt": "2026-10-01T00:00:00+00:00",
            "status": "active",
        }
        with self.assertRaisesRegex(ValueError, "ambiguous wallet ownership"):
            WalletRegistry.from_dict(
                {"wallets": [{**item, "kolId": "kol-1"}, {**item, "kolId": "kol-2"}]}
            )


if __name__ == "__main__":
    unittest.main()
