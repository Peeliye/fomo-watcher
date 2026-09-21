from __future__ import annotations

import json
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from fomo.execution.capabilities import CapabilityRegistry, CapabilityStatus, FakeSignerCapability
from fomo.execution.core import PreflightEvidence, SharedExecutionCore, validate_serialized_transaction
from fomo.execution.journal import ExecutionJournal
from fomo.execution.readiness import execution_readiness
from fomo.signals.strategy import ExecutionIntent


def intent(name: str, source: str = "fomo_push", amount: str = "7") -> ExecutionIntent:
    return ExecutionIntent(name, f"signal:{name}", source, "test", f"allocation:{name}", "1", "buy",
                           "USDC", "TOKEN", Decimal(amount))


class JsonParser:
    def self_check(self):
        return CapabilityStatus("transaction_parser", False, False, "test_only", {})

    def parse(self, serialized_transaction: bytes):
        return json.loads(serialized_transaction)


class ReadyCapability:
    def __init__(self, name: str):
        self.name = name

    def self_check(self):
        return CapabilityStatus(self.name, True, True, "ok", {})


class BrokenCapability:
    def self_check(self):
        raise RuntimeError("provider offline")


class ExecutionCoreTests(unittest.TestCase):
    def test_fake_signer_and_missing_adapters_are_never_ready(self):
        registry = CapabilityRegistry()
        registry.register("signer", FakeSignerCapability())
        status = registry.status()
        self.assertFalse(status["ready"])
        self.assertFalse(status["capabilities"]["signer"]["implemented"])
        self.assertEqual(status["capabilities"]["broadcaster"]["reason"], "adapter_not_registered")

    def test_capability_self_check_exception_fails_closed(self):
        registry = CapabilityRegistry()
        registry.register("quote_adapter", BrokenCapability())
        result = registry.status()
        self.assertFalse(result["ready"])
        self.assertEqual(result["capabilities"]["quote_adapter"]["reason"], "self_check_failed")

    def test_ready_signer_and_broadcaster_do_not_hide_missing_capabilities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "wallet.json").write_text(json.dumps({
                "walletId": "test", "mode": "live", "accounts": [{
                    "accountId": "evm", "family": "evm", "address": "0x1111111111111111111111111111111111111111",
                    "chainIds": ["1"],
                }],
            }), encoding="utf-8")
            cfg = {"execution": {"wallet_profile": "wallet.json", "enabled_chain_ids": [1]}}
            registry = CapabilityRegistry()
            registry.register("signer", ReadyCapability("signer"))
            registry.register("broadcaster", ReadyCapability("broadcaster"))
            with patch("fomo.execution.readiness.rpc_pool_readiness", return_value={
                "chains": {"1": [{"httpConfigured": True}]},
            }), patch("fomo.execution.readiness.route_readiness", return_value={
                "mode": "live", "minimumIndependentRoutes": 1,
                "chains": [{"chainId": 1, "providers": [{"ready": True}]}],
            }):
                result = execution_readiness(root, cfg, registry)
            self.assertEqual(result["stage"], "capabilities")
            self.assertFalse(result["ready"])
            self.assertIn("execution_capabilities_required", result["blockers"])

    def test_fake_all_ready_capabilities_cannot_override_persistent_live_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "wallet.json").write_text(json.dumps({
                "walletId": "test", "mode": "live", "accounts": [{
                    "accountId": "evm", "family": "evm", "address": "0x1111111111111111111111111111111111111111",
                    "chainIds": ["1"],
                }],
            }), encoding="utf-8")
            cfg = {"execution": {"wallet_profile": "wallet.json", "enabled_chain_ids": [1]}}
            registry = CapabilityRegistry()
            for name in CapabilityRegistry.REQUIRED:
                registry.register(name, ReadyCapability(name))
            with patch("fomo.execution.readiness.rpc_pool_readiness", return_value={
                "chains": {"1": [{"httpConfigured": True}]},
            }), patch("fomo.execution.readiness.route_readiness", return_value={
                "mode": "live", "minimumIndependentRoutes": 1,
                "chains": [{"chainId": 1, "providers": [{"ready": True}]}],
            }):
                result = execution_readiness(root, cfg, registry)
            self.assertFalse(result["ready"])
            self.assertFalse(result["liveArmed"])
            self.assertEqual(result["stage"], "live_lock")

    def test_preflight_requires_firm_quote_and_independent_sanity_price(self):
        evidence = PreflightEvidence(
            True, True, True, Decimal("30"), Decimal("25"), True, True, True,
            1, 0, True, True, True, True, True, True, True, True,
        )
        allowed, blockers = SharedExecutionCore().preflight(intent("a"), evidence)
        self.assertFalse(allowed)
        self.assertEqual(blockers, ("independent_sanity_price_required",))

    def test_final_serialized_bytes_are_parsed_and_scoped(self):
        payload = {
            "chainId": "1", "wallet": "0xwallet", "tokenOut": "TOKEN", "sellAmount": "10",
            "minimumOutputAmount": "9", "targets": ["router"], "operations": ["swap"], "approvals": [],
        }
        decision, digest, parsed = validate_serialized_transaction(
            json.dumps(payload).encode(), JsonParser(), expected_chain_id="1", expected_wallet="0xwallet",
            expected_token_out="TOKEN", maximum_sell_amount=10, trusted_targets={"router"},
        )
        self.assertTrue(decision.valid)
        self.assertEqual(parsed["targets"], ["router"])
        self.assertEqual(len(digest), 64)

    def test_concurrent_sources_cannot_double_spend_reservation(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 10)
            barrier = threading.Barrier(3)
            outcomes: list[str] = []

            def reserve(item: ExecutionIntent) -> None:
                barrier.wait()
                try:
                    journal.reserve_intent(item)
                    outcomes.append("reserved")
                except ValueError as error:
                    outcomes.append(str(error))

            threads = [
                threading.Thread(target=reserve, args=(intent("fomo", "fomo_push"),)),
                threading.Thread(target=reserve, args=(intent("wallet", "wallet_rpc_evm"),)),
            ]
            for thread in threads: thread.start()
            barrier.wait()
            for thread in threads: thread.join()
            self.assertEqual(outcomes.count("reserved"), 1)
            self.assertEqual(outcomes.count("insufficient_unreserved_balance"), 1)
            journal.close()

    def test_broadcast_hash_is_idempotent_and_recoverable_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            journal = ExecutionJournal(path, account_id="live")
            journal.set_capital_snapshot("live", 20)
            job = intent("lifecycle", amount="10")
            journal.reserve_intent(job)
            for state in ("quoted", "simulated", "built", "signed"):
                journal.transition_job(job.intent_id, state)
            with self.assertRaisesRegex(ValueError, "precommitted"):
                journal.transition_job(job.intent_id, "submitted")
            with self.assertRaisesRegex(ValueError, "precommitted"):
                journal.persist_broadcast(job.intent_id, "0xtx", "provider", "7", "a" * 64)
            self.assertFalse(journal.claim_submission(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            self.assertTrue(journal.claim_submission(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            journal.close()
            reopened = ExecutionJournal(path, account_id="live")
            self.assertEqual(reopened.recoverable_broadcasts()[0]["tx_hash"], "0xtx")
            self.assertEqual(reopened.recoverable_broadcasts()[0]["state"], "submitted")
            with self.assertRaisesRegex(ValueError, "different transaction hash"):
                reopened.claim_submission(job.intent_id, "0xother", "provider", "7", "a" * 64)
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                reopened.claim_submission(job.intent_id, "0xtx", "provider", "8", "a" * 64)
            self.assertFalse(reopened.persist_broadcast(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            self.assertTrue(reopened.persist_broadcast(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            reopened.transition_job(job.intent_id, "replaced")
            reopened.close()


if __name__ == "__main__":
    unittest.main()
