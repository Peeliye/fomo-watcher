from __future__ import annotations

import json
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path

from fomo.execution.capabilities import CapabilityRegistry, FakeSignerCapability
from fomo.execution.core import PreflightEvidence, SharedExecutionCore, validate_serialized_transaction
from fomo.execution.journal import ExecutionJournal
from fomo.signals.strategy import ExecutionIntent


def intent(name: str, source: str = "fomo_push", amount: str = "7") -> ExecutionIntent:
    return ExecutionIntent(name, f"signal:{name}", source, "test", f"allocation:{name}", "1", "buy",
                           "USDC", "TOKEN", Decimal(amount))


class JsonParser:
    def parse(self, serialized_transaction: bytes):
        return json.loads(serialized_transaction)


class ExecutionCoreTests(unittest.TestCase):
    def test_fake_signer_and_missing_adapters_are_never_ready(self):
        registry = CapabilityRegistry()
        registry.register("signer", FakeSignerCapability())
        status = registry.status()
        self.assertFalse(status["ready"])
        self.assertFalse(status["capabilities"]["signer"]["implemented"])
        self.assertEqual(status["capabilities"]["broadcaster"]["reason"], "adapter_not_registered")

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
            for state in ("quoted", "simulated", "built", "signed", "submitted"):
                journal.transition_job(job.intent_id, state)
            self.assertFalse(journal.persist_broadcast(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            self.assertTrue(journal.persist_broadcast(job.intent_id, "0xtx", "provider", "7", "a" * 64)["duplicate"])
            journal.close()
            reopened = ExecutionJournal(path, account_id="live")
            self.assertEqual(reopened.recoverable_broadcasts()[0]["tx_hash"], "0xtx")
            reopened.transition_job(job.intent_id, "replaced")
            reopened.close()


if __name__ == "__main__":
    unittest.main()
