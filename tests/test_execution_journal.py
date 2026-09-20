import tempfile
import unittest
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from fomo.execution.journal import ExecutionJournal, execution_snapshot, reconciliation_snapshot


class ExecutionJournalTests(unittest.TestCase):
    def test_intent_is_idempotent_and_live_execution_is_locked(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            journal = ExecutionJournal(path)
            event = SimpleNamespace(id="event-1", user_id="kol-1", handle="alice", network_id=1, ca="0x1111111111111111111111111111111111111111", symbol="MEME", kind="buy", amount_usd=100)
            decision = {"signalId": "fomo:event-1", "outcome": "needs_data", "blockers": ["asset_snapshot_required"], "policyVersion": 1, "registryVersion": 2}
            self.assertFalse(journal.record_risk_decision(event, decision)["duplicate"])
            self.assertTrue(journal.record_risk_decision(event, decision)["duplicate"])
            journal.close()
            snapshot = execution_snapshot(path)
            self.assertEqual(snapshot["total"], 1)
            self.assertEqual(snapshot["states"], {"blocked": 1})
            self.assertFalse(snapshot["control"]["liveArmed"])
            self.assertTrue(snapshot["control"]["circuitBreakerTripped"])
            self.assertTrue(snapshot["intents"][0]["readOnly"])

    def test_verified_receipt_is_idempotent_and_reconciles_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            journal = ExecutionJournal(path)
            event = SimpleNamespace(id="event-2", user_id="kol-1", handle="alice", network_id=1, ca="0x1111111111111111111111111111111111111111", symbol="MEME", kind="buy", amount_usd=100)
            decision = {"signalId": "fomo:event-2", "outcome": "approved_for_shadow", "blockers": []}
            intent_id = journal.record_risk_decision(event, decision)["intentId"]
            receipt = {"txHash": "0xabc", "intentId": intent_id, "chainId": 1, "status": "confirmed", "blockNumber": 123, "actualUsd": 99.5, "feeUsd": 0.25}
            self.assertFalse(journal.record_receipt(receipt)["duplicate"])
            self.assertTrue(journal.record_receipt(receipt)["duplicate"])
            with self.assertRaises(ValueError):
                journal.record_receipt({**receipt, "status": "failed"})
            journal.close()
            result = reconciliation_snapshot(path)
            self.assertEqual(result["confirmed"], 1)
            self.assertEqual(result["items"][0]["actualUsd"], 99.5)
            self.assertEqual(execution_snapshot(path)["states"], {"confirmed": 1})

    def test_post_trade_advisory_is_observed_not_a_pretrade_block(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            journal = ExecutionJournal(path)
            event = SimpleNamespace(id="post-1", user_id="kol-1", handle="alice", network_id=1,
                                    ca="0x1111111111111111111111111111111111111111",
                                    symbol="MEME", kind="buy", amount_usd=100)
            journal.record_risk_decision(event, {
                "signalId": "fomo:post-1", "phase": "post_trade", "outcome": "rejected",
                "blockers": ["sell_simulation_failed"],
            })
            journal.close()
            self.assertEqual(execution_snapshot(path)["states"], {"post_trade_observed": 1})

    def test_receipt_chain_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            event = SimpleNamespace(id="event-3", user_id="kol-1", handle="alice", network_id=1, ca="0x1111111111111111111111111111111111111111", symbol="MEME", kind="buy", amount_usd=100)
            intent_id = journal.record_risk_decision(event, {"signalId": "fomo:event-3", "outcome": "approved_for_shadow", "blockers": []})["intentId"]
            with self.assertRaises(ValueError):
                journal.record_receipt({"txHash": "0xdef", "intentId": intent_id, "chainId": 56, "status": "confirmed"})
            journal.close()

    def test_intent_transition_failure_rolls_back_and_connection_is_reusable(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            journal.db.execute(
                """CREATE TRIGGER fail_intent_transition BEFORE INSERT ON execution_transitions
                   BEGIN SELECT RAISE(ABORT,'injected'); END"""
            )
            journal.db.commit()
            event = SimpleNamespace(id="event-fault", user_id="kol-1", handle="alice", network_id=1,
                                    ca="0x1111111111111111111111111111111111111111", symbol="MEME",
                                    kind="buy", amount_usd=100)
            decision = {"signalId": "fomo:event-fault", "outcome": "needs_data", "blockers": ["test"]}
            with self.assertRaises(sqlite3.IntegrityError):
                journal.record_risk_decision(event, decision)
            self.assertFalse(journal.db.in_transaction)
            self.assertEqual(journal.db.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0], 0)
            journal.db.execute("DROP TRIGGER fail_intent_transition")
            journal.db.commit()
            self.assertFalse(journal.record_risk_decision(event, decision)["duplicate"])
            journal.close()

    def test_receipt_transition_failure_rolls_back_all_three_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            event = SimpleNamespace(id="event-receipt-fault", user_id="kol-1", handle="alice", network_id=1,
                                    ca="0x1111111111111111111111111111111111111111", symbol="MEME",
                                    kind="buy", amount_usd=100)
            intent_id = journal.record_risk_decision(
                event, {"signalId": "fomo:event-receipt-fault", "outcome": "approved_for_shadow", "blockers": []}
            )["intentId"]
            journal.db.execute(
                """CREATE TRIGGER fail_receipt_transition BEFORE INSERT ON execution_transitions
                   WHEN NEW.reason LIKE 'receipt_%'
                   BEGIN SELECT RAISE(ABORT,'injected'); END"""
            )
            journal.db.commit()
            receipt = {"txHash": "0xfault", "intentId": intent_id, "chainId": 1, "status": "confirmed"}
            with self.assertRaises(sqlite3.IntegrityError):
                journal.record_receipt(receipt)
            self.assertFalse(journal.db.in_transaction)
            self.assertEqual(journal.db.execute("SELECT COUNT(*) FROM execution_receipts").fetchone()[0], 0)
            self.assertEqual(
                journal.db.execute("SELECT state FROM execution_intents WHERE intent_id=?", (intent_id,)).fetchone()[0],
                "shadow_ready",
            )
            journal.db.execute("DROP TRIGGER fail_receipt_transition")
            journal.db.commit()
            self.assertFalse(journal.record_receipt(receipt)["duplicate"])
            journal.close()


if __name__ == "__main__":
    unittest.main()
