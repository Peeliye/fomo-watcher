from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fomo.app import Event, State, process_post_trade_event
from fomo.execution.journal import ExecutionJournal


def event(event_id: str) -> Event:
    return Event(
        id=event_id,
        kind="buy",
        handle="alice",
        user_id="kol-1",
        created_at=datetime.now(timezone.utc).isoformat(),
        symbol="MEME",
        ca="0x1111111111111111111111111111111111111111",
        network_id=1,
        amount_usd=100,
        price=1,
    )


class IntelligenceStub:
    def __init__(self, failures: int = 0):
        self.failures = failures
        self.calls = 0
        self.events: set[str] = set()

    def record_event(self, item: Event) -> bool:
        self.calls += 1
        if self.calls <= self.failures:
            raise sqlite3.OperationalError("injected")
        before = len(self.events)
        self.events.add(item.id)
        return len(self.events) != before


class RiskStub:
    def __init__(self):
        self.calls = 0
        self.audit_calls = 0

    def evaluate_event(self, item: Event, _context=None, *, persist_audit: bool = True):
        self.calls += 1
        return {
            "eventId": item.id,
            "signalId": f"fomo:{item.id}",
            "phase": "post_trade",
            "outcome": "needs_data",
            "blockers": ["test"],
        }

    def append_audit(self, _decision) -> None:
        self.audit_calls += 1


class PostTradeReliabilityTests(unittest.TestCase):
    def test_intelligence_failure_retries_without_completing_task(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            item = event("intel-retry")
            state.enqueue_post_trade(item)
            intelligence = IntelligenceStub(failures=1)
            with self.assertRaises(sqlite3.OperationalError):
                process_post_trade_event(state, item, intelligence=intelligence)
            self.assertEqual(state.post_trade_step(item.id, "intelligence")[0], False)
            process_post_trade_event(state, item, intelligence=intelligence)
            self.assertEqual(intelligence.events, {item.id})
            self.assertEqual(intelligence.calls, 2)
            state.db.close()

    def test_risk_is_not_repeated_when_journal_fails_then_process_restarts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.sqlite3"
            state = State(str(state_path))
            item = event("journal-retry")
            state.enqueue_post_trade(item)
            risk = RiskStub()
            intelligence = IntelligenceStub()
            journal = ExecutionJournal(root / "execution.sqlite3")
            journal.db.execute(
                """CREATE TRIGGER fail_transition BEFORE INSERT ON execution_transitions
                   BEGIN SELECT RAISE(ABORT,'injected'); END"""
            )
            journal.db.commit()
            with self.assertRaises(sqlite3.IntegrityError):
                process_post_trade_event(state, item, risk, None, journal, intelligence)
            self.assertFalse(journal.db.in_transaction)
            self.assertEqual(risk.calls, 1)
            self.assertEqual(state.db.execute("SELECT COUNT(*) FROM risk_decisions").fetchone()[0], 1)
            state.db.close()

            state = State(str(state_path))
            journal.db.execute("DROP TRIGGER fail_transition")
            journal.db.commit()
            process_post_trade_event(state, item, risk, None, journal, intelligence)
            process_post_trade_event(state, item, risk, None, journal, intelligence)
            self.assertEqual(risk.calls, 1)
            self.assertEqual(risk.audit_calls, 1)
            self.assertEqual(journal.db.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0], 1)
            self.assertEqual(journal.db.execute("SELECT COUNT(*) FROM execution_transitions").fetchone()[0], 1)
            self.assertEqual(state.db.execute("SELECT COUNT(*) FROM risk_decisions").fetchone()[0], 1)
            self.assertTrue(all(state.post_trade_step(item.id, step)[0]
                                for step in ("intelligence", "risk", "journal", "risk_audit")))
            journal.close()
            state.db.close()

    def test_prune_preserves_pending_and_retry_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = State(str(Path(directory) / "state.sqlite3"))
            now = 1.0
            with state.db:
                state.db.execute(
                    "INSERT INTO notification_outbox(event_id,channel,event_json,status,created_at) VALUES('pending','x','{}','pending',?)",
                    (now,),
                )
                state.db.execute(
                    "INSERT INTO notification_outbox(event_id,channel,event_json,status,created_at) VALUES('dead','x','{}','dead',?)",
                    (now,),
                )
            state.prune(1, 1, 2, 100)
            rows = state.db.execute("SELECT event_id,status FROM notification_outbox ORDER BY event_id").fetchall()
            self.assertEqual([(row[0], row[1]) for row in rows], [("pending", "pending")])
            state.db.close()


if __name__ == "__main__":
    unittest.main()
