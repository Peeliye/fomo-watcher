from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fomo.execution.journal import ExecutionJournal
from fomo.execution.service import ExecutionService
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id


class SignalQueueTests(unittest.TestCase):
    def test_source_dedupe_single_service_and_live_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            now = datetime.now(timezone.utc).isoformat()
            payload = {"id": "e1", "userId": "followed", "type": "swap_buy", "networkId": 1,
                       "tokenAddress": "TOKEN", "quoteToken": "USDC", "usdAmount": 100,
                       "createdAt": now}
            signal_id = deterministic_signal_id("fomo_push", "e1")
            self.assertTrue(queue.enqueue(signal_id, "fomo_push", "raw_fomo", payload, now))
            self.assertFalse(queue.enqueue(signal_id, "fomo_push", "raw_fomo", payload, now))
            service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _signal: None)
            result = service.process_one()
            assert result is not None
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["reason"], "live_disabled_or_circuit_open")
            self.assertIn("localQueueDelayMs", result)
            self.assertIn("upstreamDeliveryDelayMs", result)
            self.assertIsNone(service.process_one())
            self.assertFalse(queue.acquire_service("other-owner"))
            state = queue.status(signal_id)
            assert state is not None
            self.assertEqual(state["attempts"], 1)
            journal.close()
            queue.close()

    def test_late_signal_is_dropped_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            old = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
            payload = {"id": "e2", "userId": "followed", "type": "swap_buy", "networkId": 1,
                       "tokenAddress": "TOKEN", "usdAmount": 100, "createdAt": old}
            signal_id = deterministic_signal_id("fomo_push", "e2")
            queue.enqueue(signal_id, "fomo_push", "raw_fomo", payload, old)
            result = ExecutionService(queue, journal, lambda: {"followed"}, lambda _signal: None).process_one()
            assert result is not None
            self.assertEqual(result["reason"], "dropped_late")
            state = queue.status(signal_id)
            assert state is not None
            self.assertEqual(state["status"], "dropped")
            journal.close()
            queue.close()

    def test_expired_claim_can_recover_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite3"
            queue = DurableSignalQueue(path)
            queue.enqueue("sig:test", "fomo_push", "raw_fomo", {"id": "x"},
                          datetime.now(timezone.utc).isoformat())
            self.assertTrue(queue.acquire_service("first"))
            first = queue.claim("first")
            assert first is not None
            self.assertEqual(first.attempts, 1)
            queue.db.execute("UPDATE signal_queue_service_lock SET lease_until_ms=0")
            queue.db.execute("UPDATE signal_queue SET lease_until_ms=0")
            queue.close()
            recovered = DurableSignalQueue(path)
            self.assertTrue(recovered.acquire_service("second"))
            second = recovered.claim("second")
            assert second is not None
            self.assertEqual(second.attempts, 2)
            recovered.finish("second", "sig:test", "dropped", {"reason": "invalid"})
            state = recovered.status("sig:test")
            assert state is not None
            self.assertEqual(state["status"], "dropped")
            recovered.close()


if __name__ == "__main__":
    unittest.main()
