from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

from fomo.execution.journal import ExecutionJournal
from fomo.execution.service import ExecutionService
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id


class SignalQueueTests(unittest.TestCase):
    def test_rh_one_shot_clock_override_is_scoped_and_still_requires_fresh_arrival(self):
        with tempfile.TemporaryDirectory() as directory:
            for name, offset, received_offset, override, expected in (
                ("old-default", -25, 0, False, "dropped_late"),
                ("old-override", -25, 0, True, "simulated"),
                ("future-override", 25, 0, True, "simulated"),
                ("stale-arrival", -25, -10, True, "dropped_late"),
            ):
                queue = DurableSignalQueue(Path(directory) / f"{name}.sqlite3")
                journal = ExecutionJournal(Path(directory) / f"{name}-execution.sqlite3")
                now = datetime.now(timezone.utc)
                event = {"id": name, "userId": "followed", "type": "swap_buy",
                         "networkId": 4663,
                         "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                         "createdAt": (now + timedelta(seconds=offset)).isoformat()}
                signal_id = deterministic_signal_id("fomo_push", name)
                queue.enqueue(signal_id, "fomo_push", "raw_fomo", event,
                              (now + timedelta(seconds=received_offset)).isoformat())
                executor = Mock(allow_broadcast=False)
                executor.create_intent.return_value = Mock(token_out=event["tokenAddress"])
                executor.execute.return_value = {"status": "simulated", "simulation_success": True,
                                                 "broadcastSent": False}
                service = ExecutionService(queue, journal, lambda: {"followed"}, lambda _: None,
                                           rh_auto_executor=executor,
                                           rh_auto_ignore_source_clock_once=override)
                result = service.process_one()
                assert result is not None
                self.assertEqual(result["reason"], expected)
                self.assertEqual(executor.execute.call_count, 1 if expected == "simulated" else 0)
                journal.close()
                queue.close()

    def test_exact_claim_does_not_fall_through_to_next_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            now = datetime.now(timezone.utc).isoformat()
            queue.enqueue("first", "fomo_push", "raw_fomo", {"id": "first"}, now)
            queue.enqueue("second", "fomo_push", "raw_fomo", {"id": "second"}, now)
            self.assertTrue(queue.acquire_service("owner"))
            first = queue.claim("owner", exact_sequence=1)
            assert first is not None
            queue.finish("owner", first.signal_id, "dropped", {"reason": "test"})
            self.assertIsNone(queue.claim("owner", exact_sequence=1))
            second = queue.claim("owner", after_sequence=1, exact_sequence=2)
            assert second is not None
            self.assertEqual(second.signal_id, "second")
            queue.close()

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
            self.assertEqual(result["status"], "dropped")
            self.assertEqual(result["reason"], "source_unconfirmed")
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
            queue.enqueue(signal_id, "fomo_push", "raw_fomo", payload,
                          datetime.now(timezone.utc).isoformat())
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

    def test_one_shot_watermark_ignores_backlog_without_modifying_it(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            now = datetime.now(timezone.utc).isoformat()
            queue.enqueue("old", "fomo_push", "raw_fomo", {"id": "old"}, now)
            watermark = int(queue.db.execute(
                "SELECT MAX(sequence) FROM signal_queue").fetchone()[0])
            service = ExecutionService(
                queue, journal, lambda: set(), lambda _signal: None,
                after_queue_sequence=watermark,
            )
            self.assertIsNone(service.process_one())
            old_status = queue.status("old")
            assert old_status is not None
            self.assertEqual(old_status["status"], "queued")
            queue.enqueue("new", "fomo_push", "raw_fomo", {"id": "new"}, now)
            result = service.process_one()
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["signalId"], "new")
            self.assertEqual(old_status["status"], "queued")
            journal.close()
            queue.close()


if __name__ == "__main__":
    unittest.main()
