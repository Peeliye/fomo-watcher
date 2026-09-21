from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from fomo.execution.signal_queue import DurableSignalQueue
from fomo.execution.journal import ExecutionJournal
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.watching.adapter import AdapterBatch, ChainCheckpoint, NormalizedWatchEvent
from fomo.watching.queue_bridge import WalletQueueBridge, WalletReorgRequiresReconciliation
from fomo.watching.wallet import WalletRpcAdapter


class FakeStore:
    def __init__(self) -> None:
        self.acked: list[str] = []

    def acknowledge(self, _adapter_id: str, delivery_id: str) -> bool:
        self.acked.append(delivery_id)
        return True


class FakeAdapter:
    adapter_id = "evm:1"

    def __init__(self, batch: AdapterBatch) -> None:
        self.batch = batch
        self.store = FakeStore()

    def poll_configured(self, _path: str) -> AdapterBatch:
        return self.batch


class WalletQueueBridgeTests(unittest.TestCase):
    def test_durable_enqueue_before_outbox_ack_and_reorg_halts(self) -> None:
        signal = TradeSignalEnvelope.create(
            source="wallet_rpc_evm", source_event_id="swap", observed_at="2026-09-21T00:00:01Z",
            source_timestamp="2026-09-21T00:00:00Z", delivery_delay_ms=1000, chain_id="1",
            actor_wallet="0x1111111111111111111111111111111111111111", kol_id=None, side="buy",
            token_in="USDC", token_out="TOKEN", source_amount="5", estimated_usd="10",
            tx_hash="0xtx", signature=None, log_index=1, instruction_index=None,
            confirmation_level="confirmed", reorg_key="h1", decoder_version="test@1",
            raw_payload_hash="a" * 64,
        )
        event = NormalizedWatchEvent(signal.signal_id, "1", signal.actor_wallet or "", "buy", "TOKEN",
                                     "5", 10_000_000, "0xtx", 1, signal.observed_at, signal)
        checkpoint = ChainCheckpoint("1", "101", 101, "h101")
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            adapter = FakeAdapter(AdapterBatch((event,), checkpoint))
            bridge = WalletQueueBridge(cast(WalletRpcAdapter, cast(Any, adapter)), queue, "unused.json")
            self.assertEqual(bridge.poll_once(), 1)
            self.assertEqual(queue.status(signal.signal_id)["status"], "queued")  # type: ignore[index]
            self.assertEqual(adapter.store.acked, [f"signal:{signal.signal_id}"])
            self.assertEqual(bridge.poll_once(), 1)
            self.assertEqual(queue.status(signal.signal_id)["attempts"], 0)  # type: ignore[index]
            adapter.batch = AdapterBatch((), checkpoint, (signal.signal_id,))
            with self.assertRaises(WalletReorgRequiresReconciliation):
                bridge.poll_once()
            queue.close()

    def test_reorg_fence_cancels_queued_signal_and_blocks_precommit(self) -> None:
        signal = TradeSignalEnvelope.create(
            source="wallet_rpc_evm", source_event_id="revert-me", observed_at="2026-09-21T00:00:01Z",
            source_timestamp="2026-09-21T00:00:00Z", delivery_delay_ms=1000, chain_id="1",
            actor_wallet="0x1111111111111111111111111111111111111111", kol_id=None, side="buy",
            token_in="USDC", token_out="TOKEN", source_amount="5", estimated_usd="10",
            tx_hash="0xtx", signature=None, log_index=1, instruction_index=None,
            confirmation_level="confirmed", reorg_key="h1", decoder_version="test@1",
            raw_payload_hash="a" * 64,
        )
        with tempfile.TemporaryDirectory() as directory:
            queue = DurableSignalQueue(Path(directory) / "queue.sqlite3")
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            queue.enqueue_envelope(signal)
            adapter = FakeAdapter(AdapterBatch((), ChainCheckpoint("1", "100", 100, "h100"),
                                              (signal.signal_id,)))
            bridge = WalletQueueBridge(cast(WalletRpcAdapter, cast(Any, adapter)), queue,
                                       "unused.json", journal)
            with self.assertRaises(WalletReorgRequiresReconciliation):
                bridge.poll_once()
            self.assertEqual(queue.status(signal.signal_id)["status"], "dropped")  # type: ignore[index]
            self.assertTrue(journal.source_reorged(signal.signal_id))
            self.assertEqual(adapter.store.acked, [f"reorg:{signal.signal_id}"])
            self.assertEqual(journal.db.execute(
                "SELECT circuit_breaker_tripped FROM execution_control WHERE singleton=1"
            ).fetchone()[0], 1)
            journal.close()
            queue.close()


if __name__ == "__main__":
    unittest.main()
