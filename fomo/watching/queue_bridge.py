"""At-least-once wallet outbox delivery into the shared execution queue."""

from __future__ import annotations

from pathlib import Path

from fomo.execution.signal_queue import DurableSignalQueue
from fomo.execution.journal import ExecutionJournal

from .wallet import WalletRpcAdapter


class WalletReorgRequiresReconciliation(RuntimeError):
    """Stop chain ingestion rather than silently reuse a reverted signal."""


class WalletQueueBridge:
    def __init__(self, adapter: WalletRpcAdapter, queue: DurableSignalQueue,
                 watchlist_path: str | Path, journal: ExecutionJournal | None = None) -> None:
        self.adapter = adapter
        self.queue = queue
        self.watchlist_path = str(watchlist_path)
        self.journal = journal

    def poll_once(self) -> int:
        batch = self.adapter.poll_configured(self.watchlist_path)
        if batch.reverted_event_ids:
            if self.journal is not None:
                for signal_id in batch.reverted_event_ids:
                    self.journal.record_source_reorg(signal_id)
                    self.queue.cancel_reorged(signal_id)
                    if not self.adapter.store.acknowledge(self.adapter.adapter_id, f"reorg:{signal_id}"):
                        raise ValueError("wallet_reorg_outbox_ack_lost")
            raise WalletReorgRequiresReconciliation("wallet_reorg_requires_execution_reconciliation")
        delivered = 0
        for event in batch.events:
            signal = event.signal
            if signal is None or signal.signal_id != event.event_id:
                raise ValueError("wallet_outbox_signal_identity_invalid")
            self.queue.enqueue_envelope(signal)
            if self.queue.status(signal.signal_id) is None:
                raise ValueError("wallet_signal_not_durable_in_execution_queue")
            if not self.adapter.store.acknowledge(self.adapter.adapter_id, f"signal:{event.event_id}"):
                raise ValueError("wallet_outbox_ack_lost")
            delivered += 1
        return delivered
