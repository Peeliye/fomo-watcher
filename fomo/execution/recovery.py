"""Read-only chain polling for committed broadcast IDs after a restart."""

from __future__ import annotations

from typing import Callable

from .interfaces import ReceiptTracker
from .journal import ExecutionJournal


class ReceiptRecoveryService:
    def __init__(self, journal: ExecutionJournal, tracker_for_chain: Callable[[str], ReceiptTracker | None]) -> None:
        self.journal = journal
        self.tracker_for_chain = tracker_for_chain

    def poll_once(self) -> dict[str, int]:
        result = {"checked": 0, "finalized": 0, "pending": 0, "unavailable": 0}
        for job in self.journal.recoverable_broadcasts():
            tracker = self.tracker_for_chain(str(job["chain_id"]))
            if tracker is None or not tracker.self_check().ready:
                result["unavailable"] += 1
                continue
            result["checked"] += 1
            receipt = tracker.receipt(str(job["tx_hash"]))
            if receipt is None or (receipt.get("status") != "reorged" and receipt.get("finality") != "finalized"):
                result["pending"] += 1
                continue
            self.journal.reconcile_job_receipt(str(job["intent_id"]), dict(receipt))
            result["finalized"] += 1
        return result
