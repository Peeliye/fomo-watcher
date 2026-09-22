from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from fomo.execution.assembly import ChainAdapters, ExecutionAssembly
from fomo.execution.capabilities import CapabilityStatus
from fomo.execution.journal import ExecutionJournal


class FakeReady:
    def __init__(self, name: str) -> None:
        self.name = name

    def self_check(self) -> CapabilityStatus:
        return CapabilityStatus(self.name, True, True, "fake_ready", {})


class ExecutionAssemblyTests(unittest.TestCase):
    def test_unassembled_and_fake_ready_components_are_never_live_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3")
            assembly = ExecutionAssembly(journal)
            self.assertEqual(assembly.chain_status("1")["blockers"], ["execution_chain_not_assembled"])
            names = ("quote_adapter", "transaction_builder", "transaction_simulator", "signer",
                     "broadcaster", "nonce_blockhash_manager", "transaction_parser", "receipt_tracker")
            fake = [cast(Any, FakeReady(name)) for name in names]
            assembly.register("1", ChainAdapters(fake[0], fake[1], fake[2], fake[3], fake[4],
                                                 fake[5], fake[6], fake[7], cast(Any, FakeReady("risk_evidence"))))
            status = assembly.chain_status("1")
            self.assertFalse(status["ready"])
            self.assertIn("unaudited_or_fake_adapter_type", status["blockers"])
            self.assertIn("final_transaction_route_format_unverified", status["blockers"])
            self.assertIn("market_evidence_provider_unavailable", status["blockers"])
            self.assertIn("live_disabled_or_circuit_open", status["blockers"])
            journal.close()


if __name__ == "__main__":
    unittest.main()
