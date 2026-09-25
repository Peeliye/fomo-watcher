from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from fomo.execution.chain_managers import EvmNonceManager, SolanaBlockhashManager
from fomo.execution.journal import ExecutionJournal
from fomo.execution.recovery import ReceiptRecoveryService
from fomo.execution.rpc_adapters import EvmReceiptTracker
from fomo.signals.strategy import ExecutionIntent


def intent(name: str, chain: str = "1", amount: str = "7") -> ExecutionIntent:
    return ExecutionIntent(name, f"signal:{name}", "wallet_rpc_evm", "test", "wallet", chain,
                           "buy", "USDC", "TOKEN", Decimal(amount))


class RpcFixture:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.pending_nonce = 7

    def call(self, method: str, params: Sequence[Any]) -> object:
        self.calls.append(method)
        if method == "eth_chainId":
            return "0x1"
        if method == "eth_getTransactionCount":
            return hex(self.pending_nonce)
        if method == "getLatestBlockhash":
            return {"value": {"blockhash": "test-blockhash", "lastValidBlockHeight": 200}}
        if method == "getBlockHeight":
            return 100
        if method == "eth_getTransactionReceipt":
            return {"transactionHash": "0xtest", "blockNumber": "0xa", "blockHash": "h10",
                    "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x3b9aca00", "logs": []}
        if method == "eth_getBlockByNumber":
            return {"hash": "h10"} if params == ["0xa", False] else {"number": "0xa"}
        raise AssertionError(method)


class LiveAdapterTests(unittest.TestCase):
    def test_journal_v4_migration_has_verified_rollback_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE preserved_history (value TEXT)")
            db.execute("INSERT INTO preserved_history VALUES ('keep-me')")
            db.execute("PRAGMA user_version=4")
            db.commit()
            db.close()
            journal = ExecutionJournal(path)
            self.assertEqual(journal.db.execute("PRAGMA user_version").fetchone()[0], 8)
            self.assertEqual(journal.db.execute("SELECT value FROM preserved_history").fetchone()[0], "keep-me")
            journal.close()
            backups = list((Path(directory) / "backups").glob("execution.pre-v8.from-v4.*.sqlite3"))
            self.assertEqual(len(backups), 1)
            backup = sqlite3.connect(backups[0])
            self.assertEqual(backup.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 4)
            self.assertEqual(backup.execute("SELECT value FROM preserved_history").fetchone()[0], "keep-me")
            backup.close()

    def test_v5_nonce_audit_migration_preserves_old_leases_in_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE execution_evm_nonces (intent_id TEXT PRIMARY KEY, chain_id TEXT, "
                       "account_id TEXT, nonce INTEGER, tx_hash TEXT)")
            db.execute("INSERT INTO execution_evm_nonces VALUES ('old','1','live',7,NULL)")
            db.execute("PRAGMA user_version=5")
            db.commit()
            db.close()
            journal = ExecutionJournal(path)
            self.assertEqual(journal.db.execute(
                "SELECT nonce FROM execution_evm_nonces WHERE intent_id='old'"
            ).fetchone()[0], 7)
            journal.close()
            backups = list((Path(directory) / "backups").glob("execution.pre-v8.from-v5.*.sqlite3"))
            self.assertEqual(len(backups), 1)
            backup = sqlite3.connect(backups[0])
            self.assertEqual(backup.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 5)
            self.assertEqual(backup.execute("SELECT nonce FROM execution_evm_nonces").fetchone()[0], 7)
            backup.close()

    def test_nonce_and_blockhash_leases_are_persistent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 30)
            evm_a, evm_b, sol = intent("a"), intent("b"), intent("sol", "1399811149")
            for item in (evm_a, evm_b, sol):
                journal.reserve_intent(item)
                journal.transition_job(item.intent_id, "quoted")
            rpc = RpcFixture()
            manager = EvmNonceManager(journal, rpc, chain_id=1, account_id="live", wallet="0xwallet")
            self.assertEqual(manager.acquire(evm_a), "7")
            self.assertEqual(manager.acquire(evm_b), "8")
            self.assertEqual(manager.acquire(evm_a), "7")
            manager.mark_used(evm_a.intent_id, "7", "0xtx-a")
            with self.assertRaisesRegex(ValueError, "conflicting"):
                manager.mark_used(evm_a.intent_id, "7", "0xtx-other")
            sol_manager = SolanaBlockhashManager(journal, rpc)
            self.assertEqual(sol_manager.acquire(sol), "test-blockhash")
            sol_manager.mark_used(sol.intent_id, "test-blockhash", "signature")
            journal.close()

    def test_broadcast_is_hard_locked_and_finalized_receipt_recovery_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 10)
            item = intent("recover")
            journal.reserve_intent(item)
            for state in ("quoted", "simulated", "built", "signed"):
                journal.transition_job(item.intent_id, state)
            journal.claim_submission(item.intent_id, "0xtest", "provider", "7", hashlib.sha256(b"tx").hexdigest())
            with self.assertRaisesRegex(ValueError, "live_execution_disabled"):
                journal.claim_broadcast_attempt(item.intent_id, "0xtest", hashlib.sha256(b"tx").hexdigest())
            rpc = RpcFixture()
            tracker = EvmReceiptTracker(rpc, 1)
            result = ReceiptRecoveryService(journal, lambda _chain: tracker).poll_once()
            self.assertEqual(result["finalized"], 1)
            self.assertNotIn("eth_sendRawTransaction", rpc.calls)
            self.assertEqual(journal.db.execute(
                "SELECT state FROM execution_jobs WHERE intent_id=?", (item.intent_id,)
            ).fetchone()[0], "confirmed")
            # Hold funds until actual token deltas are reconciled; no synthetic fill.
            self.assertEqual(journal.db.execute(
                "SELECT status FROM execution_reservations WHERE intent_id=?", (item.intent_id,)
            ).fetchone()[0], "active")
            journal.close()

    def test_failed_before_send_releases_capital_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 10)
            item = intent("failed")
            journal.reserve_intent(item)
            journal.fail_unsubmitted_job(item.intent_id, reason="simulation_failed")
            self.assertEqual(journal.db.execute(
                "SELECT reserved_usd_micros FROM capital_accounts WHERE account_id='live'"
            ).fetchone()[0], 0)
            with self.assertRaisesRegex(ValueError, "unsubmitted_job_required"):
                journal.fail_unsubmitted_job(item.intent_id, reason="again")
            journal.close()

    def test_failed_unsigned_nonce_can_be_reused_but_external_pending_wins(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            journal = ExecutionJournal(path, account_id="live")
            journal.set_capital_snapshot("live", 30)
            rpc = RpcFixture()
            manager = EvmNonceManager(journal, rpc, chain_id=1, account_id="live", wallet="0xwallet")
            first = intent("first")
            journal.reserve_intent(first)
            journal.transition_job(first.intent_id, "quoted")
            self.assertEqual(manager.acquire(first), "7")
            journal.fail_unsubmitted_job(first.intent_id, reason="build_failed")
            self.assertIsNone(journal.db.execute(
                "SELECT 1 FROM execution_evm_nonces WHERE intent_id=?", (first.intent_id,)
            ).fetchone())
            self.assertEqual([row[0] for row in journal.db.execute(
                "SELECT action FROM execution_nonce_lease_events WHERE intent_id=? ORDER BY lease_event_id",
                (first.intent_id,),
            )], ["acquired", "released"])
            journal.close()

            reopened = ExecutionJournal(path, account_id="live")
            manager = EvmNonceManager(reopened, rpc, chain_id=1, account_id="live", wallet="0xwallet")
            second = intent("second")
            reopened.reserve_intent(second)
            reopened.transition_job(second.intent_id, "quoted")
            self.assertEqual(manager.acquire(second), "7")
            reopened.fail_unsubmitted_job(second.intent_id, reason="sign_failed")
            rpc.pending_nonce = 9  # Another wallet process occupied 7 and 8.
            third = intent("third")
            reopened.reserve_intent(third)
            reopened.transition_job(third.intent_id, "quoted")
            self.assertEqual(manager.acquire(third), "9")
            reopened.close()

    def test_nonce_release_rolls_back_with_funds_when_audit_write_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 10)
            item = intent("rollback")
            journal.reserve_intent(item)
            journal.transition_job(item.intent_id, "quoted")
            manager = EvmNonceManager(journal, RpcFixture(), chain_id=1,
                                      account_id="live", wallet="0xwallet")
            manager.acquire(item)
            journal.db.execute("CREATE TRIGGER fail_release BEFORE INSERT ON execution_nonce_lease_events "
                               "WHEN NEW.action='released' BEGIN SELECT RAISE(ABORT,'audit failed'); END")
            with self.assertRaises(sqlite3.IntegrityError):
                journal.fail_unsubmitted_job(item.intent_id, reason="build_failed")
            self.assertEqual(journal.db.execute(
                "SELECT state FROM execution_jobs WHERE intent_id=?", (item.intent_id,)
            ).fetchone()[0], "quoted")
            self.assertEqual(journal.db.execute(
                "SELECT reserved_usd_micros FROM capital_accounts WHERE account_id='live'"
            ).fetchone()[0], 7_000_000)
            self.assertIsNotNone(journal.db.execute(
                "SELECT 1 FROM execution_evm_nonces WHERE intent_id=?", (item.intent_id,)
            ).fetchone())
            journal.close()

    def test_parallel_nonces_are_unique_and_precommitted_nonce_is_never_recycled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 30)
            items = (intent("parallel-a"), intent("parallel-b"))
            for item in items:
                journal.reserve_intent(item)
                journal.transition_job(item.intent_id, "quoted")
            manager = EvmNonceManager(journal, RpcFixture(), chain_id=1,
                                      account_id="live", wallet="0xwallet")
            barrier = threading.Barrier(3)
            values: list[int] = []

            def acquire(item: ExecutionIntent) -> None:
                barrier.wait()
                values.append(int(manager.acquire(item)))

            threads = [threading.Thread(target=acquire, args=(item,)) for item in items]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join()
            self.assertEqual(sorted(values), [7, 8])
            protected = items[0]
            nonce = manager.acquire(protected)
            for state in ("simulated", "built", "signed"):
                journal.transition_job(protected.intent_id, state)
            manager.mark_used(protected.intent_id, nonce, "0xprotected")
            with self.assertRaisesRegex(ValueError, "signed_nonce_requires_manual_reconciliation"):
                journal.fail_unsubmitted_job(protected.intent_id, reason="claim_failed")
            journal.claim_submission(protected.intent_id, "0xprotected", "provider", nonce,
                                     hashlib.sha256(b"signed").hexdigest())
            with self.assertRaisesRegex(ValueError, "unsubmitted_job_required"):
                journal.fail_unsubmitted_job(protected.intent_id, reason="uncertain_broadcast")
            self.assertIsNotNone(journal.db.execute(
                "SELECT 1 FROM execution_evm_nonces WHERE intent_id=?", (protected.intent_id,)
            ).fetchone())
            journal.close()

    def test_source_reorg_fence_blocks_submission_and_broadcast_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = ExecutionJournal(Path(directory) / "execution.sqlite3", account_id="live")
            journal.set_capital_snapshot("live", 30)
            first, second = intent("source-reorg-a"), intent("source-reorg-b")
            for item in (first, second):
                journal.reserve_intent(item)
                for state in ("quoted", "simulated", "built", "signed"):
                    journal.transition_job(item.intent_id, state)
            digest = hashlib.sha256(b"signed").hexdigest()
            journal.claim_submission(second.intent_id, "0xsecond", "provider", "8", digest)
            journal.record_source_reorg(first.signal_id)
            journal.record_source_reorg(second.signal_id)
            with self.assertRaisesRegex(ValueError, "source_signal_reorged"):
                journal.claim_submission(first.intent_id, "0xfirst", "provider", "7", digest)
            with self.assertRaisesRegex(ValueError, "source_signal_reorged"):
                journal.claim_broadcast_attempt(second.intent_id, "0xsecond", digest)
            self.assertEqual(journal.recoverable_broadcasts()[0]["tx_hash"], "0xsecond")
            journal.close()


if __name__ == "__main__":
    unittest.main()
