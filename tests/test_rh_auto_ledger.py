from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from fomo.execution.rh_auto_ledger import RhAutoLedger

TOKEN = "0x83a49b808f8d5e02cb2931cd2352988f498e5ba3"
WALLET = "0x4ccb77f12801ee8853a9cc3782828678c8f5584b"


def test_one_buy_per_ca_survives_restart_and_uncertain_send() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "auto.sqlite3"
        first = RhAutoLedger(path)
        first.claim(token_out=TOKEN, signal_id="signal-1", wallet=WALLET,
                    route="pons-v4", amount_in_wei=10**12)
        with pytest.raises(ValueError, match="rh_auto_duplicate_token_or_signal"):
            first.claim(token_out=TOKEN, signal_id="signal-2", wallet=WALLET,
                        route="pons-v4", amount_in_wei=10**12)
        first.signed(token_out=TOKEN, signal_id="signal-1", nonce=7,
                     tx_hash="0x" + "1" * 64, unsigned_hash="0x" + "2" * 64)
        first.broadcast_started(TOKEN, "signal-1")
        first.uncertain(TOKEN, "signal-1")
        first.close()
        recovered = RhAutoLedger(path)
        try:
            assert recovered.lookup(TOKEN) == {
                "signalId": "signal-1", "state": "uncertain", "nonce": 7,
                "txHash": "0x" + "1" * 64, "receiptStatus": None,
            }
            with pytest.raises(ValueError, match="rh_auto_duplicate_token_or_signal"):
                recovered.claim(token_out=TOKEN, signal_id="signal-2", wallet=WALLET,
                                route="v3", amount_in_wei=10**12)
            recovered.receipt(TOKEN, "signal-1", 1)
            item = recovered.lookup(TOKEN)
            assert item is not None and item["state"] == "confirmed"
        finally:
            recovered.close()


def test_failed_receipt_is_not_success_or_rebuy_permission() -> None:
    with tempfile.TemporaryDirectory() as directory:
        ledger = RhAutoLedger(Path(directory) / "auto.sqlite3")
        try:
            ledger.claim(token_out=TOKEN, signal_id="signal-1", wallet=WALLET,
                         route="v2", amount_in_wei=10**12)
            ledger.signed(token_out=TOKEN, signal_id="signal-1", nonce=1,
                          tx_hash="0x" + "1" * 64, unsigned_hash="0x" + "2" * 64)
            ledger.receipt(TOKEN, "signal-1", 0)
            item = ledger.lookup(TOKEN)
            assert item is not None and item["state"] == "failed"
            with pytest.raises(ValueError, match="rh_auto_duplicate_token_or_signal"):
                ledger.claim(token_out=TOKEN, signal_id="signal-2", wallet=WALLET,
                             route="v2", amount_in_wei=10**12)
        finally:
            ledger.close()


def test_concurrent_claims_for_same_ca_have_one_winner() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "auto.sqlite3"
        starter = RhAutoLedger(path)
        starter.close()

        def claim(signal_id: str) -> bool:
            ledger = RhAutoLedger(path)
            try:
                try:
                    ledger.claim(token_out=TOKEN, signal_id=signal_id, wallet=WALLET,
                                 route="v3", amount_in_wei=10**12)
                    return True
                except ValueError as error:
                    assert str(error) == "rh_auto_duplicate_token_or_signal"
                    return False
            finally:
                ledger.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ("signal-1", "signal-2")))
        assert sorted(results) == [False, True]
