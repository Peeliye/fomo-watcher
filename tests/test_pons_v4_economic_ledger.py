"""Live CA fence tests; fake callbacks never sign or send."""

from __future__ import annotations

import sqlite3

import pytest

from fomo.execution.pons_v4_economic_ledger import PonsV4EconomicLedger


def test_ca_claim_is_atomic_across_connections_and_users(tmp_path):
    path = tmp_path / "economic.sqlite3"
    first, second = PonsV4EconomicLedger(path), PonsV4EconomicLedger(path)
    try:
        key = "4663:0x314ad0f11422842d28b4f950a64cd40fafb029fd"
        first.claim(key, "signal-user-a")
        with pytest.raises(ValueError, match="duplicate_economic_event"):
            second.claim(key, "signal-user-b")
        first.transition(key, "signal-user-a", "preflight_failed")
        second.claim(key, "signal-user-b")
        second.transition(key, "signal-user-b", "send_attempted", nonce=4,
                          tx_hash="0x" + "ab" * 32)
        with pytest.raises(ValueError, match="state_conflict"):
            second.transition(key, "signal-user-b", "preflight_failed")
        with pytest.raises(ValueError, match="duplicate_economic_event"):
            first.claim(key, "signal-user-c")
    finally:
        first.close()
        second.close()
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT state,nonce,tx_hash FROM pons_economic_events").fetchone() == (
            "send_attempted", 4, "0x" + "ab" * 32,
        )
        assert [row[0] for row in db.execute("SELECT action FROM pons_economic_audit ORDER BY id")] == [
            "reserved", "preflight_failed", "reserved", "send_attempted",
        ]


def test_existing_unknown_schema_fails_closed_without_change(tmp_path):
    path = tmp_path / "economic.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE pons_economic_events(economic_key TEXT PRIMARY KEY,signal_id TEXT)")
        db.execute("INSERT INTO pons_economic_events VALUES('old-trade','old-signal')")
        db.execute("PRAGMA user_version=1")
    with pytest.raises(ValueError, match="migration_required"):
        PonsV4EconomicLedger(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM pons_economic_events").fetchone()[0] == 1
