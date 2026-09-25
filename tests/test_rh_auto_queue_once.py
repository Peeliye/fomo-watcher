from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id
from scripts import rh_auto_queue_once as once


def test_one_shot_source_clock_override_requires_new_local_arrival(monkeypatch) -> None:
    received_ms = 1_790_264_000_000
    monkeypatch.setattr(once.time, "time", lambda: (received_ms + 100) / 1000)
    received_at = datetime.fromtimestamp(received_ms / 1000, timezone.utc).isoformat()
    for offset_ms in (-25_000, 25_000):
        event = {"id": f"source-clock-{offset_ms}", "userId": "followed",
                 "type": "swap_buy", "networkId": 4663,
                 "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                 "createdAt": datetime.fromtimestamp(
                     (received_ms + offset_ms) / 1000, timezone.utc).isoformat()}
        row = {"source": "fomo_push", "payload_kind": "raw_fomo",
               "status": "queued", "payload_json": json.dumps(event),
               "received_at": received_at,
               "signal_id": deterministic_signal_id("fomo_push", event["id"])}
        assert not once._eligible(row, {"followed"})
        assert once._eligible(row, {"followed"}, ignore_source_clock=True)
        monkeypatch.setattr(once.time, "time", lambda: (received_ms + 5_001) / 1000)
        assert not once._eligible(row, {"followed"}, ignore_source_clock=True)
        monkeypatch.setattr(once.time, "time", lambda: (received_ms + 100) / 1000)


def test_one_shot_arms_only_for_new_followed_buy_then_disarms(tmp_path: Path,
                                                               monkeypatch) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    monkeypatch.setattr(once, "QUEUE", queue_path)
    monkeypatch.setattr(once, "JOURNAL", tmp_path / "execution.sqlite3")
    monkeypatch.setattr(once, "LEDGER", tmp_path / "auto.sqlite3")
    monkeypatch.setattr(once, "RESULT", tmp_path / "result.json")
    monkeypatch.setattr(once, "_preflight", lambda: None)
    monkeypatch.setattr(once, "_sidecar_healthy", lambda: True)
    monkeypatch.setattr(once, "_refresh_following", lambda: {"followed"})
    monkeypatch.setattr(once, "_receipt_details", lambda _: {"receiptBlock": 123})
    states: list[str] = []
    monkeypatch.setattr(once, "arm_auto", lambda *args, **kwargs: states.append("armed"))
    monkeypatch.setattr(once, "disarm", lambda *args, **kwargs: states.append("disarmed"))
    monkeypatch.setattr(once.ExecutionService, "process_one", lambda _: {
        "status": "processed", "reason": "broadcast_confirmed", "simulation_success": True,
        "broadcastSent": True, "selectedProtocol": "pons_v4_reviewed_hook",
        "transactionHash": "0x" + "1" * 64, "receiptStatus": 1,
        "tokenOut": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
    })

    def enqueue_after_watermark(document: dict[str, object]) -> None:
        if document["state"] != "waiting":
            return
        now = datetime.now(timezone.utc).isoformat()
        event = {"id": "real-format-event", "userId": "followed", "type": "swap_buy",
                 "networkId": 4663,
                 "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                 "createdAt": now}
        queue = DurableSignalQueue(queue_path)
        try:
            queue.enqueue(deterministic_signal_id("fomo_push", event["id"]),
                          "fomo_push", "raw_fomo", event, now)
        finally:
            queue.close()

    monkeypatch.setattr(once, "_write_result", enqueue_after_watermark)
    result = once.run(max_wait_minutes=0)
    assert states[0:2] == ["armed", "disarmed"]
    assert states[-1] == "disarmed"
    assert result["broadcastSent"] is True
    assert result["receiptBlock"] == 123
    assert result["eligibleRhBuys"] == 1


def test_one_shot_reuses_queue_service_after_rejected_candidate(tmp_path: Path,
                                                                 monkeypatch) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    monkeypatch.setattr(once, "QUEUE", queue_path)
    monkeypatch.setattr(once, "JOURNAL", tmp_path / "execution.sqlite3")
    monkeypatch.setattr(once, "LEDGER", tmp_path / "auto.sqlite3")
    monkeypatch.setattr(once, "_preflight", lambda: None)
    monkeypatch.setattr(once, "_sidecar_healthy", lambda: True)
    monkeypatch.setattr(once, "_refresh_following", lambda: {"followed"})
    monkeypatch.setattr(once, "load_followed_ids", lambda _: {"followed"})
    monkeypatch.setattr(once, "arm_auto", lambda *args, **kwargs: None)
    monkeypatch.setattr(once, "disarm", lambda *args, **kwargs: None)
    monkeypatch.setattr(once, "_receipt_details", lambda _: {"receiptBlock": 123})
    calls: list[str] = []

    def execute(_, intent, signal, **kwargs):
        calls.append(signal.source_event_id)
        if len(calls) == 1:
            raise ValueError("rh_auto_no_supported_route")
        return {"status": "broadcast_confirmed", "simulation_success": True,
                "broadcastSent": True, "transactionHash": "0x" + "1" * 64,
                "receiptStatus": 1}

    monkeypatch.setattr(once.RhAutoExecutor, "execute", execute)

    def write_result(document: dict[str, object]) -> None:
        if document["state"] != "waiting":
            return
        queue = DurableSignalQueue(queue_path)
        try:
            for index in (1, 2):
                now = datetime.now(timezone.utc).isoformat()
                event = {"id": f"candidate-{index}", "userId": "followed",
                         "type": "swap_buy", "networkId": 4663,
                         "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                         "createdAt": now}
                queue.enqueue(deterministic_signal_id("fomo_push", event["id"]),
                              "fomo_push", "raw_fomo", event, now)
        finally:
            queue.close()

    monkeypatch.setattr(once, "_write_result", write_result)
    result = once.run(max_wait_minutes=1, ignore_source_clock=True)
    assert calls == ["candidate-1", "candidate-2"]
    assert result["state"] == "completed"
    assert result["broadcastSent"] is True
    assert result["rejectedRhBuys"] == 1


def test_one_shot_refuses_unhealthy_sidecar_before_arming(tmp_path: Path,
                                                          monkeypatch) -> None:
    monkeypatch.setattr(once, "JOURNAL", tmp_path / "execution.sqlite3")
    monkeypatch.setattr(once, "_preflight", lambda: None)
    monkeypatch.setattr(once, "_sidecar_healthy", lambda: False)
    monkeypatch.setattr(once, "arm_auto", lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("must not arm")))
    import pytest
    with pytest.raises(ValueError, match="rh_auto_sidecar_unhealthy"):
        once.run(max_wait_minutes=1)


def test_one_shot_stops_if_sidecar_drops_while_waiting(tmp_path: Path,
                                                        monkeypatch) -> None:
    monkeypatch.setattr(once, "QUEUE", tmp_path / "queue.sqlite3")
    monkeypatch.setattr(once, "JOURNAL", tmp_path / "execution.sqlite3")
    monkeypatch.setattr(once, "LEDGER", tmp_path / "auto.sqlite3")
    monkeypatch.setattr(once, "_preflight", lambda: None)
    monkeypatch.setattr(once, "_refresh_following", lambda: {"followed"})
    checks = iter((True, False))
    monkeypatch.setattr(once, "_sidecar_healthy", lambda: next(checks))
    monkeypatch.setattr(once, "_write_result", lambda _: None)
    monkeypatch.setattr(once, "arm_auto", lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("must not arm")))
    import pytest
    with pytest.raises(ValueError, match="rh_auto_sidecar_unhealthy"):
        once.run(max_wait_minutes=1)
