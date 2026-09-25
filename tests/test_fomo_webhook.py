from __future__ import annotations

import http.client
import json
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import HTTPServer
from pathlib import Path
from unittest.mock import patch

from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id
from scripts.fomo_webhook import _run_auto_worker, make_handler


def _send(port: int, payload: object, *, token: str | None = "t" * 40) -> tuple[int, dict[str, object]]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    connection.request("POST", "/fomo/4663/buy", json.dumps(payload).encode(), headers)
    response = connection.getresponse()
    result = response.status, json.loads(response.read())
    connection.close()
    return result


def test_webhook_durable_enqueue_and_duplicate() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "signal-queue.sqlite3"
        wakeups: list[int] = []
        server = HTTPServer(("127.0.0.1", 0), make_handler(
            queue_path=path, bearer_token="t" * 40,
            on_enqueue=lambda: wakeups.append(1)))
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            event = {
                "id": "real-format-event-1", "tradeId": "trade-1", "userId": "followed",
                "type": "swap_buy", "networkId": 4663,
                "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }
            frame = {"type": "data", "topicType": "trading_activity", "payload": event}
            assert _send(server.server_port, frame, token=None)[0] == 401
            status, result = _send(server.server_port, frame)
            assert status == 202
            assert result == {"status": "queued", "signalId": deterministic_signal_id("fomo_push", event["id"]),
                              "tradeExecuted": False}
            assert _send(server.server_port, frame)[0] == 200
            assert len(wakeups) == 2
            queue = DurableSignalQueue(path)
            try:
                rows = queue.db.execute("SELECT source,payload_kind,status,payload_json FROM signal_queue").fetchall()
                assert len(rows) == 1
                assert tuple(rows[0])[:3] == ("fomo_push", "raw_fomo", "queued")
                assert json.loads(rows[0]["payload_json"]) == event
            finally:
                queue.close()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=3)


def test_webhook_rejects_untrusted_or_wrong_route_events() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "signal-queue.sqlite3"
        server = HTTPServer(("127.0.0.1", 0), make_handler(queue_path=path, bearer_token="t" * 40))
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            event = {"id": "e1", "userId": "u1", "type": "swap_buy", "networkId": 4663,
                     "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                     "createdAt": datetime.now(timezone.utc).isoformat()}
            for change in ({"networkId": 1}, {"type": "swap_sell"}, {"tokenAddress": "bad"},
                           {"id": ""}, {"userId": ""}, {"createdAt": "bad"}):
                assert _send(server.server_port, {**event, **change})[0] == 422
            assert not path.exists()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=3)


def test_webhook_requires_secret() -> None:
    try:
        make_handler(queue_path=Path("unused"), bearer_token="short")
    except ValueError as error:
        assert str(error) == "webhook_token_missing_or_short"
    else:
        raise AssertionError("short token accepted")


def test_webhook_wake_consumes_raw_event_without_timer_or_live(tmp_path: Path) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    following_path = tmp_path / "following.json"
    following_path.write_text(json.dumps({
        "updatedAt": int(time.time() * 1000), "followingIds": ["followed"],
    }), encoding="utf-8")
    wake, ready, failed, stop = (threading.Event(), threading.Event(),
                                 threading.Event(), threading.Event())
    server = HTTPServer(("127.0.0.1", 0), make_handler(
        queue_path=queue_path, bearer_token="t" * 40,
        on_enqueue=wake.set, worker_healthy=lambda: ready.is_set() and not failed.is_set(),
    ))
    http_worker = threading.Thread(target=server.serve_forever, daemon=True)
    http_worker.start()
    with patch("fomo.execution.rh_auto_executor.RhAutoExecutor.execute", return_value={
        "status": "simulated", "simulation_success": True,
        "broadcastSent": False, "protocol": "pons_v4_reviewed_hook",
    }) as execute:
        trade_worker = threading.Thread(target=_run_auto_worker, kwargs={
            "wake": wake, "ready": ready, "failed": failed, "stop": stop,
            "queue_path": queue_path, "journal_path": tmp_path / "execution.sqlite3",
            "following_path": following_path, "native_in_wei": 10**12,
            "slippage_bps": 100, "allow_broadcast": False,
        }, daemon=True)
        trade_worker.start()
        try:
            assert ready.wait(timeout=3)
            event = {"id": "wake-event", "userId": "followed", "type": "swap_buy",
                     "networkId": 4663,
                     "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                     "createdAt": datetime.now(timezone.utc).isoformat()}
            assert _send(server.server_port, event)[0] == 202
            deadline = time.monotonic() + 3
            state = None
            while time.monotonic() < deadline:
                queue = DurableSignalQueue(queue_path)
                try:
                    state = queue.status(deterministic_signal_id("fomo_push", "wake-event"))
                finally:
                    queue.close()
                if state is not None and state["status"] == "processed":
                    break
                time.sleep(0.01)
            assert state is not None and state["status"] == "processed"
            assert not failed.is_set()
            assert execute.call_args.kwargs["live_armed"] is False
        finally:
            stop.set()
            wake.set()
            trade_worker.join(timeout=3)
            server.shutdown()
            server.server_close()
            http_worker.join(timeout=3)


def test_worker_recovers_durable_event_when_http_wakeup_is_lost(tmp_path: Path) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    following_path = tmp_path / "following.json"
    following_path.write_text(json.dumps({
        "updatedAt": int(time.time() * 1000), "followingIds": ["followed"],
    }), encoding="utf-8")
    wake, ready, failed, stop = (threading.Event(), threading.Event(),
                                 threading.Event(), threading.Event())
    with patch("fomo.execution.rh_auto_executor.RhAutoExecutor.execute", return_value={
        "status": "simulated", "simulation_success": True, "broadcastSent": False,
        "protocol": "pons_v4_reviewed_hook",
    }) as execute:
        worker = threading.Thread(target=_run_auto_worker, kwargs={
            "wake": wake, "ready": ready, "failed": failed, "stop": stop,
            "queue_path": queue_path, "journal_path": tmp_path / "execution.sqlite3",
            "following_path": following_path, "native_in_wei": 10**12,
            "slippage_bps": 100, "allow_broadcast": False,
        }, daemon=True)
        worker.start()
        try:
            assert ready.wait(timeout=3)
            # Let the startup wake drain, then enqueue exactly as the sidecar
            # does when its best-effort HTTP forward fails.
            time.sleep(0.1)
            now = datetime.now(timezone.utc).isoformat()
            event = {"id": "lost-wake-event", "userId": "followed", "type": "swap_buy",
                     "networkId": 4663,
                     "tokenAddress": "0x314ad0f11422842d28b4f950a64cd40fafb029fd",
                     "createdAt": now}
            queue = DurableSignalQueue(queue_path)
            try:
                signal_id = deterministic_signal_id("fomo_push", event["id"])
                queue.enqueue(signal_id, "fomo_push", "raw_fomo", event, now)
            finally:
                queue.close()
            deadline = time.monotonic() + 3
            state = None
            while time.monotonic() < deadline:
                queue = DurableSignalQueue(queue_path)
                try:
                    state = queue.status(signal_id)
                finally:
                    queue.close()
                if state is not None and state["status"] == "processed":
                    break
                time.sleep(0.01)
            assert state is not None and state["status"] == "processed"
            assert execute.call_args.kwargs["live_armed"] is False
            assert not failed.is_set()
        finally:
            stop.set()
            wake.set()
            worker.join(timeout=3)
