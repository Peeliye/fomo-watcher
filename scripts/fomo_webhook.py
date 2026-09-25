"""Loopback-only FOMO event ingress; durable enqueue, never execute a trade.

The sender forwards the original ``trading_activity`` payload (or its FOMO
``data`` frame) as JSON. A separate execution service owns all trade decisions.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import re
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping

from dotenv import dotenv_values

from fomo.execution.journal import ExecutionJournal
from fomo.execution.rh_auto_executor import RhAutoExecutor
from fomo.execution.service import ExecutionService, load_followed_ids
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_QUEUE = PROJECT_DIR / "data" / "signal-queue.sqlite3"
MAX_BODY_BYTES = 131_072
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def _event_payload(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValueError("invalid_event")
    if document.get("type") == "data":
        if document.get("topicType") != "trading_activity" or not isinstance(document.get("payload"), dict):
            raise ValueError("invalid_event_frame")
        payload: dict[str, Any] = document["payload"]
    else:
        payload = document
    body_value = payload.get("body")
    body = dict(body_value) if isinstance(body_value, dict) else {}
    event = {**body, **payload}
    source_id = str(event.get("id") or event.get("tradeId") or "").strip()
    if not source_id or len(source_id) > 256:
        raise ValueError("missing_source_event_id")
    if str(event.get("networkId") or "") != "4663":
        raise ValueError("unsupported_chain")
    if str(event.get("type") or "").lower() not in {"swap_buy", "single_user_buy"}:
        raise ValueError("unsupported_event_type")
    if not _ADDRESS.fullmatch(str(event.get("tokenAddress") or "")):
        raise ValueError("invalid_token_address")
    if not str(event.get("userId") or "").strip():
        raise ValueError("missing_user_id")
    try:
        timestamp = datetime.fromisoformat(str(event.get("createdAt") or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid_event_timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError("invalid_event_timestamp")
    return payload


def make_handler(*, queue_path: Path, bearer_token: str,
                 on_enqueue: Callable[[], None] | None = None,
                 worker_healthy: Callable[[], bool] | None = None) -> type[BaseHTTPRequestHandler]:
    if not bearer_token or len(bearer_token) < 32:
        raise ValueError("webhook_token_missing_or_short")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            # Standard HTTP logging includes user-controlled paths and headers.
            return

        def _reply(self, code: int, body: Mapping[str, object]) -> None:
            encoded = json.dumps(dict(body), separators=(",", ":")).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self) -> None:
            if self.path != "/fomo/4663/buy":
                self._reply(404, {"error": "not_found"})
                return
            header = self.headers.get("Authorization", "")
            expected = "Bearer " + bearer_token
            if not hmac.compare_digest(header, expected):
                self._reply(401, {"error": "unauthorized"})
                return
            if worker_healthy is not None and not worker_healthy():
                self._reply(503, {"error": "execution_worker_unavailable"})
                return
            if self.headers.get("Transfer-Encoding"):
                self._reply(400, {"error": "chunked_body_not_supported"})
                return
            if self.headers.get_content_type() != "application/json":
                self._reply(415, {"error": "json_required"})
                return
            try:
                size = int(self.headers.get("Content-Length", ""))
            except ValueError:
                size = 0
            if size < 1 or size > MAX_BODY_BYTES:
                self._reply(413, {"error": "invalid_body_size"})
                return
            try:
                payload = _event_payload(json.loads(self.rfile.read(size)))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                reason = str(exc) if isinstance(exc, ValueError) and str(exc) in {
                    "invalid_event", "invalid_event_frame", "missing_source_event_id",
                    "unsupported_chain", "unsupported_event_type", "invalid_token_address",
                    "missing_user_id", "invalid_event_timestamp",
                } else "invalid_json"
                self._reply(422, {"error": reason})
                return
            body_value = payload.get("body")
            body = dict(body_value) if isinstance(body_value, dict) else {}
            event = {**body, **payload}
            signal_id = deterministic_signal_id("fomo_push", str(event.get("id") or event.get("tradeId")).strip())
            received_at = datetime.now(timezone.utc).isoformat()
            try:
                queue = DurableSignalQueue(queue_path)
                try:
                    inserted = queue.enqueue(signal_id, "fomo_push", "raw_fomo", payload, received_at)
                finally:
                    queue.close()
            except Exception:
                # SQLite exceptions may include filesystem details. No trade occurs.
                self._reply(503, {"error": "durable_enqueue_failed"})
                return
            # The realtime sidecar may have durably inserted the same event
            # before forwarding it here. A duplicate POST is still the wakeup
            # for the local worker; queue claiming remains idempotent.
            if on_enqueue is not None:
                on_enqueue()
            self._reply(202 if inserted else 200, {
                "status": "queued" if inserted else "duplicate",
                "signalId": signal_id,
                "tradeExecuted": False,
            })

    return Handler


def _run_auto_worker(*, wake: threading.Event, ready: threading.Event | None = None,
                     failed: threading.Event,
                     stop: threading.Event, queue_path: Path, journal_path: Path,
                     following_path: Path, native_in_wei: int,
                     slippage_bps: int, allow_broadcast: bool) -> None:
    queue: DurableSignalQueue | None = None
    journal: ExecutionJournal | None = None
    try:
        queue = DurableSignalQueue(queue_path)
        journal = ExecutionJournal(journal_path)
        executor = RhAutoExecutor(native_in_wei=native_in_wei,
                                  slippage_bps=slippage_bps,
                                  ledger_path=PROJECT_DIR / "data" / "rh-auto-buys.sqlite3",
                                  execution_db_path=journal_path,
                                  allow_broadcast=allow_broadcast)
        service = ExecutionService(queue, journal,
                                   lambda: load_followed_ids(following_path),
                                   lambda _: None, rh_auto_executor=executor)
        if ready is not None:
            ready.set()
        wake.set()  # Recover queued, unacknowledged events after a restart.
        while not stop.is_set():
            # The sidecar writes the durable queue before its best-effort HTTP
            # wakeup. Poll as a fallback so a lost wakeup cannot strand a buy.
            wake.wait(timeout=0.25)
            wake.clear()
            while not stop.is_set():
                result = service.process_one()
                if result is None:
                    break
                print(json.dumps({"signalId": result.get("signalId"),
                                  "status": result.get("status"),
                                  "reason": result.get("reason"),
                                  "simulationSuccess": result.get("simulation_success"),
                                  "broadcastSent": result.get("broadcastSent"),
                                  "transactionHash": result.get("transactionHash"),
                                  "receiptStatus": result.get("receiptStatus")}), flush=True)
    except Exception as error:
        # RPC or filesystem exceptions may contain credentials. Only an error
        # category leaves this process; ingress then refuses more events.
        failed.set()
        print(json.dumps({"autoWorkerFailed": True, "errorType": type(error).__name__}), flush=True)
    finally:
        if journal is not None:
            journal.close()
        if queue is not None:
            queue.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--auto", action="store_true",
                        help="wake a local execution consumer on each durable POST")
    parser.add_argument("--auto-allow-broadcast", action="store_true",
                        help="requires a separately armed database and operator scope")
    parser.add_argument("--native-in-wei", type=int, default=10**12)
    parser.add_argument("--slippage-bps", type=int, default=100)
    parser.add_argument("--journal", type=Path, default=PROJECT_DIR / "data" / "execution.sqlite3")
    parser.add_argument("--following", type=Path, default=PROJECT_DIR / "data" / "following-ids.json")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port_out_of_range")
    if args.auto_allow_broadcast and not args.auto:
        parser.error("broadcast_requires_auto_consumer")
    if args.auto and (args.native_in_wei != 10**12 or args.slippage_bps != 100):
        parser.error("auto_size_or_slippage_outside_confirmed_scope")
    token = os.environ.get("FOMO_WEBHOOK_TOKEN", "") or str(
        dotenv_values(PROJECT_DIR / ".env").get("FOMO_WEBHOOK_TOKEN") or ""
    )
    if len(token) < 32:
        parser.error("webhook_token_missing_or_short")
    wake = threading.Event()
    ready = threading.Event()
    failed = threading.Event()
    stop = threading.Event()
    worker = None
    if args.auto:
        load_followed_ids(args.following.resolve())
    handler = make_handler(queue_path=args.queue.resolve(), bearer_token=token,
                           on_enqueue=wake.set if args.auto else None,
                           worker_healthy=(lambda: worker is not None and worker.is_alive()
                                           and ready.is_set() and not failed.is_set()) if args.auto else None)
    with HTTPServer(("127.0.0.1", args.port), handler) as server:
        if args.auto:
            worker = threading.Thread(target=_run_auto_worker, kwargs={
                "wake": wake, "ready": ready, "failed": failed, "stop": stop,
                "queue_path": args.queue.resolve(), "journal_path": args.journal.resolve(),
                "following_path": args.following.resolve(),
                "native_in_wei": args.native_in_wei, "slippage_bps": args.slippage_bps,
                "allow_broadcast": args.auto_allow_broadcast,
            }, daemon=True)
            worker.start()
        print(json.dumps({"webhookListening": True, "host": "127.0.0.1", "port": args.port,
                          "path": "/fomo/4663/buy", "autoConsumer": args.auto,
                          "allowBroadcast": args.auto_allow_broadcast,
                          "tradingReady": False}), flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            stop.set()
            wake.set()
            if worker is not None:
                worker.join(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
