"""Bounded append-only NDJSON audit archives with daily gzip rotation."""
from __future__ import annotations

import gzip
import atexit
import json
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_locks: dict[str, threading.Lock] = {}
_guard = threading.Lock()
_writers: dict[str, "AsyncAuditWriter"] = {}


def append_ndjson(path: str | Path, record: dict[str, Any], retention_days: int = 30) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with _guard:
        lock = _locks.setdefault(str(target.resolve()), threading.Lock())
    with lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        legacy_marker = target.with_name(target.name + ".active-day")
        rotation_marker = target.with_name(target.name + ".rotation.json")
        generation = 0
        try:
            rotation = json.loads(rotation_marker.read_text(encoding="utf-8"))
            active_day = str(rotation.get("activeDay") or today)
            generation = max(0, int(rotation.get("generation") or 0))
            try:
                legacy_day = legacy_marker.read_text(encoding="ascii").strip()
                if legacy_day and legacy_day != active_day:
                    active_day = legacy_day
            except OSError:
                pass
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            try:
                active_day = legacy_marker.read_text(encoding="ascii").strip() or today
            except OSError:
                active_day = today
        if active_day != today and target.exists() and target.stat().st_size:
            suffix = "" if generation == 0 else f".g{generation}"
            archive = target.with_name(f"{target.name}.{active_day}{suffix}.gz")
            temporary = archive.with_suffix(archive.suffix + ".tmp")
            with target.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as output:
                while chunk := source.read(1024 * 1024):
                    output.write(chunk)
            os.replace(temporary, archive)
            target.write_bytes(b"")
            generation += 1
        marker_temp = rotation_marker.with_suffix(rotation_marker.suffix + ".tmp")
        marker_temp.write_text(
            json.dumps({"activeDay": today, "generation": generation}, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(marker_temp, rotation_marker)
        legacy_marker.write_text(today, encoding="ascii")
        with target.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(2, int(retention_days)))
        for archive in target.parent.glob(target.name + ".*.gz"):
            try:
                if datetime.fromtimestamp(archive.stat().st_mtime, timezone.utc) < cutoff:
                    archive.unlink()
            except OSError:
                continue


class AuditBackpressureError(RuntimeError):
    pass


class AsyncAuditWriter:
    """Bounded single writer that keeps compression off latency-sensitive callers."""

    def __init__(self, path: Path, retention_days: int, max_queue: int = 10_000):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.retention_days = retention_days
        self.queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=max(1, max_queue))
        self.stopping = threading.Event()
        self.last_error: str | None = None
        self.persisted = 0
        self.thread = threading.Thread(target=self._run, name=f"audit-writer-{path.name}", daemon=True)
        self.thread.start()

    def enqueue(self, record: dict[str, Any]) -> float:
        started = time.perf_counter()
        try:
            self.queue.put(record, timeout=0.05)
        except queue.Full as exc:
            raise AuditBackpressureError("audit queue is full") from exc
        return round((time.perf_counter() - started) * 1000, 3)

    def _run(self) -> None:
        while True:
            item = self.queue.get()
            if item is None:
                self.queue.task_done()
                return
            while not self.stopping.is_set():
                try:
                    append_ndjson(self.path, item, self.retention_days)
                except Exception as exc:
                    self.last_error = type(exc).__name__
                    self.stopping.wait(1.0)
                    continue
                self.last_error = None
                self.persisted += 1
                break
            self.queue.task_done()

    def close(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.01)
        self.stopping.set()
        try:
            self.queue.put_nowait(None)
        except queue.Full:
            pass
        self.thread.join(timeout=max(0.0, deadline - time.monotonic()))


def enqueue_ndjson(
    path: str | Path,
    record: dict[str, Any],
    retention_days: int = 30,
    max_queue: int = 10_000,
) -> float:
    target = Path(path)
    key = str(target.resolve())
    with _guard:
        writer = _writers.get(key)
        if writer is None:
            writer = AsyncAuditWriter(target, max(2, int(retention_days)), max_queue)
            _writers[key] = writer
    return writer.enqueue(record)


def shutdown_audit_writers() -> None:
    with _guard:
        writers = list(_writers.values())
        _writers.clear()
    for writer in writers:
        writer.close()


atexit.register(shutdown_audit_writers)
