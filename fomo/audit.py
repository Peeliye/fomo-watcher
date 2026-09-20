"""Bounded append-only NDJSON audit archives with daily gzip rotation."""
from __future__ import annotations

import gzip
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_locks: dict[str, threading.Lock] = {}
_guard = threading.Lock()


def append_ndjson(path: str | Path, record: dict[str, Any], retention_days: int = 30) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with _guard:
        lock = _locks.setdefault(str(target.resolve()), threading.Lock())
    with lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        marker = target.with_name(target.name + ".active-day")
        try:
            active_day = marker.read_text(encoding="ascii").strip()
        except OSError:
            active_day = today
        if active_day != today and target.exists() and target.stat().st_size:
            archive = target.with_name(f"{target.name}.{active_day}.gz")
            temporary = archive.with_suffix(archive.suffix + ".tmp")
            with target.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as output:
                while chunk := source.read(1024 * 1024):
                    output.write(chunk)
            os.replace(temporary, archive)
            target.write_bytes(b"")
        marker.write_text(today, encoding="ascii")
        with target.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(2, int(retention_days)))
        for archive in target.parent.glob(target.name + ".*.gz"):
            try:
                if datetime.fromtimestamp(archive.stat().st_mtime, timezone.utc) < cutoff:
                    archive.unlink()
            except OSError:
                continue

