"""Consistent runtime backup, verification, and guarded restore (never reads .env)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


DATABASES = (
    "state.sqlite3", "portfolio.sqlite3", "execution.sqlite3", "verified-performance.sqlite3",
    "wallet-intelligence.sqlite3", "leaderboard.sqlite3", "rpc-health.sqlite3",
)
FILES = (
    "wallet-registry.json", "watch-wallets.json", "execution-wallet.json", "risk-policy.example.json",
    "data/exit-policy.json", "data/network-settings.json", "data/fast-executor-config.json",
)


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup(project: Path, destination: Path) -> dict[str, object]:
    destination.mkdir(parents=True, exist_ok=False)
    for name in DATABASES:
        source = project / "data" / name
        if not source.exists():
            continue
        read = sqlite3.connect(source)
        write = sqlite3.connect(destination / name)
        try:
            read.backup(write)
        finally:
            write.close()
            read.close()
    for name in FILES:
        source = project / name
        if source.exists():
            shutil.copy2(source, destination / source.name)
    entries = [
        {"name": item.name, "bytes": item.stat().st_size, "sha256": _hash(item)}
        for item in sorted(destination.iterdir()) if item.is_file()
    ]
    manifest = {"version": 1, "createdAt": datetime.now(timezone.utc).isoformat(), "files": entries}
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return verify(destination)


def verify(directory: Path) -> dict[str, object]:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    failures: list[str] = []
    for entry in manifest.get("files", []):
        path = directory / str(entry["name"])
        if not path.exists() or path.stat().st_size != int(entry["bytes"]) or _hash(path) != entry["sha256"]:
            failures.append(str(entry["name"]))
            continue
        if path.suffix == ".sqlite3":
            db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            try:
                if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    failures.append(path.name + ":integrity")
            finally:
                db.close()
    return {"ok": not failures, "directory": str(directory), "failures": failures,
            "files": len(manifest.get("files", []))}


def restore(project: Path, source: Path) -> dict[str, object]:
    validation = verify(source)
    if not validation["ok"]:
        raise ValueError("backup verification failed")
    safety = project / "data" / "backups" / f"pre-restore-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    backup(project, safety)
    targets = {name: project / "data" / name for name in DATABASES}
    targets.update({Path(name).name: project / name for name in FILES})
    for name, target in targets.items():
        archived = source / name
        if not archived.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".restoring")
        shutil.copy2(archived, temporary)
        os.replace(temporary, target)
    return {"ok": True, "restoredFrom": str(source), "preRestoreBackup": str(safety)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("backup", "verify", "restore"))
    parser.add_argument("--project", default=".")
    parser.add_argument("--path")
    args = parser.parse_args()
    project = Path(args.project).resolve()
    if args.path:
        path = Path(args.path).resolve()
    else:
        path = project / "data" / "backups" / f"runtime-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    result = backup(project, path) if args.command == "backup" else verify(path) if args.command == "verify" else restore(project, path)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
