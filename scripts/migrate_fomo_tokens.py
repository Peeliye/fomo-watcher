"""Move a legacy Fomo session file into the OS credential store without outputting secrets."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import keyring
from dotenv import dotenv_values


SERVICE = "codex-fomo-watcher"


def migrate(path: Path, *, redact: bool = True) -> dict[str, object]:
    values = dotenv_values(path)
    access = str(values.get("FOMO_ACCESS_TOKEN") or "").strip()
    refresh = str(values.get("FOMO_REFRESH_TOKEN") or "").strip()
    if not access or not refresh:
        return {"ok": False, "reason": "tokens_not_found", "path": str(path)}
    try:
        keyring.set_password(SERVICE, "access_token", access)
        keyring.set_password(SERVICE, "refresh_token", refresh)
    except Exception:
        os.chmod(path, 0o600)
        return {
            "ok": True, "storedIn": "permission_restricted_disk_fallback",
            "legacyFileRedacted": False, "reason": "system_secret_store_unavailable", "path": str(path),
        }
    if redact:
        retained = [
            line for line in path.read_text(encoding="utf-8").splitlines()
            if not line.startswith(("FOMO_ACCESS_TOKEN=", "FOMO_REFRESH_TOKEN="))
        ]
        temporary = path.with_suffix(path.suffix + ".migrating")
        temporary.write_text("\n".join(retained) + ("\n" if retained else ""), encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    return {"ok": True, "storedIn": "system_secret_store", "legacyFileRedacted": redact, "path": str(path)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="data/.fomo-session.env")
    parser.add_argument("--keep-file", action="store_true", help="keep legacy values but force minimum permissions")
    args = parser.parse_args()
    path = Path(args.path)
    result = migrate(path, redact=not args.keep_file)
    if args.keep_file and path.exists():
        os.chmod(path, 0o600)
    print({key: value for key, value in result.items() if key not in {"access", "refresh"}})


if __name__ == "__main__":
    main()
