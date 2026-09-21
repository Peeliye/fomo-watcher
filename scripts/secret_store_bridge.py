"""Internal stdin/stdout bridge for sidecars; never logs credential values."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import keyring
from dotenv import dotenv_values


SERVICE = "codex-fomo-watcher"


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "store":
        payload = json.load(sys.stdin)
        access, refresh = str(payload.get("access") or ""), str(payload.get("refresh") or "")
        if not access or not refresh:
            return 2
        keyring.set_password(SERVICE, "access_token", access)
        keyring.set_password(SERVICE, "refresh_token", refresh)
        return 0
    if command == "get-access":
        value = keyring.get_password(SERVICE, "access_token") or ""
        if not value:
            return 3
        sys.stdout.write(value)
        return 0
    if command == "migrate-session":
        # Only the dedicated fallback file is eligible; never inspect .env.
        path = Path("data/.fomo-session.env")
        if not path.is_file():
            return 4
        payload = dotenv_values(path)
        access = str(payload.get("FOMO_ACCESS_TOKEN") or "")
        refresh = str(payload.get("FOMO_REFRESH_TOKEN") or "")
        if not access or not refresh:
            return 5
        keyring.set_password(SERVICE, "access_token", access)
        keyring.set_password(SERVICE, "refresh_token", refresh)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
