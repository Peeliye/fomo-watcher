"""Internal stdin/stdout bridge for sidecars; never logs credential values."""

from __future__ import annotations

import json
import sys

import keyring


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
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
