from __future__ import annotations

import io
import unittest
from unittest.mock import patch

from scripts import secret_store_bridge


class SecretStoreBridgeTests(unittest.TestCase):
    def test_dedicated_session_migration_uses_keyring_without_stdout(self) -> None:
        output = io.StringIO()
        with patch("sys.argv", ["secret_store_bridge", "migrate-session"]), \
                patch("pathlib.Path.is_file", return_value=True), \
                patch("scripts.secret_store_bridge.dotenv_values", return_value={
                    "FOMO_ACCESS_TOKEN": "test-access", "FOMO_REFRESH_TOKEN": "test-refresh",
                }) as parse, \
                patch("scripts.secret_store_bridge.keyring.set_password") as store, \
                patch("sys.stdout", output):
            self.assertEqual(secret_store_bridge.main(), 0)
        self.assertEqual(parse.call_args.args[0].as_posix(), "data/.fomo-session.env")
        self.assertEqual(store.call_count, 2)
        self.assertEqual(output.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
