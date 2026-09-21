from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.execution_service import main


class ExecutionServiceReadinessTests(unittest.TestCase):
    def test_readiness_does_not_create_or_migrate_execution_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "execution.sqlite3"
            queue = Path(directory) / "queue.sqlite3"
            output = io.StringIO()
            with patch("sys.argv", ["execution_service", "--readiness", "--journal", str(journal),
                                    "--queue", str(queue)]), patch("sys.stdout", output):
                self.assertEqual(main(), 0)
            result = json.loads(output.getvalue())
            self.assertFalse(any(chain["ready"] for chain in result["chains"]))
            self.assertFalse(journal.exists())
            self.assertFalse(queue.exists())


if __name__ == "__main__":
    unittest.main()
