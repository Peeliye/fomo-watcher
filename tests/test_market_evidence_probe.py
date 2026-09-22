from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.market_evidence_probe import main


class MarketEvidenceProbeTests(unittest.TestCase):
    def test_missing_credential_reports_false_without_url_or_broadcast(self):
        output = io.StringIO()
        args = ["market_evidence_probe", "--chain-id", "1", "--wallet",
                "0x1111111111111111111111111111111111111111", "--token-in",
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "--token-decimals",
                "18", "--rpc-primary-env", "P03_TEST_UNUSED_RPC"]
        with (patch("sys.argv", args), patch.dict("os.environ", {"PYTH_API_KEY": ""}),
              redirect_stdout(output)):
            self.assertEqual(main(), 2)
        self.assertIn('"ready": false', output.getvalue())
        self.assertIn('"broadcastAttempted": false', output.getvalue())
        self.assertNotIn("https://", output.getvalue())


if __name__ == "__main__":
    unittest.main()
