from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from fomo.web.server import public_execution_readiness


class DashboardPrivacyTests(TestCase):
    @patch("fomo.web.server.execution_readiness")
    def test_public_readiness_removes_execution_wallet_details(self, readiness):
        readiness.return_value = {
            "walletId": "primary-multichain",
            "accounts": [{"accountId": "evm", "address": "0xsecret"}],
            "chains": [{"chainId": 1, "accountId": "evm", "address": "0xsecret", "rpcConfigured": True}],
            "stage": "routing",
        }

        result = public_execution_readiness(Path("."), {})

        self.assertNotIn("walletId", result)
        self.assertNotIn("accounts", result)
        self.assertNotIn("accountId", result["chains"][0])
        self.assertNotIn("address", result["chains"][0])
        self.assertTrue(result["chains"][0]["rpcConfigured"])
