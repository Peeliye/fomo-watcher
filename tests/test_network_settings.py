from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fomo.execution.networks import apply_network_settings, save_enabled_chain_ids


class NetworkSettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.cfg = {
            "copy_trading": {"network_ids": [1, 56]},
            "execution": {"enabled_chain_ids": [1, 56]},
            "rpc_pool": {"endpoints": {"1": [], "56": []}},
            "rpc_management": {"network_settings_path": "data/networks.json"},
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_persisted_networks_override_both_runtime_gates(self) -> None:
        path = self.root / "data" / "networks.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"enabledChainIds": [56]}), encoding="utf-8")
        enabled = apply_network_settings(self.root, self.cfg)
        self.assertEqual(enabled, ["56"])
        self.assertEqual(self.cfg["copy_trading"]["network_ids"], [56])
        self.assertEqual(self.cfg["execution"]["enabled_chain_ids"], [56])

    def test_all_networks_may_be_disabled_as_master_stop(self) -> None:
        enabled = save_enabled_chain_ids(self.root, self.cfg, [])
        self.assertEqual(enabled, [])
        self.assertEqual(self.cfg["_runtime_enabled_chain_ids"], [])


if __name__ == "__main__":
    unittest.main()
