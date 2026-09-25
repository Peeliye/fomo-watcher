"""The retired Pons policy must never enter generic V3/V4 paths."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from fomo.execution.direct_v3 import CHAIN_CONFIGS as V3_CONFIGS, ROBINHOOD_PROBE_POOL
from fomo.execution.direct_v4 import (CHAIN_CONFIGS as V4_CONFIGS, V4PoolKey,
                                      ROBINHOOD_PROBE_KEY)
from fomo.execution.v4_discovery import INITIALIZE_TOPIC, decode_initialize
from scripts.direct_pons_hook_probe import main as pons_probe

ROOT = Path(__file__).resolve().parents[1]
WORM = "0x3ba500f1ababbcf0f0247d06d1c56fd7e6c4c09d"
PLTR = "0x894e1ec2d74ffe5aef8dc8a9e84686accb964f2a"
HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
POOL_ID = "0xf49ee5b66c4e7163026e5d25e6a1f93cff46dca4d3752f712d181d9392c1d567"


class PonsIsolationTests(unittest.TestCase):
    def test_retired_net_quote_probe_argument_is_disabled(self):
        with self.assertRaisesRegex(ValueError, "pons_probe_arguments_disabled"):
            pons_probe(["--net-quote"])

    def test_generic_modules_do_not_import_retired_policy(self):
        paths = ("fomo/execution/direct_v2.py", "fomo/execution/direct_v3.py",
                 "fomo/execution/direct_v4.py", "fomo/execution/v4_transaction.py",
                 "fomo/execution/v4_discovery.py", "scripts/direct_v4_probe.py")
        for name in paths:
            with self.subTest(name=name):
                source = (ROOT / name).read_text(encoding="utf-8")
                tree = ast.parse(source)
                imports = [node.module or "" for node in ast.walk(tree)
                           if isinstance(node, ast.ImportFrom)]
                imports += [alias.name for node in ast.walk(tree)
                            if isinstance(node, ast.Import) for alias in node.names]
                self.assertFalse(any("pons_v4_hook" in value for value in imports))
                self.assertNotIn(POOL_ID, source.lower())
                self.assertNotIn(HOOK, source.lower())

    def test_4663_v3_and_zero_hook_defaults_remain_generic(self):
        self.assertEqual(V3_CONFIGS[4663].chain_id, 4663)
        self.assertEqual(ROBINHOOD_PROBE_POOL.token0,
                         "0x0bd7d308f8e1639fab988df18a8011f41eacad73")
        ROBINHOOD_PROBE_KEY.validate(V4_CONFIGS[4663])
        self.assertEqual(ROBINHOOD_PROBE_KEY.hooks, "0x" + "00" * 20)
        with self.assertRaisesRegex(ValueError, "v4_pool_key_unapproved"):
            V4PoolKey(WORM, PLTR, 0, 200, HOOK).validate(V4_CONFIGS[4663])

    def test_hook_discovery_is_identity_only_and_never_quotable(self):
        log = {"address": V4_CONFIGS[4663].pool_manager,
               "topics": [INITIALIZE_TOPIC, POOL_ID,
                          "0x" + WORM[2:].rjust(64, "0"),
                          "0x" + PLTR[2:].rjust(64, "0")],
               "data": "0x" + "".join(f"{value:064x}" for value in
                                      (0, 200, int(HOOK, 16), 1 << 96, 0)),
               "blockNumber": "0x64", "blockHash": "0x" + "ab" * 32,
               "removed": False}
        found = decode_initialize(log, chain_id=4663,
                                  expected_pool_id=POOL_ID, expected_token=WORM)
        self.assertTrue(found.identity_verified)
        self.assertFalse(found.quote_allowed)
        self.assertEqual(found.reject_reason, "v4_nonzero_hook_rejected")


if __name__ == "__main__":
    unittest.main()
