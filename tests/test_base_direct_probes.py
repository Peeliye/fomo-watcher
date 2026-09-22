"""Offline Base L0 routing and pool identity checks; no RPC requests."""

from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from fomo.execution.direct_v2 import CHAIN_CONFIGS as V2_CHAINS
from fomo.execution.direct_v3 import BASE_PROBE_POOL, CHAIN_CONFIGS as V3_CHAINS
from fomo.execution.direct_v4 import BASE_PROBE_KEY, CHAIN_CONFIGS as V4_CHAINS
from scripts.direct_v2_probe import main as v2_main
from scripts.direct_v3_probe import main as v3_main
from scripts.direct_v4_probe import main as v4_main


class BaseDirectProbeTests(unittest.TestCase):
    def test_configured_identity_and_pool_id(self):
        self.assertEqual(V2_CHAINS["8453"].probe_pair,
                         "0x88a43bbdf9d098eec7bceda4e2494615dfd9bb9c")
        self.assertEqual(BASE_PROBE_POOL.fee, 500)
        self.assertEqual(V3_CHAINS[8453].router_variant, "swap_router_02")
        BASE_PROBE_KEY.validate(V4_CHAINS[8453])
        self.assertEqual(BASE_PROBE_KEY.pool_id,
                         "0xe070797535b13431808f8fc81fdbe7b41362960ed0b55bc2b6117c49c51b7eb9")

    def test_no_base_rpc_never_falls_back_to_ethereum_rpc(self):
        for probe in (v2_main, v3_main, v4_main):
            with self.subTest(probe=probe.__module__):
                output = io.StringIO()
                with (patch.dict("os.environ", {"RPC_BASE_URL": "",
                                              "RPC_ETHEREUM_URL": "https://example.invalid/eth"}),
                      patch("dotenv.main.find_dotenv", return_value=""),
                      redirect_stdout(output)):
                    self.assertEqual(probe(["--chain", "8453"]), 2)
                self.assertEqual(json.loads(output.getvalue())["status"], "未注入")


if __name__ == "__main__":
    unittest.main()
