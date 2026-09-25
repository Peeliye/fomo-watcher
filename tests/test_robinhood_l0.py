import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from fomo.execution.direct_v2 import CHAIN_CONFIGS as V2_CONFIGS
from fomo.execution.direct_v3 import CHAIN_CONFIGS as V3_CONFIGS, ROBINHOOD_PROBE_POOL
from fomo.execution.direct_v4 import (CHAIN_CONFIGS as V4_CONFIGS,
                                      ROBINHOOD_PROBE_KEY, ROBINHOOD_PROBE_POOL_ID)
from fomo.execution.v3_transaction import (directional_price_limit,
                                           encode_exact_input_single,
                                           decode_exact_input_single)
from scripts.direct_v2_probe import main as v2_probe
from scripts.direct_v3_probe import main as v3_probe
from scripts.direct_v4_probe import main as v4_probe


class RobinhoodL0Tests(unittest.TestCase):
    def test_chain_addresses_and_pool_ids(self):
        self.assertEqual(V2_CONFIGS["4663"].weth, ROBINHOOD_PROBE_POOL.token0)
        self.assertEqual(V2_CONFIGS["4663"].usdc, ROBINHOOD_PROBE_POOL.token1)
        self.assertEqual(V3_CONFIGS[4663].router_variant, "swap_router_02")
        self.assertEqual(V3_CONFIGS[4663].router,
                         "0xcaf681a66d020601342297493863e78c959e5cb2")
        self.assertEqual(V4_CONFIGS[4663].pool_manager,
                         "0x8366a39cc670b4001a1121b8f6a443a643e40951")
        self.assertEqual(ROBINHOOD_PROBE_KEY.pool_id, ROBINHOOD_PROBE_POOL_ID)

    def test_router02_codec_binds_deadline_in_outer_multicall(self):
        target = ROBINHOOD_PROBE_POOL
        data = encode_exact_input_single(
            token_in=target.token0, token_out=target.token1,
            fee=target.fee, recipient="0x" + "11" * 20,
            deadline=1234567890, amount_in=10**15,
            minimum_output=1,
            sqrt_price_limit_x96=directional_price_limit(
                target.token0, target.token0, target.token1),
            token0=target.token0, token1=target.token1, chain_id=4663)
        decoded = decode_exact_input_single(data, chain_id=4663)
        self.assertEqual(decoded["amountIn"], 10**15)
        self.assertEqual(decoded["deadline"], 1234567890)
        malformed = bytearray(data)
        malformed[67] = 0x60
        with self.assertRaisesRegex(ValueError, "v3_multicall_scope_invalid"):
            decode_exact_input_single(bytes(malformed), chain_id=4663)
        inner_length = 4 + 7 * 32
        self.assertEqual(len(data), 4 + 5 * 32 + (inner_length + 31) // 32 * 32)

    def test_missing_robinhood_rpc_never_falls_back_to_other_chain(self):
        for probe in (v2_probe, v3_probe, v4_probe):
            with self.subTest(probe=probe.__module__):
                output = io.StringIO()
                with (patch.dict("os.environ", {"RPC_ROBINHOOD_URL": "",
                                             "RPC_ETHEREUM_URL": "https://example.invalid/eth",
                                             "RPC_BASE_URL": "https://example.invalid/base"}),
                      redirect_stdout(output)):
                    self.assertEqual(probe(["--chain", "4663"]), 2)
                self.assertEqual(json.loads(output.getvalue())["status"], "未注入")


if __name__ == "__main__":
    unittest.main()
