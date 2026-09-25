import unittest

from fomo.execution.direct_v4 import V4PoolKey
from fomo.execution.v4_discovery import INITIALIZE_TOPIC, decode_initialize, discover_pool


WORM_TOKEN = "0x3ba500f1ababbcf0f0247d06d1c56fd7e6c4c09d"
WORM_POOL_ID = "0xf49ee5b66c4e7163026e5d25e6a1f93cff46dca4d3752f712d181d9392c1d567"
PLTR = "0x894e1ec2d74ffe5aef8dc8a9e84686accb964f2a"
HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
HASH = "0x" + "12" * 32
MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"


def fixture_log(*, hooks=HOOK, block_hash=HASH, tick=0):
    words = (0, 200, int(hooks, 16), 1 << 96, tick % (1 << 256))
    return {"address": MANAGER, "topics": [INITIALIZE_TOPIC, WORM_POOL_ID,
            "0x" + WORM_TOKEN[2:].rjust(64, "0"),
            "0x" + PLTR[2:].rjust(64, "0")],
            "data": "0x" + "".join(f"{value:064x}" for value in words),
            "blockNumber": "0x64", "blockHash": block_hash, "removed": False}


class FixtureRpc:
    def __init__(self, log):
        self.log = log
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_getBlockByNumber":
            return {"number": "0x64", "hash": HASH}
        if method == "eth_getLogs":
            return [self.log]
        raise AssertionError(method)


class V4DiscoveryTests(unittest.TestCase):
    def test_known_pool_key_rehashes_and_rejects_hooked_quote(self):
        result = decode_initialize(fixture_log(), chain_id=4663,
                                   expected_pool_id=WORM_POOL_ID, expected_token=WORM_TOKEN)
        self.assertEqual(result.key, V4PoolKey(WORM_TOKEN, PLTR, 0, 200, HOOK))
        self.assertEqual(result.key.pool_id, WORM_POOL_ID)
        self.assertTrue(result.identity_verified)
        self.assertFalse(result.quote_allowed)
        self.assertEqual(result.reject_reason, "v4_nonzero_hook_rejected")

    def test_same_block_log_and_header_are_required(self):
        rpc = FixtureRpc(fixture_log())
        result = discover_pool(rpc, chain_id=4663, pool_id=WORM_POOL_ID, token=WORM_TOKEN)
        self.assertEqual(result.block_hash, HASH)
        self.assertEqual([method for method, _ in rpc.calls],
                         ["eth_getBlockByNumber", "eth_getLogs",
                          "eth_getBlockByNumber", "eth_getLogs"])
        self.assertEqual(rpc.calls[-1][1][0]["blockHash"], HASH)
        with self.assertRaisesRegex(ValueError, "v4_initialize_block_reorged"):
            discover_pool(FixtureRpc(fixture_log(block_hash="0x" + "34" * 32)),
                          chain_id=4663, pool_id=WORM_POOL_ID, token=WORM_TOKEN)

    def test_wrong_pool_identity_has_no_quote(self):
        result = decode_initialize(fixture_log(hooks="0x" + "00" * 20), chain_id=4663,
                                   expected_pool_id=WORM_POOL_ID, expected_token=WORM_TOKEN)
        self.assertFalse(result.identity_verified)
        self.assertFalse(result.quote_allowed)
        self.assertEqual(result.reject_reason, "v4_pool_identity_mismatch")

    def test_negative_initial_tick_is_valid_abi_int24(self):
        result = decode_initialize(fixture_log(tick=-887), chain_id=4663,
                                   expected_pool_id=WORM_POOL_ID, expected_token=WORM_TOKEN)
        self.assertTrue(result.identity_verified)
        self.assertFalse(result.quote_allowed)


if __name__ == "__main__":
    unittest.main()
