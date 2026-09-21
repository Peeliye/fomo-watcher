from __future__ import annotations

import unittest
from decimal import Decimal

from fomo.watching.adapter import ChainCheckpoint
from fomo.watching.decoder import EvmSwapDecoder, SolanaSwapDecoder
from fomo.watching.evm_rpc import TRANSFER_TOPIC, EvmRpcProvider
from fomo.watching.rpc_transport import RpcUnavailable
from fomo.watching.solana_rpc import SolanaRpcProvider


ACTOR = "0x1111111111111111111111111111111111111111"
ROUTER = "0x2222222222222222222222222222222222222222"
USDC = "0x3333333333333333333333333333333333333333"
TOKEN = "0x4444444444444444444444444444444444444444"


def transfer(token: str, sender: str, recipient: str, amount: int, index: int) -> dict:
    return {"address": token, "topics": [TRANSFER_TOPIC, "0x" + sender[2:].rjust(64, "0"),
                                           "0x" + recipient[2:].rjust(64, "0")],
            "data": hex(amount), "logIndex": hex(index)}


class EvmRpcFixture:
    def call(self, method, params):
        if method == "eth_getBlockByNumber":
            tag, full = params
            if tag == "finalized":
                return {"number": "0x64", "hash": "h100"}
            if tag == "latest":
                return {"number": "0x65", "hash": "h101"}
            if tag == "0x64":
                return {"number": "0x64", "hash": "h100"}
            if tag == "0x65":
                row: dict[str, object] = {"number": "0x65", "hash": "h101", "parentHash": "h100", "timestamp": "0x5f5e100"}
                if full:
                    row["transactions"] = [{"from": ACTOR, "to": ROUTER, "hash": "0xtx", "nonce": "0x7"}]
                return row
        if method == "eth_getTransactionReceipt":
            return {"blockHash": "h101", "status": "0x1", "logs": [
                transfer(USDC, ACTOR, ROUTER, 10_000_000, 2),
                transfer(TOKEN, ROUTER, ACTOR, 5_000_000_000_000_000_000, 3),
            ]}
        raise AssertionError((method, params))


class SolanaRpcFixture:
    def call(self, method, params):
        if method == "getSlot":
            return 101
        if method == "getBlock":
            return {"blockhash": "h101"} if params[0] == 101 else {"blockhash": "h100"}
        if method == "getSignaturesForAddress":
            return [{"signature": "sig101", "slot": 101, "err": None, "confirmationStatus": "confirmed"}]
        if method == "getTransaction":
            return {"slot": 101, "blockTime": 100_000_000, "transaction": {
                "message": {"accountKeys": ["actor", "router"],
                            "instructions": [{"programId": "swap-program"}]},
            }, "meta": {"err": None, "innerInstructions": [{"index": 0, "instructions": [
                {"programId": "swap-program"}]}],
                "preTokenBalances": [
                    {"owner": "actor", "mint": "USDC", "uiTokenAmount": {"amount": "10000000"}},
                    {"owner": "actor", "mint": "TOKEN", "uiTokenAmount": {"amount": "0"}},
                ], "postTokenBalances": [
                    {"owner": "actor", "mint": "USDC", "uiTokenAmount": {"amount": "0"}},
                    {"owner": "actor", "mint": "TOKEN", "uiTokenAmount": {"amount": "5000000000"}},
                ]}}
        raise AssertionError((method, params))


class RpcProviderTests(unittest.TestCase):
    def test_evm_receipt_transfer_deltas_decode_only_router_swap(self):
        provider = EvmRpcProvider("1", EvmRpcFixture(), allowed_routers={ROUTER},
                                  quote_assets={USDC: (6, Decimal("1"))})
        events, checkpoint = provider.events_after(ChainCheckpoint("1", "100", 100, "h100"), [ACTOR])
        self.assertEqual(checkpoint.block_number, 101)
        self.assertEqual(len(events), 1)
        decoded = EvmSwapDecoder({USDC}, {ROUTER}).decode(events[0], ACTOR)
        assert decoded is not None
        self.assertEqual((decoded.side, decoded.token_out, decoded.estimated_usd), ("buy", TOKEN, "10"))
        with self.assertRaisesRegex(RpcUnavailable, "gap"):
            EvmRpcProvider("1", EvmRpcFixture(), allowed_routers={ROUTER}, maximum_blocks_per_poll=1).events_after(
                ChainCheckpoint("1", "99", 99, "h99"), [ACTOR])

    def test_solana_signature_and_inner_instruction_swap(self):
        provider = SolanaRpcProvider(SolanaRpcFixture(), allowed_program_ids={"swap-program"},
                                     quote_assets={"USDC": (6, Decimal("1"))})
        events, checkpoint = provider.events_after(
            ChainCheckpoint("1399811149", "100", 100, "h100"), ["actor"])
        self.assertEqual(checkpoint.block_hash, "h101")
        self.assertEqual(len(events), 1)
        decoded = SolanaSwapDecoder({"USDC"}, {"swap-program"}).decode(events[0], "actor")
        assert decoded is not None
        self.assertEqual((decoded.side, decoded.token_out, decoded.estimated_usd), ("buy", "TOKEN", "10"))


if __name__ == "__main__":
    unittest.main()
