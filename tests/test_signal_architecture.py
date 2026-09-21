from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from fomo.execution.fast_path import evaluate_copy_buy
from fomo.signals import FomoCopyStrategy, FomoPushAdapter, WalletCopyStrategy
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.watching import CheckpointStore, EvmSwapDecoder, EvmWalletRpcAdapter
from fomo.watching.adapter import ChainCheckpoint


ROOT = Path(__file__).resolve().parents[1]


class Provider:
    def __init__(self) -> None:
        self.calls = 0
        self.pending: list[dict] = []
        self.hashes = {99: "h99", 100: "h100", 101: "h101"}

    def head(self):
        return ChainCheckpoint("1", "100", 100, self.hashes[100])

    def canonical_hash(self, block_number):
        return self.hashes.get(block_number)

    def events_after(self, checkpoint, wallets):
        self.calls += 1
        return self.pending, ChainCheckpoint("1", "101", 101, self.hashes[101])


class SignalArchitectureTests(unittest.TestCase):
    def test_envelope_is_immutable_and_source_dedupe_keys_are_distinct(self):
        values = dict(source_event_id="same", observed_at="2026-09-21T00:00:01Z",
                      source_timestamp="2026-09-21T00:00:00Z", delivery_delay_ms=1000,
                      chain_id="1", actor_wallet="0x1111111111111111111111111111111111111111",
                      kol_id=None, side="buy", token_in="USDC", token_out="TOKEN",
                      source_amount="1", estimated_usd="10", tx_hash="0x1", signature=None,
                      log_index=0, instruction_index=None, confirmation_level="confirmed",
                      reorg_key="h1", decoder_version="test@1", raw_payload_hash="a" * 64)
        evm = TradeSignalEnvelope.create(source="wallet_rpc_evm", **values)
        fomo = TradeSignalEnvelope.create(source="fomo_push", **values)
        self.assertNotEqual(evm.signal_id, fomo.signal_id)
        with self.assertRaises(AttributeError):
            evm.side = "sell"  # type: ignore[misc]

    def test_fomo_and_wallet_strategies_reject_each_others_source(self):
        adapter = FomoPushAdapter({"k1"})
        fomo = adapter.normalize({"id": "f1", "type": "swap_buy", "createdAt": "2026-09-21T00:00:00Z",
                                  "userId": "k1", "networkId": 1, "tokenAddress": "TOKEN", "usdAmount": 100},
                                 "2026-09-21T00:00:01Z")
        assert fomo is not None
        self.assertIsNotNone(FomoCopyStrategy(10).create_intent(fomo))
        with self.assertRaises(ValueError):
            WalletCopyStrategy({"fixedUsd": 10}).create_intent(fomo)

    def test_wallet_bootstraps_at_head_and_does_not_scan_history(self):
        with tempfile.TemporaryDirectory() as directory:
            provider = Provider()
            store = CheckpointStore(Path(directory) / "checkpoints.sqlite3")
            adapter = EvmWalletRpcAdapter("evm-1", "1", provider,
                EvmSwapDecoder({"usdc"}, {"0xrouter"}), store)
            batch = adapter.poll(["0xactor"])
            self.assertEqual(batch.events, ())
            self.assertEqual(provider.calls, 0)
            self.assertEqual(store.load("evm-1", "1").block_number, 100)
            store.close()

    def test_wallet_checkpoint_dedupes_and_reports_reorg_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            provider = Provider()
            store = CheckpointStore(Path(directory) / "checkpoints.sqlite3")
            adapter = EvmWalletRpcAdapter("evm-1", "1", provider,
                EvmSwapDecoder({"usdc"}, {"0xrouter"}), store)
            adapter.poll(["0xactor"])
            provider.pending = [{
                "txHash": "0xtx", "nonce": 1, "logIndex": 2, "blockNumber": 101, "blockHash": "h101",
                "blockTime": "2026-09-21T00:00:00Z", "confirmationLevel": "confirmed",
                "actorWallet": "0xactor", "to": "0xrouter", "estimatedUsd": "10",
                "receipt": {"status": 1, "logs": [{}]},
                "preBalances": {"USDC": 100, "TOKEN": 0}, "postBalances": {"USDC": 90, "TOKEN": 5},
            }]
            first = adapter.poll(["0xactor"])
            duplicate = adapter.poll(["0xactor"])
            self.assertEqual((len(first.events), len(duplicate.events)), (1, 0))
            provider.hashes[101] = "fork101"
            provider.pending = []
            rollback = adapter.poll(["0xactor"])
            self.assertEqual(rollback.reverted_event_ids, (first.events[0].event_id,))
            store.close()

    def test_evm_decoder_ignores_one_sided_transfer_and_decodes_swap(self):
        decoder = EvmSwapDecoder({"usdc"}, {"0xrouter"})
        base = {"actorWallet": "0xactor", "to": "0xrouter", "receipt": {"status": 1, "logs": [{}]}}
        self.assertIsNone(decoder.decode({**base, "preBalances": {"TOKEN": 0}, "postBalances": {"TOKEN": 10}}, "0xactor"))
        result = decoder.decode({**base, "preBalances": {"USDC": 100, "TOKEN": 0},
                                 "postBalances": {"USDC": 90, "TOKEN": 5}}, "0xactor")
        self.assertEqual(result.side, "buy")

    def test_python_gate_matches_versioned_golden_vectors(self):
        golden = json.loads((ROOT / "golden" / "fast-path-v1.json").read_text(encoding="utf-8"))
        now = datetime.fromtimestamp(golden["nowMs"] / 1000, timezone.utc)
        cfg = golden["config"]
        settings = {
            "network_ids": cfg["networkIds"], "event_types": cfg["eventTypes"],
            "active_buy_event_types": cfg["activeBuyEventTypes"], "fixed_usd": cfg["fixedUsd"],
            "min_target_buy_usd": cfg["minTargetBuyUsd"], "min_market_cap_usd": cfg["minMarketCapUsd"],
            "source_policies": {"fomo_push": {"maximum_age_ms": cfg["sourcePolicies"]["fomo_push"]["maximumAgeMs"]}},
        }
        for vector in golden["vectors"]:
            raw = vector["event"]
            item = SimpleNamespace(kind="buy", source_type=raw.get("type", ""), ca=raw.get("tokenAddress", ""),
                network_id=raw.get("networkId", 0), created_at=raw.get("createdAt", ""),
                amount_usd=raw.get("usdAmount", 0), market_cap=raw.get("marketCap", 0), trade_id=raw.get("tradeId", ""))
            self.assertEqual(evaluate_copy_buy(item, settings, now=now).status, vector["status"], vector["name"])


if __name__ == "__main__":
    unittest.main()
