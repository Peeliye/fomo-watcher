from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fomo.signals.envelope import TradeSignalEnvelope, raw_payload_hash
from fomo.execution.evm_transaction import VaultEvmSigner, keccak256
from fomo.execution.interfaces import BuiltTransaction
from scripts.uniswap_v3_swap_once import (CHAIN_ID, WETH, SignalDedupe,
                                          SwapOnceRpc, execute_once,
                                          intent_from_signal, main,
                                          _send_and_wait)

TOKEN = "0x39dbed3a2bd333467115de45665cc57f813c4571"
POOL = "0x10cc6bd38112cac182db90b6a71d8bb5939526ba"
WALLET = "0x4ccb77f12801ee8853a9cc3782828678c8f5584b"


def word(value: int) -> str:
    return "0x" + f"{value:064x}"


class RpcFixture(SwapOnceRpc):
    supports_multicall3 = True
    last_provider = "fixture"

    def __init__(self, *, allowance: int = 10**18,
                 reject_simulation: bool = False) -> None:
        self.allowance = allowance
        self.reject_simulation = reject_simulation
        self.calls: list[tuple[str, list]] = []

    def call(self, method, params):
        self.calls.append((method, list(params)))
        if method == "eth_chainId":
            return hex(CHAIN_ID)
        if method == "eth_maxPriorityFeePerGas":
            return hex(1)
        if method == "eth_getTransactionCount":
            return hex(7)
        if method == "eth_getBlockByNumber":
            return {"baseFeePerGas": hex(10)}
        if method == "eth_estimateGas":
            if self.reject_simulation:
                raise ValueError("swap_once_simulation_rejected")
            return hex(180_000)
        if method == "eth_call":
            call = params[0]
            data = call["data"]
            if data.startswith("0x1698ee82"):
                return word(int(POOL, 16))
            if data.startswith("0x22afcccb"):
                return word(200)
            if data.startswith("0x70a08231"):
                return word(10**15)
            if data.startswith("0xdd62ed3e"):
                return word(10**12 if len(params) == 3 else self.allowance)
            return "0x"
        raise AssertionError((method, params))


class SwapOnceTests(unittest.TestCase):
    @staticmethod
    def signal(*, source_amount: str = "1000000000000", chain_id: str = "4663",
               token_in: str = WETH, token_out: str = TOKEN,
               side: str = "buy") -> TradeSignalEnvelope:
        now = datetime.now(timezone.utc).isoformat()
        return TradeSignalEnvelope.create(
            source="wallet_rpc_evm", source_event_id="4663:0xabc:7",
            observed_at=now, source_timestamp=now, delivery_delay_ms=0,
            chain_id=chain_id, actor_wallet=WALLET, kol_id=None, side=side,
            token_in=token_in, token_out=token_out, source_amount=source_amount,
            estimated_usd=None, tx_hash="0x" + "ab" * 32, signature=None,
            log_index=7, instruction_index=None, confirmation_level="confirmed",
            reorg_key="4663:0xabc", decoder_version="evm-swap-v1",
            raw_payload_hash=raw_payload_hash({"tx": "0xabc", "logIndex": 7}),
        )

    @staticmethod
    def snapshot():
        return SimpleNamespace(
            quote=lambda **kwargs: SimpleNamespace(amount_out=4_000_000_000_000_000),
        )

    def run_execute(self, rpc: RpcFixture):
        reader = SimpleNamespace(snapshot=lambda **kwargs: self.snapshot())
        context = SimpleNamespace(block_height=200)
        with (patch("scripts.uniswap_v3_swap_once._wallet_profile",
                    return_value=(WALLET, {"backend": "disabled"})),
              patch("scripts.uniswap_v3_swap_once.pin_v3_block_context",
                    return_value=context),
              patch("scripts.uniswap_v3_swap_once.UniswapV3PoolReader",
                    return_value=reader),
              patch("scripts.uniswap_v3_swap_once._signer") as signer):
            result = execute_once(
                token_out=TOKEN, amount_in=10**12, fee=10_000,
                slippage_bps=100, broadcast=False, rpc=rpc,
            )
        signer.assert_not_called()
        return result

    def test_default_simulation_builds_and_calls_final_router_without_broadcast(self):
        rpc = RpcFixture()
        result = self.run_execute(rpc)
        self.assertTrue(result["simulation_success"])
        self.assertEqual(result["chainId"], CHAIN_ID)
        self.assertEqual(result["tokenIn"], WETH)
        self.assertEqual(result["tokenOut"], TOKEN)
        self.assertEqual(result["pool"], POOL)
        self.assertEqual(result["amountIn"], str(10**12))
        self.assertEqual(result["quotedOut"], "4000000000000000")
        self.assertEqual(result["minOut"], "3960000000000000")
        self.assertTrue(result["allowanceSufficient"])
        self.assertFalse(result["approveRequired"])
        self.assertNotIn("transactionHash", result)
        methods = [method for method, _ in rpc.calls]
        self.assertIn("eth_estimateGas", methods)
        self.assertNotIn("eth_sendRawTransaction", methods)
        self.assertNotIn("eth_getLogs", methods)

    def test_insufficient_allowance_simulates_exact_approve_and_verified_override(self):
        rpc = RpcFixture(allowance=0)
        result = self.run_execute(rpc)
        self.assertTrue(result["approveRequired"])
        approve_calls = [params[0] for method, params in rpc.calls
                         if method == "eth_call"
                         and params[0]["data"].startswith("0x095ea7b3")]
        self.assertEqual(len(approve_calls), 1)
        self.assertEqual(approve_calls[0]["to"], WETH)
        self.assertEqual(approve_calls[0]["data"][10:74],
                         "caf681a66d020601342297493863e78c959e5cb2".rjust(64, "0"))
        self.assertEqual(int(approve_calls[0]["data"][74:], 16), 10**12)
        self.assertTrue(any(method == "eth_call" and len(params) == 3
                            for method, params in rpc.calls))
        self.assertNotIn("eth_sendRawTransaction", [method for method, _ in rpc.calls])

    def test_cli_defaults_to_simulation_and_modes_are_mutually_exclusive(self):
        output = io.StringIO()
        with (patch("scripts.uniswap_v3_swap_once.execute_once",
                    return_value={"simulation_success": True}) as execute,
              redirect_stdout(output)):
            self.assertEqual(main([
                "--token-out", TOKEN, "--amount-in-units", "1",
                "--fee", "10000",
            ]), 0)
        self.assertFalse(execute.call_args.kwargs["broadcast"])
        self.assertEqual(json.loads(output.getvalue()), {"simulation_success": True})
        with self.assertRaises(SystemExit):
            main(["--token-out", TOKEN, "--amount-in-units", "1",
                  "--fee", "10000", "--simulate", "--broadcast"])
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main([
                "--token-out", TOKEN, "--amount-in-units", "1",
                "--fee", "10000", "--broadcast",
            ]), 1)
        self.assertEqual(json.loads(output.getvalue())["reason"],
                         "swap_once_broadcast_requires_signal")

    def test_realistic_signal_maps_to_exact_asset_intent_and_rejects_wrong_scope(self):
        signal = self.signal()
        intent = intent_from_signal(signal)
        self.assertEqual(intent.signal_id, signal.signal_id)
        self.assertEqual(intent.chain_id, "4663")
        self.assertEqual(intent.token_in, WETH)
        self.assertEqual(intent.token_out, TOKEN)
        self.assertEqual(intent.requested_asset_amount, "1000000000000")
        for invalid in (
            self.signal(chain_id="1"), self.signal(token_in=TOKEN),
            self.signal(side="sell"), self.signal(source_amount="1.5"),
        ):
            with self.assertRaisesRegex(ValueError, "swap_once_signal_"):
                intent_from_signal(invalid)

    def test_signal_dedupe_allows_simulation_then_one_broadcast_only(self):
        with tempfile.TemporaryDirectory() as directory:
            dedupe = SignalDedupe(directory)
            signal_id = self.signal().signal_id
            calls: list[str] = []

            def simulated():
                calls.append("simulate")
                return {"simulation_success": True}

            self.assertTrue(dedupe.execute(
                signal_id, broadcast=False, operation=simulated,
            )["simulation_success"])
            with self.assertRaisesRegex(ValueError, "swap_once_duplicate_signal"):
                dedupe.execute(signal_id, broadcast=False, operation=simulated)

            def broadcasted():
                calls.append("broadcast")
                return {"transactionHash": "0x" + "12" * 32, "receiptStatus": 1}

            result = dedupe.execute(signal_id, broadcast=True, operation=broadcasted)
            self.assertEqual(result["receiptStatus"], 1)
            with self.assertRaisesRegex(ValueError, "swap_once_duplicate_signal"):
                dedupe.execute(signal_id, broadcast=True, operation=broadcasted)
            self.assertEqual(calls, ["simulate", "broadcast"])

    def test_uncertain_broadcast_failure_remains_fenced(self):
        with tempfile.TemporaryDirectory() as directory:
            dedupe = SignalDedupe(directory)
            signal_id = self.signal().signal_id

            def failed():
                raise ValueError("swap_once_broadcast_uncertain")

            with self.assertRaisesRegex(ValueError, "swap_once_broadcast_uncertain"):
                dedupe.execute(signal_id, broadcast=True, operation=failed)
            with self.assertRaisesRegex(ValueError, "swap_once_duplicate_signal"):
                dedupe.execute(signal_id, broadcast=True, operation=lambda: {})

    def test_simulation_failure_never_reaches_broadcast(self):
        rpc = RpcFixture(reject_simulation=True)
        reader = SimpleNamespace(snapshot=lambda **kwargs: self.snapshot())
        context = SimpleNamespace(block_height=200)
        with (patch("scripts.uniswap_v3_swap_once._wallet_profile",
                    return_value=(WALLET, {"backend": "os_credential_store"})),
              patch("scripts.uniswap_v3_swap_once.pin_v3_block_context",
                    return_value=context),
              patch("scripts.uniswap_v3_swap_once.UniswapV3PoolReader",
                    return_value=reader),
              patch("scripts.uniswap_v3_swap_once._signer",
                    return_value=SimpleNamespace())):
            with self.assertRaisesRegex(ValueError, "swap_once_simulation_rejected"):
                execute_once(
                    token_out=TOKEN, amount_in=10**12, fee=10_000,
                    slippage_bps=100, broadcast=True, rpc=rpc,
                )
        self.assertNotIn("eth_sendRawTransaction", [method for method, _ in rpc.calls])

    def test_failed_receipt_is_never_reported_as_success(self):
        signed = b"signed-transaction"
        transaction_hash = "0x" + keccak256(signed).hex()

        class ReceiptRpc(RpcFixture):
            def call(self, method, params):
                if method == "eth_sendRawTransaction":
                    return transaction_hash
                if method == "eth_getTransactionReceipt":
                    return {"transactionHash": transaction_hash,
                            "blockHash": "0x" + "34" * 32, "status": "0x0"}
                return super().call(method, params)

        class TestSigner(VaultEvmSigner):
            def __init__(self):
                pass

            def sign(self, transaction):
                return signed

        with self.assertRaisesRegex(ValueError, "swap_once_receipt_failed"):
            _send_and_wait(
                ReceiptRpc(), signer=TestSigner(),
                transaction=BuiltTransaction(b"", "test", "0"),
            )

    def test_signal_cli_builds_intent_and_uses_durable_fence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            signal_file = root / "signal.json"
            signal = self.signal()
            signal_file.write_text(json.dumps(signal.to_dict()), encoding="utf-8")
            output = io.StringIO()
            with (patch("scripts.uniswap_v3_swap_once.execute_once",
                        return_value={"simulation_success": True}) as execute,
                  redirect_stdout(output)):
                code = main([
                    "--signal-file", str(signal_file), "--dedupe-dir", str(root / "dedupe"),
                    "--fee", "10000", "--simulate",
                ])
            self.assertEqual(code, 0)
            self.assertFalse(execute.call_args.kwargs["broadcast"])
            self.assertEqual(execute.call_args.kwargs["token_out"], TOKEN)
            self.assertEqual(execute.call_args.kwargs["amount_in"], 10**12)
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["signalId"], signal.signal_id)
            self.assertEqual(payload["intentId"], f"intent:{signal.signal_id}")


if __name__ == "__main__":
    unittest.main()
