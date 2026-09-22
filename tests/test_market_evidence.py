from __future__ import annotations

import base64
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Sequence
from unittest.mock import patch

from bip_utils import Base58Encoder

from fomo.execution.market_evidence import (EvmMarketEvidenceProvider, PriceObservation,
                                            PythHermesPriceAdapter,
                                            SolanaMarketEvidenceProvider,
                                            capture_native_balance, capture_token_balance,
                                            collect_market_evidence, evaluate_evm_funding_evidence,
                                            evaluate_market_evidence,
                                            minimum_evm_gas_reserve_usd, require_spend_balance)
from fomo.execution.market_evidence import solana_fee_for_signed_transaction
from fomo.execution.interfaces import ExecutableQuote
from fomo.execution.rpc_pool import RpcEndpoint
from fomo.signals.strategy import ExecutionIntent
from fomo.watching.rpc_transport import FailoverJsonRpc, RpcUnavailable


FEED = "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"
SOL_FEED = "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"
BLOCK = "0x" + "cd" * 32


class HermesFixture:
    def __init__(self, *, feed_id: str = FEED, requested_feed_id: str = FEED,
                 age_seconds: int = 0, confidence: str = "100000") -> None:
        self.feed_id = feed_id
        self.requested_feed_id = requested_feed_id
        self.age_seconds = age_seconds
        self.confidence = confidence
        self.calls = 0

    def request(self, method, url, *, params, payload, headers):
        self.calls += 1
        assert method == "GET" and "pyth.dourolabs.app" in url
        assert params == {"ids[]": "0x" + self.requested_feed_id, "parsed": "true"}
        assert payload is None and headers["Authorization"].startswith("Bearer ")
        return {"parsed": [{"id": self.feed_id, "price": {
            "price": "250000000000", "conf": self.confidence, "expo": -8,
            "publish_time": int(time.time()) - self.age_seconds,
        }}]}


class EvmRpcFixture:
    last_provider = "verified-primary"

    def __init__(self, *, changed_hash: bool = False):
        self.changed_hash = changed_hash
        self.calls: list[tuple[str, Any]] = []

    def call(self, method, params) -> Any:
        self.calls.append((method, params))
        if method == "eth_getBlockByNumber":
            return {"number": "0x64", "hash": "0x" + "ef" * 32 if self.changed_hash and params[0] != "latest" else BLOCK}
        if method == "eth_getBalance":
            return "0xde0b6b3a7640000"
        raise AssertionError(method)


class SolanaRpcFixture:
    last_provider = "verified-solana"

    def __init__(self, *, stale_context: bool = False):
        self.stale_context = stale_context

    def call(self, method, params):
        if method == "getSlot":
            return 400
        if method == "getBalance":
            return {"context": {"slot": 399 if self.stale_context else 400}, "value": 2_000_000_000}
        if method == "getBlock":
            return {"blockhash": "solana-block-hash"}
        raise AssertionError(method)


class EvmTokenRpcFixture(EvmRpcFixture):
    def __init__(self, *, wrong_decimals: bool = False, changed_hash: bool = False,
                 decimals: int = 6, units: int = 42_000_000):
        super().__init__(changed_hash=changed_hash)
        self.wrong_decimals = wrong_decimals
        self.decimals = decimals
        self.units = units

    def call(self, method, params):
        if method == "eth_call":
            self.calls.append((method, params))
            if params[0]["data"] == "0x313ce567":
                return "0x" + f"{self.decimals + int(self.wrong_decimals):064x}"
            return "0x" + f"{self.units:064x}"
        return super().call(method, params)


class SolanaTokenRpcFixture(SolanaRpcFixture):
    def __init__(self, *, wrong_owner: bool = False, stale_context: bool = False,
                 unsupported_program: bool = False, decimals: int = 6):
        super().__init__(stale_context=stale_context)
        self.wrong_owner = wrong_owner
        self.unsupported_program = unsupported_program
        self.decimals = decimals

    def call(self, method, params):
        if method == "getTokenAccountsByOwner":
            wallet, mint = params[0], params[1]["mint"]
            return {"context": {"slot": 399 if self.stale_context else 400}, "value": [
                {"account": {"owner": "unsupported" if self.unsupported_program else
                             "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                             "data": {"parsed": {"type": "account", "info": {
                    "mint": mint, "owner": "stranger" if self.wrong_owner else wallet,
                    "tokenAmount": {"amount": "24000000", "decimals": self.decimals},
                }}}}},
                {"account": {"owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                             "data": {"parsed": {"type": "account", "info": {
                    "mint": mint, "owner": wallet,
                    "tokenAmount": {"amount": "18000000", "decimals": self.decimals},
                }}}}},
            ]}
        return super().call(method, params)


class SolanaFeeRpcFixture(SolanaRpcFixture):
    def __init__(self, *, fee: int | None = 10_000, context_slot: int = 400,
                 changed_hash: bool = False):
        super().__init__()
        self.fee = fee
        self.context_slot = context_slot
        self.changed_hash = changed_hash
        self.message_base64 = ""

    def call(self, method, params):
        if method == "getFeeForMessage":
            self.message_base64 = params[0]
            return {"context": {"slot": self.context_slot}, "value": self.fee}
        if method == "getBlock" and self.changed_hash:
            return {"blockhash": "different-block"}
        return super().call(method, params)


class SolanaFundingRpcFixture(SolanaTokenRpcFixture):
    def __init__(self):
        super().__init__(decimals=9)

    def call(self, method, params):
        if method == "getFeeForMessage":
            return {"context": {"slot": 400}, "value": 10_000}
        return super().call(method, params)


def price(chain: str, asset: str, price_usd: str) -> PriceObservation:
    now = int(time.time() * 1000)
    return PriceObservation(chain, asset, Decimal(price_usd), Decimal("0.1"),
                            SOL_FEED if asset == "SOL" else FEED, now, now,
                            "f" * 64)


class MarketEvidenceTests(unittest.TestCase):
    def test_pyth_identity_freshness_confidence_and_missing_key(self):
        with self.assertRaisesRegex(ValueError, "approved_pyth_feed_binding_required"):
            PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id="ab" * 32,
                                   transport=HermesFixture())
        adapter = PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id=FEED,
                                         transport=HermesFixture())
        with patch.dict("os.environ", {"PYTH_API_KEY": ""}):
            self.assertFalse(adapter.self_check().ready)
            with self.assertRaisesRegex(ValueError, "credential_unavailable"):
                adapter.latest()
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only"}):
            observed = adapter.latest()
            self.assertEqual(observed.price_usd, Decimal("2500"))
            self.assertTrue(observed.independent_of("0x_allowance_holder"))
            self.assertFalse(observed.independent_of("pyth_hermes"))
            for fixture, error in (
                (HermesFixture(feed_id="cd" * 32), "identity_mismatch"),
                (HermesFixture(age_seconds=60), "stale_or_unreliable"),
                (HermesFixture(confidence="10000000000"), "stale_or_unreliable"),
            ):
                with self.subTest(error=error):
                    bad = PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id=FEED, transport=fixture)
                    with self.assertRaisesRegex(ValueError, error):
                        bad.latest()

    def test_evm_balance_is_pinned_to_one_block_and_native_usd_is_not_zero(self):
        rpc = EvmRpcFixture()
        snapshot = capture_native_balance(rpc, chain_id=1, wallet="0x" + "11" * 20,
                                          price=price("1", "ETH", "2500"))
        self.assertEqual(snapshot.native_units, 10**18)
        self.assertEqual(snapshot.reserve_usd, Decimal("2500"))
        self.assertEqual(snapshot.rpc_provider, "verified-primary")
        self.assertIn(("eth_getBalance", ["0x" + "11" * 20, "0x64"]), rpc.calls)
        with self.assertRaisesRegex(ValueError, "block_reorged"):
            capture_native_balance(EvmRpcFixture(changed_hash=True), chain_id=1,
                                   wallet="0x" + "11" * 20, price=price("1", "ETH", "2500"))

    def test_solana_slot_and_unknown_chain_fail_closed(self):
        snapshot = capture_native_balance(SolanaRpcFixture(), chain_id=1399811149, wallet="wallet",
                                          price=price("1399811149", "SOL", "100"))
        self.assertEqual(snapshot.reserve_usd, Decimal("200"))
        with self.assertRaisesRegex(ValueError, "context_stale"):
            capture_native_balance(SolanaRpcFixture(stale_context=True), chain_id=1399811149,
                                   wallet="wallet", price=price("1399811149", "SOL", "100"))
        with self.assertRaisesRegex(ValueError, "chain_not_audited"):
            capture_native_balance(EvmRpcFixture(), chain_id=5042, wallet="wallet",
                                   price=price("5042", "USDC", "1"))

    def test_quote_price_and_gas_evidence_must_be_scoped_and_fresh(self):
        now = int(time.time() * 1000)
        observed = price("1", "ETH", "2500")
        wallet = "0x" + "11" * 20
        balance = capture_native_balance(EvmRpcFixture(), chain_id=1, wallet=wallet,
                                         price=observed, now_ms=now)
        quote = ExecutableQuote("0x_allowance_holder", "42", "40",
                                datetime.fromtimestamp(now / 1000, timezone.utc).isoformat(),
                                "10", True, ("route",))
        facts = evaluate_market_evidence(quote, price=observed, balance=balance,
                                         expected_chain_id=1, expected_wallet=wallet,
                                         minimum_native_gas_reserve_usd=Decimal("25"),
                                         planned_native_spend_units=0, now_ms=now)
        self.assertEqual(facts.native_gas_reserve_usd, Decimal("2500"))
        self.assertEqual(facts.independent_sanity_price_count, 1)
        self.assertEqual(facts.balance_block_hash, BLOCK)
        for change, error in (
            ({"expected_wallet": "0x" + "22" * 20}, "scope_or_reserve_invalid"),
            ({"minimum_native_gas_reserve_usd": Decimal("3000")}, "scope_or_reserve_invalid"),
            ({"now_ms": now + 6000}, "stale_or_unreliable"),
        ):
            with self.subTest(change=change):
                args = {"price": observed, "balance": balance, "expected_chain_id": 1,
                        "expected_wallet": wallet, "minimum_native_gas_reserve_usd": Decimal("25"),
                        "planned_native_spend_units": 0, "now_ms": now}
                args.update(change)
                with self.assertRaisesRegex(ValueError, error):
                    evaluate_market_evidence(quote, **args)
        same_provider = ExecutableQuote("pyth_hermes", "42", "40", quote.captured_at,
                                        "10", True, ("route",))
        with self.assertRaisesRegex(ValueError, "scope_or_reserve_invalid"):
            evaluate_market_evidence(same_provider, price=observed, balance=balance,
                                     expected_chain_id=1, expected_wallet=wallet,
                                     minimum_native_gas_reserve_usd=Decimal("25"),
                                     planned_native_spend_units=0, now_ms=now)
        stale_quote = ExecutableQuote(quote.provider, "42", "40",
                                     (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat(),
                                     "10", True, ("route",))
        with self.assertRaisesRegex(ValueError, "quote_stale"):
            evaluate_market_evidence(stale_quote, price=observed, balance=balance,
                                     expected_chain_id=1, expected_wallet=wallet,
                                     minimum_native_gas_reserve_usd=Decimal("25"),
                                     planned_native_spend_units=0, now_ms=now)
        with self.assertRaisesRegex(ValueError, "scope_or_reserve_invalid"):
            evaluate_market_evidence(quote, price=observed, balance=balance,
                                     expected_chain_id=1, expected_wallet=wallet,
                                     minimum_native_gas_reserve_usd=Decimal("25"),
                                     planned_native_spend_units=10**18 - 1, now_ms=now)

    def test_collection_does_not_pass_without_price_credential_or_reserve(self):
        now = int(time.time() * 1000)
        wallet = "0x" + "11" * 20
        quote = ExecutableQuote("0x_allowance_holder", "42", "40",
                                datetime.fromtimestamp(now / 1000, timezone.utc).isoformat(),
                                "10", True, ("route",))
        adapter = PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id=FEED,
                                         transport=HermesFixture())
        with patch.dict("os.environ", {"PYTH_API_KEY": ""}):
            with self.assertRaisesRegex(ValueError, "credential_unavailable"):
                collect_market_evidence(quote, price_adapter=adapter, rpc=EvmRpcFixture(),
                                        wallet=wallet, minimum_native_gas_reserve_usd=Decimal("25"),
                                        planned_native_spend_units=0,
                                        now_ms=now)
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only"}):
            facts = collect_market_evidence(quote, price_adapter=adapter, rpc=EvmRpcFixture(),
                                            wallet=wallet,
                                            minimum_native_gas_reserve_usd=Decimal("25"),
                                            planned_native_spend_units=0, now_ms=now)
            self.assertEqual(facts.native_gas_reserve_usd, Decimal("2500"))
            with self.assertRaisesRegex(ValueError, "scope_or_reserve_invalid"):
                collect_market_evidence(quote, price_adapter=adapter, rpc=EvmRpcFixture(),
                                        wallet=wallet,
                                        minimum_native_gas_reserve_usd=Decimal("3000"),
                                        planned_native_spend_units=0, now_ms=now)

    def test_evm_spend_token_balance_is_block_pinned_and_decimals_checked(self):
        wallet, token = "0x" + "11" * 20, "0x" + "22" * 20
        rpc = EvmTokenRpcFixture()
        snapshot = capture_token_balance(rpc, chain_id=1, wallet=wallet, token=token,
                                         expected_decimals=6)
        self.assertEqual(snapshot.units, 42_000_000)
        self.assertTrue(all(params[1] == "0x64" for method, params in rpc.calls if method == "eth_call"))
        require_spend_balance(snapshot, chain_id=1, wallet=wallet, token=token,
                              required_units=40_000_000)
        with self.assertRaisesRegex(ValueError, "insufficient_or_mismatched"):
            require_spend_balance(snapshot, chain_id=1, wallet=wallet, token=token,
                                  required_units=43_000_000)
        with self.assertRaisesRegex(ValueError, "decimals_mismatch"):
            capture_token_balance(EvmTokenRpcFixture(wrong_decimals=True), chain_id=1,
                                  wallet=wallet, token=token, expected_decimals=6)
        with self.assertRaisesRegex(ValueError, "block_reorged"):
            capture_token_balance(EvmTokenRpcFixture(changed_hash=True), chain_id=1,
                                  wallet=wallet, token=token, expected_decimals=6)

    def test_gas_floor_uses_parsed_final_transaction_fee_cap(self):
        observed = price("1", "ETH", "2500")
        scope = {"chainId": "1", "gasLimit": 200_000, "maxFeePerGasWei": 2_000_000_000}
        self.assertEqual(minimum_evm_gas_reserve_usd(scope, observed), Decimal("1.2"))
        for changed in ({"chainId": "56"}, {"gasLimit": 0}, {"maxFeePerGasWei": 0}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                minimum_evm_gas_reserve_usd({**scope, **changed}, observed)

    def test_solana_spend_token_accounts_are_scoped_and_aggregated(self):
        wallet, mint = "solana-wallet", "solana-token-mint"
        snapshot = capture_token_balance(SolanaTokenRpcFixture(), chain_id=1399811149,
                                         wallet=wallet, token=mint, expected_decimals=6)
        self.assertEqual(snapshot.units, 42_000_000)
        require_spend_balance(snapshot, chain_id=1399811149, wallet=wallet, token=mint,
                              required_units=40_000_000)
        with self.assertRaisesRegex(ValueError, "account_scope_invalid"):
            capture_token_balance(SolanaTokenRpcFixture(wrong_owner=True), chain_id=1399811149,
                                  wallet=wallet, token=mint, expected_decimals=6)
        with self.assertRaisesRegex(ValueError, "account_scope_invalid"):
            capture_token_balance(SolanaTokenRpcFixture(unsupported_program=True), chain_id=1399811149,
                                  wallet=wallet, token=mint, expected_decimals=6)
        with self.assertRaisesRegex(ValueError, "context_stale"):
            capture_token_balance(SolanaTokenRpcFixture(stale_context=True), chain_id=1399811149,
                                  wallet=wallet, token=mint, expected_decimals=6)

    def test_evm_funding_requires_same_view_and_signed_spend_and_gas(self):
        now = int(time.time() * 1000)
        wallet, token = "0x" + "11" * 20, "0x" + "22" * 20
        observed = price("1", "ETH", "2500")
        native = capture_native_balance(EvmRpcFixture(), chain_id=1, wallet=wallet,
                                        price=observed, now_ms=now)
        spend = capture_token_balance(EvmTokenRpcFixture(), chain_id=1, wallet=wallet,
                                      token=token, expected_decimals=6, now_ms=now)
        quote = ExecutableQuote("0x_allowance_holder", "42", "40",
                                datetime.fromtimestamp(now / 1000, timezone.utc).isoformat(),
                                "10", True, ("route",))
        scope = {"chainId": "1", "tokenIn": token, "sellAmount": "40000000",
                 "gasLimit": 200_000, "maxFeePerGasWei": 2_000_000_000}
        result = evaluate_evm_funding_evidence(
            quote, price=observed, native_balance=native, token_balance=spend,
            parsed_signed_scope=scope, expected_chain_id=1, expected_wallet=wallet,
            expected_token_in=token, required_spend_units=40_000_000, now_ms=now,
        )
        self.assertEqual(result.minimum_gas_reserve_usd, Decimal("1.2"))
        self.assertEqual(result.market.native_gas_reserve_usd, Decimal("2500"))
        for changed, error in (
            ({"token_balance": replace(spend, block_hash="different")}, "inconsistent_chain_view"),
            ({"parsed_signed_scope": {**scope, "sellAmount": "41000000"}}, "signed_spend_scope_mismatch"),
            ({"required_spend_units": 43_000_000}, "insufficient_or_mismatched"),
        ):
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                arguments = {"price": observed, "native_balance": native, "token_balance": spend,
                             "parsed_signed_scope": scope, "expected_chain_id": 1,
                             "expected_wallet": wallet, "expected_token_in": token,
                             "required_spend_units": 40_000_000, "now_ms": now}
                arguments.update(changed)
                evaluate_evm_funding_evidence(quote, **arguments)

    def test_provider_requires_real_transport_for_readiness_and_quote_price_proof(self):
        wallet = "0x" + "11" * 20
        token = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        adapter = PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id=FEED,
                                         transport=HermesFixture())
        rpc = EvmTokenRpcFixture(decimals=18, units=10**18)
        provider = EvmMarketEvidenceProvider(chain_id=1, wallet=wallet, token_in=token,
                                             token_decimals=18, price_adapter=adapter, rpc=rpc)
        self.assertFalse(provider.self_check().ready)
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only"}):
            observed = adapter.latest()
            proof = {"source": observed.source, "feedId": observed.feed_id,
                     "priceUsd": str(observed.price_usd), "publishedAtMs": observed.published_at_ms,
                     "payloadSha256": observed.payload_sha256}
            quote = ExecutableQuote("0x_allowance_holder", "42", "40",
                                    datetime.now(timezone.utc).isoformat(), "10", True,
                                    ("route",), {"inputPriceProof": proof})
            intent = ExecutionIntent("i", "s", "wallet_rpc_evm", "test", "wallet", "1",
                                     "buy", token, "0x" + "33" * 20, Decimal("10"))
            scope = {"chainId": "1", "wallet": wallet, "tokenIn": token,
                     "sellAmount": str(10**16), "gasLimit": 200_000,
                     "maxFeePerGasWei": 2_000_000_000}
            funding = provider.assess(intent, quote, scope, b"signed")
            self.assertEqual(funding.minimum_gas_reserve_usd, Decimal("1.2"))
            with self.assertRaisesRegex(ValueError, "price_proof_missing"):
                provider.assess(intent, replace(quote, execution_payload={}), scope, b"signed")

    def test_solana_fee_uses_exact_final_message_and_fails_closed(self):
        observed = price("1399811149", "SOL", "100")
        balance = capture_native_balance(SolanaRpcFixture(), chain_id=1399811149,
                                         wallet="wallet", price=observed)
        message = bytes([0x80, 1, 0, 0, 1]) + b"\x02" * 32 + b"\x03" * 32 + b"\0\0"
        signed = b"\x01" + b"\x04" * 64 + message
        rpc = SolanaFeeRpcFixture()
        fee = solana_fee_for_signed_transaction(rpc, signed_transaction=signed, balance=balance)
        self.assertEqual(rpc.message_base64, base64.b64encode(message).decode("ascii"))
        self.assertEqual(fee.fee_lamports, 10_000)
        self.assertEqual(fee.minimum_gas_reserve_usd, Decimal("0.0012"))
        for transaction, fixture, error in (
            (b"\x01" + b"\0" * 64 + message, SolanaFeeRpcFixture(), "unsigned_transaction"),
            (signed, SolanaFeeRpcFixture(fee=None), "rpc_context_invalid"),
            (signed, SolanaFeeRpcFixture(changed_hash=True), "rpc_context_invalid"),
            (signed, SolanaFeeRpcFixture(context_slot=399), "stale_or_zero"),
            (signed[:-len(message)] + bytes([0x81]) + message[1:], SolanaFeeRpcFixture(),
             "message_version_unsupported"),
        ):
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                solana_fee_for_signed_transaction(fixture, signed_transaction=transaction,
                                                  balance=balance)

    def test_solana_provider_requires_parsed_native_spend_and_real_probes(self):
        wallet = Base58Encoder.Encode(b"\x05" * 32)
        mint = "So11111111111111111111111111111111111111112"
        adapter = PythHermesPriceAdapter(
            chain_id=1399811149, asset="SOL", feed_id=SOL_FEED,
            transport=HermesFixture(feed_id=SOL_FEED, requested_feed_id=SOL_FEED),
        )
        provider = SolanaMarketEvidenceProvider(wallet=wallet, token_in=mint,
                                                price_adapter=adapter, rpc=SolanaFundingRpcFixture())
        self.assertFalse(provider.self_check().ready)
        message = bytes([0x80, 1, 0, 0, 1]) + b"\x02" * 32 + b"\x03" * 32 + b"\0\0"
        signed = b"\x01" + b"\x04" * 64 + message
        intent = ExecutionIntent("i", "s", "wallet_rpc_solana", "test", "wallet", "1399811149",
                                 "buy", mint, "output", Decimal("10"))
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only"}):
            observed = adapter.latest()
            quote = ExecutableQuote("jupiter_metis", "42", "40",
                                    datetime.now(timezone.utc).isoformat(), "10", True, ("route",), {
                                        "inputPriceProof": {
                                            "source": observed.source, "feedId": observed.feed_id,
                                            "priceUsd": str(observed.price_usd),
                                            "publishedAtMs": observed.published_at_ms,
                                            "payloadSha256": observed.payload_sha256,
                                        },
                                    })
            scope = {"chainId": "1399811149", "wallet": wallet, "tokenIn": mint,
                     "sellAmount": "4000000", "nativeSpendLamports": "0"}
            result = provider.assess(intent, quote, scope, signed)
            self.assertEqual(result.fee.fee_lamports, 10_000)
            with self.assertRaisesRegex(ValueError, "balance_rpc_invalid_quantity"):
                provider.assess(intent, quote, {key: value for key, value in scope.items()
                                                if key != "nativeSpendLamports"}, signed)

    def test_market_balance_read_never_mixes_failover_endpoints(self):
        wallet = "0x" + "11" * 20
        token = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        calls: list[tuple[str, str]] = []

        def requester(endpoint: RpcEndpoint, method: str, params: Sequence[Any]) -> Any:
            calls.append((endpoint.provider, method))
            if method == "eth_chainId":
                return "0x38" if endpoint.provider == "backup" else "0x1"
            if method == "eth_blockNumber":
                return "0x64"
            if method == "eth_getBlockByNumber":
                return {"number": "0x64", "hash": BLOCK}
            if method == "eth_getBalance":
                raise OSError("simulated primary disconnect")
            raise AssertionError(method)

        rpc = FailoverJsonRpc("1", [
            RpcEndpoint("1", "primary", "primary", priority=1,
                        public_http_url="https://primary.example.com"),
            RpcEndpoint("1", "backup", "backup", priority=2,
                        public_http_url="https://backup.example.com"),
        ], requester=requester)
        adapter = PythHermesPriceAdapter(chain_id=1, asset="ETH", feed_id=FEED,
                                         transport=HermesFixture())
        provider = EvmMarketEvidenceProvider(chain_id=1, wallet=wallet, token_in=token,
                                             token_decimals=18, price_adapter=adapter, rpc=rpc)
        self.assertFalse(provider.self_check().ready)
        intent = ExecutionIntent("i", "s", "wallet_rpc_evm", "test", "wallet", "1",
                                 "buy", token, "0x" + "33" * 20, Decimal("10"))
        with patch.dict("os.environ", {"PYTH_API_KEY": "test-only"}):
            observed = adapter.latest()
            quote = ExecutableQuote("0x_allowance_holder", "42", "40",
                                    datetime.now(timezone.utc).isoformat(), "10", True,
                                    ("route",), {"inputPriceProof": {
                                        "source": observed.source, "feedId": observed.feed_id,
                                        "priceUsd": str(observed.price_usd),
                                        "publishedAtMs": observed.published_at_ms,
                                        "payloadSha256": observed.payload_sha256,
                                    }})
            with self.assertRaises(RpcUnavailable) as error:
                provider.assess(intent, quote, {"wallet": wallet}, b"signed")
        self.assertNotIn("example.com", str(error.exception))
        self.assertNotIn(("backup", "eth_getBalance"), calls)


if __name__ == "__main__":
    unittest.main()
