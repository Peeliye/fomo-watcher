"""One source-neutral, durable pre-broadcast workflow.

The coordinator is intentionally not installed by the default service. An
audited risk-evidence provider and all runtime capabilities must be supplied;
otherwise every signal remains blocked. A claimed hash is never sent twice.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from typing import Mapping, Protocol

from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityRegistry, CapabilityStatus
from .core import PreflightEvidence, SharedExecutionCore, validate_serialized_transaction
from .interfaces import (Broadcaster, ExecutableQuote, NonceBlockhashManager,
                         QuoteAdapter, ReceiptTracker, Signer, TransactionBuilder, TransactionParser,
                         TransactionSimulator)
from .journal import ExecutionJournal
from .market_evidence import MarketEvidenceFacts


@dataclass(frozen=True, slots=True)
class ScopeLimits:
    wallet: str
    maximum_input_units: Decimal
    trusted_targets: frozenset[str]


class RiskEvidenceProvider(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def assess(self, intent: ExecutionIntent, signal: TradeSignalEnvelope,
               quote: ExecutableQuote, simulation: Mapping[str, object]) -> tuple[PreflightEvidence, ScopeLimits]: ...


class MarketFundingEvidence(Protocol):
    market: MarketEvidenceFacts
    minimum_gas_reserve_usd: Decimal


class MarketEvidenceProvider(Protocol):
    def self_check(self) -> CapabilityStatus: ...
    def assess(self, intent: ExecutionIntent, quote: ExecutableQuote,
               parsed_signed_scope: Mapping[str, object],
               signed_transaction: bytes) -> MarketFundingEvidence: ...


class ExecutionCoordinator:
    def __init__(self, journal: ExecutionJournal, *, quote_adapter: QuoteAdapter,
                 builder: TransactionBuilder, simulator: TransactionSimulator,
                 signer: Signer, broadcaster: Broadcaster, nonce_manager: NonceBlockhashManager,
                 parser: TransactionParser, receipt_tracker: ReceiptTracker,
                 risk_evidence: RiskEvidenceProvider,
                 market_evidence: MarketEvidenceProvider) -> None:
        self.journal = journal
        self.quote_adapter = quote_adapter
        self.builder = builder
        self.simulator = simulator
        self.signer = signer
        self.broadcaster = broadcaster
        self.nonce_manager = nonce_manager
        self.parser = parser
        self.receipt_tracker = receipt_tracker
        self.risk_evidence = risk_evidence
        self.market_evidence = market_evidence

    def _control_ready(self) -> bool:
        row = self.journal.db.execute(
            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
        ).fetchone()
        return bool(row and row["live_armed"] and not row["circuit_breaker_tripped"])

    def _capabilities_ready(self) -> bool:
        registry = CapabilityRegistry()
        for name, adapter in (
            ("quote_adapter", self.quote_adapter), ("transaction_builder", self.builder),
            ("transaction_simulator", self.simulator), ("signer", self.signer),
            ("broadcaster", self.broadcaster), ("nonce_blockhash_manager", self.nonce_manager),
            ("transaction_parser", self.parser), ("receipt_tracker", self.receipt_tracker),
        ):
            registry.register(name, adapter)
        risk = self.risk_evidence.self_check()
        market = self.market_evidence.self_check()
        return (registry.status()["ready"] and risk.name == "risk_evidence"
                and risk.implemented and risk.ready and market.name == "market_evidence"
                and market.implemented and market.ready)

    def execute(self, intent: ExecutionIntent, signal: TradeSignalEnvelope) -> Mapping[str, object]:
        if intent.signal_id != signal.signal_id or not self._control_ready():
            raise ValueError("live_execution_disabled_or_signal_mismatch")
        reservation = self.journal.reserve_intent(intent)
        if reservation["duplicate"]:
            return {"status": "already_recorded", "intentId": intent.intent_id}
        submission_claimed = False
        try:
            quotes = tuple(self.quote_adapter.quote(intent))
            if len(quotes) != 1 or not quotes[0].firm:
                raise ValueError("single_firm_quote_required")
            quote = quotes[0]
            if not self._capabilities_ready():
                raise ValueError("execution_capability_self_check_failed")
            self.journal.transition_job(intent.intent_id, "quoted", metadata={"quote": asdict(quote)})
            nonce_or_blockhash = self.nonce_manager.acquire(intent)
            unsigned = self.builder.build(intent, quote, nonce_or_blockhash)
            simulation = self.simulator.simulate(unsigned)
            if simulation.get("passed") is not True:
                raise ValueError("simulation_not_proven")
            self.journal.transition_job(intent.intent_id, "simulated", metadata={"simulation": dict(simulation)})
            evidence, limits = self.risk_evidence.assess(intent, signal, quote, simulation)
            if not limits.wallet or limits.maximum_input_units <= 0 or not limits.trusted_targets:
                raise ValueError("scope_limits_missing")
            self.journal.transition_job(intent.intent_id, "built", metadata={
                "provider": unsigned.provider, "nonceOrBlockhash": unsigned.nonce_or_blockhash,
            })
            signed = self.signer.sign(unsigned)
            scope, digest, parsed = validate_serialized_transaction(
                signed, self.parser, expected_chain_id=intent.chain_id,
                expected_wallet=limits.wallet, expected_token_out=intent.token_out,
                maximum_sell_amount=limits.maximum_input_units,
                trusted_targets=set(limits.trusted_targets),
            )
            if not scope.valid or str(parsed.get("tokenIn") or "").casefold() != intent.token_in.casefold():
                raise ValueError("signed_transaction_scope_rejected")
            funding = self.market_evidence.assess(intent, quote, parsed, signed)
            evidence = replace(
                evidence, balance_fresh=funding.market.balance_fresh,
                gas_reserve_usd=funding.market.native_gas_reserve_usd,
                minimum_gas_reserve_usd=funding.minimum_gas_reserve_usd,
                quote_fresh=funding.market.quote_fresh,
                independent_sanity_price_count=funding.market.independent_sanity_price_count,
            )
            allowed, blockers = SharedExecutionCore().preflight(intent, evidence)
            if not allowed:
                raise ValueError("preflight_rejected:" + ",".join(blockers))
            self.journal.transition_job(intent.intent_id, "signed", metadata={"serializedTxHash": digest})
            tx_hash = self.broadcaster.transaction_id(signed)
            if hashlib.sha256(signed).hexdigest() != digest:
                raise ValueError("signed_bytes_changed")
            self.nonce_manager.mark_used(intent.intent_id, nonce_or_blockhash, tx_hash)
            self.journal.claim_submission(intent.intent_id, tx_hash, unsigned.provider, nonce_or_blockhash, digest)
            submission_claimed = True
            result = self.broadcaster.broadcast(signed)
            if result.tx_hash != tx_hash:
                raise ValueError("broadcast_identity_mismatch")
            return {"status": "broadcast", "intentId": intent.intent_id, "txHash": tx_hash}
        except Exception:
            if not submission_claimed:
                try:
                    self.journal.fail_unsubmitted_job(intent.intent_id, reason="prebroadcast_failure")
                except ValueError:
                    pass
            # Once the hash has been claimed, receipt recovery owns the intent.
            # Sending again here would double-broadcast after an ambiguous RPC failure.
            raise
