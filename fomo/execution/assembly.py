"""Explicit per-chain execution assembly; no configuration-only readiness."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.strategy import ExecutionIntent

from .capabilities import CapabilityRegistry
from .chain_managers import EvmNonceManager, SolanaBlockhashManager
from .coordinator import ExecutionCoordinator, RiskEvidenceProvider
from .evm_transaction import EvmEip1559Builder, EvmTransactionParser, VaultEvmSigner
from .interfaces import (Broadcaster, NonceBlockhashManager, QuoteAdapter, ReceiptTracker,
                         Signer, TransactionBuilder, TransactionParser, TransactionSimulator)
from .journal import ExecutionJournal
from .quote_adapters import JupiterQuoteAdapter, ZeroXQuoteAdapter
from .rpc_adapters import EvmReceiptTracker, EvmRpcBroadcaster, SolanaReceiptTracker, SolanaRpcBroadcaster
from .simulators import EvmTransactionSimulator, SolanaTransactionSimulator
from .solana_transaction import SolanaLegacyBuilder, SolanaLegacyParser, VaultSolanaSigner


@dataclass(frozen=True)
class ChainAdapters:
    quote: QuoteAdapter
    builder: TransactionBuilder
    simulator: TransactionSimulator
    signer: Signer
    broadcaster: Broadcaster
    nonce_manager: NonceBlockhashManager
    parser: TransactionParser
    receipt_tracker: ReceiptTracker
    risk_evidence: RiskEvidenceProvider | None


class ExecutionAssembly:
    def __init__(self, journal: ExecutionJournal) -> None:
        self.journal = journal
        self._chains: dict[str, ChainAdapters] = {}

    def register(self, chain_id: str, adapters: ChainAdapters) -> None:
        if str(chain_id) in self._chains:
            raise ValueError("duplicate_chain_execution_assembly")
        self._chains[str(chain_id)] = adapters

    @staticmethod
    def _audited_types(chain_id: str, item: ChainAdapters) -> bool:
        if chain_id == "1399811149":
            return (type(item.quote) is JupiterQuoteAdapter and type(item.builder) is SolanaLegacyBuilder
                    and type(item.simulator) is SolanaTransactionSimulator
                    and type(item.signer) is VaultSolanaSigner and type(item.broadcaster) is SolanaRpcBroadcaster
                    and type(item.nonce_manager) is SolanaBlockhashManager
                    and type(item.parser) is SolanaLegacyParser and type(item.receipt_tracker) is SolanaReceiptTracker)
        if chain_id in {"1", "56", "8453", "4663", "5042"}:
            return (type(item.quote) is ZeroXQuoteAdapter and type(item.builder) is EvmEip1559Builder
                    and type(item.simulator) is EvmTransactionSimulator and type(item.signer) is VaultEvmSigner
                    and type(item.broadcaster) is EvmRpcBroadcaster and type(item.nonce_manager) is EvmNonceManager
                    and type(item.parser) is EvmTransactionParser and type(item.receipt_tracker) is EvmReceiptTracker)
        return False

    def chain_status(self, chain_id: str) -> dict[str, Any]:
        item = self._chains.get(str(chain_id))
        if item is None:
            return {"chainId": str(chain_id), "ready": False, "adapterReady": False,
                    "liveArmed": False, "blockers": ["execution_chain_not_assembled"]}
        registry = CapabilityRegistry()
        for name, adapter in (
            ("quote_adapter", item.quote), ("transaction_builder", item.builder),
            ("transaction_simulator", item.simulator), ("signer", item.signer),
            ("broadcaster", item.broadcaster), ("nonce_blockhash_manager", item.nonce_manager),
            ("transaction_parser", item.parser), ("receipt_tracker", item.receipt_tracker),
        ):
            registry.register(name, adapter)
        capability = registry.status()
        blockers = []
        if not self._audited_types(str(chain_id), item):
            blockers.append("unaudited_or_fake_adapter_type")
        if str(chain_id) != "1399811149" and any(
            str(getattr(adapter, "chain_id", "")) != str(chain_id)
            for adapter in (item.quote, item.builder, item.simulator, item.broadcaster,
                            item.nonce_manager, item.receipt_tracker)
        ):
            blockers.append("adapter_chain_binding_mismatch")
        if not capability["ready"]:
            blockers.append("capability_self_check_failed")
        # 0x Settler and Jupiter versioned transactions are not proven by the
        # current strict V2/legacy parsers. Never infer compatibility from an
        # API key or ROUTE_* flag.
        blockers.append("final_transaction_route_format_unverified")
        try:
            risk = item.risk_evidence.self_check() if item.risk_evidence is not None else None
        except Exception:
            risk = None
        if risk is None or risk.name != "risk_evidence" or not risk.implemented or not risk.ready:
            blockers.append("risk_evidence_provider_unavailable")
        control = self.journal.db.execute(
            "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
        ).fetchone()
        live = bool(control and control["live_armed"] and not control["circuit_breaker_tripped"])
        if not live:
            blockers.append("live_disabled_or_circuit_open")
        return {"chainId": str(chain_id), "ready": not blockers, "adapterReady": capability["ready"],
                "liveArmed": live, "blockers": blockers, "capabilities": capability["capabilities"]}

    def execute(self, intent: ExecutionIntent, signal: TradeSignalEnvelope) -> Mapping[str, Any]:
        if not self.chain_status(intent.chain_id)["ready"]:
            raise ValueError("execution_chain_not_ready")
        item = self._chains[intent.chain_id]
        assert item.risk_evidence is not None
        return ExecutionCoordinator(
            self.journal, quote_adapter=item.quote, builder=item.builder, simulator=item.simulator,
            signer=item.signer, broadcaster=item.broadcaster, nonce_manager=item.nonce_manager,
            parser=item.parser, receipt_tracker=item.receipt_tracker, risk_evidence=item.risk_evidence,
        ).execute(intent, signal)
