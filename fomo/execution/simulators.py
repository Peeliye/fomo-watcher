"""Chain RPC simulations; return evidence only after a successful node result."""

from __future__ import annotations

import base64
from typing import Any, Mapping

from fomo.watching.rpc_transport import RpcTransport

from .capabilities import CapabilityStatus
from .evm_transaction import decode_eip1559
from .interfaces import BuiltTransaction


class EvmTransactionSimulator:
    def __init__(self, rpc: RpcTransport, chain_id: int, wallet: str) -> None:
        self.rpc = rpc
        self.chain_id = int(chain_id)
        self.wallet = wallet

    def self_check(self) -> CapabilityStatus:
        try:
            value = self.rpc.call("eth_chainId", [])
            ready = int(value, 16) == self.chain_id if isinstance(value, str) else int(value) == self.chain_id
        except Exception:
            ready = False
        return CapabilityStatus("transaction_simulator", True, ready,
                                "ok" if ready else "rpc_chain_verification_failed", {})

    def simulate(self, transaction: BuiltTransaction) -> Mapping[str, Any]:
        fields, _ = decode_eip1559(transaction.serialized, signed=False)
        if fields.chain_id != self.chain_id:
            raise ValueError("simulation_chain_mismatch")
        call = {"from": self.wallet, "to": "0x" + fields.to.hex(), "data": "0x" + fields.data.hex(),
                "value": hex(fields.value), "gas": hex(fields.gas_limit)}
        self.rpc.call("eth_call", [call, "pending"])
        estimated_gas = self.rpc.call("eth_estimateGas", [call, "pending"])
        gas = int(estimated_gas, 16) if isinstance(estimated_gas, str) else int(estimated_gas)
        if gas <= 0 or gas > fields.gas_limit:
            raise ValueError("simulation_gas_exceeds_transaction_limit")
        return {"passed": True, "estimatedGas": gas, "gasLimit": fields.gas_limit}


class SolanaTransactionSimulator:
    def __init__(self, rpc: RpcTransport) -> None:
        self.rpc = rpc

    def self_check(self) -> CapabilityStatus:
        try:
            ready = int(self.rpc.call("getSlot", [{"commitment": "confirmed"}])) > 0
        except Exception:
            ready = False
        return CapabilityStatus("transaction_simulator", True, ready,
                                "ok" if ready else "solana_rpc_unavailable", {})

    def simulate(self, transaction: BuiltTransaction) -> Mapping[str, Any]:
        result = self.rpc.call("simulateTransaction", [base64.b64encode(transaction.serialized).decode(), {
            "encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": False,
            "commitment": "confirmed", "innerInstructions": True,
        }])
        value = result.get("value") if isinstance(result, Mapping) else None
        if not isinstance(value, Mapping) or value.get("err") is not None:
            raise ValueError("solana_transaction_simulation_failed")
        return {"passed": True, "unitsConsumed": value.get("unitsConsumed"),
                "innerInstructions": value.get("innerInstructions")}
