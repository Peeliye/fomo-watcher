"""Solana wallet RPC adapter (processed/confirmed/finalized and blockhash expiry)."""

from __future__ import annotations

from typing import Any, Mapping

from .wallet import WalletRpcAdapter


class SolanaWalletRpcAdapter(WalletRpcAdapter):
    source = "wallet_rpc_solana"

    def _identity(self, transaction: Mapping[str, Any]) -> tuple[str, int | None, int | None]:
        signature = str(transaction.get("signature") or "").strip()
        if not signature or transaction.get("instructionIndex") is None:
            raise ValueError("Solana wallet event requires signature and instructionIndex")
        if bool(transaction.get("blockhashExpired")):
            raise ValueError("Solana blockhash expired")
        return signature, None, int(transaction["instructionIndex"])

    def _confirmation(self, transaction: Mapping[str, Any]) -> str:
        value = str(transaction.get("confirmationLevel") or "confirmed")
        if value not in {"processed", "confirmed", "finalized"}:
            raise ValueError("unsupported Solana confirmation level")
        return value
