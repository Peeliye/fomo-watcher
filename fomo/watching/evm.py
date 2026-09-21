"""EVM wallet RPC adapter (pending/confirmed/finalized, log index, nonce, reorg)."""

from __future__ import annotations

from typing import Any, Mapping

from .wallet import WalletRpcAdapter


class EvmWalletRpcAdapter(WalletRpcAdapter):
    source = "wallet_rpc_evm"

    def _identity(self, transaction: Mapping[str, Any]) -> tuple[str, int | None, int | None]:
        tx_hash = str(transaction.get("txHash") or "").strip()
        if not tx_hash or transaction.get("nonce") is None:
            raise ValueError("EVM wallet event requires txHash and nonce")
        return tx_hash, int(transaction.get("logIndex") or 0), None

    def _confirmation(self, transaction: Mapping[str, Any]) -> str:
        value = str(transaction.get("confirmationLevel") or "confirmed")
        if value not in {"pending", "confirmed", "finalized"}:
            raise ValueError("unsupported EVM confirmation level")
        return value
