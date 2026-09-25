"""Receipt evidence for one confirmed Fomo buy of the reviewed 4663 token."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from fomo.signals.envelope import TradeSignalEnvelope
from .evm_transaction import keccak256
from .pons_v4_once import CHAIN_ID, TOKEN_OUT


HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
TRANSFER_TOPIC = "0x" + keccak256(b"Transfer(address,address,uint256)").hex()


class ReceiptRpc(Protocol):
    def call(self, method: str, params: Sequence[Any]) -> Any: ...


def _quantity(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("pons_signal_source_unconfirmed")
    try:
        number = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("pons_signal_source_unconfirmed") from error
    if number < 0:
        raise ValueError("pons_signal_source_unconfirmed")
    return number


@dataclass(frozen=True, slots=True)
class SourceReceiptEvidence:
    signal_id: str
    tx_hash: str
    block_number: int
    block_hash: str
    confirmations: int
    transfer_amount: int

    @property
    def economic_key(self) -> str:
        return f"{CHAIN_ID}:{self.tx_hash}:{TOKEN_OUT}:buy"


class PonsV4SourceVerifier:
    def __init__(self, rpc: ReceiptRpc, *, minimum_confirmations: int) -> None:
        if type(minimum_confirmations) is not int or minimum_confirmations < 1:
            raise ValueError("pons_signal_minimum_confirmations_invalid")
        self.rpc = rpc
        self.minimum_confirmations = minimum_confirmations

    def verify(self, signal: TradeSignalEnvelope) -> SourceReceiptEvidence:
        tx_hash = str(signal.tx_hash or "")
        actor = str(signal.actor_wallet or "")
        if (signal.source != "fomo_push" or signal.chain_id != str(CHAIN_ID)
                or signal.side != "buy" or signal.token_out.lower() != TOKEN_OUT
                or not HASH.fullmatch(tx_hash) or not ADDRESS.fullmatch(actor)
                or int(actor, 16) == 0):
            raise ValueError("pons_signal_source_unconfirmed")
        tx_hash, actor = tx_hash.lower(), actor.lower()
        if _quantity(self.rpc.call("eth_chainId", [])) != CHAIN_ID:
            raise ValueError("pons_signal_source_wrong_chain")
        receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
        if not isinstance(receipt, Mapping):
            raise ValueError("pons_signal_source_unconfirmed")
        block_hash = str(receipt.get("blockHash") or "").lower()
        block_number = _quantity(receipt.get("blockNumber"))
        if (str(receipt.get("transactionHash") or "").lower() != tx_hash
                or not HASH.fullmatch(block_hash)
                or _quantity(receipt.get("status")) != 1):
            raise ValueError("pons_signal_source_unconfirmed")
        header = self.rpc.call("eth_getBlockByNumber", [hex(block_number), False])
        if (not isinstance(header, Mapping)
                or str(header.get("hash") or "").lower() != block_hash
                or _quantity(header.get("number")) != block_number):
            raise ValueError("pons_signal_source_reorged")
        head = _quantity(self.rpc.call("eth_blockNumber", []))
        confirmations = head - block_number + 1
        if confirmations < self.minimum_confirmations:
            raise ValueError("pons_signal_source_unconfirmed")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise ValueError("pons_signal_source_unconfirmed")
        received = 0
        for log in logs:
            if not isinstance(log, Mapping) or str(log.get("address") or "").lower() != TOKEN_OUT:
                continue
            topics = log.get("topics")
            if (not isinstance(topics, list) or len(topics) != 3
                    or str(topics[0]).lower() != TRANSFER_TOPIC
                    or not HASH.fullmatch(str(topics[1]))
                    or not HASH.fullmatch(str(topics[2]))
                    or str(topics[2]).lower() != "0x" + "0" * 24 + actor[2:]
                    or str(log.get("transactionHash") or "").lower() != tx_hash
                    or str(log.get("blockHash") or "").lower() != block_hash
                    or log.get("removed") is True):
                continue
            amount_text = log.get("data")
            if not isinstance(amount_text, str) or not HASH.fullmatch(amount_text):
                continue
            received += int(amount_text, 16)
        if received <= 0:
            raise ValueError("pons_signal_target_transfer_missing")
        return SourceReceiptEvidence(signal.signal_id, tx_hash, block_number,
                                     block_hash, confirmations, received)
