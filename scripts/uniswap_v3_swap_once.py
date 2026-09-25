"""One-shot Robinhood Chain Uniswap V3 WETH-to-token swap.

The default mode performs real RPC reads plus eth_call/eth_estimateGas only.
Broadcasting requires both an explicit flag and an OS-vault signer profile.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from curl_cffi import requests as cf
from dotenv import dotenv_values

from fomo.execution.direct_v3 import (CHAIN_CONFIGS, UniswapV3PoolReader,
                                      V3PoolTarget, pin_v3_block_context)
from fomo.execution.evm_transaction import (Eip1559Fields, VaultEvmSigner,
                                            decode_eip1559, keccak256)
from fomo.execution.interfaces import BuiltTransaction
from fomo.execution.url_safety import validate_endpoint_url
from fomo.execution.v3_transaction import (build_unsigned_swap,
                                           decode_exact_input_single,
                                           minimum_out)
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.signals.strategy import ExecutionIntent

CHAIN_ID = 4663
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
PROJECT_DIR = Path(__file__).resolve().parents[1]
PROFILE_PATH = PROJECT_DIR / "execution-wallet.json"
_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_READ_WRITE_METHODS = frozenset({
    "eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_call",
    "eth_estimateGas", "eth_getTransactionCount", "eth_maxPriorityFeePerGas",
    "eth_sendRawTransaction", "eth_getTransactionReceipt", "eth_getBalance",
    "eth_getCode", "eth_getLogs",
})


def intent_from_signal(signal: TradeSignalEnvelope) -> ExecutionIntent:
    """Map one normalized buy signal to this script's exact-amount intent."""
    token_in = _address(signal.token_in)
    token_out = _address(signal.token_out)
    if (signal.chain_id != str(CHAIN_ID) or signal.side != "buy"
            or token_in != WETH or token_out == WETH
            or signal.confirmation_level not in {"confirmed", "finalized"}
            or signal.source_amount is None):
        raise ValueError("swap_once_signal_scope_invalid")
    try:
        amount = int(signal.source_amount)
    except (TypeError, ValueError) as error:
        raise ValueError("swap_once_signal_amount_invalid") from error
    if amount <= 0 or str(amount) != signal.source_amount:
        raise ValueError("swap_once_signal_amount_invalid")
    return ExecutionIntent(
        intent_id=f"intent:{signal.signal_id}", signal_id=signal.signal_id,
        source=signal.source, strategy_id="v3-swap-once-v1",
        allocation_key=f"swap-once:{signal.chain_id}", chain_id=signal.chain_id,
        side="buy", token_in=token_in, token_out=token_out,
        requested_usd=Decimal("0"), requested_asset_amount=str(amount),
    )


class SignalDedupe:
    """Minimal durable signal fence; no SQLite, retries or execution journal."""

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory).resolve()

    def _paths(self, signal_id: str) -> tuple[Path, Path]:
        digest = hashlib.sha256(signal_id.encode()).hexdigest()
        return self.directory / f"{digest}.json", self.directory / f"{digest}.lock"

    @staticmethod
    def _write(path: Path, payload: Mapping[str, Any]) -> None:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(dict(payload), separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, path)

    def execute(self, signal_id: str, *, broadcast: bool,
                operation: Callable[[], Mapping[str, Any]]) -> Mapping[str, Any]:
        self.directory.mkdir(parents=True, exist_ok=True)
        record, lock = self._paths(signal_id)
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as error:
            raise ValueError("swap_once_duplicate_signal") from error
        os.close(descriptor)
        previous: Mapping[str, Any] | None = None
        try:
            if record.is_file():
                try:
                    loaded = json.loads(record.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    raise ValueError("swap_once_dedupe_record_invalid") from error
                if (not isinstance(loaded, Mapping)
                        or loaded.get("signalId") != signal_id
                        or loaded.get("state") not in {"simulated", "broadcasting", "succeeded"}):
                    raise ValueError("swap_once_dedupe_record_invalid")
                previous = loaded
            if broadcast:
                if previous is not None and previous.get("state") != "simulated":
                    raise ValueError("swap_once_duplicate_signal")
                # Persist before any signing/broadcast call. An uncertain failure
                # remains fenced and requires explicit human recovery.
                self._write(record, {"signalId": signal_id, "state": "broadcasting"})
            elif previous is not None:
                raise ValueError("swap_once_duplicate_signal")
            result = operation()
            state = "succeeded" if broadcast else "simulated"
            completed: dict[str, Any] = {"signalId": signal_id, "state": state}
            if broadcast:
                completed["transactionHash"] = result.get("transactionHash")
            self._write(record, completed)
            return result
        finally:
            try:
                lock.unlink()
            except FileNotFoundError:
                pass


def _read_signal(path: Path | str) -> TradeSignalEnvelope:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("swap_once_signal_file_invalid") from error
    if not isinstance(payload, Mapping):
        raise ValueError("swap_once_signal_file_invalid")
    try:
        return TradeSignalEnvelope.from_dict(payload)
    except (TypeError, ValueError) as error:
        raise ValueError("swap_once_signal_invalid") from error


def _address(value: str) -> str:
    text = str(value).lower()
    if not _ADDRESS.fullmatch(text) or not int(text, 16):
        raise ValueError("swap_once_address_invalid")
    return text


def _quantity(value: Any) -> int:
    number = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    if number < 0:
        raise ValueError("swap_once_quantity_invalid")
    return number


def _word(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError("swap_once_word_invalid")
    return int(value, 16)


def _selector(signature: str) -> str:
    return "0x" + keccak256(signature.encode())[:4].hex()


class SwapOnceRpc:
    """Single-endpoint JSON-RPC client with a narrow method allowlist."""

    supports_multicall3 = True

    def __init__(self, url: str, *, timeout_seconds: float = 20.0) -> None:
        self.url, first = validate_endpoint_url(url)
        _, second = validate_endpoint_url(url)
        if first != second or not self.url.startswith("https://"):
            raise ValueError("swap_once_rpc_endpoint_invalid")
        self.timeout_seconds = timeout_seconds
        self.last_provider = "robinhood-primary"
        self.multicall_subcall_count = 0

    def record_multicall_subcalls(self, count: int) -> None:
        if not 0 < count <= 64:
            raise ValueError("swap_once_multicall_scope_invalid")
        self.multicall_subcall_count += count

    def call(self, method: str, params: Sequence[Any]) -> Any:
        if method not in _READ_WRITE_METHODS:
            raise ValueError("swap_once_rpc_method_forbidden")
        response = cf.post(
            self.url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
            headers={"Accept": "application/json"}, timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise ValueError(f"swap_once_rpc_http_{response.status_code}")
        body = response.json()
        if not isinstance(body, Mapping) or body.get("jsonrpc") != "2.0" or body.get("id") != 1:
            raise ValueError("swap_once_rpc_response_invalid")
        if body.get("error") is not None:
            error = body.get("error")
            code = error.get("code") if isinstance(error, Mapping) else None
            raise ValueError(f"swap_once_rpc_rejected:{method}:{code}")
        if "result" not in body:
            raise ValueError("swap_once_rpc_response_invalid")
        return body["result"]


def _rpc_url() -> str:
    injected = os.getenv("RPC_ROBINHOOD_URL", "").strip()
    if injected:
        return injected
    configured = str(dotenv_values(PROJECT_DIR / ".env").get("RPC_ROBINHOOD_URL") or "").strip()
    if not configured:
        raise ValueError("swap_once_rpc_not_configured")
    return configured


def _wallet_profile() -> tuple[str, Mapping[str, Any]]:
    try:
        profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
        account = next(
            item for item in profile["accounts"]
            if item.get("family") == "evm" and str(CHAIN_ID) in item.get("chainIds", [])
        )
        wallet = _address(account["address"])
        signer = profile.get("signer")
    except (OSError, ValueError, KeyError, StopIteration, TypeError, json.JSONDecodeError) as error:
        raise ValueError("swap_once_wallet_profile_invalid") from error
    if not isinstance(signer, Mapping):
        signer = {}
    return wallet, signer


def _eth_call_word(rpc: SwapOnceRpc, *, target: str, data: str, tag: str,
                   state_override: Mapping[str, Any] | None = None) -> int:
    params: list[Any] = [{"to": _address(target), "data": data}, tag]
    if state_override is not None:
        params.append(dict(state_override))
    return _word(rpc.call("eth_call", params))


def _factory_pool(rpc: SwapOnceRpc, *, token0: str, token1: str, fee: int,
                  tag: str) -> str | None:
    data = (_selector("getPool(address,address,uint24)")
            + f"{int(token0, 16):064x}{int(token1, 16):064x}{fee:064x}")
    value = _eth_call_word(rpc, target=CHAIN_CONFIGS[CHAIN_ID].factory, data=data, tag=tag)
    if value == 0:
        return None
    if value >= 2**160:
        raise ValueError("swap_once_factory_pool_invalid")
    return f"0x{value:040x}"


def _tick_spacing(rpc: SwapOnceRpc, *, fee: int, tag: str) -> int:
    data = _selector("feeAmountTickSpacing(uint24)") + f"{fee:064x}"
    raw = _eth_call_word(rpc, target=CHAIN_CONFIGS[CHAIN_ID].factory, data=data, tag=tag)
    low = raw & ((1 << 24) - 1)
    upper = raw >> 24
    negative = bool(low & (1 << 23))
    if upper != ((1 << 232) - 1 if negative else 0):
        raise ValueError("swap_once_tick_spacing_invalid")
    spacing = low - (1 << 24) if negative else low
    if not 0 < spacing < 32768:
        raise ValueError("swap_once_tick_spacing_invalid")
    return spacing


def _balance_and_allowance(rpc: SwapOnceRpc, *, wallet: str, router: str,
                           tag: str) -> tuple[int, int]:
    balance = _eth_call_word(
        rpc, target=WETH, data="0x70a08231" + wallet[2:].rjust(64, "0"), tag=tag,
    )
    allowance = _eth_call_word(
        rpc, target=WETH,
        data="0xdd62ed3e" + wallet[2:].rjust(64, "0") + router[2:].rjust(64, "0"),
        tag=tag,
    )
    return balance, allowance


def _weth_override(*, wallet: str, router: str, balance: int,
                   allowance: int) -> Mapping[str, Any]:
    if not 0 < balance < 2**256 or not 0 < allowance < 2**256:
        raise ValueError("swap_once_override_amount_invalid")
    owner = int(wallet, 16).to_bytes(32, "big")
    spender = int(router, 16).to_bytes(32, "big")
    balance_slot = keccak256(owner + (3).to_bytes(32, "big"))
    allowance_outer = keccak256(owner + (4).to_bytes(32, "big"))
    allowance_slot = keccak256(spender + allowance_outer)
    return {WETH: {"stateDiff": {
        "0x" + balance_slot.hex(): "0x" + balance.to_bytes(32, "big").hex(),
        "0x" + allowance_slot.hex(): "0x" + allowance.to_bytes(32, "big").hex(),
    }}}


def _verify_weth_override(rpc: SwapOnceRpc, *, wallet: str, router: str,
                          balance: int, allowance: int, tag: str,
                          override: Mapping[str, Any]) -> None:
    observed_balance = _eth_call_word(
        rpc, target=WETH, data="0x70a08231" + wallet[2:].rjust(64, "0"), tag=tag,
        state_override=override,
    )
    observed_allowance = _eth_call_word(
        rpc, target=WETH,
        data="0xdd62ed3e" + wallet[2:].rjust(64, "0") + router[2:].rjust(64, "0"),
        tag=tag, state_override=override,
    )
    if observed_balance != balance or observed_allowance != allowance:
        raise ValueError("swap_once_override_verification_failed")


def _fees(rpc: SwapOnceRpc) -> tuple[int, int]:
    priority = _quantity(rpc.call("eth_maxPriorityFeePerGas", []))
    pending = rpc.call("eth_getBlockByNumber", ["pending", False])
    if not isinstance(pending, Mapping) or pending.get("baseFeePerGas") is None:
        raise ValueError("swap_once_eip1559_fee_unavailable")
    base = _quantity(pending["baseFeePerGas"])
    maximum = base * 2 + priority
    if priority <= 0 or maximum < priority:
        raise ValueError("swap_once_eip1559_fee_invalid")
    return priority, maximum


def _approve_transaction(*, wallet: str, router: str, amount: int, nonce: int,
                         gas_limit: int, priority_fee: int,
                         maximum_fee: int) -> BuiltTransaction:
    data = bytes.fromhex("095ea7b3" + router[2:].rjust(64, "0") + f"{amount:064x}")
    fields = Eip1559Fields(CHAIN_ID, nonce, priority_fee, maximum_fee, gas_limit,
                           bytes.fromhex(WETH[2:]), 0, data)
    transaction = BuiltTransaction(fields.unsigned_bytes(), "uniswap_v3_swap_once", str(nonce))
    decoded, _ = decode_eip1559(transaction.serialized, signed=False)
    if (decoded.chain_id != CHAIN_ID or decoded.nonce != nonce
            or "0x" + decoded.to.hex() != WETH or decoded.value != 0
            or decoded.data != data or wallet == router):
        raise ValueError("swap_once_approve_scope_invalid")
    return transaction


def _call_object(transaction: BuiltTransaction, wallet: str) -> dict[str, str]:
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    return {"from": wallet, "to": "0x" + fields.to.hex(),
            "data": "0x" + fields.data.hex(), "value": hex(fields.value),
            "gas": hex(fields.gas_limit), "maxFeePerGas": hex(fields.maximum_fee),
            "maxPriorityFeePerGas": hex(fields.priority_fee)}


def _estimate_and_call(rpc: SwapOnceRpc, *, transaction: BuiltTransaction,
                       wallet: str, tag: str,
                       state_override: Mapping[str, Any] | None = None) -> int:
    call = _call_object(transaction, wallet)
    estimate_params: list[Any] = [call, tag]
    call_params: list[Any] = [call, tag]
    if state_override is not None:
        estimate_params.append(dict(state_override))
        call_params.append(dict(state_override))
    estimate = _quantity(rpc.call("eth_estimateGas", estimate_params))
    if not 21_000 <= estimate <= 1_000_000:
        raise ValueError("swap_once_estimated_gas_invalid")
    rpc.call("eth_call", call_params)
    return estimate


def _send_and_wait(rpc: SwapOnceRpc, *, signer: VaultEvmSigner,
                   transaction: BuiltTransaction, timeout_seconds: int = 180) -> tuple[str, int]:
    signed = signer.sign(transaction)
    local_hash = "0x" + keccak256(signed).hex()
    sent = str(rpc.call("eth_sendRawTransaction", ["0x" + signed.hex()])).lower()
    if sent != local_hash:
        raise ValueError("swap_once_broadcast_hash_mismatch")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        receipt = rpc.call("eth_getTransactionReceipt", [local_hash])
        if receipt is None:
            time.sleep(2)
            continue
        if (not isinstance(receipt, Mapping)
                or str(receipt.get("transactionHash") or "").lower() != local_hash
                or not _HASH.fullmatch(str(receipt.get("blockHash") or ""))):
            raise ValueError("swap_once_receipt_invalid")
        status = _quantity(receipt.get("status", 0))
        if status != 1:
            raise ValueError("swap_once_receipt_failed")
        return local_hash, status
    raise ValueError("swap_once_receipt_timeout")


def _signer(signer_profile: Mapping[str, Any], wallet: str) -> VaultEvmSigner:
    if (signer_profile.get("backend") != "os_credential_store"
            or not isinstance(signer_profile.get("reference"), str)
            or not signer_profile["reference"]):
        raise ValueError("swap_once_vault_signer_not_enabled")
    signer = VaultEvmSigner(str(signer_profile["reference"]), wallet)
    if not signer.self_check().ready:
        raise ValueError("swap_once_vault_signer_unavailable")
    return signer


def execute_once(*, token_out: str, amount_in: int, fee: int,
                 slippage_bps: int, broadcast: bool,
                 rpc: SwapOnceRpc | None = None) -> Mapping[str, Any]:
    started = time.monotonic()
    token = _address(token_out)
    if (token == WETH or not 0 < amount_in < 2**128 or not 0 < fee < 1_000_000
            or not 0 <= slippage_bps <= 500):
        raise ValueError("swap_once_input_invalid")
    client = rpc or SwapOnceRpc(_rpc_url())
    if _quantity(client.call("eth_chainId", [])) != CHAIN_ID:
        raise ValueError("swap_once_wrong_chain")
    config = CHAIN_CONFIGS[CHAIN_ID]
    token0, token1 = sorted((WETH, token))
    pool = _factory_pool(client, token0=token0, token1=token1, fee=fee, tag="latest")
    if pool is None:
        raise ValueError("swap_once_pool_missing")
    context = pin_v3_block_context(client, chain_id=CHAIN_ID, confirmations=0)
    tag = hex(context.block_height)
    spacing = _tick_spacing(client, fee=fee, tag=tag)
    target = V3PoolTarget(pool, token0, token1, fee, spacing, -1, -1)
    snapshot = UniswapV3PoolReader(rpc=client, chain_id=CHAIN_ID, target=target).snapshot(
        context=context,
    )
    quote = snapshot.quote(token_in=WETH, amount_in=amount_in)
    quoted_out = quote.amount_out
    min_out = minimum_out(quoted_out, slippage_bps)
    wallet, signer_profile = _wallet_profile()
    balance, allowance = _balance_and_allowance(
        client, wallet=wallet, router=config.router, tag=tag,
    )
    if balance < amount_in:
        raise ValueError("swap_once_weth_balance_insufficient")
    allowance_sufficient = allowance >= amount_in
    approve_required = not allowance_sufficient
    priority_fee, maximum_fee = _fees(client)
    nonce = _quantity(client.call("eth_getTransactionCount", [wallet, "pending"]))
    state_override: Mapping[str, Any] | None = None
    signer: VaultEvmSigner | None = None
    if broadcast:
        signer = _signer(signer_profile, wallet)
    if approve_required:
        approve = _approve_transaction(
            wallet=wallet, router=config.router, amount=amount_in, nonce=nonce,
            gas_limit=150_000, priority_fee=priority_fee, maximum_fee=maximum_fee,
        )
        approve_gas = _estimate_and_call(
            client, transaction=approve, wallet=wallet, tag=tag,
        )
        approve = _approve_transaction(
            wallet=wallet, router=config.router, amount=amount_in, nonce=nonce,
            gas_limit=min(1_000_000, max(21_000, approve_gas * 120 // 100)),
            priority_fee=priority_fee, maximum_fee=maximum_fee,
        )
        if broadcast:
            assert signer is not None
            _send_and_wait(client, signer=signer, transaction=approve)
            # Approval changed chain state. Re-pin the pool, re-quote and verify
            # the mined allowance before constructing or simulating the swap.
            context = pin_v3_block_context(client, chain_id=CHAIN_ID, confirmations=0)
            tag = hex(context.block_height)
            spacing = _tick_spacing(client, fee=fee, tag=tag)
            refreshed_target = V3PoolTarget(pool, token0, token1, fee, spacing, -1, -1)
            snapshot = UniswapV3PoolReader(
                rpc=client, chain_id=CHAIN_ID, target=refreshed_target,
            ).snapshot(context=context)
            quote = snapshot.quote(token_in=WETH, amount_in=amount_in)
            quoted_out = quote.amount_out
            min_out = minimum_out(quoted_out, slippage_bps)
            balance, allowance = _balance_and_allowance(
                client, wallet=wallet, router=config.router, tag=tag,
            )
            if balance < amount_in or allowance < amount_in:
                raise ValueError("swap_once_post_approve_state_invalid")
            nonce = _quantity(client.call("eth_getTransactionCount", [wallet, "pending"]))
        else:
            state_override = _weth_override(
                wallet=wallet, router=config.router, balance=balance,
                allowance=amount_in,
            )
            _verify_weth_override(
                client, wallet=wallet, router=config.router, balance=balance,
                allowance=amount_in, tag=tag, override=state_override,
            )
    deadline = int(time.time()) + 180
    unsigned = build_unsigned_swap(
        chain_id=CHAIN_ID, router=config.router, token0=token0, token1=token1,
        fee=fee, token_in=WETH, wallet=wallet, amount_in=amount_in,
        quoted_out=quoted_out, slippage_bps=slippage_bps, nonce=nonce,
        gas_limit=1_000_000, priority_fee_wei=priority_fee,
        maximum_fee_wei=maximum_fee, deadline=deadline,
    )
    fields, _ = decode_eip1559(unsigned.serialized, signed=False)
    decoded = decode_exact_input_single(fields.data, chain_id=CHAIN_ID)
    if (fields.chain_id != CHAIN_ID or fields.nonce != nonce
            or "0x" + fields.to.hex() != config.router or fields.value != 0
            or decoded["tokenIn"] != WETH or decoded["tokenOut"] != token
            or decoded["fee"] != fee or decoded["recipient"] != wallet
            or decoded["amountIn"] != amount_in
            or decoded["amountOutMinimum"] != min_out
            or decoded["deadline"] != deadline):
        raise ValueError("swap_once_unsigned_scope_invalid")
    estimated_gas = _estimate_and_call(
        client, transaction=unsigned, wallet=wallet, tag=tag,
        state_override=state_override,
    )
    final = build_unsigned_swap(
        chain_id=CHAIN_ID, router=config.router, token0=token0, token1=token1,
        fee=fee, token_in=WETH, wallet=wallet, amount_in=amount_in,
        quoted_out=quoted_out, slippage_bps=slippage_bps, nonce=nonce,
        gas_limit=min(1_000_000, max(21_000, estimated_gas * 120 // 100)),
        priority_fee_wei=priority_fee, maximum_fee_wei=maximum_fee,
        deadline=deadline,
    )
    final_fields, _ = decode_eip1559(final.serialized, signed=False)
    final_swap = decode_exact_input_single(final_fields.data, chain_id=CHAIN_ID)
    if (final_fields.chain_id != CHAIN_ID or final_fields.nonce != nonce
            or "0x" + final_fields.to.hex() != config.router
            or final_swap != decoded):
        raise ValueError("swap_once_final_scope_invalid")
    # Simulate the exact final bytes after the gas limit rebuild.
    _estimate_and_call(client, transaction=final, wallet=wallet, tag=tag,
                       state_override=state_override)
    tx_hash: str | None = None
    receipt_status: int | None = None
    if broadcast:
        assert signer is not None
        tx_hash, receipt_status = _send_and_wait(client, signer=signer, transaction=final)
    output: dict[str, Any] = {
        "chainId": CHAIN_ID, "tokenIn": WETH, "tokenOut": token,
        "pool": pool, "fee": fee, "amountIn": str(amount_in),
        "quotedOut": str(quoted_out), "minOut": str(min_out),
        "allowanceSufficient": allowance_sufficient,
        "approveRequired": approve_required, "estimatedGas": estimated_gas,
        "simulation_success": True,
        "totalElapsedMs": int((time.monotonic() - started) * 1000),
    }
    if tx_hash is not None:
        output.update({"transactionHash": tx_hash, "receiptStatus": receipt_status})
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-file")
    parser.add_argument("--dedupe-dir", default=str(PROJECT_DIR / "data" / "swap-once-dedupe"))
    parser.add_argument("--token-out")
    parser.add_argument("--amount-in-units", type=int)
    parser.add_argument("--fee", required=True, type=int)
    parser.add_argument("--slippage-bps", type=int, default=100)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--simulate", action="store_true")
    mode.add_argument("--broadcast", action="store_true")
    options = parser.parse_args(argv)
    try:
        if options.signal_file:
            if options.token_out is not None or options.amount_in_units is not None:
                raise ValueError("swap_once_signal_and_direct_args_conflict")
            if options.fee != 10_000:
                raise ValueError("swap_once_signal_fee_unapproved")
            signal = _read_signal(options.signal_file)
            intent = intent_from_signal(signal)
            requested_asset_amount = intent.requested_asset_amount
            if requested_asset_amount is None:
                raise ValueError("swap_once_signal_amount_invalid")

            def run_signal() -> Mapping[str, Any]:
                executed = dict(execute_once(
                    token_out=intent.token_out,
                    amount_in=int(requested_asset_amount),
                    fee=options.fee,
                    slippage_bps=options.slippage_bps,
                    broadcast=bool(options.broadcast),
                ))
                executed.update({"signalId": signal.signal_id, "intentId": intent.intent_id})
                return executed

            result = SignalDedupe(options.dedupe_dir).execute(
                signal.signal_id, broadcast=bool(options.broadcast), operation=run_signal,
            )
        else:
            if options.token_out is None or options.amount_in_units is None:
                raise ValueError("swap_once_direct_args_required")
            if options.broadcast:
                raise ValueError("swap_once_broadcast_requires_signal")
            result = execute_once(
                token_out=options.token_out, amount_in=options.amount_in_units,
                fee=options.fee, slippage_bps=options.slippage_bps,
                broadcast=bool(options.broadcast),
            )
    except Exception as error:
        reason = str(error)
        safe = reason if reason.startswith("swap_once_") or reason.startswith("v3_") else type(error).__name__
        print(json.dumps({"simulation_success": False, "reason": safe}))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
