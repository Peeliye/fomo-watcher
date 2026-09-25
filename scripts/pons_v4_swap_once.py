"""One pinned Pons V4 swap; simulation by default, scoped operator gates for send."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from coincurve import PublicKey
from curl_cffi import requests as cf

from fomo.execution.direct_v4 import CHAIN_CONFIGS, ZERO, _call
from fomo.execution.evm_transaction import decode_eip1559, keccak256
from fomo.execution.pons_v4_hook import (HOOK, REVIEWED_RUNTIME_CODE_HASH,
                                         verify_runtime_code)
from fomo.execution.pons_v4_once import (CHAIN_ID, INITIALIZE_BLOCK, KEY, POOL_ID, ROUTE_ID,
                                         TOKEN_OUT, build_unsigned, checked_key,
                                         decode_unsigned, quote_gross)
from fomo.execution.v4_discovery import INITIALIZE_TOPIC, decode_initialize
from scripts.uniswap_v3_swap_once import (_READ_WRITE_METHODS, _quantity, _rpc_url,
                                         _signer, _wallet_profile, SwapOnceRpc)

MAX_GAS = 1_000_000
FIXED_REVIEWED_GAS = 350_000
HASH = re.compile(r"0x[0-9a-fA-F]{64}")
SIGNAL_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
PROJECT_DIR = Path(__file__).resolve().parents[1]
SIGNAL_DB = PROJECT_DIR / "data" / "pons-v4-once-signals.sqlite3"
EXECUTION_DB = PROJECT_DIR / "data" / "execution.sqlite3"
MAX_BROADCAST_BLOCK_DISTANCE = 192
MIN_DEADLINE_REMAINING_S = 45
MAX_FEE_PER_GAS_WEI = 100_000_000_000
MAX_PRIORITY_FEE_WEI = 10_000_000_000
MAX_GAS_COST_WEI = 100_000_000_000_000
PONS_RPC_METHODS = _READ_WRITE_METHODS | {"eth_getTransactionByHash"}
BATCH_READ_METHODS = PONS_RPC_METHODS - {"eth_sendRawTransaction"}
LEGACY_SIGNAL_COLUMNS = ("signal_id", "wallet", "chain_id", "token_out", "amount_in_wei",
                         "state", "nonce", "tx_hash", "unsigned_hash", "updated_at")
INTERMEDIATE_SIGNAL_COLUMNS = LEGACY_SIGNAL_COLUMNS + ("released_nonce", "release_reason")
SIGNAL_COLUMNS = INTERMEDIATE_SIGNAL_COLUMNS + ("released_tx_hash",)


def migrate_signal_ledger(path: Path = SIGNAL_DB) -> Path | None:
    """Back up and extend only the Pons signal ledger; never discard signed evidence."""
    if not path.is_file():
        raise ValueError("pons_once_signal_db_missing")
    with closing(sqlite3.connect(path, timeout=5, isolation_level=None)) as source:
        source.execute("PRAGMA busy_timeout=5000")
        columns = tuple(row[1] for row in source.execute("PRAGMA table_info(pons_signals)"))
        if columns == SIGNAL_COLUMNS:
            return None
        if columns not in (LEGACY_SIGNAL_COLUMNS, INTERMEDIATE_SIGNAL_COLUMNS):
            raise ValueError("pons_once_signal_db_schema_unknown")
        before = source.execute("SELECT * FROM pons_signals ORDER BY rowid").fetchall()
        backup_dir = path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / f"{path.stem}.{uuid.uuid4().hex}.sqlite3"
        with closing(sqlite3.connect(backup)) as copy:
            source.backup(copy)
            if (copy.execute("PRAGMA integrity_check").fetchone() != ("ok",)
                    or tuple(row[1] for row in copy.execute(
                        "PRAGMA table_info(pons_signals)")) != columns
                    or copy.execute("SELECT * FROM pons_signals ORDER BY rowid").fetchall() != before):
                raise ValueError("pons_once_signal_backup_invalid")
        source.execute("BEGIN IMMEDIATE")
        try:
            if source.execute("SELECT * FROM pons_signals ORDER BY rowid").fetchall() != before:
                raise ValueError("pons_once_signal_db_changed_during_backup")
            if columns == LEGACY_SIGNAL_COLUMNS:
                source.execute("ALTER TABLE pons_signals ADD COLUMN released_nonce INTEGER")
                source.execute("ALTER TABLE pons_signals ADD COLUMN release_reason TEXT")
            source.execute("ALTER TABLE pons_signals ADD COLUMN released_tx_hash TEXT")
            if (source.execute("PRAGMA integrity_check").fetchone() != ("ok",)
                    or [row[:len(columns)] for row in source.execute(
                        "SELECT * FROM pons_signals ORDER BY rowid")] != before):
                raise ValueError("pons_once_signal_migration_invalid")
            source.commit()
        except BaseException:
            source.rollback()
            raise
        return backup


class PonsRpc(SwapOnceRpc):
    """Same endpoint guard as the existing client, with sanitized revert bytes."""

    last_revert_data: str | None = None

    def __init__(self, url: str, *, timeout_seconds: float = 20.0) -> None:
        super().__init__(url, timeout_seconds=timeout_seconds)
        self.http_round_trips = 0
        self.json_rpc_calls = 0
        self.multicall_subcall_count = 0
        self._identity_verified = False
        self.wallet_profile: tuple[str, Mapping[str, Any]] | None = None
        self.cached_lower_tick: int | None = None
        self._session = cf.Session()

    def close(self) -> None:
        self._session.close()

    def record_multicall_subcalls(self, count: int) -> None:
        self.multicall_subcall_count += count

    def call(self, method: str, params: Sequence[Any]) -> Any:
        self.last_revert_data = None
        if method not in PONS_RPC_METHODS:
            raise ValueError("pons_once_rpc_method_forbidden")
        self.http_round_trips += 1
        self.json_rpc_calls += 1
        response = self._session.post(
            self.url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
            headers={"Accept": "application/json"}, timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise ValueError(f"pons_once_rpc_http_{response.status_code}")
        body = response.json()
        if not isinstance(body, Mapping) or body.get("jsonrpc") != "2.0" or body.get("id") != 1:
            raise ValueError("pons_once_rpc_response_invalid")
        if body.get("error") is not None:
            error = body["error"]
            code = error.get("code") if isinstance(error, Mapping) else None
            if isinstance(error, Mapping):
                data = error.get("data")
                if isinstance(data, Mapping):
                    data = data.get("data")
                if isinstance(data, str) and re.fullmatch(r"0x[0-9a-fA-F]*", data):
                    self.last_revert_data = data.lower()
            raise ValueError(f"pons_once_rpc_rejected:{method}:{code}")
        if "result" not in body:
            raise ValueError("pons_once_rpc_response_invalid")
        return body["result"]

    def call_batch(self, requests: Sequence[tuple[str, list[Any]]]) -> list[Any]:
        if not 1 < len(requests) <= 8 or any(method not in BATCH_READ_METHODS
                                              for method, _ in requests):
            raise ValueError("pons_once_batch_scope_invalid")
        payload = [{"jsonrpc": "2.0", "id": index + 1, "method": method,
                    "params": params} for index, (method, params) in enumerate(requests)]
        self.http_round_trips += 1
        self.json_rpc_calls += len(requests)
        response = self._session.post(self.url, json=payload, headers={"Accept": "application/json"},
                           timeout=self.timeout_seconds, allow_redirects=False)
        if response.status_code != 200:
            raise ValueError(f"pons_once_rpc_http_{response.status_code}")
        body = response.json()
        if not isinstance(body, list) or len(body) != len(requests):
            raise ValueError("pons_once_batch_response_invalid")
        by_id = {item.get("id"): item for item in body if isinstance(item, Mapping)}
        if len(by_id) != len(requests):
            raise ValueError("pons_once_batch_response_invalid")
        values = []
        for index in range(len(requests)):
            item = by_id.get(index + 1)
            if (not isinstance(item, Mapping) or item.get("jsonrpc") != "2.0"
                    or item.get("error") is not None or "result" not in item):
                raise ValueError("pons_once_batch_call_failed")
            values.append(item["result"])
        return values


class SignalLedger:
    """One-way claim: a send attempt is never automatically replayed."""

    SCHEMA = """CREATE TABLE pons_signals (
      signal_id TEXT PRIMARY KEY, wallet TEXT NOT NULL, chain_id INTEGER NOT NULL,
      token_out TEXT NOT NULL, amount_in_wei TEXT NOT NULL,
      state TEXT NOT NULL, nonce INTEGER, tx_hash TEXT UNIQUE,
      unsigned_hash TEXT, updated_at INTEGER NOT NULL,
      released_nonce INTEGER, release_reason TEXT, released_tx_hash TEXT,
      UNIQUE(wallet,chain_id,nonce)
    )"""

    def __init__(self, path: Path = SIGNAL_DB) -> None:
        self.path = path
        existed = path.exists()
        if not existed:
            path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=5, isolation_level=None)
        try:
            self.db.execute("PRAGMA busy_timeout=5000")
            self.db.execute("PRAGMA synchronous=FULL")
            if existed:
                row = self.db.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='pons_signals'"
                ).fetchone()
                if row is None:
                    raise ValueError("pons_once_signal_db_schema_unknown")
                columns = tuple(item[1] for item in self.db.execute(
                    "PRAGMA table_info(pons_signals)"))
                if columns != SIGNAL_COLUMNS:
                    raise ValueError("pons_once_signal_db_migration_required")
            else:
                self.db.execute(self.SCHEMA)
        except Exception:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def _update(self, signal_id: str, state: str, *, nonce: int | None = None,
                tx_hash: str | None = None, unsigned_hash: str | None = None) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if nonce is None:
                cursor = self.db.execute(
                    "UPDATE pons_signals SET state=?,updated_at=? WHERE signal_id=?",
                    (state, int(time.time()), signal_id))
            else:
                cursor = self.db.execute(
                    "UPDATE pons_signals SET state=?,nonce=?,tx_hash=?,unsigned_hash=?,updated_at=? "
                    "WHERE signal_id=? AND nonce IS NULL AND state='claimed'",
                    (state, nonce, tx_hash, unsigned_hash, int(time.time()), signal_id))
            if cursor.rowcount != 1:
                raise ValueError("pons_once_signal_state_conflict")
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def claim(self, signal_id: str, wallet: str, amount_in: int) -> None:
        if not SIGNAL_ID.fullmatch(signal_id):
            raise ValueError("pons_once_signal_id_invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO pons_signals(signal_id,wallet,chain_id,token_out,"
                "amount_in_wei,state,nonce,tx_hash,unsigned_hash,updated_at) "
                "VALUES(?,?,?,?,?,'claimed',NULL,NULL,NULL,?)",
                (signal_id, wallet, CHAIN_ID, TOKEN_OUT, str(amount_in), int(time.time())))
            self.db.commit()
        except sqlite3.IntegrityError as error:
            self.db.rollback()
            raise ValueError("pons_once_duplicate_signal") from error
        except Exception:
            self.db.rollback()
            raise

    def mark_attempt(self, signal_id: str, nonce: int, tx_hash: str,
                     unsigned_hash: str) -> None:
        try:
            self._update(signal_id, "send_attempted", nonce=nonce,
                         tx_hash=tx_hash, unsigned_hash=unsigned_hash)
        except sqlite3.IntegrityError as error:
            raise ValueError("pons_once_nonce_conflict") from error

    def abort_before_send(self, signal_id: str, tx_hash: str, reason: str) -> None:
        """Release only a proven pre-broadcast attempt, retaining hash and nonce evidence."""
        if reason not in {"pre_send_check_failed", "operator_verified_pre_send_failure"}:
            raise ValueError("pons_once_release_reason_invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE pons_signals SET state='not_submitted',released_nonce=nonce,"
                "released_tx_hash=tx_hash,nonce=NULL,tx_hash=NULL,"
                "release_reason=?,updated_at=? WHERE signal_id=? "
                "AND state='send_attempted' AND nonce IS NOT NULL AND tx_hash=?",
                (reason, int(time.time()), signal_id, tx_hash),
            ).rowcount
            if changed != 1:
                raise ValueError("pons_once_pre_send_release_conflict")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def mark_broadcast_started(self, signal_id: str, tx_hash: str) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE pons_signals SET state='broadcast_started',updated_at=? "
                "WHERE signal_id=? AND state='send_attempted' AND tx_hash=? AND nonce IS NOT NULL",
                (int(time.time()), signal_id, tx_hash),
            ).rowcount
            if changed != 1:
                raise ValueError("pons_once_broadcast_state_conflict")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def mark(self, signal_id: str, state: str) -> None:
        if state not in {"aborted", "uncertain", "submitted", "confirmed", "failed"}:
            raise ValueError("pons_once_signal_state_invalid")
        self._update(signal_id, state)

    def lookup(self, signal_id: str) -> tuple[str, str, int] | None:
        row = self.db.execute(
            "SELECT state,tx_hash,nonce FROM pons_signals WHERE signal_id=?",
            (signal_id,)).fetchone()
        if row is None or not isinstance(row[1], str) or row[2] is None:
            return None
        return str(row[0]), str(row[1]), int(row[2])


def _live_armed(path: Path = EXECUTION_DB, *, native_in_wei: int | None = None) -> bool:
    if not path.is_file() or native_in_wei is None:
        return False
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as db:
            row = db.execute(
                "SELECT live_armed,circuit_breaker_tripped FROM execution_control WHERE singleton=1"
            ).fetchone()
            scope = db.execute(
                "SELECT chain_id,token_out,native_in_wei,route "
                "FROM execution_operator_scope WHERE singleton=1"
            ).fetchone()
            return bool(row and row[0] == 1 and row[1] == 0 and scope
                        and scope[0] == CHAIN_ID and str(scope[1]).lower() == TOKEN_OUT
                        and str(scope[2]) == str(native_in_wei) and scope[3] == ROUTE_ID)
    except (sqlite3.Error, OSError):
        return False


def operator_confirmation_text(signal_id: str, native_in_wei: int) -> str:
    return f"BUY ONCE {CHAIN_ID} {TOKEN_OUT} {native_in_wei} {POOL_ID} {signal_id}"


def _pinned_header(rpc: PonsRpc, tag: str | None = None) -> Mapping[str, Any]:
    if tag is None:
        height = _quantity(rpc.call("eth_blockNumber", []))
        tag = hex(height)
    result = rpc.call("eth_getBlockByNumber", [tag, False])
    if (not isinstance(result, Mapping) or not HASH.fullmatch(str(result.get("hash") or ""))
            or _quantity(result.get("number")) != int(tag, 16)):
        raise ValueError("pons_once_header_invalid")
    return result


def _verify_static_identity(rpc: PonsRpc, tag: str) -> None:
    checked_key(KEY)
    config = CHAIN_CONFIGS[CHAIN_ID]
    logs = rpc.call("eth_getLogs", [{"address": config.pool_manager,
                                     "fromBlock": hex(INITIALIZE_BLOCK),
                                     "toBlock": hex(INITIALIZE_BLOCK),
                                     "topics": [INITIALIZE_TOPIC, POOL_ID]}])
    if not isinstance(logs, list) or len(logs) != 1:
        raise ValueError("pons_once_initialize_missing")
    event = decode_initialize(logs[0], chain_id=CHAIN_ID,
                              expected_pool_id=POOL_ID, expected_token=TOKEN_OUT)
    if (not event.identity_verified or event.key != KEY
            or event.block_height != INITIALIZE_BLOCK):
        raise ValueError("pons_once_initialize_mismatch")
    origin = _pinned_header(rpc, hex(INITIALIZE_BLOCK))
    if str(origin["hash"]).lower() != event.block_hash:
        raise ValueError("pons_once_initialize_reorged")
    code = rpc.call("eth_getCode", [HOOK, tag])
    verify_runtime_code(code, REVIEWED_RUNTIME_CODE_HASH)
    manager = _call(rpc, HOOK, "poolManager()", tag)
    if (not isinstance(manager, str) or len(manager) != 66
            or int(manager, 16) != int(config.pool_manager, 16)):
        raise ValueError("pons_once_manager_mismatch")
    rpc._identity_verified = True


def _revert_reason(data: str | None) -> str:
    if not isinstance(data, str) or not re.fullmatch(r"0x[0-9a-fA-F]*", data):
        return "revert_data_unavailable"
    payload = bytes.fromhex(data[2:])
    if payload == bytes.fromhex("3b99b53d"):
        return "SliceOutOfBounds()"
    if len(payload) >= 4 and payload[:4].hex() == "08c379a0" and len(payload) >= 68:
        offset = int.from_bytes(payload[4:36], "big")
        start = 4 + offset
        if start + 32 <= len(payload):
            size = int.from_bytes(payload[start:start + 32], "big")
            if start + 32 + size <= len(payload) and size <= 512:
                return "Error(" + payload[start + 32:start + 32 + size].decode("utf-8", "replace") + ")"
    if len(payload) == 36 and payload[:4].hex() == "4e487b71":
        return f"Panic({int.from_bytes(payload[4:], 'big')})"
    return "custom_error_" + ("0x" + payload[:4].hex() if len(payload) >= 4 else "empty")


def _chain_fees(rpc: PonsRpc) -> tuple[int, int]:
    # Robinhood may report a zero suggested tip. A one-wei EIP-1559 tip is
    # syntactically valid, while the latest pending base fee remains RPC sourced.
    priority = rpc.call("eth_maxPriorityFeePerGas", [])
    pending = rpc.call("eth_getBlockByNumber", ["pending", False])
    return _fee_values(priority, pending)


def _fee_values(priority_value: Any, pending: Any) -> tuple[int, int]:
    priority = max(1, _quantity(priority_value))
    if not isinstance(pending, Mapping) or pending.get("baseFeePerGas") is None:
        raise ValueError("pons_once_eip1559_fee_unavailable")
    maximum = 2 * _quantity(pending["baseFeePerGas"]) + priority
    if priority > MAX_PRIORITY_FEE_WEI or maximum > MAX_FEE_PER_GAS_WEI:
        raise ValueError("pons_once_gas_fee_cap_exceeded")
    return priority, maximum


def _verify_signed(signed: bytes, unsigned: bytes, wallet: str) -> str:
    if not isinstance(signed, bytes):
        raise ValueError("pons_once_signed_bytes_invalid")
    actual, parts = decode_eip1559(signed, signed=True)
    expected, _ = decode_eip1559(unsigned, signed=False)
    if actual != expected:
        raise ValueError("pons_once_signed_scope_changed")
    parity, r, s = (int.from_bytes(parts[index], "big") for index in (9, 10, 11))
    if parity not in (0, 1) or not 0 < r or not 0 < s:
        raise ValueError("pons_once_signature_invalid")
    recoverable = r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([parity])
    public = PublicKey.from_signature_and_message(
        recoverable, keccak256(unsigned), hasher=None)
    recovered = "0x" + keccak256(public.format(compressed=False)[1:])[-20:].hex()
    if recovered != wallet:
        raise ValueError("pons_once_signer_wallet_mismatch")
    return "0x" + keccak256(signed).hex()


def _ensure_broadcast_fresh(rpc: PonsRpc, summary: Mapping[str, Any],
                            transaction: Any, wallet: str) -> None:
    if summary.get("simulation_success") is not True:
        raise ValueError("pons_once_simulation_not_verified")
    fields, _ = decode_eip1559(transaction.serialized, signed=False)
    checks = [
        ("eth_blockNumber", []),
        ("eth_getBlockByNumber", [hex(int(summary["block"])), False]),
        ("eth_chainId", []),
        ("eth_getTransactionCount", [wallet, "pending"]),
        ("eth_getBalance", [wallet, "pending"]),
        ("eth_getBlockByNumber", ["pending", False]),
    ]
    try:
        values = rpc.call_batch(checks)
    except ValueError as error:
        if str(error) != "pons_once_batch_call_failed":
            raise
        # One bounded re-read of every field, never a partial batch result.
        # A failed individual read still stops before signing or sending.
        values = [rpc.call(method, params) for method, params in checks]
    head_value, block, chain_value, nonce_value, balance_value, pending = values
    head = _quantity(head_value)
    now = int(time.time())
    distance = head - int(summary["block"])
    if (not isinstance(block, Mapping)
            or str(block.get("hash") or "").lower() != summary["blockHash"]
            or _quantity(block["number"]) != summary["block"]
            or not 1 <= distance <= MAX_BROADCAST_BLOCK_DISTANCE
            or int(summary["deadline"]) - now < MIN_DEADLINE_REMAINING_S):
        raise ValueError("pons_once_broadcast_quote_stale")
    if _quantity(chain_value) != CHAIN_ID:
        raise ValueError("pons_once_wrong_chain")
    if _quantity(nonce_value) != fields.nonce:
        raise ValueError("pons_once_pending_nonce_changed")
    if _quantity(balance_value) < fields.value + fields.gas_limit * fields.maximum_fee:
        raise ValueError("pons_once_native_balance_insufficient")
    if (fields.gas_limit * fields.maximum_fee > MAX_GAS_COST_WEI
            or not isinstance(pending, Mapping) or pending.get("baseFeePerGas") is None
            or fields.maximum_fee < _quantity(pending["baseFeePerGas"]) + fields.priority_fee):
        raise ValueError("pons_once_fee_cap_stale")


def _wait_receipt(rpc: PonsRpc, tx_hash: str, *, timeout_seconds: int = 90) -> int:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        receipt = rpc.call("eth_getTransactionReceipt", [tx_hash])
        if receipt is None:
            time.sleep(2)
            continue
        if (not isinstance(receipt, Mapping)
                or str(receipt.get("transactionHash") or "").lower() != tx_hash
                or not HASH.fullmatch(str(receipt.get("blockHash") or ""))):
            raise ValueError("pons_once_receipt_invalid")
        status = _quantity(receipt.get("status", 0))
        if status not in (0, 1):
            raise ValueError("pons_once_receipt_invalid")
        block_number = _quantity(receipt.get("blockNumber"))
        confirmed_block = _pinned_header(rpc, hex(block_number))
        if str(confirmed_block["hash"]).lower() != str(receipt["blockHash"]).lower():
            raise ValueError("pons_once_receipt_reorged")
        return status
    raise ValueError("pons_once_receipt_timeout")


def inspect_attempt(*, signal_id: str, rpc: PonsRpc,
                    ledger_path: Path = SIGNAL_DB) -> Mapping[str, Any]:
    """Read-only network recovery by persisted hash; never sign or send."""
    if not ledger_path.is_file():
        raise ValueError("pons_once_signal_not_found")
    ledger = SignalLedger(ledger_path)
    try:
        item = ledger.lookup(signal_id)
        if item is None:
            raise ValueError("pons_once_signal_not_found")
        _, tx_hash, nonce = item
        transaction = rpc.call("eth_getTransactionByHash", [tx_hash])
        if transaction is not None and (
                not isinstance(transaction, Mapping)
                or str(transaction.get("hash") or "").lower() != tx_hash
                or _quantity(transaction.get("nonce")) != nonce):
            raise ValueError("pons_once_transaction_identity_mismatch")
        receipt = rpc.call("eth_getTransactionReceipt", [tx_hash])
        if receipt is None:
            ledger.mark(signal_id, "uncertain")
            return {"signalId": signal_id, "transactionHash": tx_hash,
                    "state": "uncertain", "transactionSeen": transaction is not None}
        if (not isinstance(receipt, Mapping)
                or str(receipt.get("transactionHash") or "").lower() != tx_hash
                or not HASH.fullmatch(str(receipt.get("blockHash") or ""))):
            raise ValueError("pons_once_receipt_invalid")
        status = _quantity(receipt.get("status", 0))
        if status not in (0, 1):
            raise ValueError("pons_once_receipt_invalid")
        block_number = _quantity(receipt.get("blockNumber"))
        block = _pinned_header(rpc, hex(block_number))
        if str(block["hash"]).lower() != str(receipt["blockHash"]).lower():
            raise ValueError("pons_once_receipt_reorged")
        state = "confirmed" if status == 1 else "failed"
        ledger.mark(signal_id, state)
        return {"signalId": signal_id, "transactionHash": tx_hash,
                "state": state, "receiptStatus": status}
    finally:
        ledger.close()


def _prepare_once(*, native_in_wei: int, slippage_bps: int,
                  rpc: PonsRpc, confirmed: bool = False,
                  fast: bool = False) -> tuple[dict[str, Any], Any, str]:
    started = time.monotonic()
    if not 0 < native_in_wei < 1 << 128 or not 0 <= slippage_bps <= 500:
        raise ValueError("pons_once_input_invalid")
    client = rpc
    if not getattr(client, "_identity_verified", False):
        if _quantity(client.call("eth_chainId", [])) != CHAIN_ID:
            raise ValueError("pons_once_wrong_chain")
        identity_header = _pinned_header(client)
        _verify_static_identity(client, hex(_quantity(identity_header["number"])))
    startup_identity_ms = int((time.monotonic() - started) * 1000)
    hot_started = time.monotonic()
    http_start = getattr(client, "http_round_trips", 0)
    rpc_start = getattr(client, "json_rpc_calls", 0)
    subcalls_start = getattr(client, "multicall_subcall_count", 0)
    chain_value, head_value = client.call_batch([
        ("eth_chainId", []),
        ("eth_blockNumber", []),
    ])
    if _quantity(chain_value) != CHAIN_ID:
        raise ValueError("pons_once_wrong_chain")
    head = _quantity(head_value)
    if head <= INITIALIZE_BLOCK + 1:
        raise ValueError("pons_once_unconfirmed_pool")
    header = _pinned_header(client, hex(head - 1))
    header_ms = int((time.monotonic() - hot_started) * 1000)
    state_started = time.monotonic()
    height = _quantity(header["number"])
    if height <= INITIALIZE_BLOCK:
        raise ValueError("pons_once_unconfirmed_pool")
    tag = hex(height)
    block_hash = str(header["hash"]).lower()
    gross, pool_state, policy = quote_gross(client, block_tag=tag, amount_in=native_in_wei)
    state_ms = int((time.monotonic() - state_started) * 1000)
    local_started = time.monotonic()
    hook_fee, creator_tax, net = policy.fee_components(gross)
    minimum = net * (10_000 - slippage_bps) // 10_000
    if minimum <= 0:
        raise ValueError("pons_once_minimum_zero")
    wallet, _ = getattr(client, "wallet_profile", None) or _wallet_profile()
    quote_and_wallet_local_ms = int((time.monotonic() - local_started) * 1000)
    wallet_started = time.monotonic()
    nonce_value, balance_value, priority_value, pending = client.call_batch([
        ("eth_getTransactionCount", [wallet, "pending"]),
        ("eth_getBalance", [wallet, "pending"]),
        ("eth_maxPriorityFeePerGas", []),
        ("eth_getBlockByNumber", ["pending", False]),
    ])
    nonce, balance = _quantity(nonce_value), _quantity(balance_value)
    priority, maximum = _fee_values(priority_value, pending)
    wallet_ms = int((time.monotonic() - wallet_started) * 1000)
    local_started = time.monotonic()
    deadline = int(time.time()) + 180
    def make(gas: int):
        built = build_unsigned(amount_in=native_in_wei, minimum_out=minimum,
                               deadline=deadline, nonce=nonce, gas_limit=gas,
                               priority_fee=priority, maximum_fee=maximum)
        decoded = decode_unsigned(built)
        if (decoded["chainId"] != CHAIN_ID or decoded["poolId"] != POOL_ID
                or decoded["tokenIn"] != ZERO or decoded["tokenOut"] != TOKEN_OUT
                or decoded["amountIn"] != native_in_wei or decoded["minOut"] != minimum
                or decoded["deadline"] != deadline or decoded["nonce"] != nonce
                or decoded["msgValue"] != native_in_wei
                or decoded["router"] != CHAIN_CONFIGS[CHAIN_ID].universal_router
                or decoded["recipientMode"] != "msg_sender"):
            raise ValueError("pons_once_final_scope_invalid")
        return built
    initial = make(MAX_GAS)
    local_ms = quote_and_wallet_local_ms + int((time.monotonic() - local_started) * 1000)
    fields, _ = decode_eip1559(initial.serialized, signed=False)
    call = {"from": wallet, "to": "0x" + fields.to.hex(),
            "data": "0x" + fields.data.hex(), "value": hex(fields.value),
            "maxFeePerGas": hex(fields.maximum_fee),
            "maxPriorityFeePerGas": hex(fields.priority_fee)}
    if balance < native_in_wei + (FIXED_REVIEWED_GAS if fast else MAX_GAS) * maximum:
        raise ValueError("pons_once_native_balance_insufficient")
    estimate_started = time.monotonic()
    estimated = (None if fast else _quantity(client.call("eth_estimateGas", [call, tag])))
    estimate_ms = int((time.monotonic() - estimate_started) * 1000)
    if estimated is not None and not 21_000 <= estimated <= MAX_GAS:
        raise ValueError("pons_once_estimated_gas_invalid")
    gas = FIXED_REVIEWED_GAS if estimated is None else min(MAX_GAS, estimated * 120 // 100)
    if gas * maximum > MAX_GAS_COST_WEI:
        raise ValueError("pons_once_gas_cost_cap_exceeded")
    if balance < native_in_wei + gas * maximum:
        raise ValueError("pons_once_native_balance_insufficient")
    final = make(gas)
    final_fields, _ = decode_eip1559(final.serialized, signed=False)
    call["data"] = "0x" + final_fields.data.hex()
    call["gas"] = hex(gas)
    call_started = time.monotonic()
    result = client.call("eth_call", [call, tag])
    call_ms = int((time.monotonic() - call_started) * 1000)
    if result != "0x":
        raise ValueError("pons_once_unexpected_call_return")
    # One end-of-read canonical hash check; no intermediate repeat headers.
    canonical_started = time.monotonic()
    final_header, head_after_value = client.call_batch([
        ("eth_getBlockByNumber", [tag, False]),
        ("eth_blockNumber", []),
    ])
    if (not isinstance(final_header, Mapping)
            or str(final_header.get("hash") or "").lower() != block_hash):
        raise ValueError("pons_once_simulation_reorged")
    head_after = _quantity(head_after_value)
    canonical_ms = int((time.monotonic() - canonical_started) * 1000)
    distance = head_after - height
    if distance < 1:
        raise ValueError("pons_once_head_view_inconsistent")
    summary = {"chainId": CHAIN_ID, "poolId": POOL_ID, "tokenOut": TOKEN_OUT,
            "hook": HOOK, "hookCodeHash": REVIEWED_RUNTIME_CODE_HASH,
            "block": height, "blockHash": block_hash, "poolState": pool_state,
            "launch": {"registered": policy.registered,
                       "memecoinIsCurrency0": policy.memecoin_is_currency0,
                       "memecoin": policy.memecoin, "quoteToken": policy.quote_token,
                       "creator": policy.creator,
                       "buybackCreatorRecipient": policy.buyback_creator_recipient,
                       "protocolFeeRecipient": policy.protocol_fee_recipient,
                       "creatorTaxBps": policy.creator_tax_bps,
                       "protocolFeeShareBps": policy.protocol_fee_share_bps,
                       "buybackBurnBps": policy.buyback_burn_bps,
                       "hookFeeBps": policy.hook_fee_bps,
                       "maxInternalPriceImpactBps": policy.max_internal_price_impact_bps,
                       "buybackEnabled": policy.buyback_enabled},
            "amountInWei": str(native_in_wei), "grossOut": str(gross),
            "hookFee": str(hook_fee), "creatorTax": str(creator_tax),
            "netOut": str(net), "minOut": str(minimum), "estimatedGas": estimated,
            "recipient": wallet, "simulation_success": True, "broadcast": False,
            "deadline": deadline, "headDistanceAfterSimulation": distance,
            "withinBlockGate": distance <= MAX_BROADCAST_BLOCK_DISTANCE,
            "headSource": "http",
            "startupIdentityMs": startup_identity_ms,
            "stagesMs": {"confirmedBlock": header_ms, "poolAndHookState": state_ms,
                         "walletFeeNonce": wallet_ms, "localQuoteAndBuild": local_ms,
                         "estimateGas": estimate_ms, "ethCall": call_ms,
                         "canonicalHeadCheck": canonical_ms},
            "rpc": {"httpRoundTrips": getattr(client, "http_round_trips", 0) - http_start,
                    "jsonRpcCalls": getattr(client, "json_rpc_calls", 0) - rpc_start,
                    "multicallSubcalls": getattr(client, "multicall_subcall_count", 0)
                    - subcalls_start},
            "hotPathMs": int((time.monotonic() - hot_started) * 1000),
            "elapsedMs": int((time.monotonic() - started) * 1000)}
    return summary, final, wallet


def execute_once(*, token_out: str = TOKEN_OUT, native_in_wei: int,
                 slippage_bps: int, signal_id: str = "",
                 broadcast: bool = False, rpc: PonsRpc | None = None,
                 ledger_path: Path = SIGNAL_DB,
                 execution_db_path: Path = EXECUTION_DB,
                 fomo_trade_key: str | None = None,
                 pre_sign_guard: Callable[[], None] | None = None,
                 pre_send_guard: Callable[[], None] | None = None,
                 on_send_attempt: Callable[[int, str], None] | None = None,
                 operator_confirmation: str | None = None,
                 fast: bool = False,
                 wait_for_receipt: bool = True) -> Mapping[str, Any]:
    if token_out.lower() != TOKEN_OUT or not SIGNAL_ID.fullmatch(signal_id):
        raise ValueError("pons_once_input_scope_invalid")
    client = rpc or PonsRpc(_rpc_url())
    if not broadcast:
        summary, _, _ = _prepare_once(native_in_wei=native_in_wei,
                                      slippage_bps=slippage_bps, rpc=client, fast=fast)
        return {**summary, "signalId": signal_id}
    operator_once = signal_id.startswith("operator:v1:")
    if operator_once:
        if operator_confirmation != operator_confirmation_text(signal_id, native_in_wei):
            raise ValueError("pons_once_operator_confirmation_invalid")
        if pre_sign_guard is not None or pre_send_guard is not None:
            raise ValueError("pons_once_operator_source_guard_invalid")
    elif not signal_id.startswith("sig:v1:fomo_push:"):
        raise ValueError("pons_once_signal_source_invalid")
    if not _live_armed(execution_db_path, native_in_wei=native_in_wei):
        raise ValueError("pons_once_live_not_armed")
    if not operator_once and (pre_sign_guard is None or pre_send_guard is None):
        raise ValueError("pons_once_source_guards_required")
    wallet, signer_profile = getattr(client, "wallet_profile", None) or _wallet_profile()
    ledger = SignalLedger(ledger_path)
    submission_started = time.monotonic()
    try:
        ledger.claim(signal_id, wallet, native_in_wei)
        attempted = False
        broadcast_started = False
        tx_hash = ""
        try:
            # Entire pool, Hook, quote, transaction and simulation are rebuilt
            # from a newly selected confirmed block for this broadcast attempt.
            summary, final, prepared_wallet = _prepare_once(
                native_in_wei=native_in_wei, slippage_bps=slippage_bps,
                rpc=client, confirmed=True, fast=fast)
            if wallet != prepared_wallet:
                raise ValueError("pons_once_wallet_changed")
            try:
                _ensure_broadcast_fresh(client, summary, final, wallet)
            except ValueError as error:
                if str(error) != "pons_once_broadcast_quote_stale":
                    raise
                # Exactly one rebuild from a new confirmed head, never a loop.
                summary, final, prepared_wallet = _prepare_once(
                    native_in_wei=native_in_wei, slippage_bps=slippage_bps,
                    rpc=client, confirmed=True, fast=fast)
                if wallet != prepared_wallet:
                    raise ValueError("pons_once_wallet_changed")
                _ensure_broadcast_fresh(client, summary, final, wallet)
            fields, _ = decode_eip1559(final.serialized, signed=False)
            if not _live_armed(execution_db_path, native_in_wei=native_in_wei):
                raise ValueError("pons_once_live_not_armed")
            if operator_once and operator_confirmation != operator_confirmation_text(
                    signal_id, native_in_wei):
                raise ValueError("pons_once_operator_confirmation_invalid")
            if pre_sign_guard is not None:
                pre_sign_guard()
            sign_started = time.monotonic()
            signer = _signer(signer_profile, wallet)
            signed = signer.sign(final)
            tx_hash = _verify_signed(signed, final.serialized, wallet)
            # Persist signal, nonce and signed transaction ID *before* send.
            ledger.mark_attempt(signal_id, fields.nonce, tx_hash,
                                "0x" + keccak256(final.serialized).hex())
            attempted = True
            if on_send_attempt is not None:
                on_send_attempt(fields.nonce, tx_hash)
            sign_persist_ms = int((time.monotonic() - sign_started) * 1000)
            _ensure_broadcast_fresh(client, summary, final, wallet)
            if not _live_armed(execution_db_path, native_in_wei=native_in_wei):
                raise ValueError("pons_once_live_not_armed")
            if operator_once and operator_confirmation != operator_confirmation_text(
                    signal_id, native_in_wei):
                raise ValueError("pons_once_operator_confirmation_invalid")
            if pre_send_guard is not None:
                pre_send_guard()
            ledger.mark_broadcast_started(signal_id, tx_hash)
            broadcast_started = True
            send_started = time.monotonic()
            try:
                returned = str(client.call("eth_sendRawTransaction", ["0x" + signed.hex()])).lower()
            except Exception as error:
                ledger.mark(signal_id, "uncertain")
                try:
                    recovery = inspect_attempt(signal_id=signal_id, rpc=client,
                                               ledger_path=ledger_path)
                except Exception:
                    recovery = {"state": "uncertain"}
                if recovery["state"] == "confirmed":
                    return {**summary, "signalId": signal_id, "broadcast": True,
                            "transactionHash": tx_hash, "receiptStatus": 1}
                if recovery["state"] == "failed":
                    raise ValueError("pons_once_receipt_failed") from error
                raise ValueError("pons_once_send_result_uncertain") from error
            if returned != tx_hash:
                ledger.mark(signal_id, "uncertain")
                raise ValueError("pons_once_send_hash_mismatch")
            send_ms = int((time.monotonic() - send_started) * 1000)
            submission_ms = int((time.monotonic() - submission_started) * 1000)
            submitted_at_ms = int(time.time() * 1000)
            ledger.mark(signal_id, "submitted")
            if not wait_for_receipt:
                return {**summary, "signalId": signal_id, "broadcast": True,
                        "transactionHash": tx_hash, "receiptStatus": None,
                        "signPersistMs": sign_persist_ms,
                        "sendRawTransactionMs": send_ms,
                        "signalToTxHashMs": submission_ms,
                        "submittedAtMs": submitted_at_ms,
                        "receiptLatencyMs": None}
            receipt_started = time.monotonic()
            try:
                status = _wait_receipt(client, tx_hash)
            except Exception:
                ledger.mark(signal_id, "uncertain")
                raise
            ledger.mark(signal_id, "confirmed" if status == 1 else "failed")
            if status != 1:
                raise ValueError("pons_once_receipt_failed")
            return {**summary, "signalId": signal_id, "broadcast": True,
                    "transactionHash": tx_hash, "receiptStatus": status,
                    "signPersistMs": sign_persist_ms,
                    "sendRawTransactionMs": send_ms,
                    "signalToTxHashMs": submission_ms,
                    "submittedAtMs": submitted_at_ms,
                    "receiptLatencyMs": int((time.monotonic() - receipt_started) * 1000)}
        except Exception:
            if not attempted:
                ledger.mark(signal_id, "aborted")
            elif not broadcast_started and operator_once:
                ledger.abort_before_send(signal_id, tx_hash, "pre_send_check_failed")
            raise
    finally:
        ledger.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-out", required=True)
    parser.add_argument("--native-in-wei", type=int, required=True)
    parser.add_argument("--slippage-bps", type=int, default=100)
    parser.add_argument("--signal-id", required=True)
    parser.add_argument("--operator-confirm", help="exact BUY ONCE scope confirmation")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--simulate", action="store_true")
    mode.add_argument("--broadcast", action="store_true")
    options = parser.parse_args(argv)
    rpc: PonsRpc | None = None
    try:
        rpc = PonsRpc(_rpc_url())
        result = execute_once(token_out=options.token_out,
                              native_in_wei=options.native_in_wei,
                              slippage_bps=options.slippage_bps,
                              signal_id=options.signal_id,
                              broadcast=options.broadcast, rpc=rpc,
                              operator_confirmation=options.operator_confirm)
    except Exception as error:
        reason = str(error)
        safe = reason if reason.startswith("pons_once_") else type(error).__name__
        data = rpc.last_revert_data if rpc is not None else None
        print(json.dumps({"simulation_success": False, "broadcast": False,
                          "reason": safe, "revertData": data,
                          "decodedRevert": _revert_reason(data)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
