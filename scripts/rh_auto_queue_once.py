"""One local FOMO queue event -> at most one RH buy attempt.

No HTTP webhook is exposed. The live switch is armed only while processing a
fresh, followed RH buy and is disarmed in every ordinary exit path.
"""

from __future__ import annotations

import argparse
import json
import msvcrt
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fomo.execution.journal import ExecutionJournal
from fomo.execution.rh_auto_executor import ROUTE_SCOPE, RhAutoExecutor
from fomo.execution.service import ExecutionService, load_followed_ids
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import deterministic_signal_id
from fomo.signals.fomo import FomoPushAdapter
from fomo.signals.freshness import decide_freshness
from scripts.execution_control import arm_auto, auto_confirmation_text, disarm, status
from scripts.uniswap_v3_swap_once import _quantity, _rpc_url, _signer, _wallet_profile
from scripts.pons_v4_swap_once import PonsRpc

ROOT = Path(__file__).resolve().parents[1]
QUEUE = ROOT / "data" / "signal-queue.sqlite3"
JOURNAL = ROOT / "data" / "execution.sqlite3"
FOLLOWING = ROOT / "data" / "following-ids.json"
LEDGER = ROOT / "data" / "rh-auto-buys.sqlite3"
RESULT = ROOT / "data" / "rh-auto-once-result.json"
LOCK = ROOT / "data" / "rh-auto-once.lock"
SIDECAR_STATUS = ROOT / "data" / "realtime-status.json"
AMOUNT = 10**12
SLIPPAGE_BPS = 100


def _write_result(document: dict[str, Any]) -> None:
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    temporary = RESULT.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
    temporary.replace(RESULT)
    print(json.dumps(document, separators=(",", ":")), flush=True)


def _refresh_following() -> set[str]:
    try:
        return load_followed_ids(FOLLOWING)
    except (OSError, ValueError, json.JSONDecodeError):
        from fomo.app import FomoClient, publish_following_ids
        document = FomoClient().get("/v2/users/current/followingIds")
        ids = {str(value) for value in document.get("followingIds", [])}
        if not ids:
            raise ValueError("rh_auto_following_unavailable")
        publish_following_ids(ids)
        return load_followed_ids(FOLLOWING)


def _sidecar_healthy() -> bool:
    try:
        document = json.loads(SIDECAR_STATUS.read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(str(document["updatedAt"]).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - updated).total_seconds()
        return (updated.tzinfo is not None and 0 <= age <= 45
                and all(document.get(key) is True for key in
                        ("running", "connected", "authenticated", "subscribed")))
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _is_rh_buy(row: Any) -> bool:
    if row["source"] != "fomo_push" or row["payload_kind"] != "raw_fomo":
        return False
    try:
        payload = json.loads(row["payload_json"])
        body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
        event = {**body, **payload}
        return (str(event.get("networkId")) == "4663"
                and str(event.get("type") or "").lower() in {"swap_buy", "single_user_buy"})
    except (ValueError, TypeError, KeyError):
        return False


def _eligible(row: Any, followed: set[str], *, ignore_source_clock: bool = False) -> bool:
    if row["source"] != "fomo_push" or row["payload_kind"] != "raw_fomo" or row["status"] != "queued":
        return False
    try:
        payload = json.loads(row["payload_json"])
        signal = FomoPushAdapter(followed).normalize(payload, str(row["received_at"]))
        if (signal is None or signal.signal_id != row["signal_id"]
                or signal.signal_id != deterministic_signal_id(signal.source, signal.source_event_id)
                or signal.chain_id != "4663" or signal.side != "buy"
                or signal.confirmation_level != "pending"):
            return False
        source_ms = int(datetime.fromisoformat(signal.source_timestamp.replace("Z", "+00:00")).timestamp() * 1000)
        observed_ms = int(datetime.fromisoformat(signal.observed_at.replace("Z", "+00:00")).timestamp() * 1000)
        now_ms = int(time.time() * 1000)
        if ignore_source_clock:
            # A one-shot diagnostic of FOMO's event clock only. Never replay
            # an old queue entry or waive the executor's send-time fence.
            return 0 <= now_ms - observed_ms <= 5000
        return decide_freshness(source_ms, observed_ms, now_ms, 5000).accepted
    except (ValueError, TypeError, KeyError):
        return False


def _preflight() -> None:
    control = status(JOURNAL)
    if control["liveArmed"] or control["circuitBreakerTripped"]:
        raise ValueError("rh_auto_control_not_disarmed_or_breaker_tripped")
    wallet, profile = _wallet_profile()
    _signer(profile, wallet)  # Capability check only; never signs here.
    rpc = PonsRpc(_rpc_url())
    try:
        if _quantity(rpc.call("eth_chainId", [])) != 4663:
            raise ValueError("rh_auto_wrong_chain")
        if _quantity(rpc.call("eth_getBalance", [wallet, "pending"])) <= AMOUNT:
            raise ValueError("rh_auto_native_balance_insufficient")
    finally:
        rpc.close()


def _receipt_details(tx_hash: str) -> dict[str, Any]:
    rpc = PonsRpc(_rpc_url())
    try:
        receipt = rpc.call("eth_getTransactionReceipt", [tx_hash])
        if not isinstance(receipt, dict) or str(receipt.get("transactionHash") or "").lower() != tx_hash.lower():
            return {"receiptDetailsAvailable": False}
        block_number = _quantity(receipt.get("blockNumber"))
        block = rpc.call("eth_getBlockByNumber", [hex(block_number), False])
        if (not isinstance(block, dict)
                or str(block.get("hash") or "").lower() != str(receipt.get("blockHash") or "").lower()):
            return {"receiptDetailsAvailable": False}
        gas_used = _quantity(receipt.get("gasUsed"))
        gas_price = _quantity(receipt.get("effectiveGasPrice"))
        return {"receiptDetailsAvailable": True, "receiptBlock": block_number,
                "receiptBlockHash": str(receipt["blockHash"]).lower(),
                "gasUsed": gas_used, "effectiveGasPriceWei": str(gas_price),
                "gasCostWei": str(gas_used * gas_price),
                "totalNativeSpentWei": str(AMOUNT + gas_used * gas_price)}
    except (ValueError, TypeError, KeyError):
        return {"receiptDetailsAvailable": False}
    finally:
        rpc.close()


def run(*, max_wait_minutes: int, ignore_source_clock: bool = False) -> dict[str, Any]:
    if max_wait_minutes < 0:
        raise ValueError("rh_auto_wait_invalid")
    _preflight()
    if not _sidecar_healthy():
        raise ValueError("rh_auto_sidecar_unhealthy")
    followed = _refresh_following()
    queue = DurableSignalQueue(QUEUE)
    journal = ExecutionJournal(JOURNAL)
    executor = RhAutoExecutor(native_in_wei=AMOUNT, slippage_bps=SLIPPAGE_BPS,
                              ledger_path=LEDGER, execution_db_path=JOURNAL,
                              allow_broadcast=True)
    try:
        cursor = int(queue.db.execute("SELECT COALESCE(MAX(sequence),0) FROM signal_queue").fetchone()[0])
        stats: dict[str, Any] = {"seenRows": 0, "rhBuyRows": 0, "eligibleRhBuys": 0,
                                 "rejectedRhBuys": 0, "lastRejectedReason": None}
        _write_result({"state": "waiting", "afterSequence": cursor,
                       "amountInWei": str(AMOUNT), "slippageBps": SLIPPAGE_BPS,
                       "ignoreSourceClock": ignore_source_clock,
                       "waitMode": "until_first_broadcast" if max_wait_minutes == 0 else "timed",
                       "maxWaitMinutes": max_wait_minutes,
                       "liveArmed": False, "broadcastSent": False, **stats})
        service = ExecutionService(queue, journal, lambda: load_followed_ids(FOLLOWING),
                                   lambda _: None, rh_auto_executor=executor,
                                   after_queue_sequence=cursor,
                                   rh_auto_ignore_source_clock_once=ignore_source_clock)
        deadline = time.monotonic() + max_wait_minutes * 60 if max_wait_minutes else None
        last_following_refresh = time.monotonic()
        last_health_check = 0.0
        last_progress_report = time.monotonic()
        while deadline is None or time.monotonic() < deadline:
            if time.monotonic() - last_health_check >= 1:
                if not _sidecar_healthy():
                    raise ValueError("rh_auto_sidecar_unhealthy")
                last_health_check = time.monotonic()
            if time.monotonic() - last_following_refresh >= 240:
                followed = _refresh_following()
                last_following_refresh = time.monotonic()
            rows = queue.db.execute(
                "SELECT sequence,signal_id,source,payload_kind,payload_json,received_at,status "
                "FROM signal_queue WHERE sequence>? ORDER BY sequence LIMIT 50", (cursor,),
            ).fetchall()
            if not rows:
                time.sleep(0.1)
                continue
            for row in rows:
                cursor = int(row["sequence"])
                stats["seenRows"] += 1
                if _is_rh_buy(row):
                    stats["rhBuyRows"] += 1
                if not _eligible(row, followed, ignore_source_clock=ignore_source_clock):
                    continue
                stats["eligibleRhBuys"] += 1
                armed = False
                try:
                    arm_auto(JOURNAL, chain_id=4663, native_in_wei=AMOUNT,
                             route=ROUTE_SCOPE, confirmation=auto_confirmation_text(AMOUNT))
                    armed = True
                    # Keep the same lease owner across candidates. A new
                    # ExecutionService would contend with our own live lease.
                    service.after_queue_sequence = cursor - 1
                    service.only_queue_sequence = cursor
                    decision = service.process_one()
                finally:
                    if armed:
                        disarm(JOURNAL)
                if decision is None:
                    raise ValueError("rh_auto_queue_claim_missing")
                output = {"state": "attempted", "signalId": decision.get("signalId"),
                          "queueStatus": decision.get("status"), "reason": decision.get("reason"),
                          "simulationSuccess": decision.get("simulation_success"),
                          "broadcastSent": decision.get("broadcastSent"),
                          "chainId": 4663, "tokenOut": decision.get("tokenOut"),
                          "amountInWei": str(AMOUNT), "slippageBps": SLIPPAGE_BPS,
                          "route": decision.get("selectedProtocol"), "pool": decision.get("pool"),
                          "poolId": decision.get("poolId"), "quoteBlock": decision.get("quoteBlock"),
                          "quotedOut": decision.get("quotedOut"), "minOut": decision.get("minOut"),
                          "estimatedGas": decision.get("estimatedGas"),
                          "transactionHash": decision.get("transactionHash"),
                          "receiptStatus": decision.get("receiptStatus"),
                          "decisionLatencyMs": decision.get("decisionLatencyMs"),
                          "afterSequence": cursor, "liveArmed": False}
                if decision.get("status") == "processed" and decision.get("broadcastSent") is True:
                    if isinstance(output["transactionHash"], str):
                        output.update(_receipt_details(output["transactionHash"]))
                    output["state"] = "completed"
                    output.update(stats)
                    _write_result(output)
                    return output
                stats["rejectedRhBuys"] += 1
                stats["lastRejectedReason"] = decision.get("reason")
                if (decision.get("status") == "blocked" or decision.get("recoveryState")
                        or decision.get("broadcastSent") is True):
                    output["state"] = "stopped_for_review"
                    output.update(stats)
                    _write_result(output)
                    return output
                output["state"] = "rejected_waiting"
                _write_result({**output, **stats})
            if time.monotonic() - last_progress_report >= 60:
                _write_result({"state": "waiting", "afterSequence": cursor,
                               "amountInWei": str(AMOUNT), "slippageBps": SLIPPAGE_BPS,
                               "ignoreSourceClock": ignore_source_clock,
                               "waitMode": "until_first_broadcast" if deadline is None else "timed",
                               "maxWaitMinutes": max_wait_minutes,
                               "liveArmed": False, "broadcastSent": False, **stats})
                last_progress_report = time.monotonic()
            time.sleep(0.1)
        output = {"state": "timed_out", "afterSequence": cursor,
                  "reason": ("no_rh_buy_during_window" if stats["rhBuyRows"] == 0
                             else "no_eligible_rh_buy_during_window" if stats["eligibleRhBuys"] == 0
                             else "no_broadcast_during_window"),
                  "liveArmed": False, "broadcastSent": False, **stats}
        _write_result(output)
        return output
    finally:
        disarm(JOURNAL)
        journal.close()
        queue.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broadcast", action="store_true")
    parser.add_argument("--confirm", required=True)
    wait = parser.add_mutually_exclusive_group(required=True)
    wait.add_argument("--max-wait-minutes", type=int)
    wait.add_argument("--wait-until-first-broadcast", action="store_true")
    parser.add_argument("--ignore-source-clock-once", action="store_true")
    args = parser.parse_args()
    confirmation = f"ONE SHOT 4663 {AMOUNT} {SLIPPAGE_BPS}"
    if args.ignore_source_clock_once:
        confirmation += " IGNORE SOURCE CLOCK"
    if args.wait_until_first_broadcast:
        confirmation += " WAIT UNTIL FIRST BROADCAST"
    if args.max_wait_minutes is not None and args.max_wait_minutes <= 0:
        parser.error("rh_auto_wait_invalid")
    wait_minutes = 0 if args.wait_until_first_broadcast else args.max_wait_minutes
    assert isinstance(wait_minutes, int)
    if not args.broadcast or args.confirm != confirmation:
        parser.error("rh_auto_one_shot_confirmation_required")
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+b") as lock:
        lock.seek(0)
        lock.write(b"0")
        lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            parser.error("rh_auto_one_shot_already_running")
        try:
            run(max_wait_minutes=wait_minutes,
                ignore_source_clock=args.ignore_source_clock_once)
        except Exception as error:
            # An unexpected failure can happen after a send attempt. Do not
            # label its broadcast status false without reconciling the ledger.
            control = status(JOURNAL)
            safe_reason = str(error) if isinstance(error, ValueError) and str(error) in {
                "rh_auto_sidecar_unhealthy", "rh_auto_queue_claim_missing",
                "rh_auto_following_unavailable", "rh_auto_control_not_disarmed_or_breaker_tripped",
                "rh_auto_wrong_chain", "rh_auto_native_balance_insufficient",
                "swap_once_vault_signer_not_enabled", "swap_once_vault_signer_unavailable",
                "swap_once_wallet_profile_invalid", "swap_once_rpc_not_configured",
            } else None
            preflight_failure = safe_reason in {
                "rh_auto_control_not_disarmed_or_breaker_tripped", "rh_auto_wrong_chain",
                "rh_auto_native_balance_insufficient", "swap_once_vault_signer_not_enabled",
                "swap_once_vault_signer_unavailable", "swap_once_wallet_profile_invalid",
                "swap_once_rpc_not_configured",
            }
            _write_result({"state": "failed" if preflight_failure else "stopped_for_review",
                           "errorType": type(error).__name__,
                           "reason": safe_reason,
                           "liveArmed": control["liveArmed"],
                           "broadcastSent": False if preflight_failure else None,
                           "sendStatusUnknown": not preflight_failure})
            return 1
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
