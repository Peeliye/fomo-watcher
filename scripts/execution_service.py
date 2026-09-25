"""Run the single durable signal consumer (live execution remains disabled)."""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from fomo.execution.assembly import ExecutionAssembly
from fomo.execution.journal import ExecutionJournal, execution_snapshot
from fomo.execution.pons_v4_signal_executor import PonsV4SignalExecutor
from fomo.execution.service import ExecutionService, load_followed_ids
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.watching.watchlist import enabled_wallets
from scripts import execution_control

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_QUEUE = PROJECT_DIR / "data" / "signal-queue.sqlite3"
SIDECAR_STATUS = PROJECT_DIR / "data" / "realtime-status.json"


def _sidecar_healthy(path: Path = SIDECAR_STATUS, *, now: datetime | None = None) -> bool:
    """An old status file must never stand in for a live FOMO connection."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        updated = datetime.fromisoformat(str(data["updatedAt"]).replace("Z", "+00:00"))
        current = now or datetime.now(timezone.utc)
        return (all(data.get(key) is True for key in
                    ("running", "connected", "authenticated", "subscribed"))
                and updated.tzinfo is not None
                and 0 <= (current - updated).total_seconds() <= 45)
    except (OSError, ValueError, KeyError, TypeError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=str(DEFAULT_QUEUE))
    parser.add_argument("--journal", default=str(PROJECT_DIR / "data" / "execution.sqlite3"))
    parser.add_argument("--watchlist", default="watch-wallets.json")
    parser.add_argument("--following", default="data/following-ids.json")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--pons-v4", action="store_true",
                        help="narrow 4663 Pons V4 signal simulation path")
    parser.add_argument("--pons-native-in-wei", type=int)
    parser.add_argument("--pons-slippage-bps", type=int)
    parser.add_argument("--pons-economic-ledger", type=Path)
    parser.add_argument("--pons-allow-broadcast", action="store_true")
    parser.add_argument("--pons-one-shot-next", action="store_true",
                        help="consume only signals enqueued after startup; disarm after one accepted signal")
    parser.add_argument("--pons-one-shot-confirm",
                        help="exact operator confirmation for the pinned one-shot route")
    parser.add_argument("--readiness", action="store_true",
                        help="report chain assembly without consuming signals or arming live")
    args = parser.parse_args()
    if args.pons_one_shot_next and (
        not args.pons_v4 or not args.pons_allow_broadcast or args.once
        or args.pons_native_in_wei is None
        or args.pons_one_shot_confirm != execution_control.confirmation_text(args.pons_native_in_wei)
    ):
        parser.error("one-shot requires Pons V4, broadcast flag, and exact operator confirmation")
    if args.pons_one_shot_confirm is not None and not args.pons_one_shot_next:
        parser.error("one-shot confirmation requires --pons-one-shot-next")
    if args.readiness:
        control = execution_snapshot(Path(args.journal), limit=1)["control"]
        print(json.dumps({"chains": [{
            "chainId": chain, "ready": False, "adapterReady": False,
            "liveArmed": bool(control.get("liveArmed", False)),
            "blockers": ["execution_chain_not_assembled", "live_disabled_or_circuit_open"],
        } for chain in ("1", "56", "4663", "5042", "8453", "1399811149")]}, ensure_ascii=False))
        return 0
    if args.pons_v4:
        if args.pons_native_in_wei is None or args.pons_slippage_bps is None:
            parser.error("Pons V4 requires explicit native-in-wei and slippage-bps")
        pons_executor = PonsV4SignalExecutor(
            native_in_wei=args.pons_native_in_wei,
            slippage_bps=args.pons_slippage_bps,
            allow_broadcast=args.pons_allow_broadcast,
            execution_db_path=Path(args.journal).resolve(),
            **({"economic_ledger_path": args.pons_economic_ledger}
               if args.pons_economic_ledger is not None else {}),
        )
    else:
        if (args.pons_allow_broadcast or args.pons_native_in_wei is not None
                or args.pons_slippage_bps is not None
                or args.pons_economic_ledger is not None):
            parser.error("Pons V4 options require --pons-v4")
        pons_executor = None
    queue_path = Path(args.queue).resolve()
    queue = DurableSignalQueue(queue_path)
    if queue.path.resolve() != queue_path:
        queue.close()
        raise ValueError("execution_queue_path_mismatch")
    queue_label = ("repo:data/signal-queue.sqlite3" if queue_path == DEFAULT_QUEUE
                   else "custom:sha256:" + hashlib.sha256(str(queue_path).encode()).hexdigest()[:12])
    print(json.dumps({"queuePath": queue_label, "queuePathVerified": True}))
    watermark = (int(queue.db.execute("SELECT COALESCE(MAX(sequence),0) FROM signal_queue").fetchone()[0])
                 if args.pons_one_shot_next else 0)
    journal = ExecutionJournal(Path(args.journal))
    assembly = None if pons_executor is not None else ExecutionAssembly(journal)

    def wallet_config(signal: TradeSignalEnvelope):
        for entry in enabled_wallets(args.watchlist, signal.chain_id):
            if (entry["address"] == signal.actor_wallet if signal.source == "wallet_rpc_solana"
                    else str(entry["address"]).casefold() == str(signal.actor_wallet).casefold()):
                return entry
        return None

    service = ExecutionService(queue, journal, lambda: load_followed_ids(args.following), wallet_config,
                               executor=assembly, pons_v4_executor=pons_executor,
                               after_queue_sequence=watermark)
    armed_here = False
    try:
        if pons_executor is not None:
            startup_ms = pons_executor.prewarm()
            print(json.dumps({"ponsStartupIdentityMs": startup_ms, "headSource": "http",
                              "wssNewHeads": "inactive_optional", "tradingReady": False}))
        if args.pons_one_shot_next:
            assert args.pons_native_in_wei is not None
            assert args.pons_one_shot_confirm is not None
            if not _sidecar_healthy():
                raise ValueError("fomo_sidecar_unhealthy")
            if execution_control.status(Path(args.journal))["liveArmed"]:
                raise ValueError("one_shot_requires_disarmed_control")
            def _stop(_signum: int, _frame: object) -> None:
                raise KeyboardInterrupt
            signal.signal(signal.SIGINT, _stop)
            signal.signal(signal.SIGTERM, _stop)
            execution_control.arm(
                Path(args.journal), chain_id=4663,
                token_out=execution_control.TOKEN_OUT,
                native_in_wei=args.pons_native_in_wei,
                route=execution_control.ROUTE_ID,
                confirmation=args.pons_one_shot_confirm,
            )
            armed_here = True
            print(json.dumps({"oneShotListening": True, "afterSequence": watermark,
                              "chainId": 4663, "tokenOut": execution_control.TOKEN_OUT,
                              "nativeInWei": str(args.pons_native_in_wei)}), flush=True)
            while True:
                if not _sidecar_healthy():
                    raise ValueError("fomo_sidecar_unhealthy")
                result = service.process_one()
                if result is None:
                    time.sleep(0.02)
                elif result.get("signalAccepted"):
                    print(json.dumps({"oneShotResult": result}, ensure_ascii=False), flush=True)
                    break
        elif args.once:
            print(json.dumps(service.process_one(), ensure_ascii=False))
        else:
            service.run_forever()
    except KeyboardInterrupt:
        print(json.dumps({"oneShotStopped": True}), flush=True)
        return 0
    except Exception as error:
        if not args.pons_v4:
            raise
        # A transport exception may contain a credential-bearing RPC URL.
        # Leave the queue claim to expire for recovery; never print the error.
        print(json.dumps({"status": "error", "reason": (
            "fomo_sidecar_unhealthy" if str(error) == "fomo_sidecar_unhealthy"
            else "pons_signal_execution_error"),
                          "errorType": type(error).__name__}))
        return 1
    finally:
        if armed_here:
            execution_control.disarm(Path(args.journal))
            print(json.dumps({"oneShotDisarmed": True}), flush=True)
        if pons_executor is not None:
            pons_executor.close()
        journal.close()
        queue.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
