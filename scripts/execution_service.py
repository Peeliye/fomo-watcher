"""Run the single durable signal consumer (live execution remains disabled)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from fomo.execution.assembly import ExecutionAssembly
from fomo.execution.journal import ExecutionJournal, execution_snapshot
from fomo.execution.service import ExecutionService, load_followed_ids
from fomo.execution.signal_queue import DurableSignalQueue
from fomo.signals.envelope import TradeSignalEnvelope
from fomo.watching.watchlist import enabled_wallets


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="data/signal-queue.sqlite3")
    parser.add_argument("--journal", default="data/execution.sqlite3")
    parser.add_argument("--watchlist", default="watch-wallets.json")
    parser.add_argument("--following", default="data/following-ids.json")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--readiness", action="store_true",
                        help="report chain assembly without consuming signals or arming live")
    args = parser.parse_args()
    if args.readiness:
        control = execution_snapshot(Path(args.journal), limit=1)["control"]
        print(json.dumps({"chains": [{
            "chainId": chain, "ready": False, "adapterReady": False,
            "liveArmed": bool(control.get("liveArmed", False)),
            "blockers": ["execution_chain_not_assembled", "live_disabled_or_circuit_open"],
        } for chain in ("1", "56", "4663", "5042", "8453", "1399811149")]}, ensure_ascii=False))
        return 0
    queue = DurableSignalQueue(Path(args.queue))
    journal = ExecutionJournal(Path(args.journal))
    assembly = ExecutionAssembly(journal)

    def wallet_config(signal: TradeSignalEnvelope):
        for entry in enabled_wallets(args.watchlist, signal.chain_id):
            if (entry["address"] == signal.actor_wallet if signal.source == "wallet_rpc_solana"
                    else str(entry["address"]).casefold() == str(signal.actor_wallet).casefold()):
                return entry
        return None

    service = ExecutionService(queue, journal, lambda: load_followed_ids(args.following), wallet_config,
                               executor=assembly)
    try:
        if args.once:
            print(json.dumps(service.process_one(), ensure_ascii=False))
        else:
            service.run_forever()
    finally:
        journal.close()
        queue.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
