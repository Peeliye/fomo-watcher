# RH auto-buy handoff (2026-09-26)

## Current state

- Code is prepared for a local, at-most-one live RH Chain (4663) buy from a new followed FOMO buy event. This has **not** produced a confirmed live buy yet.
- The last live runner exited before any post-watermark queue event: `rh_auto_sidecar_unhealthy` at 2026-09-25 14:24:36 UTC. Its start watermark was sequence 1228. No transaction hash was produced; `live_armed` was false after exit.
- The runner is not currently active. The monitoring automation for that run was deleted. Never infer a live process from the presence of `data/rh-auto-once-result.json`.
- The source-event clock discrepancy was waived only by the explicit one-shot diagnostic flag. The local-arrival 5-second gate, send-time fence, operator scope, simulation, and route restrictions remain in effect.

## Relevant paths

- `sidecar/realtime-sidecar.mjs` durably enqueues raw FOMO events; `sidecar/local-fomo-forwarder.mjs` sends a best-effort loopback webhook wakeup.
- `scripts/fomo_webhook.py` also polls the durable queue, so a lost wakeup cannot strand an event.
- `scripts/rh_auto_queue_once.py` starts after the current queue watermark, accepts only fresh followed 4663 buys, arms around each candidate, disarms in `finally`, and has an explicit `--wait-until-first-broadcast` mode. It must run under the same Windows account that owns the OS wallet credential. In the Codex sandbox account, the credential is not visible; under the operator account the read-only preflight passed.
- `fomo/execution/rh_auto_executor.py` enforces an expiring arm, route/scope checks, one-token-once ledger, simulation and a 15-second send fence.
- Only reviewed V2, V3, V4 zero-hook, and reviewed Pons V4-hook routes are eligible. Unknown hooks and unsupported input pairs fail closed. Do not treat every FOMO CA as buyable.

## Known blocker for next test

The runner's `_sidecar_healthy()` requires `data/realtime-status.json` to be at most 45 seconds old and all four connection flags true. On the last run it failed this check at 14:24:36 UTC, although the sidecar status became healthy again minutes later. Investigate whether the status writer pauses during idle or whether there was a real disconnect. Do not simply remove the health fence; use independently verifiable process/connection and heartbeat evidence, and test transient gaps offline before another live run.

## Verification already completed

- Python: 509 passed, 1 skipped, 72 subtests passed.
- Ruff: all checks passed.
- Pyright: 0 errors.
- Node sidecar tests: 14 passed.

No `.env`, OS credential, RPC endpoint, private key, runtime SQLite database, or log is included in Git. Live signing and broadcast must never be replayed from a historical FOMO event.
