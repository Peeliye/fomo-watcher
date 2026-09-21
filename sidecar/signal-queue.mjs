// SQLite ingress shared with fomo.execution.signal_queue. No trading happens here.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

const schema = `
CREATE TABLE IF NOT EXISTS signal_queue (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  payload_kind TEXT NOT NULL CHECK(payload_kind IN ('raw_fomo','envelope')),
  payload_json TEXT NOT NULL,
  received_at TEXT NOT NULL,
  enqueued_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('queued','claimed','processed','dropped','blocked')),
  lease_owner TEXT,
  lease_until_ms INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  decision_json TEXT,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signal_queue_claim
  ON signal_queue(status,lease_until_ms,sequence);
CREATE TABLE IF NOT EXISTS signal_queue_service_lock (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  owner TEXT NOT NULL,
  lease_until_ms INTEGER NOT NULL
);
PRAGMA user_version=1;
`;

export function createSignalQueue(path) {
  mkdirSync(dirname(path), { recursive: true });
  const existed = existsSync(path);
  const db = new DatabaseSync(path, { timeout: 5000 });
  const version = Number(db.prepare("PRAGMA user_version").get().user_version);
  if (existed && version !== 1) {
    db.close();
    throw new Error("signal_queue_migration_requires_python_backup");
  }
  db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
  db.exec(schema);
  const insert = db.prepare(`INSERT OR IGNORE INTO signal_queue
    (signal_id,source,payload_kind,payload_json,received_at,enqueued_at_ms,status,updated_at_ms)
    VALUES(?,'fomo_push','raw_fomo',?,?,?,'queued',?)`);
  return {
    enqueueFomo(payload, receivedAt) {
      const body = payload?.body && typeof payload.body === "object" ? payload.body : {};
      const event = { ...body, ...payload };
      const sourceEventId = String(event.id || event.tradeId || "").trim();
      if (!sourceEventId) return { queued: false, reason: "missing_source_event_id" };
      const signalId = `sig:v1:fomo_push:${createHash("sha256").update(`fomo_push\0${sourceEventId}`).digest("hex")}`;
      const now = Date.now();
      const result = insert.run(signalId, JSON.stringify(payload), receivedAt, now, now);
      return { queued: result.changes === 1, signalId };
    },
    close() { db.close(); },
  };
}
