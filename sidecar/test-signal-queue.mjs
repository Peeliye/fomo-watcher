import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { createSignalQueue } from "./signal-queue.mjs";

test("Fomo producer shares durable schema and source-namespaced dedupe", () => {
  const root = mkdtempSync(join(tmpdir(), "fomo-queue-"));
  try {
    const path = join(root, "queue.sqlite3");
    const queue = createSignalQueue(path);
    const event = { id: "same", type: "swap_buy", userId: "k1" };
    const first = queue.enqueueFomo(event, "2026-09-21T00:00:00Z");
    const second = queue.enqueueFomo(event, "2026-09-21T00:00:01Z");
    assert.equal(first.queued, true);
    assert.equal(second.queued, false);
    assert.equal(first.signalId, second.signalId);
    queue.close();
    const db = new DatabaseSync(path);
    try {
      const row = db.prepare("SELECT source,payload_kind,status FROM signal_queue").get();
      assert.equal(row.source, "fomo_push");
      assert.equal(row.payload_kind, "raw_fomo");
      assert.equal(row.status, "queued");
      assert.equal(db.prepare("PRAGMA user_version").get().user_version, 1);
    } finally { db.close(); }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
