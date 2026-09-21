import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireSingletonLock } from "./process-lock.mjs";

test("debug and realtime cannot own the same writer lock", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fomo-sidecar-lock-"));
  const path = join(directory, "writer.lock");
  try {
    const release = await acquireSingletonLock(path, "debug");
    await assert.rejects(acquireSingletonLock(path, "realtime"), /sidecar_writer_lock_held/);
    await release();
    const releaseRealtime = await acquireSingletonLock(path, "realtime");
    await releaseRealtime();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
