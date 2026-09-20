import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzip } from "node:zlib";
import { promisify } from "node:util";
import { createNdjsonQueue, evaluateShadow } from "./fast-shadow.mjs";

const gunzipAsync = promisify(gunzip);

const now = Date.parse("2026-09-10T00:00:01Z");
const config = {
  enabled: true,
  networkIds: [4663, 1399811149],
  eventTypes: ["swap_buy"],
  fixedUsd: 10,
  minTargetBuyUsd: 100,
  maxSignalAgeSeconds: 5,
  minMarketCapUsd: 100000,
  whitelistMaxAgeSeconds: 600,
};
const whitelist = { followingIds: ["followed"], updatedAt: now - 1000 };

test("fresh followed Solana buy is eligible", () => {
  const record = evaluateShadow({
    id: "1", type: "swap_buy", createdAt: "2026-09-10T00:00:00Z",
    userId: "followed", userHandle: "alice", ticker: "MEME",
    tokenAddress: "mint", networkId: 1399811149, usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:01Z", config, whitelist, now);
  assert.equal(record.status, "eligible");
  assert.equal(record.proposedBuyUsd, 10);
  assert.equal(record.signalAgeMs, 1000);
  assert.equal(record.upstreamAgeMs, 1000);
  assert.equal(record.upstreamClockSkewMs, 0);
  assert.equal(record.localQueueMs, 0);
});

test("latency fields separate upstream delivery, local queue, and source clock skew", () => {
  const delayed = evaluateShadow({
    id: "latency-1", type: "swap_buy", createdAt: "2026-09-10T00:00:00Z",
    userId: "followed", networkId: 4663, tokenAddress: "0xabc",
    usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:00.800Z", config, whitelist, now);
  assert.equal(delayed.upstreamAgeMs, 800);
  assert.equal(delayed.upstreamClockSkewMs, 0);
  assert.equal(delayed.localQueueMs, 200);

  const futureStamped = evaluateShadow({
    id: "latency-2", type: "swap_buy", createdAt: "2026-09-10T00:00:01.500Z",
    userId: "followed", networkId: 4663, tokenAddress: "0xabc",
    usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:01Z", config, whitelist, now);
  assert.equal(futureStamped.upstreamAgeMs, 0);
  assert.equal(futureStamped.upstreamClockSkewMs, 500);
  assert.equal(futureStamped.localQueueMs, 0);
});

test("stranger fails closed", () => {
  const record = evaluateShadow({
    id: "2", type: "swap_buy", createdAt: "2026-09-10T00:00:00Z",
    userId: "stranger", networkId: 4663, usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:01Z", config, whitelist, now);
  assert.equal(record.status, "not_following");
});

test("stale whitelist fails closed", () => {
  const stale = { followingIds: ["followed"], updatedAt: now - 700000 };
  const record = evaluateShadow({
    id: "3", type: "swap_buy", createdAt: "2026-09-10T00:00:00Z",
    userId: "followed", networkId: 4663, usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:01Z", config, stale, now);
  assert.equal(record.status, "following_whitelist_unavailable");
});

test("passive transfer is never eligible even when configured", () => {
  const record = evaluateShadow({
    id: "4", type: "large_transfer_in", createdAt: "2026-09-10T00:00:00Z",
    userId: "followed", networkId: 4663, tokenAddress: "0xabc",
    usdAmount: 500, marketCap: 1000000,
  }, "2026-09-10T00:00:01Z", {
    ...config,
    eventTypes: [...config.eventTypes, "large_transfer_in"],
    activeBuyEventTypes: [...config.eventTypes, "large_transfer_in"],
  }, whitelist, now);
  assert.equal(record.status, "passive_asset_event");
  assert.equal(record.proposedBuyUsd, 0);
});

test("missing asset metadata is deferred after the fast path", () => {
  const record = evaluateShadow({
    id: "5", type: "swap_buy", createdAt: "2026-09-10T00:00:00Z",
    userId: "followed", networkId: 4663, tokenAddress: "0xabc", usdAmount: 500,
  }, "2026-09-10T00:00:01Z", { ...config, deferAssetChecks: true }, whitelist, now);
  assert.equal(record.status, "eligible");
  assert.deepEqual(record.deferredChecks, ["missing_market_cap"]);
  assert.equal(record.stage, "fast_path_ready");
});

test("ordered async audit queue preserves a 1000-event burst", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fomo-audit-"));
  try {
    const path = join(directory, "events.ndjson");
    const queue = createNdjsonQueue(path, { maxQueue: 2000 });
    await Promise.all(Array.from({ length: 1000 }, (_, id) => queue.enqueue({ id })));
    await queue.close();
    const rows = (await readFile(path, "utf8")).trim().split("\n").map(JSON.parse);
    assert.deepEqual(rows.map(row => row.id), Array.from({ length: 1000 }, (_, id) => id));
    assert.equal(queue.stats().persisted, 1000);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("audit queue applies bounded backpressure", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fomo-backpressure-"));
  try {
    const queue = createNdjsonQueue(join(directory, "events.ndjson"), { maxQueue: 1 });
    const first = queue.enqueue({ id: 1 });
    await assert.rejects(queue.enqueue({ id: 2 }), /audit_queue_backpressure/);
    await first;
    await queue.close();
    assert.equal(queue.stats().rejected, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("async rotation uses generation marker and keeps order", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fomo-rotation-"));
  try {
    const path = join(directory, "events.ndjson");
    await writeFile(path, '{"id":"old"}\n');
    await writeFile(`${path}.rotation.json`, JSON.stringify({ activeDay: "2020-01-01", generation: 0 }));
    const queue = createNdjsonQueue(path, { maxQueue: 10 });
    await queue.enqueue({ id: "new-first" });
    await queue.enqueue({ id: "new-second" });
    await queue.close();
    assert.equal((await gunzipAsync(await readFile(`${path}.2020-01-01.gz`))).toString(), '{"id":"old"}\n');
    const rows = (await readFile(path, "utf8")).trim().split("\n").map(JSON.parse);
    assert.deepEqual(rows.map(row => row.id), ["new-first", "new-second"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
