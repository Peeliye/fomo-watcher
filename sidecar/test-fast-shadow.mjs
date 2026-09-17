import assert from "node:assert/strict";
import test from "node:test";
import { evaluateShadow } from "./fast-shadow.mjs";

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
