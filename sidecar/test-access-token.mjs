import test from "node:test";
import assert from "node:assert/strict";
import { accessTokenExpiration, selectNewestValidAccessToken } from "./secret-store.mjs";

const jwt = exp => `header.${Buffer.from(JSON.stringify({ exp })).toString("base64url")}.signature`;

test("expired vault token cannot shadow a refreshed session token", () => {
  const now = 1_000;
  const expiredVault = jwt(900);
  const refreshedSession = jwt(2_000);
  assert.equal(selectNewestValidAccessToken([expiredVault, refreshedSession], now), refreshedSession);
});

test("selects the newest unexpired token regardless of source order", () => {
  const older = jwt(1_200);
  const newer = jwt(1_500);
  assert.equal(selectNewestValidAccessToken([newer, older], 1_000), newer);
  assert.equal(selectNewestValidAccessToken([older, newer], 1_000), newer);
});

test("fails closed when all candidates are expired or malformed", () => {
  assert.equal(selectNewestValidAccessToken([jwt(999), "malformed", ""], 1_000), "");
  assert.equal(accessTokenExpiration("malformed"), 0);
});
