import assert from "node:assert/strict";
import test from "node:test";
import { forwardLocalFomoBuy, shouldForwardFomoBuy } from "./local-fomo-forwarder.mjs";

const token = "t".repeat(40);
const payload = { id: "event-1", networkId: 4663, type: "swap_buy",
  tokenAddress: "0x314ad0f11422842d28b4f950a64cd40fafb029fd" };

test("only a Robinhood buy with a token address is forwarded", async () => {
  assert.equal(shouldForwardFomoBuy(payload), true);
  for (const change of [{ networkId: 1 }, { type: "swap_sell" }, { tokenAddress: "bad" }]) {
    assert.equal(shouldForwardFomoBuy({ ...payload, ...change }), false);
  }
  let calls = 0;
  assert.equal(await forwardLocalFomoBuy(payload, "short", async () => { calls += 1; return { status: 202 }; }), false);
  assert.equal(calls, 0);
});

test("forwarder posts only to fixed loopback endpoint and accepts duplicate wake", async () => {
  let target = "";
  let posted = null;
  const ok = await forwardLocalFomoBuy(payload, token, async (url, options) => {
    target = url;
    posted = options;
    return { status: 200 };
  });
  assert.equal(ok, true);
  assert.equal(target, "http://127.0.0.1:8765/fomo/4663/buy");
  assert.equal(posted.method, "POST");
  assert.deepEqual(JSON.parse(posted.body), payload);
});

test("forwarding failure is reported without a retry", async () => {
  assert.equal(await forwardLocalFomoBuy(payload, token, async () => { throw Error("offline"); }), false);
});
