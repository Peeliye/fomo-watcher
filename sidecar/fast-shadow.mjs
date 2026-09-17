import { appendFileSync, readFileSync, statSync } from "node:fs";

export function loadJson(path, fallback = null) {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

export function evaluateShadow(payload, receivedAt, config, whitelist, nowMs = Date.now()) {
  const event = payload?.body && !payload.tokenAddress ? { ...payload.body, ...payload } : payload || {};
  const networkId = Number(event.networkId || 0);
  const targetBuyUsd = Number(event.usdAmount || event.amountUsd || 0);
  const marketCapUsd = Number(event.marketCap || event.fdv || 0);
  const createdMs = Date.parse(event.createdAt || receivedAt);
  const signalAgeMs = Number.isFinite(createdMs) ? Math.max(0, nowMs - createdMs) : null;
  const allowedEvents = new Set(config?.eventTypes || ["swap_buy", "single_user_buy"]);
  const allowedNetworks = new Set((config?.networkIds || []).map(Number));
  const ids = new Set((whitelist?.followingIds || []).map(String));
  const whitelistAgeMs = whitelist?.updatedAt ? nowMs - Number(whitelist.updatedAt) : Infinity;

  let status = "eligible";
  if (!config?.enabled) status = "disabled";
  else if (!whitelist || whitelistAgeMs > Number(config.whitelistMaxAgeSeconds || 600) * 1000) status = "following_whitelist_unavailable";
  else if (!ids.has(String(event.userId || ""))) status = "not_following";
  else if (!allowedEvents.has(String(event.type || ""))) status = "unsupported_event_type";
  else if (!allowedNetworks.has(networkId)) status = "unsupported_network";
  else if (signalAgeMs === null || signalAgeMs > Number(config.maxSignalAgeSeconds || 5) * 1000) status = "stale_signal";
  else if (targetBuyUsd < Number(config.minTargetBuyUsd || 0)) status = "target_trade_too_small";
  else if (!marketCapUsd) status = "missing_market_cap";
  else if (marketCapUsd < Number(config.minMarketCapUsd || 0)) status = "market_cap_too_small";

  return {
    recordedAt: new Date(nowMs).toISOString(),
    receivedAt,
    eventId: String(event.id || event.tradeId || ""),
    sourceType: String(event.type || ""),
    status,
    handle: String(event.userHandle || ""),
    userId: String(event.userId || ""),
    symbol: String(event.ticker || ""),
    networkId,
    ca: String(event.tokenAddress || ""),
    signalAgeMs: signalAgeMs === null ? null : Math.round(signalAgeMs * 1000) / 1000,
    targetBuyUsd,
    marketCapUsd,
    proposedBuyUsd: status === "eligible" ? Number(config.fixedUsd || 0) : 0,
    stage: "risk_evaluated",
  };
}

export function appendShadow(path, record) {
  appendFileSync(path, JSON.stringify(record) + "\n");
}

export function fileMtime(path) {
  try { return statSync(path).mtimeMs; } catch { return 0; }
}
