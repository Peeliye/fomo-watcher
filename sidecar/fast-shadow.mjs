import { appendFileSync, existsSync, readFileSync, readdirSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { gzipSync } from "node:zlib";

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
  const activeBuyEvents = new Set(config?.activeBuyEventTypes || ["swap_buy", "single_user_buy"]);
  const allowedNetworks = new Set((config?.networkIds || []).map(Number));
  const ids = new Set((whitelist?.followingIds || []).map(String));
  const whitelistAgeMs = whitelist?.updatedAt ? nowMs - Number(whitelist.updatedAt) : Infinity;

  const sourceType = String(event.type || "").toLowerCase();
  const passiveEvent = ["transfer", "airdrop", "mint", "deposit", "receive", "token_deploy"]
    .some(marker => sourceType.includes(marker));
  const deferredChecks = [];
  let status = "eligible";
  if (!config?.enabled) status = "disabled";
  else if (!whitelist || whitelistAgeMs > Number(config.whitelistMaxAgeSeconds || 600) * 1000) status = "following_whitelist_unavailable";
  else if (!ids.has(String(event.userId || ""))) status = "not_following";
  else if (passiveEvent) status = "passive_asset_event";
  else if (!allowedEvents.has(sourceType) || !activeBuyEvents.has(sourceType)) status = "unsupported_event_type";
  else if (!String(event.tokenAddress || "")) status = "missing_ca";
  else if (!allowedNetworks.has(networkId)) status = "unsupported_network";
  else if (signalAgeMs === null || signalAgeMs > Number(config.maxSignalAgeSeconds || 5) * 1000) status = "stale_signal";
  else if (targetBuyUsd < Number(config.minTargetBuyUsd || 0)) status = "target_trade_too_small";
  else if (!marketCapUsd && config?.deferAssetChecks !== false) deferredChecks.push("missing_market_cap");
  else if (marketCapUsd < Number(config.minMarketCapUsd || 0) && config?.deferAssetChecks !== false) deferredChecks.push("market_cap_too_small");
  else if (!marketCapUsd) status = "missing_market_cap";
  else if (marketCapUsd < Number(config.minMarketCapUsd || 0)) status = "market_cap_too_small";

  return {
    recordedAt: new Date(nowMs).toISOString(),
    receivedAt,
    eventId: String(event.id || event.tradeId || ""),
    sourceType,
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
    deferredChecks,
    stage: status === "eligible" ? "fast_path_ready" : "fast_path_blocked",
  };
}

export function appendShadow(path, record) {
  appendNdjson(path, record);
}

export function appendNdjson(path, record) {
  const today = new Date().toISOString().slice(0, 10);
  const marker = `${path}.active-day`;
  let activeDay = today;
  try { activeDay = readFileSync(marker, "utf8").trim() || today; } catch {}
  if (activeDay !== today && existsSync(path) && statSync(path).size > 0) {
    const archive = `${path}.${activeDay}.gz`;
    writeFileSync(archive, gzipSync(readFileSync(path), { level: 6 }));
    writeFileSync(path, "");
  }
  writeFileSync(marker, today);
  appendFileSync(path, JSON.stringify(record) + "\n");
  const cutoff = Date.now() - 30 * 86400_000;
  try {
    for (const name of readdirSync(dirname(path))) {
      if (name.startsWith(`${basename(path)}.`) && name.endsWith(".gz")) {
        const archive = join(dirname(path), name);
        if (statSync(archive).mtimeMs < cutoff) unlinkSync(archive);
      }
    }
  } catch {}
}

export function fileMtime(path) {
  try { return statSync(path).mtimeMs; } catch { return 0; }
}
