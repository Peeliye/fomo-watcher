import { readFileSync, statSync } from "node:fs";
import { appendFile, readFile, readdir, rename, stat, truncate, unlink, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { promisify } from "node:util";
import { gzip } from "node:zlib";

const gzipAsync = promisify(gzip);

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
  return appendNdjson(path, record);
}

const queues = new Map();

export function appendNdjson(path, record) {
  let queue = queues.get(path);
  if (!queue) {
    queue = createNdjsonQueue(path);
    queues.set(path, queue);
  }
  return queue.enqueue(record);
}

export function fileMtime(path) {
  try { return statSync(path).mtimeMs; } catch { return 0; }
}

export function createNdjsonQueue(path, options = {}) {
  const maxQueue = Math.max(1, Number(options.maxQueue || 10_000));
  const retentionDays = Math.max(2, Number(options.retentionDays || 30));
  const pending = [];
  let running = false;
  let closing = false;
  let idleResolve = null;
  let persisted = 0;
  let rejected = 0;
  let rotationInitialized = false;
  let activeDayState = "";
  let generationState = 0;

  async function rotateIfNeeded() {
    const today = new Date().toISOString().slice(0, 10);
    const marker = `${path}.rotation.json`;
    const legacyMarker = `${path}.active-day`;
    if (!rotationInitialized) {
      activeDayState = today;
      try {
        const value = JSON.parse(await readFile(marker, "utf8"));
        activeDayState = String(value.activeDay || today);
        generationState = Math.max(0, Number(value.generation || 0));
      } catch {
        try { activeDayState = (await readFile(legacyMarker, "utf8")).trim() || today; } catch {}
      }
      rotationInitialized = true;
    }
    if (activeDayState !== today) {
      try {
        const info = await stat(path);
        if (info.size > 0) {
          const suffix = generationState === 0 ? "" : `.g${generationState}`;
          const archive = `${path}.${activeDayState}${suffix}.gz`;
          const compressed = await gzipAsync(await readFile(path), { level: 6 });
          await writeFile(`${archive}.tmp`, compressed);
          await rename(`${archive}.tmp`, archive);
          await truncate(path, 0);
          generationState += 1;
        }
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
      }
      await writeFile(`${marker}.tmp`, JSON.stringify({ activeDay: today, generation: generationState }));
      await rename(`${marker}.tmp`, marker);
      await writeFile(legacyMarker, today);
      const cutoff = Date.now() - retentionDays * 86400_000;
      try {
        for (const name of await readdir(dirname(path))) {
          if (!name.startsWith(`${basename(path)}.`) || !name.endsWith(".gz")) continue;
          const archive = join(dirname(path), name);
          if ((await stat(archive)).mtimeMs < cutoff) await unlink(archive);
        }
      } catch {}
      activeDayState = today;
    }
  }

  async function pump() {
    if (running) return;
    running = true;
    try {
      while (pending.length) {
        const item = pending[0];
        try {
          await rotateIfNeeded();
          await appendFile(path, item.line, "utf8");
          persisted += 1;
          item.resolve({
            persistenceLatencyMs: Math.round((performance.now() - item.enqueuedAt) * 1000) / 1000,
            queueDepth: pending.length - 1,
          });
        } catch (error) {
          item.reject(error);
        } finally {
          pending.shift();
        }
      }
    } finally {
      running = false;
      if (!pending.length && idleResolve) {
        idleResolve();
        idleResolve = null;
      }
      if (pending.length) void pump();
    }
  }

  return {
    enqueue(record) {
      if (closing || pending.length >= maxQueue) {
        rejected += 1;
        return Promise.reject(new Error("audit_queue_backpressure"));
      }
      const line = JSON.stringify(record) + "\n";
      const enqueuedAt = performance.now();
      const promise = new Promise((resolve, reject) => pending.push({ line, enqueuedAt, resolve, reject }));
      void pump();
      return promise;
    },
    async close() {
      closing = true;
      if (!running && !pending.length) return;
      await new Promise(resolve => { idleResolve = resolve; });
    },
    stats() { return { depth: pending.length, maxQueue, persisted, rejected, running }; },
  };
}
