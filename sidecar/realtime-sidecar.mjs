import dotenv from "dotenv";
import { readFileSync } from "node:fs";
import { readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createNdjsonQueue, evaluateShadow } from "./fast-shadow.mjs";
import { acquireSingletonLock } from "./process-lock.mjs";
import { loadVaultAccessToken } from "./secret-store.mjs";

const root = resolve(import.meta.dirname, "..");
const envFile = resolve(root, ".env");
const sessionFile = resolve(root, "data", ".fomo-session.env");
const eventFile = resolve(root, "data", "ws-events.ndjson");
const statusFile = resolve(root, "data", "realtime-status.json");
const shadowFile = resolve(root, "data", "shadow-executions.ndjson");
const fastConfigFile = resolve(root, "data", "fast-executor-config.json");
const followingFile = resolve(root, "data", "following-ids.json");
const releaseWriterLock = await acquireSingletonLock(resolve(root, "data", "fomo-events-writer.lock"), "realtime");
const configText = readFileSync(resolve(root, "config.yaml"), "utf8");
const topicId = configText.match(/^realtime_topic_id:\s*["']?([^\s"']+)/m)?.[1] || "";
if (!topicId) throw new Error("config.yaml 缺少 realtime_topic_id");

let ws = null;
let authenticated = false;
let subscribed = false;
let frames = 0;
let lastFrameAt = null;
let lastMessageAt = null;
let connectedAt = null;
let reconnectDelay = 1000;
let reconnectTimer = null;
let socketStartedAt = null;
let activeTokenExpiration = 0;
let stopping = false;
let fastConfig = null;
let following = null;
let fastConfigMtime = 0;
let followingMtime = 0;
let statusTimer = null;
let statusExtra = {};
let statusWrites = Promise.resolve();
const eventQueue = createNdjsonQueue(eventFile, { maxQueue: 10_000, retentionDays: 30 });
const shadowQueue = createNdjsonQueue(shadowFile, { maxQueue: 10_000, retentionDays: 30 });

async function fileMtime(path) {
  try { return (await stat(path)).mtimeMs; } catch { return 0; }
}

async function loadJson(path, fallback = null) {
  try { return JSON.parse(await readFile(path, "utf8")) ?? fallback; } catch { return fallback; }
}

async function refreshFastState() {
  const nextConfigMtime = await fileMtime(fastConfigFile);
  if (nextConfigMtime !== fastConfigMtime) {
    fastConfig = await loadJson(fastConfigFile, null);
    fastConfigMtime = nextConfigMtime;
  }
  const nextFollowingMtime = await fileMtime(followingFile);
  if (nextFollowingMtime !== followingMtime) {
    following = await loadJson(followingFile, null);
    followingMtime = nextFollowingMtime;
  }
}

function tokenExpiration(token) {
  try { return JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString("utf8")).exp || 0; }
  catch { return 0; }
}

function currentAccessToken() {
  const candidates = [loadVaultAccessToken(root), ...[envFile, sessionFile].map(path => {
    try { return dotenv.parse(readFileSync(path, "utf8")).FOMO_ACCESS_TOKEN || ""; }
    catch { return ""; }
  })];
  return candidates.sort((a, b) => tokenExpiration(b) - tokenExpiration(a))[0] || "";
}

async function flushStatus() {
  statusTimer = null;
  const payload = {
    pid: process.pid,
    running: !stopping,
    connected: ws?.readyState === WebSocket.OPEN,
    authenticated,
    subscribed,
    topicId,
    frames,
    lastFrameAt,
    lastMessageAt,
    updatedAt: new Date().toISOString(),
    eventQueue: eventQueue.stats(),
    shadowQueue: shadowQueue.stats(),
    ...statusExtra,
  };
  statusExtra = {};
  statusWrites = statusWrites.then(async () => {
    await writeFile(`${statusFile}.tmp`, JSON.stringify(payload, null, 2));
    await rename(`${statusFile}.tmp`, statusFile);
  }).catch(() => {});
  await statusWrites;
}

function status(extra = {}) {
  statusExtra = { ...statusExtra, ...extra };
  if (!statusTimer) statusTimer = setTimeout(() => { void flushStatus(); }, 250);
}

function sendChallenge() {
  const jwt = currentAccessToken();
  if (ws?.readyState === WebSocket.OPEN) {
    activeTokenExpiration = tokenExpiration(jwt);
    ws.send(JSON.stringify({ type: "challengeResponse", jwt }));
  }
}

function scheduleReconnect(reason, immediate = false) {
  if (stopping) return;
  authenticated = false;
  subscribed = false;
  const previous = ws;
  ws = null;
  try { previous?.close(4000, reason); } catch {}
  if (reconnectTimer) {
    status({ reason });
    return;
  }
  const delay = immediate ? 0 : reconnectDelay;
  if (!immediate) reconnectDelay = Math.min(reconnectDelay * 2, 30000);
  status({ reason, reconnectInMs: delay });
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function connect() {
  if (stopping) return;
  authenticated = false;
  subscribed = false;
  socketStartedAt = Date.now();
  let socket;
  try {
    socket = new WebSocket("wss://prod-api.fomo.family/ws", {
      headers: { Origin: "https://fomo.family" },
    });
  } catch (error) {
    scheduleReconnect(`connect-failed:${error?.message || "unknown"}`);
    return;
  }
  ws = socket;
  socket.addEventListener("open", () => {
    if (ws !== socket) return;
    connectedAt = Date.now();
    lastMessageAt = new Date().toISOString();
    sendChallenge();
    status();
  });
  socket.addEventListener("message", event => {
    if (ws !== socket) return;
    lastMessageAt = new Date().toISOString();
    let message;
    try { message = JSON.parse(String(event.data)); } catch { return; }
    if (message.type === "challenge") {
      sendChallenge();
    } else if (message.type === "challengeAccepted") {
      authenticated = true;
      reconnectDelay = 1000;
      ws.send(JSON.stringify({ type: "subscribe", topicType: "trading_activity", topicId }));
    } else if (message.type === "subscribed" && message.topicType === "trading_activity" && message.topicId === topicId) {
      subscribed = true;
    } else if (message.type === "data" && message.topicType === "trading_activity" && message.topicId === topicId) {
      const ingressStarted = process.hrtime.bigint();
      const receivedAt = new Date().toISOString();
      const shadow = evaluateShadow(message.payload, receivedAt, fastConfig, following);
      shadow.decisionLatencyMs = Math.round(Number(process.hrtime.bigint() - ingressStarted) / 1000) / 1000;
      const enqueueStarted = performance.now();
      const eventPersisted = eventQueue.enqueue({ receivedAt, payload: message.payload });
      shadow.durableEnqueueLatencyMs = Math.round((performance.now() - enqueueStarted) * 1000) / 1000;
      const shadowPersisted = shadowQueue.enqueue(shadow);
      Promise.all([eventPersisted, shadowPersisted]).then(results => {
        status({
          persistenceLatencyMs: Math.max(...results.map(item => item.persistenceLatencyMs)),
          endToEndLatencyMs: Math.round((performance.now() - enqueueStarted + shadow.decisionLatencyMs) * 1000) / 1000,
        });
      }).catch(error => {
        status({ reason: error?.message === "audit_queue_backpressure" ? "audit-backpressure" : "audit-write-error" });
        if (ws === socket && ws.readyState === WebSocket.OPEN) ws.close(1013, "audit-backpressure");
      });
      frames += 1;
      lastFrameAt = new Date().toISOString();
    } else if (message.type === "error") {
      status({ errorCode: message.code || "WS_ERROR", errorMessage: message.message || "" });
    }
    status();
  });
  socket.addEventListener("close", event => {
    if (ws !== socket) return;
    scheduleReconnect(`closed:${event.code || 0}`);
  });
  socket.addEventListener("error", () => {
    if (ws !== socket) return;
    scheduleReconnect("socket-error");
  });
}

async function shutdown(signal) {
  stopping = true;
  status({ reason: signal });
  ws?.close();
  const forced = setTimeout(() => process.exit(1), 5000);
  await Promise.allSettled([eventQueue.close(), shadowQueue.close()]);
  await flushStatus();
  await releaseWriterLock();
  clearTimeout(forced);
  process.exit(0);
}

process.on("SIGINT", () => { void shutdown("SIGINT"); });
process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
await refreshFastState();
connect();
setInterval(() => status(), 15000);
setInterval(() => { void refreshFastState(); }, 1000);
setInterval(() => {
  const now = Date.now();
  if (!ws) {
    if (!reconnectTimer) scheduleReconnect("socket-missing", true);
    return;
  }
  if (ws.readyState === WebSocket.CONNECTING) {
    if (socketStartedAt && now - socketStartedAt > 15000) scheduleReconnect("connect-timeout");
    return;
  }
  if (ws.readyState !== WebSocket.OPEN) {
    scheduleReconnect("socket-not-open");
    return;
  }
  if (connectedAt && now - connectedAt > 15000 && (!authenticated || !subscribed)) {
    scheduleReconnect(!authenticated ? "authentication-timeout" : "subscription-timeout");
    return;
  }
  const lastActivity = lastMessageAt ? Date.parse(lastMessageAt) : connectedAt;
  if (lastActivity && now - lastActivity > 90000) {
    scheduleReconnect("stale-connection");
    return;
  }
  const newestExpiration = tokenExpiration(currentAccessToken());
  if (activeTokenExpiration && activeTokenExpiration * 1000 - now < 30000 && newestExpiration > activeTokenExpiration) {
    scheduleReconnect("token-rotated", true);
  }
}, 15000);
