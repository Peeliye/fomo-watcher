import dotenv from "dotenv";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { appendNdjson, appendShadow, evaluateShadow, fileMtime, loadJson } from "./fast-shadow.mjs";

const root = resolve(import.meta.dirname, "..");
const envFile = resolve(root, ".env");
const sessionFile = resolve(root, "data", ".fomo-session.env");
const eventFile = resolve(root, "data", "ws-events.ndjson");
const statusFile = resolve(root, "data", "realtime-status.json");
const shadowFile = resolve(root, "data", "shadow-executions.ndjson");
const fastConfigFile = resolve(root, "data", "fast-executor-config.json");
const followingFile = resolve(root, "data", "following-ids.json");
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

function refreshFastState() {
  const nextConfigMtime = fileMtime(fastConfigFile);
  if (nextConfigMtime !== fastConfigMtime) {
    fastConfig = loadJson(fastConfigFile, null);
    fastConfigMtime = nextConfigMtime;
  }
  const nextFollowingMtime = fileMtime(followingFile);
  if (nextFollowingMtime !== followingMtime) {
    following = loadJson(followingFile, null);
    followingMtime = nextFollowingMtime;
  }
}

function tokenExpiration(token) {
  try { return JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString("utf8")).exp || 0; }
  catch { return 0; }
}

function currentAccessToken() {
  const candidates = [envFile, sessionFile].map(path => {
    try { return dotenv.parse(readFileSync(path, "utf8")).FOMO_ACCESS_TOKEN || ""; }
    catch { return ""; }
  });
  return candidates.sort((a, b) => tokenExpiration(b) - tokenExpiration(a))[0] || "";
}

function status(extra = {}) {
  writeFileSync(statusFile, JSON.stringify({
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
    ...extra,
  }, null, 2));
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
      refreshFastState();
      const shadow = evaluateShadow(message.payload, receivedAt, fastConfig, following);
      shadow.decisionLatencyMs = Math.round(Number(process.hrtime.bigint() - ingressStarted) / 1000) / 1000;
      appendShadow(shadowFile, shadow);
      appendNdjson(eventFile, { receivedAt, payload: message.payload });
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

function shutdown(signal) {
  stopping = true;
  status({ reason: signal });
  ws?.close();
  setTimeout(() => process.exit(0), 250);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
connect();
setInterval(() => status(), 15000);
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
