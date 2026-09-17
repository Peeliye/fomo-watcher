import { chromium } from "playwright-core";
import dotenv from "dotenv";
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
dotenv.config({ path: resolve(root, ".env"), quiet: true });
const sessionFile = resolve(root, "data", ".fomo-session.env");
dotenv.config({ path: sessionFile, quiet: true, override: true });
const profileDir = process.env.CHROME_USER_DATA_DIR || resolve(root, "data", "fomo-browser-profile");
const profileName = process.env.CHROME_PROFILE_NAME || "Default";
const eventFile = resolve(root, "data", "ws-events.ndjson");
const statusFile = resolve(root, "data", "sidecar-status.json");
const requestLog = resolve(root, "data", "following-requests.log");
const protocolLog = resolve(root, "data", "ws-protocol.log");
const chrome = process.env.CHROME_PATH || "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
// 常驻监控默认完全无窗口；仅在排障时显式设置 SIDECAR_HEADLESS=false。
const headless = process.env.SIDECAR_HEADLESS !== "false";
mkdirSync(dirname(eventFile), { recursive: true });

let wsConnected = false;
let frames = 0;
let lastFrameAt = null;
let tokenExpiresAt = null;
let refreshInProgress = false;
const status = (extra = {}) => writeFileSync(statusFile, JSON.stringify({
  pid: process.pid, running: true, headless, profileName, wsConnected, frames, lastFrameAt,
  tokenExpiresAt, updatedAt: new Date().toISOString(), ...extra,
}, null, 2));

let browser = null;
let context;
if (process.env.CHROME_CDP_URL) {
  browser = await chromium.connectOverCDP(process.env.CHROME_CDP_URL);
  context = browser.contexts()[0];
  if (!context) throw new Error("Chrome CDP 已连接，但没有可用的浏览器上下文");
} else {
  context = await chromium.launchPersistentContext(profileDir, {
    executablePath: chrome,
    headless,
    viewport: { width: 1280, height: 850 },
    args: ["--disable-blink-features=AutomationControlled", `--profile-directory=${profileName}`],
  });
}
// 用用户已在本机保存的 Fomo 会话初始化独立浏览器；不需要再次走 Google 登录。
// 只在当前 localStorage 没有值时写入，之后由 Fomo 内置 Privy SDK 自动刷新和轮换。
if (headless || process.env.INJECT_FOMO_SESSION === "true") await context.addInitScript(({ access, refresh }) => {
  if (location.hostname !== "fomo.family") return;
  if (localStorage.getItem("fomo:sidecar_force_refresh") === "1") return;
  const exp = token => {
    try {
      const part = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
      return JSON.parse(atob(part)).exp || 0;
    } catch { return 0; }
  };
  const stored = localStorage.getItem("privy:token") || "";
  // 新导出的会话比 profile 内会话更新时，仅覆盖一次；SDK 后续旋转不会被旧 .env 回滚。
  if (access && exp(access) > exp(stored)) {
    localStorage.setItem("privy:token", access);
    if (refresh) localStorage.setItem("privy:refresh_token", refresh);
  }
}, { access: process.env.FOMO_ACCESS_TOKEN || "", refresh: process.env.FOMO_REFRESH_TOKEN || "" });
const page = context.pages()[0] || await context.newPage();

const persistBrowserSession = async () => {
  const tokens = await page.evaluate(() => ({
    access: localStorage.getItem("privy:token") || "",
    refresh: localStorage.getItem("privy:refresh_token") || "",
  })).catch(() => ({ access: "", refresh: "" }));
  if (!tokens.access || !tokens.refresh) return false;
  try {
    const part = tokens.access.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    tokenExpiresAt = new Date(JSON.parse(Buffer.from(part, "base64url").toString("utf8")).exp * 1000).toISOString();
  } catch { tokenExpiresAt = null; }
  writeFileSync(sessionFile, `FOMO_ACCESS_TOKEN=${tokens.access}\nFOMO_REFRESH_TOKEN=${tokens.refresh}\n`);
  return true;
};

const accessSecondsRemaining = async () => page.evaluate(() => {
  try {
    const token = localStorage.getItem("privy:token") || "";
    const part = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(part)).exp - Date.now() / 1000;
  } catch { return -1; }
}).catch(() => -1);

const tokenExpiration = token => {
  try {
    const part = token.split(".")[1];
    return JSON.parse(Buffer.from(part, "base64url").toString("utf8")).exp || 0;
  } catch { return 0; }
};

const activatePrivy = async () => {
  const login = page.getByRole("button", { name: /^login$/i }).first();
  if (await login.isVisible().catch(() => false)) {
    await login.click().catch(() => {});
    await page.waitForTimeout(4000);
  }
};

const forcePrivySdkRefresh = async () => {
  const seedAccess = process.env.FOMO_ACCESS_TOKEN || "";
  const seedRefresh = process.env.FOMO_REFRESH_TOKEN || "";
  const previousExpiration = tokenExpiration(seedAccess);
  await page.evaluate(() => {
    localStorage.setItem("fomo:sidecar_force_refresh", "1");
    localStorage.removeItem("privy:token");
  });
  await page.reload({ waitUntil: "domcontentloaded", timeout: 60000 });
  await activatePrivy();
  await page.waitForTimeout(5000);
  const refreshed = await page.evaluate(() => localStorage.getItem("privy:token") || "").catch(() => "");
  await page.evaluate(() => localStorage.removeItem("fomo:sidecar_force_refresh")).catch(() => {});
  if (tokenExpiration(refreshed) > previousExpiration) {
    await persistBrowserSession();
    status({ forcedRefresh: "succeeded" });
    return true;
  }
  await page.evaluate(({ access, refresh }) => {
    if (access) localStorage.setItem("privy:token", access);
    if (refresh) localStorage.setItem("privy:refresh_token", refresh);
  }, { access: seedAccess, refresh: seedRefresh });
  await page.reload({ waitUntil: "domcontentloaded", timeout: 60000 }).catch(() => {});
  status({ forcedRefresh: "restored-seed" });
  return false;
};

const refreshWithPrivySdk = async () => {
  if (refreshInProgress) return;
  refreshInProgress = true;
  try {
    status({ refreshState: "reloading-sdk" });
    // Privy's client SDK restores and rotates its one-time refresh token during
    // application initialization. Reload shortly before expiry to invoke it.
    await page.reload({ waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForTimeout(5000);
    const saved = await persistBrowserSession();
    status({ refreshState: saved ? "ready" : "missing-session", pageUrl: page.url(), sessionSaved: saved });
  } catch (error) {
    status({ refreshState: "failed", refreshError: String(error?.message || error) });
  } finally {
    refreshInProgress = false;
  }
};

page.on("request", request => {
  const url = request.url();
  if (/follow(?:ing|ers)?/i.test(url)) appendFileSync(requestLog, `${new Date().toISOString()} ${request.method()} ${url}\n`);
});
page.on("websocket", ws => {
  if (!ws.url().includes("prod-api.fomo.family/ws")) return;
  wsConnected = true;
  status({ wsUrl: ws.url() });
  ws.on("framesent", frame => {
    if (typeof frame.payload !== "string") return;
    try {
      const message = JSON.parse(frame.payload);
      if (message.type === "challengeResponse") message.jwt = message.jwt ? "<present>" : "<missing>";
      appendFileSync(protocolLog, `${new Date().toISOString()} SENT ${JSON.stringify(message)}\n`);
    } catch {}
  });
  ws.on("framereceived", frame => {
    if (typeof frame.payload !== "string") return;
    try {
      const msg = JSON.parse(frame.payload);
      if (msg?.type !== "data" || msg?.topicType !== "trading_activity" || !msg?.payload) return;
      appendFileSync(eventFile, JSON.stringify({ receivedAt: new Date().toISOString(), payload: msg.payload }) + "\n");
      frames += 1;
      lastFrameAt = new Date().toISOString();
      status({ wsUrl: ws.url() });
    } catch {}
  });
  ws.on("close", () => { wsConnected = false; status(); });
});

page.on("close", () => status({ running: false, reason: "page_closed" }));
const startUrl = process.env.SIDECAR_START_URL || "https://fomo.family/tokens/robinhood/0x39dbed3a2bd333467115de45665cc57f813c4571";
await page.goto(startUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
// Headless 恢复时等 React 首次 hydration 完成后才写入会话，避免服务端
// logged-out HTML 与客户端预注入登录态冲突（React #418）。Privy 会在随后
// 打开 Login 时读取 refresh token 并自行轮换。
if (headless && process.env.FOMO_REFRESH_TOKEN) {
  await page.evaluate(({ access, refresh }) => {
    if (!localStorage.getItem("privy:token") && access) {
      localStorage.setItem("privy:token", access);
    }
    if (!localStorage.getItem("privy:refresh_token")) {
      localStorage.setItem("privy:refresh_token", refresh);
    }
  }, {
    access: process.env.FOMO_ACCESS_TOKEN || "",
    refresh: process.env.FOMO_REFRESH_TOKEN,
  });
}
// 公开落地页不会仅凭已有会话自动跳转；触发正常 Login 入口让 Privy 恢复会话。
await activatePrivy();
if (process.env.PRIVY_FORCE_REFRESH_ON_START === "true") await forcePrivySdkRefresh();
const sessionSaved = await persistBrowserSession();
status({ pageUrl: page.url(), pageTitle: await page.title(), sessionSaved });
setInterval(async () => {
  const saved = await persistBrowserSession();
  status({ pageUrl: page.url(), sessionSaved: saved });
}, 15000);
setInterval(async () => {
  const remaining = await accessSecondsRemaining();
  if (remaining < 120) await refreshWithPrivySdk();
}, 30000);

const shutdown = async signal => {
  status({ running: false, reason: signal });
  if (!process.env.CHROME_CDP_URL) await context.close();
  else await browser?.close().catch(() => {});
  process.exit(0);
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
