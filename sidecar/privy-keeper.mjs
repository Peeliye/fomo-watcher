import { chromium } from "playwright-core";
import dotenv from "dotenv";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const envFile = resolve(root, ".env");
const sessionFile = resolve(root, "data", ".fomo-session.env");
const statusFile = resolve(root, "data", "privy-status.json");
const debugFile = resolve(root, "data", "privy-sdk.log");
const profileDir = resolve(root, "data", "privy-browser-profile");
const bundle = readFileSync(resolve(import.meta.dirname, "privy-keeper.bundle.js"), "utf8");
const chrome = process.env.CHROME_PATH || "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
mkdirSync(dirname(sessionFile), { recursive: true });

const decodeExpiration = token => {
  try {
    const payload = JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString("utf8"));
    return Number(payload.exp || 0);
  } catch { return 0; }
};

const newestSeed = () => {
  const candidates = [dotenv.parse(readFileSync(envFile, "utf8"))];
  try { candidates.push(dotenv.parse(readFileSync(sessionFile, "utf8"))); } catch {}
  return candidates.reduce((best, item) => {
    const access = String(item.FOMO_ACCESS_TOKEN || "").trim().replace(/^"|"$/g, "");
    const refresh = String(item.FOMO_REFRESH_TOKEN || "").trim().replace(/^"|"$/g, "");
    return decodeExpiration(access) > decodeExpiration(best.access) ? { access, refresh } : best;
  }, { access: "", refresh: "" });
};

const seed = newestSeed();
if (!seed.access || !seed.refresh) throw new Error("缺少有效的 Fomo Privy 会话种子");
const claims = JSON.parse(Buffer.from(seed.access.split(".")[1], "base64url").toString("utf8"));
const appId = String(claims.aid || claims.aud || "").trim();
if (!appId) throw new Error("Fomo Privy 会话缺少应用标识");
let browser;
let context;
let page;
let stopping = false;

const status = async extra => {
  const keeper = await page?.evaluate(() => window.__privyKeeper || {}).catch(() => ({}));
  const tokens = await page?.evaluate(() => ({
    access: (() => { const raw = localStorage.getItem("privy:token") || ""; try { return JSON.parse(raw); } catch { return raw; } })(),
    refresh: (() => { const raw = localStorage.getItem("privy:refresh_token") || ""; try { return JSON.parse(raw); } catch { return raw; } })(),
  })).catch(() => ({ access: "", refresh: "" }));
  writeFileSync(statusFile, JSON.stringify({
    pid: process.pid,
    running: !stopping,
    headless: true,
    sdkReady: Boolean(keeper?.ready),
    authenticated: Boolean(keeper?.authenticated),
    lastSdkRefreshAt: keeper?.lastRefreshAt || null,
    sdkError: keeper?.error || null,
    tokenExpiresAt: decodeExpiration(tokens.access) ? new Date(decodeExpiration(tokens.access) * 1000).toISOString() : null,
    updatedAt: new Date().toISOString(),
    ...extra,
  }, null, 2));
};

const persist = async () => {
  const tokens = await page.evaluate(() => ({
    access: (() => { const raw = localStorage.getItem("privy:token") || ""; try { return JSON.parse(raw); } catch { return raw; } })(),
    refresh: (() => { const raw = localStorage.getItem("privy:refresh_token") || ""; try { return JSON.parse(raw); } catch { return raw; } })(),
  }));
  if (!tokens.access || !tokens.refresh) return false;
  writeFileSync(sessionFile, `FOMO_ACCESS_TOKEN=${tokens.access}\nFOMO_REFRESH_TOKEN=${tokens.refresh}\n`);
  return true;
};

const importNewerDiskSession = async () => {
  const disk = newestSeed();
  const current = await page.evaluate(() => {
    const raw = localStorage.getItem("privy:token") || "";
    try { return JSON.parse(raw); } catch { return raw; }
  }).catch(() => "");
  if (decodeExpiration(disk.access) <= decodeExpiration(current)) return false;
  await page.evaluate(({ access, refresh }) => {
    localStorage.setItem("privy:token", JSON.stringify(access));
    localStorage.setItem("privy:refresh_token", JSON.stringify(refresh));
  }, disk);
  await page.reload({ waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(3000);
  return true;
};

context = await chromium.launchPersistentContext(profileDir, {
  executablePath: chrome,
  headless: true,
  viewport: { width: 800, height: 600 },
  args: ["--disable-blink-features=AutomationControlled"],
});
await context.addInitScript(({ access, refresh }) => {
  if (location.hostname !== "fomo.family") return;
  const stored = localStorage.getItem("privy:token") || "";
  let current = stored;
  try { current = JSON.parse(stored); } catch {}
  const exp = token => {
    try { return JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/"))).exp || 0; }
    catch { return 0; }
  };
  if (exp(access) > exp(current)) {
    localStorage.setItem("privy:token", JSON.stringify(access));
    localStorage.setItem("privy:refresh_token", JSON.stringify(refresh));
  }
}, seed);
page = context.pages()[0] || await context.newPage();
page.on("response", response => {
  if (response.url().includes("auth.privy.io")) {
    writeFileSync(debugFile, `${new Date().toISOString()} HTTP ${response.status()} ${response.request().method()} ${response.url()}\n`, { flag: "a" });
  }
});
page.on("console", message => {
  if (message.type() === "error") writeFileSync(debugFile, `${new Date().toISOString()} CONSOLE ${message.text().slice(0, 500)}\n`, { flag: "a" });
});
await page.route("https://fomo.family/__privy_keeper", route => route.fulfill({
  status: 200,
  contentType: "text/html; charset=utf-8",
  body: `<!doctype html><html><body><div id="root"></div><script>window.__PRIVY_APP_ID__=${JSON.stringify(appId)}</script><script>${bundle}</script></body></html>`,
}));
await page.goto("https://fomo.family/__privy_keeper", { waitUntil: "domcontentloaded", timeout: 60000 });
await page.waitForTimeout(5000);
await persist();
await status({ state: "started" });

setInterval(async () => {
  await importNewerDiskSession().catch(() => false);
  const saved = await persist().catch(() => false);
  await status({ state: saved ? "ready" : "missing-session" });
}, 15000);

const shutdown = async signal => {
  stopping = true;
  await status({ state: signal });
  await context.close().catch(() => {});
  process.exit(0);
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
