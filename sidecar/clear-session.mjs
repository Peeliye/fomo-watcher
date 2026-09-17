import { chromium } from "playwright-core";

const endpoint = process.env.CHROME_CDP_URL || "http://127.0.0.1:9223";
const browser = await chromium.connectOverCDP(endpoint);
const pages = browser.contexts().flatMap((context) => context.pages());
let page = pages.find((candidate) => candidate.url().startsWith("https://fomo.family"));

if (!page) {
  page = pages.find(
    (candidate) =>
      !candidate.url().startsWith("devtools://") &&
      !candidate.url().startsWith("chrome://"),
  );
  if (page) {
    await page.goto("https://fomo.family/", { waitUntil: "domcontentloaded" });
  }
}

if (!page) {
  throw new Error("No fomo.family page is open in the Sidecar Chrome profile");
}

const result = await page.evaluate(() => {
  localStorage.removeItem("privy:token");
  localStorage.removeItem("privy:refresh_token");
  return {
    tokenPresent: localStorage.getItem("privy:token") !== null,
    refreshTokenPresent: localStorage.getItem("privy:refresh_token") !== null,
  };
});

await page.reload({ waitUntil: "domcontentloaded" });
console.log(JSON.stringify({ cleared: true, ...result, url: page.url() }));
process.exit(0);
