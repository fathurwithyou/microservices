// Captures the browser evidence for the M2 report into docs/figures/m2/.
//   node scripts/m2-screenshots.mjs analytics-normal   (or analytics-partial)
// Needs: kubectl port-forward svc/demo-console 15173:80, FIELD_TEAM_CLIENT_SECRET
// in the environment, Google Chrome installed (Playwright channel "chrome").
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(path.join(here, "../apps/demo-console/package.json"));
const { chromium } = require("@playwright/test");

const name = process.argv[2] || "analytics-normal";
const base = process.env.CONSOLE_URL || "http://localhost:15173";
const out = path.join(here, "../docs/figures/m2", `${name}.png`);

const browser = await chromium.launch({ channel: "chrome" });
const page = await browser.newPage({ viewport: { width: 1500, height: 1000 }, colorScheme: "light" });
page.on("pageerror", (error) => console.error("pageerror:", String(error).slice(0, 400)));
await page.goto(`${base}/analytics`);
await page.locator("input").nth(1).waitFor({ timeout: 15000 });
await page.locator("input").nth(1).fill(process.env.FIELD_TEAM_CLIENT_SECRET || "");
await page.getByRole("button", { name: "Issue token" }).nth(1).click();
await page.waitForTimeout(1500);
if (process.env.RANGE) await page.locator("select").first().selectOption({ label: process.env.RANGE });
await page.getByRole("button", { name: "Muat" }).click();
await page.waitForTimeout(Number(process.env.WAIT_MS || 6000));
// Password-type or removed after login; blank any remaining secret inputs so
// a credential can never appear in the image.
for (const input of await page.locator("input").all()) await input.fill("").catch(() => {});
await page.screenshot({ path: out, fullPage: true });
await browser.close();
console.log(out);
