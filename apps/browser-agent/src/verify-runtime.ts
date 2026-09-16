import { chromium } from "playwright-core";

import { chromiumLaunchOptions } from "./browser-runtime.ts";

const browser = await chromium.launch(chromiumLaunchOptions([]));
const browserVersion = browser.version();
try {
  const context = await browser.newContext();
  try {
    const page = await context.newPage();
    await page.goto("about:blank");
  } finally {
    await context.close();
  }
} finally {
  await browser.close();
}
process.stdout.write(`${JSON.stringify({ browserVersion, status: "ok" })}\n`);
