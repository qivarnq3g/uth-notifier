import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { chromium, type BrowserContext, type Response } from "playwright-core";

type FollowedSource = {
  id: string;
  name: string;
  url: string;
  verified: boolean;
};

type JsonObject = Record<string, unknown>;

const searchUserAgent =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36";
const maxGraphqlResponseBytes = 2 * 1024 * 1024;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textAt(value: unknown, key: string): string {
  if (!isObject(value)) return "";
  const candidate = value[key];
  return typeof candidate === "string" ? candidate : "";
}

function collectFollowing(value: unknown, sources: Map<string, FollowedSource>): void {
  if (Array.isArray(value)) {
    for (const item of value) collectFollowing(item, sources);
    return;
  }
  if (!isObject(value)) return;

  const actions = value.actions_renderer;
  const node = value.node;
  if (
    value.__typename === "TimelineAppCollectionItem" &&
    isObject(actions) &&
    actions.__typename === "TimelineAppCollectionItemFollowersListActionsRenderer" &&
    isObject(node)
  ) {
    const title = value.title;
    const id = textAt(node, "id");
    const name = textAt(title, "text") || textAt(node, "name");
    const url = textAt(value, "url") || textAt(node, "url");
    if (id && name && url) {
      sources.set(id, {
        id,
        name,
        url,
        verified: node.is_verified === true,
      });
    }
  }

  for (const nested of Object.values(value)) collectFollowing(nested, sources);
}

function parseJsonPayload(raw: string, sources: Map<string, FollowedSource>): void {
  const candidates = raw
    .split(/\r?\n/)
    .map((line) => line.trim().replace(/^for \(;;\);/, ""))
    .filter(Boolean);
  for (const candidate of candidates) {
    try {
      collectFollowing(JSON.parse(candidate), sources);
    } catch {
      continue;
    }
  }
}

async function collectDocumentScripts(
  context: BrowserContext,
  sources: Map<string, FollowedSource>,
): Promise<void> {
  for (const page of context.pages()) {
    const scripts = await page
      .locator('script[type="application/json"]')
      .allTextContents();
    for (const script of scripts) parseJsonPayload(script, sources);
  }
}

async function collectResponse(
  response: Response,
  sources: Map<string, FollowedSource>,
): Promise<void> {
  if (!response.url().includes("/api/graphql/")) return;
  try {
    const contentLength = Number(response.headers()["content-length"] ?? "0");
    if (contentLength > maxGraphqlResponseBytes) return;
    const body = await response.text();
    if (Buffer.byteLength(body, "utf8") > maxGraphqlResponseBytes) return;
    parseJsonPayload(body, sources);
  } catch {
    return;
  }
}

async function main(): Promise<void> {
  const profileUrl = process.argv[2];
  const outputPath = process.argv[3];
  const executablePath =
    process.env.CHROME_PATH ??
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  if (!profileUrl || !outputPath) {
    throw new Error("usage: following.ts <facebook-following-url> <output-json>");
  }

  const browser = await chromium.launch({ executablePath, headless: true });
  try {
    const context = await browser.newContext({
      userAgent: searchUserAgent,
      locale: "vi-VN",
      viewport: { width: 1280, height: 900 },
    });
    const page = await context.newPage();
    const sources = new Map<string, FollowedSource>();
    const pending = new Set<Promise<void>>();
    page.on("response", (response) => {
      const task = collectResponse(response, sources).finally(() => pending.delete(task));
      pending.add(task);
    });

    await page.goto(profileUrl, { waitUntil: "domcontentloaded", timeout: 45_000 });
    await collectDocumentScripts(context, sources);
    let stableRounds = 0;
    let previousCount = sources.size;
    for (let round = 0; round < 40 && stableRounds < 5; round += 1) {
      await page.keyboard.press("Escape").catch(() => undefined);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1_500);
      await Promise.allSettled([...pending]);
      await collectDocumentScripts(context, sources);
      if (sources.size === previousCount) stableRounds += 1;
      else stableRounds = 0;
      previousCount = sources.size;
    }

    await Promise.allSettled([...pending]);
    const result = {
      schema_version: "facebook-following-report.v1",
      profile_url: profileUrl,
      fetched_at: new Date().toISOString(),
      source_count: sources.size,
      sources: [...sources.values()].sort((left, right) =>
        left.name.localeCompare(right.name, "vi"),
      ),
    };
    const absoluteOutput = resolve(outputPath);
    await mkdir(dirname(absoluteOutput), { recursive: true });
    await writeFile(absoluteOutput, `${JSON.stringify(result, null, 2)}\n`, "utf8");
    process.stdout.write(`${absoluteOutput}\n`);
  } finally {
    await browser.close();
  }
}

await main();
