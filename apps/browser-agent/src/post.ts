import { resolve4 } from "node:dns/promises";
import { realpathSync } from "node:fs";
import { isIP } from "node:net";
import { pathToFileURL } from "node:url";
import { chromium, type Response } from "playwright-core";

const searchUserAgent =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36";

export const chromiumCrashReportingArgs = [
  "--disable-breakpad",
  "--disable-crash-reporter",
] as const;

type BrowserSnapshot = {
  schema_version: "facebook-browser-snapshot.v1";
  source_url: string;
  final_url: string;
  fetched_at: string;
  status: number | null;
  latency_ms: number;
  discovered_post_url: string | null;
  discovered_published_at: string | null;
  discovered_external_post_id: string | null;
  discovered_text: string | null;
  discovered_post_origin: "dom" | "graphql" | null;
  newest_dom_post_unresolved: boolean;
  network_requested_mode: BrowserNetworkMode;
  network_effective_mode: BrowserEffectiveNetworkMode;
  network_remote_family: BrowserRemoteFamily;
  network_fallback_reason: string | null;
  login_overlay_detected: boolean;
  login_overlay_dismissed: boolean;
  login_route_detected: boolean;
  html: string;
};

export type BrowserNetworkMode = "system" | "prefer_ipv4";
type BrowserEffectiveNetworkMode = "system" | "ipv4";
export type BrowserRemoteFamily = "ipv4" | "ipv6" | "unknown";

type BrowserNetworkSelection = {
  requestedMode: BrowserNetworkMode;
  effectiveMode: BrowserEffectiveNetworkMode;
  ipv4Address: string | null;
  fallbackReason: string | null;
};

type LoginOverlayActivity = {
  detected: boolean;
  dismissed: boolean;
};

type JsonObject = Record<string, unknown>;

export type GraphqlPost = {
  url: string;
  text: string | null;
  publishedAt: string | null;
  externalPostId: string | null;
};

export type GraphqlInspection = {
  payloads: string[];
  posts: GraphqlPost[];
};

type GraphqlSignals = {
  postIds: Set<string>;
  urls: Set<string>;
  timestamps: Set<number>;
  texts: Set<string>;
  ownerIdentities: Set<string>;
};

const maxGraphqlResponseBytes = 2 * 1024 * 1024;
const maxGraphqlSnapshotBytes = 4 * 1024 * 1024;
const maxNormalizedGraphqlBytes = 1024 * 1024;
const maxCapturedGraphqlPosts = 100;
const historySweepTarget = 20;
const historySweepPasses = 5;
const historySweepPauseMs = 900;
const maxGraphqlPayloads = 12;
const maxGraphqlWalkDepth = 64;
const maxGraphqlWalkNodes = 50_000;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizedIdentity(value: string): string {
  return value.trim().replace(/^@/, "").toLowerCase();
}

function isPlausiblePostId(value: string): boolean {
  return /^\d{8,}$/.test(value) || /^pfbid[a-z0-9]+$/i.test(value);
}

export function preferredExternalPostId(
  ...values: Array<string | null | undefined>
): string | null {
  const plausible = values.filter(
    (value): value is string => value !== null && value !== undefined && isPlausiblePostId(value),
  );
  return plausible.find((value) => /^\d{8,}$/.test(value)) ?? plausible[0] ?? null;
}

function pageIdentity(sourceUrl: string): string {
  const segments = new URL(sourceUrl).pathname.split("/").filter(Boolean);
  if (segments[0] === "people" && /^\d+$/.test(segments[2] ?? "")) {
    return segments[2];
  }
  return segments[0] ?? "";
}

function verifiedNumericPageIdentity(sourceUrl: string): string | null {
  const parsed = new URL(sourceUrl);
  const segments = parsed.pathname.split("/").filter(Boolean);
  if (segments[0] === "people" && /^\d+$/.test(segments[2] ?? "")) {
    return segments[2] ?? null;
  }
  const id = parsed.searchParams.get("id");
  return id && /^\d+$/.test(id) ? id : null;
}

function sourceIdentities(sourceUrl: string): Set<string> {
  const parsed = new URL(sourceUrl);
  const segments = parsed.pathname.split("/").filter(Boolean);
  const identities = new Set<string>();
  for (const segment of segments) {
    if (!["people", "pages", "profile.php"].includes(segment.toLowerCase())) {
      identities.add(normalizedIdentity(segment));
    }
  }
  const id = parsed.searchParams.get("id");
  if (id) identities.add(normalizedIdentity(id));
  const primary = pageIdentity(sourceUrl);
  if (primary) identities.add(normalizedIdentity(primary));
  return identities;
}

function pagePluginUrl(sourceUrl: string): string {
  const url = new URL("https://www.facebook.com/plugins/page.php");
  url.searchParams.set("href", sourceUrl);
  url.searchParams.set("tabs", "timeline");
  url.searchParams.set("width", "500");
  url.searchParams.set("height", "800");
  return url.toString();
}

export function isFacebookLoginRoute(rawUrl: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return false;
  }
  const host = parsed.hostname.toLowerCase();
  if (!(host === "facebook.com" || host.endsWith(".facebook.com"))) {
    return false;
  }
  const path = parsed.pathname.toLowerCase();
  return (
    path === "/login" ||
    path.startsWith("/login/") ||
    path === "/checkpoint" ||
    path.startsWith("/checkpoint/")
  );
}

export function parseBrowserNetworkMode(
  rawMode: string | undefined,
): BrowserNetworkMode {
  const mode = rawMode?.trim().toLowerCase() || "system";
  if (mode === "system" || mode === "prefer_ipv4") return mode;
  throw new Error(
    `FACEBOOK_BROWSER_NETWORK_MODE must be system or prefer_ipv4, got ${mode}`,
  );
}

export function remoteAddressFamily(address: string | null): BrowserRemoteFamily {
  if (!address) return "unknown";
  const version = isIP(address);
  if (version === 4) return "ipv4";
  if (version === 6) return "ipv6";
  return "unknown";
}

export function chromiumNetworkArgs(ipv4Address: string | null): string[] {
  if (!ipv4Address || isIP(ipv4Address) !== 4) return [];
  return [
    "--disable-quic",
    `--host-resolver-rules=MAP www.facebook.com ${ipv4Address}, EXCLUDE localhost`,
  ];
}

function boundedError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.replace(/[\r\n]+/g, " ").slice(0, 240);
}

async function selectBrowserNetwork(
  requestedMode: BrowserNetworkMode,
): Promise<BrowserNetworkSelection> {
  if (requestedMode === "system") {
    return {
      requestedMode,
      effectiveMode: "system",
      ipv4Address: null,
      fallbackReason: null,
    };
  }
  try {
    const addresses = await resolve4("www.facebook.com");
    const ipv4Address = addresses.find((address) => isIP(address) === 4) ?? null;
    if (ipv4Address) {
      return {
        requestedMode,
        effectiveMode: "ipv4",
        ipv4Address,
        fallbackReason: null,
      };
    }
    return {
      requestedMode,
      effectiveMode: "system",
      ipv4Address: null,
      fallbackReason: "ipv4_dns_returned_no_address",
    };
  } catch (error) {
    return {
      requestedMode,
      effectiveMode: "system",
      ipv4Address: null,
      fallbackReason: `ipv4_dns_failed:${boundedError(error)}`,
    };
  }
}

export type DiscoveredPost = {
  url: string;
  text: string | null;
  publishedAt: string | null;
  externalPostId: string | null;
  origin: "dom" | "graphql";
};

function sameDiscoveredPost(
  left: DiscoveredPost,
  right: DiscoveredPost,
): boolean {
  const leftLocator = postLocator(left.url);
  const rightLocator = postLocator(right.url);
  return (
    (leftLocator !== "" && leftLocator === rightLocator) ||
    (left.externalPostId !== null &&
      left.externalPostId === right.externalPostId)
  );
}

export function selectNewestDiscoveredPost(
  graphqlPost: DiscoveredPost | null,
  domPost: DiscoveredPost | null,
): DiscoveredPost | null {
  if (!graphqlPost) return domPost;
  if (!domPost) return graphqlPost;
  if (sameDiscoveredPost(graphqlPost, domPost)) {
    const graphqlTime = graphqlPost.publishedAt
      ? Date.parse(graphqlPost.publishedAt)
      : Number.NEGATIVE_INFINITY;
    const domTime = domPost.publishedAt
      ? Date.parse(domPost.publishedAt)
      : Number.NEGATIVE_INFINITY;
    return {
      url: domPost.url,
      text:
        (domPost.text?.length ?? 0) > (graphqlPost.text?.length ?? 0)
          ? domPost.text
          : graphqlPost.text,
      publishedAt:
        domTime >= graphqlTime ? domPost.publishedAt : graphqlPost.publishedAt,
      externalPostId: preferredExternalPostId(
        domPost.externalPostId,
        graphqlPost.externalPostId,
      ),
      origin: domTime > graphqlTime ? "dom" : "graphql",
    };
  }
  if (!domPost.publishedAt) return graphqlPost;
  if (!graphqlPost.publishedAt) return domPost;
  return Date.parse(domPost.publishedAt) >= Date.parse(graphqlPost.publishedAt)
    ? domPost
    : graphqlPost;
}

function facebookPostUrl(raw: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(raw.replaceAll("\\/", "/"), "https://www.facebook.com");
  } catch {
    return null;
  }
  const host = parsed.hostname.toLowerCase();
  if (!(host === "facebook.com" || host.endsWith(".facebook.com"))) return null;
  if (
    parsed.pathname !== "/permalink.php" &&
    !/\/(posts|videos|reel)\//i.test(parsed.pathname)
  ) {
    return null;
  }
  const storyId = parsed.searchParams.get("story_fbid");
  const ownerId = parsed.searchParams.get("id");
  parsed.protocol = "https:";
  parsed.hostname = "www.facebook.com";
  parsed.hash = "";
  parsed.search = "";
  parsed.pathname = `/${parsed.pathname.split("/").filter(Boolean).join("/")}`;
  if (storyId && ownerId && parsed.pathname === "/permalink.php") {
    parsed.searchParams.set("story_fbid", storyId);
    parsed.searchParams.set("id", ownerId);
  }
  return parsed.toString().replace(/\/$/, "");
}

function identitiesFromUrl(raw: string): Set<string> {
  const identities = new Set<string>();
  let parsed: URL;
  try {
    parsed = new URL(raw, "https://www.facebook.com");
  } catch {
    return identities;
  }
  const ownerId = parsed.searchParams.get("id");
  if (ownerId) identities.add(normalizedIdentity(ownerId));
  const segments = parsed.pathname.split("/").filter(Boolean);
  const marker = segments.findIndex((segment) =>
    ["posts", "videos", "reel"].includes(segment.toLowerCase()),
  );
  if (marker > 0) {
    identities.add(normalizedIdentity(segments[marker - 1] ?? ""));
  }
  if (segments[0] === "people" && segments[2]) {
    identities.add(normalizedIdentity(segments[2]));
  }
  return identities;
}

function addOwnerIdentities(value: unknown, identities: Set<string>): void {
  const stack: unknown[] = [value];
  let visited = 0;
  while (stack.length > 0 && visited < 256) {
    const current = stack.pop();
    visited += 1;
    if (Array.isArray(current)) {
      stack.push(...current);
      continue;
    }
    if (!isObject(current)) continue;
    for (const [key, nested] of Object.entries(current)) {
      if (
        ["id", "username", "vanity", "profile_name"].includes(key) &&
        (typeof nested === "string" || typeof nested === "number")
      ) {
        identities.add(normalizedIdentity(String(nested)));
      }
      if (
        ["url", "profile_url", "uri"].includes(key) &&
        typeof nested === "string"
      ) {
        for (const identity of identitiesFromUrl(nested)) identities.add(identity);
        try {
          const segments = new URL(nested, "https://www.facebook.com").pathname
            .split("/")
            .filter(Boolean);
          if (segments[0]) identities.add(normalizedIdentity(segments[0]));
        } catch {
          continue;
        }
      }
      if (Array.isArray(nested) || isObject(nested)) stack.push(nested);
    }
  }
}

function messageText(value: unknown): string | null {
  if (typeof value === "string") {
    const text = value.trim();
    return text || null;
  }
  if (!isObject(value) || typeof value.text !== "string") return null;
  const text = value.text.trim();
  return text || null;
}

function timestampValue(value: unknown): number | null {
  const timestamp =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number.parseInt(value, 10)
        : Number.NaN;
  const latestAllowed = Math.floor(Date.now() / 1_000) + 2 * 24 * 60 * 60;
  return Number.isSafeInteger(timestamp) &&
    timestamp >= 1_072_915_200 &&
    timestamp <= latestAllowed
    ? timestamp
    : null;
}

function collectGraphqlSignals(root: unknown): GraphqlSignals {
  const signals: GraphqlSignals = {
    postIds: new Set<string>(),
    urls: new Set<string>(),
    timestamps: new Set<number>(),
    texts: new Set<string>(),
    ownerIdentities: new Set<string>(),
  };
  const stack: Array<{ value: unknown; depth: number }> = [{ value: root, depth: 0 }];
  let visited = 0;
  while (stack.length > 0 && visited < maxGraphqlWalkNodes) {
    const current = stack.pop();
    if (!current || current.depth > maxGraphqlWalkDepth) continue;
    visited += 1;
    if (Array.isArray(current.value)) {
      for (const nested of current.value) {
        stack.push({ value: nested, depth: current.depth + 1 });
      }
      continue;
    }
    if (!isObject(current.value)) continue;
    for (const [key, nested] of Object.entries(current.value)) {
      if (
        ["post_id", "top_level_post_id", "subscription_target_id"].includes(key) &&
        (typeof nested === "string" || typeof nested === "number")
      ) {
        const postId = String(nested);
        if (isPlausiblePostId(postId)) signals.postIds.add(postId);
      }
      if (
        ["creation_time", "publish_time"].includes(key)
      ) {
        const timestamp = timestampValue(nested);
        if (timestamp !== null) signals.timestamps.add(timestamp);
      }
      if (["message", "message_context"].includes(key)) {
        const text = messageText(nested);
        if (text) signals.texts.add(text);
      }
      if (
        ["url", "permalink_url", "wwwURL"].includes(key) &&
        typeof nested === "string"
      ) {
        const url = facebookPostUrl(nested);
        if (url) signals.urls.add(url);
      }
      if (["owning_profile", "actors", "actor", "author"].includes(key)) {
        addOwnerIdentities(nested, signals.ownerIdentities);
      }
      if (Array.isArray(nested) || isObject(nested)) {
        stack.push({ value: nested, depth: current.depth + 1 });
      }
    }
  }
  return signals;
}

function candidateScopes(root: unknown): unknown[] {
  const scopes: unknown[] = [];
  const stack: Array<{ value: unknown; depth: number }> = [{ value: root, depth: 0 }];
  let visited = 0;
  while (stack.length > 0 && visited < maxGraphqlWalkNodes) {
    const current = stack.pop();
    if (!current || current.depth > maxGraphqlWalkDepth) continue;
    visited += 1;
    if (Array.isArray(current.value)) {
      for (const nested of current.value) {
        stack.push({ value: nested, depth: current.depth + 1 });
      }
      continue;
    }
    if (!isObject(current.value)) continue;
    if (isObject(current.value.story)) {
      scopes.push(current.value);
    } else {
      const directPostId =
        current.value.post_id ??
        current.value.top_level_post_id ??
        (isObject(current.value.feedback)
          ? current.value.feedback.subscription_target_id
          : null);
      const directTimestamp =
        current.value.creation_time ?? current.value.publish_time;
      if (
        (typeof directPostId === "string" || typeof directPostId === "number") &&
        timestampValue(directTimestamp) !== null
      ) {
        scopes.push(current.value);
      }
    }
    for (const nested of Object.values(current.value)) {
      if (Array.isArray(nested) || isObject(nested)) {
        stack.push({ value: nested, depth: current.depth + 1 });
      }
    }
  }
  return scopes;
}

function matchesSource(
  signals: GraphqlSignals,
  sourceUrl: string,
  url: string | null,
): boolean {
  const expected = sourceIdentities(sourceUrl);
  const observed = new Set(signals.ownerIdentities);
  if (url) {
    for (const identity of identitiesFromUrl(url)) observed.add(identity);
  }
  for (const identity of observed) {
    if (expected.has(normalizedIdentity(identity))) return true;
  }
  return false;
}

function sourcePostUrl(sourceUrl: string, postLocator: string): string {
  const parsed = new URL(sourceUrl);
  parsed.protocol = "https:";
  parsed.hostname = "www.facebook.com";
  parsed.search = "";
  parsed.hash = "";
  parsed.pathname = `/${pageIdentity(sourceUrl)}/posts/${postLocator}`;
  return parsed.toString().replace(/\/$/, "");
}

function urlMatchesSource(url: string, sourceUrl: string): boolean {
  const expected = sourceIdentities(sourceUrl);
  for (const identity of identitiesFromUrl(url)) {
    if (expected.has(normalizedIdentity(identity))) return true;
  }
  return false;
}

function postFromSignals(
  signals: GraphqlSignals,
  sourceUrl: string,
): GraphqlPost | null {
  const urls = [...signals.urls];
  const matchingUrl =
    urls.find((url) => matchesSource(signals, sourceUrl, url)) ?? null;
  const externalPostId = preferredExternalPostId(
    ...signals.postIds,
    matchingUrl ? postLocator(matchingUrl) : null,
  );
  if (!matchesSource(signals, sourceUrl, matchingUrl)) return null;
  const locator = matchingUrl ? postLocator(matchingUrl) : "";
  const url =
    matchingUrl && urlMatchesSource(matchingUrl, sourceUrl)
      ? matchingUrl
      : locator
        ? sourcePostUrl(sourceUrl, locator)
        : externalPostId
          ? sourcePostUrl(sourceUrl, externalPostId)
          : null;
  if (!url) return null;
  const timestamp =
    signals.timestamps.size > 0 ? Math.max(...signals.timestamps) : null;
  const text =
    [...signals.texts].sort((left, right) => right.length - left.length)[0] ?? null;
  return {
    url,
    text,
    publishedAt:
      timestamp === null ? null : new Date(timestamp * 1_000).toISOString(),
    externalPostId,
  };
}

function mergeGraphqlPosts(posts: GraphqlPost[]): GraphqlPost[] {
  const merged = new Map<string, GraphqlPost>();
  for (const post of posts) {
    const key = post.externalPostId ?? postLocator(post.url) ?? post.url;
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, post);
      continue;
    }
    const existingTime = existing.publishedAt
      ? Date.parse(existing.publishedAt)
      : Number.NEGATIVE_INFINITY;
    const postTime = post.publishedAt
      ? Date.parse(post.publishedAt)
      : Number.NEGATIVE_INFINITY;
    merged.set(key, {
      url: postTime >= existingTime ? post.url : existing.url,
      text:
        (post.text?.length ?? 0) > (existing.text?.length ?? 0)
          ? post.text
          : existing.text,
      publishedAt:
        postTime >= existingTime ? post.publishedAt : existing.publishedAt,
      externalPostId: preferredExternalPostId(
        existing.externalPostId,
        post.externalPostId,
      ),
    });
  }
  return [...merged.values()].sort((left, right) => {
    const leftTime = left.publishedAt
      ? Date.parse(left.publishedAt)
      : Number.NEGATIVE_INFINITY;
    const rightTime = right.publishedAt
      ? Date.parse(right.publishedAt)
      : Number.NEGATIVE_INFINITY;
    return rightTime - leftTime;
  });
}

function parseGraphqlRoots(raw: string): unknown[] {
  const roots: unknown[] = [];
  for (const line of raw.split(/\r?\n/)) {
    const candidate = line.trim().replace(/^for\s*\(;;\);\s*/, "");
    if (!candidate) continue;
    try {
      roots.push(JSON.parse(candidate));
    } catch {
      continue;
    }
  }
  return roots;
}

function serializeGraphqlRoot(root: unknown): string | null {
  try {
    return JSON.stringify(root).replaceAll("<", "\\u003c");
  } catch {
    return null;
  }
}

export function inspectGraphqlBody(
  raw: string,
  sourceUrl: string,
): GraphqlInspection {
  const payloads: string[] = [];
  const posts: GraphqlPost[] = [];
  for (const root of parseGraphqlRoots(raw)) {
    const serialized = serializeGraphqlRoot(root);
    if (serialized) payloads.push(serialized);
    for (const scope of candidateScopes(root)) {
      const post = postFromSignals(collectGraphqlSignals(scope), sourceUrl);
      if (post) posts.push(post);
    }
  }
  return { payloads, posts: mergeGraphqlPosts(posts) };
}

export function appendGraphqlSnapshot(
  html: string,
  payloads: string[],
  posts: GraphqlPost[],
  sourceUrl: string,
): string {
  const numericPageIdentity = verifiedNumericPageIdentity(sourceUrl);
  const normalizedPosts = posts
    .filter(
      (post) =>
        post.externalPostId !== null &&
        post.publishedAt !== null &&
        post.text !== null,
    )
    .map((post) => ({
      post_id: post.externalPostId,
      creation_time: Math.floor(Date.parse(post.publishedAt as string) / 1_000),
      message: { text: post.text },
      url:
        numericPageIdentity && /^\d{8,}$/.test(post.externalPostId as string)
          ? `https://www.facebook.com/${numericPageIdentity}/posts/${post.externalPostId}`
          : post.url,
    }));
  const normalizedPayload =
    normalizedPosts.length > 0
      ? serializeGraphqlRoot({ posts: normalizedPosts })
      : null;
  const normalizedScript =
    normalizedPayload &&
    Buffer.byteLength(normalizedPayload, "utf8") <= maxNormalizedGraphqlBytes
      ? `<script type="application/json" data-uth-source="graphql-normalized">${normalizedPayload}</script>`
      : "";
  const scripts: string[] = [];
  let scriptBytes = 0;
  if (normalizedScript) {
    scripts.push(normalizedScript);
    scriptBytes += Buffer.byteLength(normalizedScript, "utf8");
  }
  for (const payload of payloads) {
    const script = `<script type="application/json" data-uth-source="graphql">${payload}</script>`;
    const bytes = Buffer.byteLength(script, "utf8");
    if (scriptBytes + bytes > maxGraphqlSnapshotBytes) continue;
    scripts.push(script);
    scriptBytes += bytes;
  }
  return scripts.length === 0 ? html : `${html}${scripts.join("")}`;
}

class GraphqlCapture {
  private payloads: string[] = [];
  private payloadSet = new Set<string>();
  private posts: GraphqlPost[] = [];
  private totalBytes = 0;

  async collect(response: Response, sourceUrl: string): Promise<void> {
    if (
      !response.url().includes("/api/graphql/") ||
      response.status() < 200 ||
      response.status() >= 300
    ) {
      return;
    }
    try {
      const contentLength = Number(response.headers()["content-length"] ?? "0");
      if (contentLength > maxGraphqlResponseBytes) return;
      const body = await response.text();
      if (Buffer.byteLength(body, "utf8") > maxGraphqlResponseBytes) return;
      const inspection = inspectGraphqlBody(body, sourceUrl);
      this.posts = mergeGraphqlPosts([
        ...this.posts,
        ...inspection.posts,
      ]).slice(0, maxCapturedGraphqlPosts);
      for (const payload of inspection.payloads) {
        if (
          this.payloads.length >= maxGraphqlPayloads ||
          this.payloadSet.has(payload)
        ) {
          continue;
        }
        const payloadBytes = Buffer.byteLength(payload, "utf8");
        if (
          this.totalBytes + payloadBytes >
          maxGraphqlSnapshotBytes - maxNormalizedGraphqlBytes
        ) {
          continue;
        }
        this.payloads.push(payload);
        this.payloadSet.add(payload);
        this.totalBytes += payloadBytes;
      }
    } catch {
      return;
    }
  }

  latestComplete(): GraphqlPost | null {
    return this.completePosts()[0] ?? null;
  }

  completePosts(): GraphqlPost[] {
    return this.posts.filter(
      (post) =>
        post.publishedAt !== null &&
        post.externalPostId !== null &&
        post.text !== null,
    );
  }

  matchingComplete(postUrl: string): GraphqlPost | null {
    const locator = postLocator(postUrl);
    if (!locator) return null;
    return (
      this.completePosts().find(
        (post) =>
          postLocator(post.url) === locator ||
          post.externalPostId === locator,
      ) ?? null
    );
  }

  appendPayloads(html: string, sourceUrl: string): string {
    return appendGraphqlSnapshot(html, this.payloads, this.posts, sourceUrl);
  }
}

async function settleGraphql(pendingGraphql: Set<Promise<void>>): Promise<void> {
  await Promise.allSettled([...pendingGraphql]);
}

async function dismissLoginOverlay(
  page: import("playwright-core").Page,
): Promise<LoginOverlayActivity> {
  if (isFacebookLoginRoute(page.url())) {
    return { detected: false, dismissed: false };
  }
  const closeButton = page
    .locator(
      '[role="dialog"] [aria-label="Đóng"], [role="dialog"] [aria-label="Close"]',
    )
    .filter({ visible: true })
    .first();
  if ((await closeButton.count()) > 0) {
    const clicked = await closeButton
      .click({ timeout: 2_000 })
      .then(() => true)
      .catch(() => false);
    if (clicked) {
      await page.waitForTimeout(250);
      return { detected: true, dismissed: true };
    }
  }
  const visibleDialog = page.locator('[role="dialog"]:visible').first();
  if ((await visibleDialog.count()) === 0) {
    return { detected: false, dismissed: false };
  }
  await page.keyboard.press("Escape").catch(() => undefined);
  await page.waitForTimeout(250);
  return {
    detected: true,
    dismissed: (await page.locator('[role="dialog"]:visible').count()) === 0,
  };
}

async function sweepHistory(
  page: import("playwright-core").Page,
  graphqlCapture: GraphqlCapture,
  pendingGraphql: Set<Promise<void>>,
): Promise<void> {
  let previousCount = graphqlCapture.completePosts().length;
  for (let pass = 0; pass < historySweepPasses; pass += 1) {
    await page.evaluate(() => {
      const height = document.body?.scrollHeight ?? document.documentElement?.scrollHeight ?? 0;
      window.scrollTo(0, height);
    });
    await page.waitForTimeout(historySweepPauseMs);
    await settleGraphql(pendingGraphql);
    const count = graphqlCapture.completePosts().length;
    if (count >= historySweepTarget) return;
    if (count === previousCount && pass >= 2) {
      await page.waitForTimeout(historySweepPauseMs);
    }
    previousCount = count;
  }
}

async function latestPost(
  page: import("playwright-core").Page,
  sourceUrl: string,
): Promise<DiscoveredPost | null> {
  const identity = pageIdentity(sourceUrl).toLowerCase();
  return page.evaluate((expectedIdentity) => {
    const candidates = [...document.querySelectorAll<HTMLAnchorElement>("a[href]")]
      .map((anchor) => ({
        href: anchor.href,
        label: (anchor.getAttribute("aria-label") ?? anchor.innerText).trim(),
      }))
      .filter((candidate) => {
        try {
          const url = new URL(candidate.href);
          return (
            url.hostname.endsWith("facebook.com") &&
            (url.pathname === "/permalink.php" ||
              /\/(posts|videos|reel)\//.test(url.pathname)) &&
            !url.searchParams.has("comment_id")
          );
        } catch {
          return false;
        }
      });
    const exact = candidates.find((candidate) => {
      const url = new URL(candidate.href);
      return (
        url.searchParams.get("id")?.toLowerCase() === expectedIdentity ||
        url.pathname
          .split("/")
          .filter(Boolean)
          .some((segment) => segment.toLowerCase() === expectedIdentity)
      );
    });
    const selected = exact ?? candidates[0];
    if (!selected) return null;
    const anchor = [...document.querySelectorAll<HTMLAnchorElement>("a[href]")].find(
      (candidate) => candidate.href === selected.href,
    );
    const container = anchor?.closest<HTMLElement>(
      '[role="article"], .userContentWrapper',
    );
    const articleText =
      container
        ?.querySelector<HTMLElement>(
          '[data-ad-preview="message"], [data-ad-comet-preview="message"], [data-testid="post_message"]',
        )
        ?.innerText.trim() ??
      container?.innerText.trim() ??
      "";
    return {
      url: selected.href,
      text: articleText || null,
      publishedAt: null,
      externalPostId: null,
      origin: "dom" as const,
    };
  }, identity);
}

function postLocator(postUrl: string): string {
  const parsed = new URL(postUrl);
  const storyId = parsed.searchParams.get("story_fbid");
  if (storyId) return storyId;
  const segments = parsed.pathname.split("/").filter(Boolean);
  const marker = segments.findIndex((segment) =>
    ["posts", "videos", "reel"].includes(segment),
  );
  return marker >= 0 ? (segments[marker + 1] ?? "") : "";
}

type PostMetadata = {
  publishedAt: string | null;
  externalPostId: string | null;
};

async function postMetadata(
  page: import("playwright-core").Page,
  postUrl: string,
): Promise<PostMetadata> {
  const locator = postLocator(postUrl);
  if (!locator) return { publishedAt: null, externalPostId: null };
  const metadata = await page.evaluate((expectedLocator) => {
    const timestamps: number[] = [];
    let deepestPostId: string | null = null;
    let deepestPostIdDepth = -1;
    const walk = (
      value: unknown,
      depth: number,
    ): { containsLocator: boolean; postIds: Set<string> } => {
      if (Array.isArray(value)) {
        let containsLocator = false;
        const postIds = new Set<string>();
        for (const item of value) {
          const child = walk(item, depth + 1);
          containsLocator ||= child.containsLocator;
          for (const postId of child.postIds) postIds.add(postId);
        }
        return { containsLocator, postIds };
      }
      if (typeof value !== "object" || value === null) {
        return {
          containsLocator:
            typeof value === "string" && value.includes(expectedLocator),
          postIds: new Set<string>(),
        };
      }
      const node = value as Record<string, unknown>;
      let containsLocator = false;
      const postIds = new Set<string>();
      const creationTime =
        typeof node.creation_time === "number"
          ? node.creation_time
          : typeof node.publish_time === "number"
            ? node.publish_time
            : null;
      const feedback =
        typeof node.feedback === "object" && node.feedback !== null
          ? (node.feedback as Record<string, unknown>)
          : null;
      const references = [
        node.url,
        node.permalink_url,
        node.videoId,
        node.post_id,
        node.top_level_post_id,
        feedback?.subscription_target_id,
        node.id,
      ]
        .filter(
          (item): item is string | number =>
            typeof item === "string" || typeof item === "number",
        )
        .map(String);
      if (
        creationTime !== null &&
        references.some((reference) => reference.includes(expectedLocator))
      ) {
        timestamps.push(creationTime);
      }
      for (const reference of references) {
        containsLocator ||= reference.includes(expectedLocator);
      }
      for (const postId of [
        node.post_id,
        node.top_level_post_id,
        feedback?.subscription_target_id,
      ]) {
        const candidate =
          typeof postId === "string" || typeof postId === "number"
            ? String(postId)
            : "";
        if (/^\d{8,}$/.test(candidate)) postIds.add(candidate);
      }
      for (const nested of Object.values(node)) {
        const child = walk(nested, depth + 1);
        containsLocator ||= child.containsLocator;
        for (const postId of child.postIds) postIds.add(postId);
      }
      if (containsLocator && postIds.size > 0 && depth > deepestPostIdDepth) {
        deepestPostId = [...postIds][0] ?? null;
        deepestPostIdDepth = depth;
      }
      return { containsLocator, postIds };
    };
    for (const script of document.querySelectorAll<HTMLScriptElement>(
      'script[type="application/json"]',
    )) {
      const raw = script.textContent ?? "";
      if (!raw.includes(expectedLocator)) continue;
      try {
        walk(JSON.parse(raw), 0);
      } catch {
        continue;
      }
    }
    const matchingAnchor = [...document.querySelectorAll<HTMLAnchorElement>("a[href]")]
      .find((anchor) => anchor.href.includes(expectedLocator));
    const container = matchingAnchor?.closest<HTMLElement>(
      '[role="article"], .userContentWrapper',
    );
    for (const element of container?.querySelectorAll<HTMLElement>("[data-utime]") ?? []) {
      const timestamp = Number.parseInt(element.dataset.utime ?? "", 10);
      if (Number.isSafeInteger(timestamp) && timestamp > 0) timestamps.push(timestamp);
    }
    return {
      timestamp: timestamps.length > 0 ? Math.max(...timestamps) : null,
      externalPostId: deepestPostId ?? (timestamps.length > 0 ? expectedLocator : null),
    };
  }, locator);
  return {
    publishedAt:
      metadata.timestamp === null
        ? null
        : new Date(metadata.timestamp * 1_000).toISOString(),
    externalPostId: metadata.externalPostId,
  };
}

async function hydrateDomPost(
  page: import("playwright-core").Page,
  post: DiscoveredPost,
  graphqlCapture: GraphqlCapture,
  pendingGraphql: Set<Promise<void>>,
  dismissAndRecordLoginOverlay: () => Promise<void>,
): Promise<DiscoveredPost> {
  const sourceMetadata = await postMetadata(page, post.url);
  await page.goto(post.url, {
    waitUntil: "domcontentloaded",
    timeout: 45_000,
  });
  await page.waitForTimeout(2_000);
  const message = await page
    .locator(
      '[data-ad-preview="message"], [data-ad-comet-preview="message"]',
    )
    .first()
    .innerText()
    .catch(() => "");
  const permalinkMetadata = await postMetadata(page, post.url);
  await dismissAndRecordLoginOverlay();
  await settleGraphql(pendingGraphql);
  const capturedPost = graphqlCapture.matchingComplete(post.url);
  return {
    url: post.url,
    text: capturedPost?.text ?? (message.trim() || post.text),
    publishedAt:
      sourceMetadata.publishedAt ??
      permalinkMetadata.publishedAt ??
      capturedPost?.publishedAt ??
      null,
    externalPostId: preferredExternalPostId(
      permalinkMetadata.externalPostId,
      sourceMetadata.externalPostId,
      capturedPost?.externalPostId,
      post.externalPostId,
      postLocator(post.url),
    ),
    origin: "dom",
  };
}

async function responseRemoteFamily(
  response: Response | null,
): Promise<BrowserRemoteFamily> {
  if (!response) return "unknown";
  const serverAddress = await response.serverAddr().catch(() => null);
  return remoteAddressFamily(serverAddress?.ipAddress ?? null);
}

async function captureSnapshot(
  sourceUrl: string,
  executablePath: string,
  network: BrowserNetworkSelection,
): Promise<BrowserSnapshot> {
  const startedAt = Date.now();
  const browser = await chromium.launch({
    executablePath,
    headless: true,
    args: [
      ...chromiumCrashReportingArgs,
      "--disable-background-networking",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-extensions",
      "--disable-sync",
      "--no-first-run",
      ...chromiumNetworkArgs(network.ipv4Address),
    ],
  });
  try {
    const context = await browser.newContext({
      userAgent: searchUserAgent,
      locale: "vi-VN",
      viewport: { width: 1280, height: 900 },
    });
    await context.route("**/*", async (route) => {
      const resourceType = route.request().resourceType();
      if (["font", "image", "media"].includes(resourceType)) {
        await route.abort();
        return;
      }
      await route.continue();
    });
    const page = await context.newPage();
    const graphqlCapture = new GraphqlCapture();
    const pendingGraphql = new Set<Promise<void>>();
    page.on("response", (graphqlResponse) => {
      const task = graphqlCapture
        .collect(graphqlResponse, sourceUrl)
        .finally(() => pendingGraphql.delete(task));
      pendingGraphql.add(task);
    });
    let loginOverlayDetected = false;
    let loginOverlayDismissed = false;
    const dismissAndRecordLoginOverlay = async (): Promise<void> => {
      const activity = await dismissLoginOverlay(page);
      loginOverlayDetected ||= activity.detected;
      loginOverlayDismissed ||= activity.dismissed;
    };
    const loadPresentation = async (
      url: string,
      initialWaitMs: number,
    ): Promise<Response | null> => {
      const loadedResponse = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: 20_000,
      });
      await page.waitForTimeout(initialWaitMs);
      await dismissAndRecordLoginOverlay();
      await settleGraphql(pendingGraphql);
      await sweepHistory(page, graphqlCapture, pendingGraphql);
      return loadedResponse;
    };
    let response =
      network.effectiveMode === "ipv4"
        ? await loadPresentation(sourceUrl, 2_000)
        : await loadPresentation(pagePluginUrl(sourceUrl), 3_000);
    if (
      network.effectiveMode !== "ipv4" &&
      graphqlCapture.completePosts().length < historySweepTarget
    ) {
      response = await loadPresentation(sourceUrl, 2_000);
    }
    const latestGraphqlPost = graphqlCapture.latestComplete();
    const graphqlPost: DiscoveredPost | null = latestGraphqlPost
      ? { ...latestGraphqlPost, origin: "graphql" }
      : null;
    const sourceFinalUrl = page.url();
    const sourceHtml = await page.content();
    const sourceIsPagePlugin =
      new URL(sourceFinalUrl).pathname === "/plugins/page.php";
    const latestDomPost = sourceIsPagePlugin
      ? null
      : await latestPost(page, sourceUrl);
    const independentDomPost =
      latestDomPost !== null &&
      (graphqlPost === null || !sameDiscoveredPost(latestDomPost, graphqlPost));
    const hydratedDomPost = independentDomPost
      ? await hydrateDomPost(
          page,
          latestDomPost,
          graphqlCapture,
          pendingGraphql,
          dismissAndRecordLoginOverlay,
        ).catch(() => latestDomPost)
      : latestDomPost;
    const newestDomPostUnresolved =
      independentDomPost &&
      hydratedDomPost !== null &&
      hydratedDomPost.publishedAt === null;
    const discoveredPost = selectNewestDiscoveredPost(
      graphqlPost,
      hydratedDomPost,
    );
    await settleGraphql(pendingGraphql);
    const networkRemoteFamily = await responseRemoteFamily(response);
    const snapshot: BrowserSnapshot = {
      schema_version: "facebook-browser-snapshot.v1",
      source_url: sourceUrl,
      final_url: sourceFinalUrl,
      fetched_at: new Date().toISOString(),
      status: response?.status() ?? null,
      latency_ms: Date.now() - startedAt,
      discovered_post_url: discoveredPost?.url ?? null,
      discovered_published_at: discoveredPost?.publishedAt ?? null,
      discovered_external_post_id: discoveredPost?.externalPostId ?? null,
      discovered_text: discoveredPost?.text ?? null,
      discovered_post_origin: discoveredPost?.origin ?? null,
      newest_dom_post_unresolved: newestDomPostUnresolved,
      network_requested_mode: network.requestedMode,
      network_effective_mode: network.effectiveMode,
      network_remote_family: networkRemoteFamily,
      network_fallback_reason: network.fallbackReason,
      login_overlay_detected: loginOverlayDetected,
      login_overlay_dismissed: loginOverlayDismissed,
      login_route_detected: isFacebookLoginRoute(sourceFinalUrl),
      html: graphqlCapture.appendPayloads(sourceHtml, sourceUrl),
    };
    await context.close();
    return snapshot;
  } finally {
    await browser.close();
  }
}

async function main(): Promise<void> {
  const sourceUrl = process.argv[2];
  const executablePath =
    process.env.CHROME_PATH ??
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  if (!sourceUrl) {
    throw new Error("usage: post.ts <facebook-page-url>");
  }
  const requestedMode = parseBrowserNetworkMode(
    process.env.FACEBOOK_BROWSER_NETWORK_MODE,
  );
  const selectedNetwork = await selectBrowserNetwork(requestedMode);
  let snapshot: BrowserSnapshot;
  try {
    snapshot = await captureSnapshot(sourceUrl, executablePath, selectedNetwork);
  } catch (error) {
    if (selectedNetwork.effectiveMode !== "ipv4") throw error;
    snapshot = await captureSnapshot(sourceUrl, executablePath, {
      requestedMode,
      effectiveMode: "system",
      ipv4Address: null,
      fallbackReason: `ipv4_runtime_failed:${boundedError(error)}`,
    });
  }
  await new Promise<void>((resolve, reject) => {
    process.stdout.write(JSON.stringify(snapshot), (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href
) {
  await main();
}
