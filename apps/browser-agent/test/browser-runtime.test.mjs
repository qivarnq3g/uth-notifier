import assert from "node:assert/strict";
import test from "node:test";

import { chromiumLaunchOptions } from "../src/browser-runtime.ts";

test("uses Playwright managed Chromium unless an explicit path is configured", () => {
  assert.deepEqual(chromiumLaunchOptions(["--disable-breakpad"], undefined), {
    headless: true,
    args: ["--disable-breakpad"],
  });
  assert.deepEqual(chromiumLaunchOptions([], "   "), {
    headless: true,
    args: [],
  });
  assert.deepEqual(chromiumLaunchOptions([], " /opt/chromium "), {
    executablePath: "/opt/chromium",
    headless: true,
    args: [],
  });
});
