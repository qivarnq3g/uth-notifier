import type { LaunchOptions } from "playwright-core";

export function chromiumLaunchOptions(
  args: readonly string[],
  chromePath: string | undefined,
): LaunchOptions {
  const executablePath = chromePath?.trim();
  return {
    ...(executablePath ? { executablePath } : {}),
    headless: true,
    args: [...args],
  };
}
