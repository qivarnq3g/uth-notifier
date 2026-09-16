import type { LaunchOptions } from "playwright-core";

export function chromiumLaunchOptions(
  args: readonly string[],
  chromePath: string | undefined = process.env.CHROME_PATH,
): LaunchOptions {
  const executablePath = chromePath?.trim();
  return {
    ...(executablePath ? { executablePath } : {}),
    headless: true,
    args: [...args],
  };
}
