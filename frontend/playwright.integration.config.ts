import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "research-invalidation-real.e2e.ts",
  outputDir: "test-results/research-invalidation-real",
  reporter: "list",
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 90_000,
  use: {
    browserName: "chromium",
    ignoreHTTPSErrors: true
  }
});
