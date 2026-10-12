import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "spider-orders-real.integration.ts",
  outputDir: "test-results/spider-orders-integration",
  reporter: "list",
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 60_000,
  use: {
    ignoreHTTPSErrors: true,
    viewport: { width: 390, height: 844 }
  }
});
