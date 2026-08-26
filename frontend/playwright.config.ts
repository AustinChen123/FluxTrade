import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.e2e.ts",
  outputDir: "test-results",
  reporter: [
    ["list"],
    ["json", { outputFile: "playwright-report/results.json" }]
  ],
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 60_000,
  webServer: [
    {
      command: "npm run dev -- --host 127.0.0.1 --port 4173 --strictPort",
      url: "http://127.0.0.1:4173",
      timeout: 60_000,
      reuseExistingServer: false
    },
    {
      command:
        "npm run preview -- --host 127.0.0.1 --port 4174 --strictPort",
      url: "http://127.0.0.1:4174",
      timeout: 60_000,
      reuseExistingServer: false
    }
  ],
  projects: [
    {
      name: "desktop-1440x900",
      use: { viewport: { width: 1440, height: 900 } }
    },
    {
      name: "tablet-1024x768",
      use: { viewport: { width: 1024, height: 768 } }
    },
    {
      name: "mobile-390x844",
      use: { viewport: { width: 390, height: 844 } }
    }
  ]
});
