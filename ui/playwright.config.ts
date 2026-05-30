import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./smoke",
  timeout: 30_000,
  expect: {
    timeout: 5_000
  },
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:5174",
    trace: "on-first-retry"
  },
  webServer: [
    {
      command: "npm run dev:smoke",
      url: "http://127.0.0.1:5174/health",
      reuseExistingServer: !process.env.CI,
      timeout: 60_000
    },
    {
      command: "npm run storybook:smoke",
      url: "http://127.0.0.1:6101/iframe.html?id=components-appshell--dashboard&viewMode=story",
      reuseExistingServer: !process.env.CI,
      timeout: 90_000
    }
  ],
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ]
});
