const path = require('node:path');

// NOTE(garrett): Pin browser downloads next to the website package so `playwright install`
// and `playwright test` always agree (some environments override the default global cache path).
process.env.PLAYWRIGHT_BROWSERS_PATH = path.join(
  __dirname,
  'node_modules',
  '.cache',
  'ms-playwright',
);

const { defineConfig, devices } = require('@playwright/test');

const port = 4287;
const host = '127.0.0.1';

module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: true,
  workers: process.env.CI ? 2 : undefined,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'github' : [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: `http://${host}:${port}`,
    trace: 'on-first-retry',
  },
  webServer: {
    command: `npm run build && node scripts/serve-build.mjs`,
    url: `http://${host}:${port}`,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
