import { defineConfig } from '@playwright/test';

const live = process.env.DEMO_E2E_MODE === 'live';
export default defineConfig({
  testDir: './tests/browser',
  testMatch: live ? 'live.spec.mjs' : 'replay.spec.mjs',
  fullyParallel: false,
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  timeout: 120000,
  expect: { timeout: 15000 },
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: process.env.DEMO_E2E_URL || (live ? 'http://127.0.0.1:3000' : 'http://127.0.0.1:4173/HarbourSense/'),
    browserName: 'chromium', viewport: { width: 1440, height: 1000 },
    trace: 'retain-on-failure', screenshot: 'only-on-failure', video: 'retain-on-failure',
  },
  webServer: live || process.env.DEMO_E2E_URL ? undefined : {
    command: 'node scripts/serve-demo.mjs', url: 'http://127.0.0.1:4173/HarbourSense/',
    reuseExistingServer: !process.env.CI, timeout: 20000,
  },
});
