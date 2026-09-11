import { test, expect } from '@playwright/test';
import { createHash } from 'node:crypto';

function watchFailures(page, allowedOrigin) {
  const failures = [];
  page.on('pageerror', error => failures.push(error.message));
  page.on('request', request => {
    const url = new URL(request.url());
    if (/^https?:$/.test(url.protocol) && (url.origin !== allowedOrigin || /\/api\//.test(url.pathname))) {
      failures.push(`Unexpected backend/external request: ${url.origin}${url.pathname}`);
    }
  });
  page.on('response', response => { if (response.status() >= 400) failures.push(`HTTP ${response.status()}: ${new URL(response.url()).pathname}`); });
  return failures;
}

for (const scenario of ['normal', 'congestion', 'crane-fault']) {
  test(`${scenario}: recorded journey plays, pauses, completes and resets without a backend`, async ({ page, baseURL }) => {
    const failures = watchFailures(page, new URL(baseURL).origin);
    await page.clock.install();
    await page.goto(baseURL);
    await expect(page.getByRole('button', { name: 'Play scenario', exact: true })).toBeEnabled();
    await page.getByRole('combobox', { name: 'Scenario', exact: true }).selectOption(scenario);
    await expect(page.getByRole('button', { name: 'Play scenario', exact: true })).toBeEnabled();
    await expect(page.getByText(/Recorded simulation · captured/)).toBeVisible();
    await page.getByRole('button', { name: 'Play scenario', exact: true }).click();
    await page.clock.runFor(4000);
    const progress = page.getByRole('progressbar', { name: 'Scenario playback progress' });
    await expect.poll(async () => Number(await progress.getAttribute('value'))).toBeGreaterThan(0);
    await page.getByRole('button', { name: 'Pause scenario', exact: true }).click();
    const paused = await progress.getAttribute('value');
    await page.clock.fastForward(10000);
    expect(await progress.getAttribute('value')).toBe(paused);
    const device = page.getByRole('button', { name: /^Device .+, / }).first();
    await device.click();
    await expect(page.getByRole('dialog', { name: 'Device details' })).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption('4');
    await page.getByRole('button', { name: 'Resume scenario', exact: true }).click();
    await page.clock.fastForward(360000);
    await expect(page.getByText('Scenario complete', { exact: true })).toBeVisible();
    await expect(page.getByRole('region', { name: 'Shipment journey' })).toContainText('Delivered');
    await page.getByRole('button', { name: 'Reset scenario', exact: true }).click();
    await expect(progress).toHaveAttribute('value', '0');
    await expect(page.getByText('Ready when you are', { exact: true })).toBeVisible();
    expect(failures).toEqual([]);
  });
}

test('playback is independent between visitors and usable on a narrow keyboard viewport', async ({ browser, baseURL }) => {
  const a = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: 'reduce' });
  const b = await browser.newContext();
  try {
    const first = await a.newPage(); const second = await b.newPage();
    await first.clock.install(); await second.clock.install();
    await Promise.all([first.goto(baseURL), second.goto(baseURL)]);
    await first.getByRole('button', { name: 'Play scenario', exact: true }).click();
    await first.clock.runFor(3000);
    await expect(second.getByRole('progressbar', { name: 'Scenario playback progress' })).toHaveAttribute('value', '0');
    await second.getByRole('button', { name: 'Reset scenario', exact: true }).click();
    await expect(first.getByRole('button', { name: 'Pause scenario', exact: true })).toBeVisible();
    expect(await first.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 2)).toBe(true);
    const device = first.getByRole('button', { name: /^Device .+, / }).first();
    await device.focus(); await first.keyboard.press('Enter');
    await expect(first.getByRole('dialog', { name: 'Device details' })).toBeVisible();
    await first.keyboard.press('Escape');
    await expect(device).toBeFocused();
  } finally { await a.close(); await b.close(); }
});

test('project resources expose the project report with a verified PDF and an on-demand viewer', async ({ page, request, baseURL }) => {
  const metadataResponse = await request.get(new URL('demo-build.json', baseURL).href);
  expect(metadataResponse.ok()).toBe(true);
  const metadata = await metadataResponse.json();
  const failures = watchFailures(page, new URL(baseURL).origin);
  const reportRequests = [];
  page.on('request', item => { if (new URL(item.url()).pathname.includes('/reports/')) reportRequests.push(item.url()); });
  await page.goto(baseURL);
  await page.getByRole('link', { name: 'Report & project info', exact: true }).click();
  const resources = page.getByRole('region', { name: 'About this project' });
  await expect(resources).toBeVisible();
  await expect(resources.getByRole('link', { name: /Engineering case study/ })).toHaveAttribute('href', 'https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/CASE_STUDY.md');
  await expect(resources.locator('iframe')).toHaveCount(0);
  expect(reportRequests).toEqual([]);
  if (metadata.report) {
    const url = new URL(`reports/${metadata.report.file}`, baseURL).href;
    const response = await request.get(url);
    expect(response.ok()).toBe(true);
    expect(response.headers()['content-type']).toContain('application/pdf');
    expect(createHash('sha256').update(await response.body()).digest('hex')).toBe(metadata.report.sha256);
    await expect(resources.getByRole('link', { name: 'Download PDF' })).toHaveAttribute('download', '');
    await resources.getByRole('button', { name: 'View report', exact: true }).click();
    await expect(resources.getByTitle(`${metadata.report.title} PDF`)).toBeVisible();
    await resources.getByRole('button', { name: 'Close report viewer', exact: true }).click();
    await expect(resources.locator('iframe')).toHaveCount(0);
  } else {
    await expect(resources.getByRole('button', { name: 'View report', exact: true })).toHaveCount(0);
  }
  expect(failures).toEqual([]);
});
