import { test, expect } from '@playwright/test';

test('real demo completes a shipment through the browser and reset clears the run', async ({ page, baseURL }) => {
  test.skip(process.env.DEMO_TEST_ALLOW_RESET !== '1', 'Explicit isolated-demo reset authorization is required.');
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(baseURL);
  await expect(page.getByRole('button', { name: 'Reset scenario', exact: true })).toBeEnabled();
  await page.getByRole('button', { name: 'Reset scenario', exact: true }).click();
  await expect(page.getByText('Ready when you are', { exact: true })).toBeVisible();
  await page.getByRole('combobox', { name: 'Scenario', exact: true }).selectOption('normal');
  await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption('4');
  await page.getByRole('button', { name: 'Play scenario', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Shipment journey' })).toContainText('demo-shipment-001', { timeout: 30000 });
  await page.getByRole('button', { name: 'Pause scenario', exact: true }).click();
  await expect(page.getByText('Paused · inspect any device', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: /^Device .+, / }).first().click();
  await expect(page.getByRole('dialog', { name: 'Device details' })).toBeVisible();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'Resume scenario', exact: true }).click();
  await expect(page.getByText('Scenario complete', { exact: true })).toBeVisible({ timeout: 90000 });
  await expect(page.getByRole('region', { name: 'Shipment journey' })).toContainText('Delivered');
  await page.getByRole('button', { name: 'Reset scenario', exact: true }).click();
  await expect(page.getByText('Ready when you are', { exact: true })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Shipment journey' })).not.toContainText('demo-shipment-001');
  expect(errors).toEqual([]);
});
