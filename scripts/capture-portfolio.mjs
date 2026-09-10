#!/usr/bin/env node
/** Capture the real, verified static demo. No fixtures, network mocks or visual overlays. */
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import { ROOT } from './demo.mjs';
import { validateReplayDirectory, normalizeBasePath } from './lib/demo-artifacts.mjs';
import { verifyBuild } from './verify-demo-build.mjs';

const BUILD = path.join(ROOT, 'dashboard', 'visualizer', 'build');
const OUTPUT = path.join(ROOT, 'docs', 'media');
const STAGING_ROOT = path.join(ROOT, 'artifacts', 'local');
const DEFAULT_URL = 'http://127.0.0.1:4173/HarbourSense/';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');

export function planWalkthrough(recording) {
  const durationMs = recording?.durationMs;
  if (!Number.isFinite(durationMs) || durationMs <= 0) throw new Error('Normal recording has no valid duration.');
  let speed = 1;
  if (durationMs < 45000 && durationMs / .5 <= 70000) speed = .5;
  while (durationMs / speed > 70000 && speed < 4) speed *= 2;
  const playbackMs = durationMs / speed;
  if (playbackMs < 30000 || playbackMs > 70000) {
    throw new Error('The complete normal recording cannot fit a useful 60–90 second walkthrough at an available playback speed.');
  }
  return { speed, durationMs, playbackMs, introMs: 3000, inspectMs: 7000,
    inspectAtMs: Math.min(durationMs * .24, 14000), timelineAtMs: durationMs * .74,
    targetMs: Math.max(65000, Math.min(83000, playbackMs + 16000)), maximumMs: 90000 };
}

function checkedUrl(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('Capture URL must be an unauthenticated HTTP(S) static-demo address without a query or fragment.');
  }
  if (!url.pathname.endsWith('/')) url.pathname += '/';
  return url;
}

async function getBytes(url) {
  const response = await fetch(url, { redirect: 'error', signal: AbortSignal.timeout(15000) });
  if (!response.ok) throw new Error(`Static preview returned HTTP ${response.status}: ${url.pathname}`);
  return Buffer.from(await response.arrayBuffer());
}

async function validatePreview(url) {
  await verifyBuild(BUILD);
  const source = await validateReplayDirectory(path.join(ROOT, 'dashboard', 'visualizer', 'public', 'replays'));
  const built = await validateReplayDirectory(path.join(BUILD, 'replays'));
  const metadata = JSON.parse(await getBytes(new URL('demo-build.json', url)));
  if (metadata.mode !== 'replay' || metadata.schemaVersion !== 1
      || url.pathname !== `${normalizeBasePath(metadata.basePath || '/')}/`) {
    throw new Error('The running preview does not identify the expected static replay build/subpath.');
  }
  const localMetadata = JSON.parse(await fs.readFile(path.join(BUILD, 'demo-build.json'), 'utf8'));
  if (metadata.builtAt !== localMetadata.builtAt) throw new Error('The running preview serves an older build.');
  const manifest = JSON.parse(await getBytes(new URL('replays/index.json', url)));
  for (const entry of built.manifest.scenarios) {
    const published = manifest.scenarios?.find(item => item.id === entry.id);
    const original = source.manifest.scenarios.find(item => item.id === entry.id);
    if (published?.sha256 !== entry.sha256 || original?.sha256 !== entry.sha256 || published?.file !== entry.file) {
      throw new Error(`${entry.id}: source, build and preview recording manifests differ.`);
    }
    const bytes = await getBytes(new URL(`replays/${entry.file}`, url));
    if (sha256(bytes) !== entry.sha256) throw new Error(`${entry.id}: served recording bytes do not match their verified hash.`);
  }
  return { ...built, metadata };
}

function watchPage(page, origin, failures) {
  page.on('pageerror', error => failures.push(`Browser error: ${error.message}`));
  page.on('console', message => { if (message.type() === 'error') failures.push(`Console error: ${message.text()}`); });
  page.on('request', request => {
    const url = new URL(request.url());
    if (!['http:', 'https:'].includes(url.protocol)) return;
    if (url.origin !== origin || /\/api(?:\/|$)/.test(url.pathname) || request.method() !== 'GET') {
      failures.push(`Unexpected request: ${request.method()} ${url.origin}${url.pathname}`);
    }
  });
  page.on('response', response => { if (response.status() >= 400) failures.push(`HTTP ${response.status()}: ${new URL(response.url()).pathname}`); });
  page.on('requestfailed', request => failures.push(`Request failed: ${new URL(request.url()).pathname}: ${request.failure()?.errorText}`));
}

function assertHealthy(failures) {
  if (failures.length) throw new Error([...new Set(failures)].join('\n'));
}

async function waitUntil(check, label, timeoutMs = 15000, failures = []) {
  const deadline = Date.now() + timeoutMs;
  do {
    assertHealthy(failures);
    if (await check()) return;
    await sleep(100);
  } while (Date.now() < deadline);
  throw new Error(`Timed out waiting for ${label}.`);
}

async function loadScenario(page, url, id, failures) {
  await page.goto(url.href, { waitUntil: 'networkidle', timeout: 20000 });
  const play = page.getByRole('button', { name: 'Play scenario', exact: true });
  await waitUntil(() => play.isEnabled(), 'the recording to load', 15000, failures);
  await page.getByRole('combobox', { name: 'Scenario', exact: true }).selectOption(id);
  await waitUntil(() => play.isEnabled(), `${id} to load`, 15000, failures);
  await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption('1');
  await page.getByText('Recorded simulation', { exact: true }).waitFor();
  assertHealthy(failures);
}

async function recordedTime(page, recording) {
  const progress = Number(await page.getByRole('progressbar', { name: 'Scenario playback progress' }).getAttribute('value'));
  return recording.durationMs * progress / 100;
}

async function hold(ms, failures) {
  const until = Date.now() + Math.max(0, ms);
  while (Date.now() < until) { assertHealthy(failures); await sleep(Math.min(250, until - Date.now())); }
}

async function stillAt(browser, url, recording, atMs, viewport, output, failures, deviceId) {
  const context = await browser.newContext({ viewport, deviceScaleFactor: 1, reducedMotion: 'reduce' });
  try {
    const page = await context.newPage();
    watchPage(page, url.origin, failures);
    await loadScenario(page, url, recording.scenario.id, failures);
    await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption('4');
    await page.getByRole('button', { name: 'Play scenario', exact: true }).click();
    await waitUntil(async () => await recordedTime(page, recording) >= atMs, 'the screenshot recording frame', recording.durationMs / 4 + 5000, failures);
    const pause = page.getByRole('button', { name: 'Pause scenario', exact: true });
    if (await pause.isVisible()) await pause.click();
    await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption('1');
    await page.getByRole('button', { name: 'Fit full port', exact: true }).click();
    // Let React Flow finish measuring and painting before saving the actual UI.
    await hold(600, failures);
    const mapFit = await page.locator('.dashboard-flow').evaluate(map => {
      const bounds = map.getBoundingClientRect();
      const ports = [...map.querySelectorAll('.react-flow__node-port')];
      const hidden = ports.filter(port => {
        const box = port.getBoundingClientRect();
        const style = getComputedStyle(port);
        return !(style.visibility !== 'hidden' && Number(style.opacity) > 0 && box.width > 0 && box.height > 0
          && box.left >= bounds.left - 1 && box.right <= bounds.right + 1
          && box.top >= bounds.top - 1 && box.bottom <= bounds.bottom + 1);
      });
      return { count: ports.length, hidden: hidden.map(port => ({ id: port.dataset.id, box: port.getBoundingClientRect().toJSON(), visibility: getComputedStyle(port).visibility })), bounds: bounds.toJSON() };
    });
    if (mapFit.count !== 25 || mapFit.hidden.length) {
      await page.screenshot({ path: output.replace('.png', '-failure.png'), fullPage: true });
      throw new Error(`The complete 25-location port map was not painted and fitted before capture: ${JSON.stringify(mapFit)}`);
    }
    if (await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1)) throw new Error('Horizontal overflow in portfolio screenshot.');
    if (deviceId) {
      await page.getByRole('button', { name: new RegExp(`^Device ${deviceId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}, `) }).click();
      await page.getByRole('dialog', { name: 'Device details' }).waitFor();
      await page.getByRole('heading', { name: 'Anomaly analysis', exact: true }).evaluate(heading => {
        const drawer = heading.closest('[role="dialog"]');
        drawer.scrollTop += heading.getBoundingClientRect().top - drawer.getBoundingClientRect().top - 540;
      });
    } else await page.evaluate(() => scrollTo(0, 0));
    await page.screenshot({ path: output, fullPage: !deviceId, animations: 'disabled' });
    assertHealthy(failures);
    return { scenarioId: recording.scenario.id, runId: recording.provenance.runId, recordedTimeMs: Math.round(await recordedTime(page, recording)), deviceId: deviceId || null, viewport };
  } finally { await context.close(); }
}

async function probeVideoDuration(browser, file) {
  const context = await browser.newContext();
  try {
    const page = await context.newPage();
    const bytes = await fs.readFile(file);
    const seconds = await page.evaluate(async base64 => {
      const video = document.createElement('video');
      const ready = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Video metadata did not load')), 15000);
        video.onloadedmetadata = () => { clearTimeout(timer); resolve(video.duration); };
        video.onerror = () => { clearTimeout(timer); reject(new Error('Captured video could not be decoded')); };
      });
      video.src = `data:video/webm;base64,${base64}`;
      return ready;
    }, bytes.toString('base64'));
    if (!Number.isFinite(seconds)) throw new Error('Captured video has no finite duration metadata.');
    return seconds;
  } finally { await context.close(); }
}

async function walkthrough(browser, url, normal, staging, failures) {
  const plan = planWalkthrough(normal);
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: 1,
    recordVideo: { dir: path.join(staging, 'raw-video'), size: { width: 1440, height: 1000 } } });
  let video;
  const chapters = [];
  const started = Date.now();
  const chapter = title => { chapters.push({ title, wallTimeMs: Date.now() - started }); console.log(`Capture: ${title}`); };
  try {
    const page = await context.newPage();
    video = page.video();
    watchPage(page, url.origin, failures);
    await loadScenario(page, url, 'normal', failures);
    chapter('Recorded simulation with synthetic scenario inputs');
    await hold(plan.introMs, failures);
    await page.getByRole('combobox', { name: 'Playback speed', exact: true }).selectOption(String(plan.speed));
    await page.getByRole('button', { name: 'Play scenario', exact: true }).click();
    chapter('Follow the normal shipment journey');
    await page.locator('#operations-map').scrollIntoViewIfNeeded();
    await page.getByRole('button', { name: 'Fit full port', exact: true }).click();
    await waitUntil(async () => await recordedTime(page, normal) >= plan.inspectAtMs, 'the inspection point', plan.playbackMs + 5000, failures);
    await page.getByRole('button', { name: 'Pause scenario', exact: true }).click();
    const pausedAt = await recordedTime(page, normal);
    chapter('Pause to inspect a crane and its captured telemetry');
    const crane = normal.initialSnapshot.edges.find(edge => edge.type === 'crane');
    if (!crane) throw new Error('Normal recording contains no crane to inspect.');
    await page.getByRole('button', { name: new RegExp(`^Device ${crane.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}, `) }).click();
    await hold(2000, failures);
    await page.getByRole('heading', { name: 'Sensor trends', exact: true }).evaluate(heading => heading.scrollIntoView({ block: 'start', behavior: 'smooth' }));
    await hold(plan.inspectMs - 2000, failures);
    if (await recordedTime(page, normal) !== pausedAt) throw new Error('Playback moved while paused during the walkthrough.');
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: 'Resume scenario', exact: true }).click();
    chapter('Resume coordinated port operations');
    await page.locator('#operations-map').scrollIntoViewIfNeeded();
    await waitUntil(async () => await recordedTime(page, normal) >= plan.timelineAtMs, 'the final shipment stages', plan.playbackMs + 5000, failures);
    chapter('Inspect the shipment lifecycle and event timeline');
    await page.getByRole('region', { name: 'Shipment journey', exact: true }).scrollIntoViewIfNeeded();
    await waitUntil(() => page.getByText('Scenario complete', { exact: true }).isVisible(), 'recorded scenario completion', plan.playbackMs + 5000, failures);
    const journey = await page.getByRole('region', { name: 'Shipment journey', exact: true }).innerText();
    if (!journey.includes('Delivered')) throw new Error('Completed playback does not show the delivered shipment.');
    chapter('The recorded shipment is delivered');
    await hold(3000, failures);
    await page.evaluate(() => scrollTo({ top: 0, behavior: 'smooth' }));
    await hold(Math.max(2500, plan.targetMs - (Date.now() - started)), failures);
    if (Date.now() - started > plan.maximumMs) throw new Error('Walkthrough exceeded the 90-second video budget.');
    assertHealthy(failures);
  } finally { await context.close(); }
  const output = path.join(staging, 'harboursense-walkthrough.webm');
  await video.saveAs(output);
  const durationSeconds = await probeVideoDuration(browser, output);
  if (durationSeconds < 60 || durationSeconds > 90) throw new Error(`Recorded video is ${durationSeconds.toFixed(2)} seconds; expected 60–90.`);
  return { file: output, durationSeconds, playbackSpeed: plan.speed, scenarioId: 'normal', runId: normal.provenance.runId, chapters };
}

export async function capturePortfolio({ url: address = process.env.PORTFOLIO_CAPTURE_URL || DEFAULT_URL, checkOnly = false } = {}) {
  const url = checkedUrl(address);
  const verified = await validatePreview(url);
  const normal = verified.recordings.find(item => item.scenario.id === 'normal');
  const fault = verified.recordings.find(item => item.scenario.id === 'crane-fault');
  const plan = planWalkthrough(normal);
  console.log(`Verified preview and all three real recordings. Normal walkthrough: ${(normal.durationMs / 1000).toFixed(1)}s recorded, ${plan.speed}× visible playback, ~${(plan.targetMs / 1000).toFixed(0)}s video.`);
  if (checkOnly) return { plan, verified: true };
  const staging = path.resolve(STAGING_ROOT, `portfolio-capture-${randomUUID()}`);
  if (!staging.startsWith(path.resolve(STAGING_ROOT) + path.sep)) throw new Error('Unsafe media staging path.');
  await fs.mkdir(staging, { recursive: true });
  const failures = [];
  const browser = await chromium.launch({ headless: true });
  try {
    const movingFrames = normal.frames.filter(frame => frame.snapshot.edges?.some(edge => edge.progressToNext > 0 && edge.progressToNext < 100));
    const overviewFrame = movingFrames.sort((a, b) => Math.abs(a.atMs - normal.durationMs * .4) - Math.abs(b.atMs - normal.durationMs * .4))[0];
    if (!overviewFrame) throw new Error('No captured movement frame exists for the port-overview screenshot.');
    const anomalyFrame = fault.frames.find(frame => frame.snapshot.craneTelemetry?.some(sample => sample.analysis?.anomalous));
    const anomaly = anomalyFrame?.snapshot.craneTelemetry.find(sample => sample.analysis?.anomalous);
    const craneId = anomaly?.craneId || anomaly?.telemetry?.craneId;
    if (!craneId) throw new Error('No recorded crane anomaly is available for the detail screenshot.');
    const screenshots = {};
    screenshots['harboursense-overview.png'] = await stillAt(browser, url, normal, overviewFrame.atMs, { width: 1440, height: 1100 }, path.join(staging, 'harboursense-overview.png'), failures);
    screenshots['harboursense-crane-detail.png'] = await stillAt(browser, url, fault, anomalyFrame.atMs, { width: 1440, height: 1100 }, path.join(staging, 'harboursense-crane-detail.png'), failures, craneId);
    screenshots['harboursense-mobile.png'] = await stillAt(browser, url, normal, overviewFrame.atMs, { width: 390, height: 844 }, path.join(staging, 'harboursense-mobile.png'), failures);
    const laptop = await stillAt(browser, url, normal, overviewFrame.atMs, { width: 1280, height: 800 }, path.join(staging, 'laptop-1280.png'), failures);
    const video = await walkthrough(browser, url, normal, staging, failures);
    assertHealthy(failures);
    const files = [...Object.keys(screenshots), path.basename(video.file)];
    const hashes = {};
    for (const file of files) hashes[file] = sha256(await fs.readFile(path.join(staging, file)));
    const evidence = { schemaVersion: 1, capturedAt: new Date().toISOString(), previewUrl: url.href,
      build: verified.metadata, recordingHashes: Object.fromEntries(verified.manifest.scenarios.map(item => [item.id, item.sha256])),
      video: { ...video, file: path.basename(video.file) }, screenshots, mediaSha256: hashes,
      laptop, checks: { realRecordingsVerified: true, noExternalOrApiRequests: true, noBrowserErrors: true, videoDurationWithin60To90Seconds: true, fullMapPaintedAndFitted: true, noMobileOverflow: true, noLaptopOverflow: true } };
    await fs.writeFile(path.join(staging, 'capture.json'), JSON.stringify(evidence, null, 2) + '\n');
    await fs.mkdir(OUTPUT, { recursive: true });
    for (const file of [...files, 'capture.json']) await fs.copyFile(path.join(staging, file), path.join(OUTPUT, file));
    await fs.copyFile(path.join(staging, 'laptop-1280.png'), path.join(STAGING_ROOT, 'portfolio-laptop-1280.png'));
    // Only this newly-created, absolute, workspace-confined staging directory is removed.
    if (!staging.startsWith(path.resolve(STAGING_ROOT) + path.sep)) throw new Error('Unsafe media cleanup path.');
    await fs.rm(staging, { recursive: true, force: true });
    console.log(`PORTFOLIO MEDIA PASS: ${video.durationSeconds.toFixed(2)}s video and 3 screenshots saved in docs/media/.`);
    return evidence;
  } catch (error) {
    await fs.writeFile(path.join(staging, 'capture-failure.json'), JSON.stringify({ error: error.message, failures }, null, 2));
    throw new Error(`${error.message}\nUnpublished capture diagnostics retained at ${path.relative(ROOT, staging)}.`);
  } finally { await browser.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const args = process.argv.slice(2);
  if (args.includes('--help')) {
    console.log('Usage: node scripts/capture-portfolio.mjs [--check]\nRequires the validated static preview at http://127.0.0.1:4173/HarbourSense/ (override with PORTFOLIO_CAPTURE_URL).\nWrites a verified 60–90 second WebM walkthrough, 1440px overview and crane-detail PNGs, a 390px mobile PNG, and capture.json to docs/media/.\n--check validates the final recordings/build/served bytes and clip budget without opening a browser or capturing media.');
  } else if (args.some(arg => arg !== '--check')) {
    console.error('Unsupported capture argument. Run with --help for usage.'); process.exitCode = 1;
  } else capturePortfolio({ checkOnly: args.includes('--check') }).catch(error => {
    console.error(`PORTFOLIO CAPTURE FAILED: ${error.message}`); process.exitCode = 1;
  });
}
