#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { ROOT, assessReadiness } from './demo.mjs';
import { SCENARIOS, validateRecording } from './lib/demo-artifacts.mjs';

const base = new URL(process.env.DEMO_RECORD_API || 'http://127.0.0.1:8000');
const destination = path.join(ROOT, 'dashboard', 'visualizer', 'public', 'replays');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function request(resource, payload) {
  const response = await fetch(new URL(resource, base), {
    method: payload ? 'POST' : 'GET', headers: payload ? { 'Content-Type': 'application/json' } : {},
    body: payload ? JSON.stringify(payload) : undefined, signal: AbortSignal.timeout(15000),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(`${resource}: HTTP ${response.status} (${result?.detail?.message || 'request rejected'})`);
  return result;
}
async function waitFor(predicate, timeout, description) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const state = await request('/api/demo/state');
    if (state.status === 'failed') throw new Error(state.error?.message || 'Scenario failed.');
    if (predicate(state)) return state;
    await sleep(750);
  }
  throw new Error(`Timed out waiting for ${description}.`);
}

try {
  if (!['localhost', '127.0.0.1', '[::1]'].includes(base.hostname) || base.protocol !== 'http:') {
    throw new Error('The recorder only controls an explicitly local isolated demo API.');
  }
  if (!process.argv.includes('--yes')) throw new Error('Recording resets the owned demo between scenarios. Pass --yes to run it.');
  const current = await request('/api/demo/state');
  if (!current.enabled || current.mode !== 'demo') throw new Error('The API is not the isolated demo.');
  const manifest = { schemaVersion: 1, recordedAt: new Date().toISOString(), scenarios: [] };
  const recordings = [];
  for (const id of SCENARIOS) {
    console.log(`RECORDING: ${id} through the real local pipeline.`);
    await request('/api/demo/reset', { commandId: randomUUID(), scenarioId: id });
    await waitFor(state => assessReadiness(state).length === 0, 60000, 'all workers after reset');
    await request('/api/demo/speed', { commandId: randomUUID(), speed: 4 });
    await request('/api/demo/start', { commandId: randomUUID(), scenarioId: id });
    const state = await waitFor(item => item.status === 'complete', 150000, `${id} completion`);
    const recording = validateRecording(await request(`/api/demo/recording?runId=${state.runId}`), id);
    const bytes = Buffer.from(JSON.stringify(recording) + '\n');
    recordings.push({ id, bytes });
    manifest.scenarios.push({ ...recording.scenario, file: `${id}.json`, durationMs: recording.durationMs,
      sha256: createHash('sha256').update(bytes).digest('hex') });
    console.log(`RECORDED: ${id}, ${recording.frames.length} observed frames, ${(recording.durationMs / 1000).toFixed(1)} simulated seconds.`);
  }
  // Replace publication inputs only after every scenario has reached its real terminal state.
  await fs.mkdir(destination, { recursive: true });
  for (const { id, bytes } of recordings) await fs.writeFile(path.join(destination, `${id}.json`), bytes);
  await fs.writeFile(path.join(destination, 'index.json'), JSON.stringify(manifest, null, 2) + '\n');
  console.log('RECORDING PASS: three completed real-pipeline scenarios saved for public playback.');
} catch (error) {
  console.error(`RECORDING FAILED: ${error.message}`);
  process.exitCode = 1;
}
