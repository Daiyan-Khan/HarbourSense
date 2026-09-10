#!/usr/bin/env node
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { ROOT, assessReadiness, composeArguments, dockerEnvironment, parseComposeStatus } from './demo.mjs';

const base = process.env.DEMO_TEST_API || 'http://127.0.0.1:8000';
const stackOptions = { apiPort: Number(new URL(base).port || 80), dashboardPort: Number(process.env.DEMO_TEST_DASHBOARD_PORT || 3000) };
const evidence = { startedAt: new Date().toISOString(), checks: [] };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const passed = (name, detail = {}) => { evidence.checks.push({ name, ...detail }); console.log(`INTEGRATION PASS: ${name}`); };
async function request(resource, body, timeout = 12000) {
  const response = await fetch(base + resource, { method: body ? 'POST' : 'GET',
    headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeout) });
  const data = await response.json();
  if (!response.ok) throw new Error(`${resource}: HTTP ${response.status}`);
  return data;
}
const command = (action, payload = {}) => request(`/api/demo/${action}`, { commandId: randomUUID(), ...payload });
async function waitState(predicate, timeout = 60000) {
  const deadline = Date.now() + timeout;
  let error;
  while (Date.now() < deadline) {
    try {
      const state = await request('/api/demo/state');
      if (state.status === 'failed') throw new Error(state.error?.message || 'Scenario failed.');
      if (predicate(state)) return state;
      error = new Error(`Current state: ${state.status}; pending: ${assessReadiness(state).join(', ')}`);
    } catch (cause) { error = cause; }
    await sleep(500);
  }
  throw new Error(`Timed out: ${error?.message || 'state unavailable'}`);
}
const motion = edges => edges.map(({ id, status, currentLocation, nextNode, progressToNext, taskPhase, path, routeRevision, taskEpoch, pickupCompleted }) => ({ id, status, currentLocation, nextNode, progressToNext, taskPhase, path, routeRevision, taskEpoch, pickupCompleted })).sort((a, b) => a.id.localeCompare(b.id));
function compose(args) {
  const result = spawnSync('docker', composeArguments(args), { cwd: ROOT, env: dockerEnvironment(stackOptions), stdio: 'inherit', timeout: 60000, windowsHide: true });
  if (result.status !== 0) throw new Error(`Owned demo service operation failed: ${args.join(' ')}`);
}

try {
  if (process.env.DEMO_TEST_ALLOW_RESET !== '1') throw new Error('Set DEMO_TEST_ALLOW_RESET=1 to authorize changes to the isolated demo.');
  const endpoint = new URL(base);
  if (endpoint.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname)
      || endpoint.username || endpoint.password || endpoint.pathname !== '/') {
    throw new Error('Integration tests only control a local isolated demo API.');
  }
  if (process.argv.includes('--faults')) {
    const inventory = spawnSync('docker', composeArguments(['ps', '--format', 'json']), {
      cwd: ROOT, env: dockerEnvironment(stackOptions), encoding: 'utf8', timeout: 15000, windowsHide: true,
    });
    const api = inventory.status === 0 && parseComposeStatus(inventory.stdout).find(service => service.Service === 'api');
    if (!api?.Publishers?.some(port => port.TargetPort === 8000 && port.PublishedPort === stackOptions.apiPort && port.URL === '127.0.0.1')) {
      throw new Error('Fault injection requires the API port owned by this isolated Compose project.');
    }
  }
  const initial = await request('/api/demo/state');
  assert.equal(initial.enabled, true); assert.equal(initial.mode, 'demo');
  await command('reset', { scenarioId: 'normal' });
  await waitState(state => assessReadiness(state).length === 0);
  const startCommand = { commandId: randomUUID(), scenarioId: 'normal' };
  const first = await request('/api/demo/start', startCommand);
  const duplicate = await request('/api/demo/start', startCommand);
  assert.equal(duplicate.replayed, true);
  assert.equal(duplicate.state.runId, first.state.runId);
  assert.equal(duplicate.state.sequence, first.state.sequence);
  passed('duplicate start is acknowledged without creating another run');
  await waitState(state => state.simTimeMs > 2500);
  await command('pause');
  const paused = await request('/api/demo/state');
  const before = motion(await request('/api/edges'));
  await sleep(1600);
  const after = await request('/api/demo/state');
  assert.equal(after.simTimeMs, paused.simTimeMs);
  assert.deepEqual(motion(await request('/api/edges')), before);
  passed('pause freezes shared clock and device progress');
  await command('speed', { speed: 4 });
  await command('resume');
  const complete = await waitState(state => state.status === 'complete', 150000);
  const recording = await request(`/api/demo/recording?runId=${complete.runId}`);
  const final = recording.frames.at(-1).snapshot;
  assert.ok(final.shipments.length > 0);
  assert.ok(final.shipments.every(item => item.status === 'delivered'));
  const observed = [...new Set(recording.frames.flatMap(frame => frame.snapshot.shipments.map(item => item.status)))];
  assert.ok(observed.includes('stored'), 'Real pipeline never recorded storage.');
  passed('shipment reaches storage and final delivery through actual workers', { runId: complete.runId, observed, durationMs: recording.durationMs });
  await command('reset', { scenarioId: 'normal' });
  const reset = await waitState(state => assessReadiness(state).length === 0);
  assert.notEqual(reset.runId, complete.runId);
  assert.equal((await request('/api/shipments')).length, 0);
  await command('start');
  await waitState(state => state.simTimeMs > 2000);
  const activeRun = (await request('/api/demo/state')).runId;
  await command('reset');
  await waitState(state => assessReadiness(state).length === 0);
  await sleep(2000);
  assert.notEqual((await request('/api/demo/state')).runId, activeRun);
  assert.equal((await request('/api/shipments')).length, 0);
  passed('reset fences an active run and leaves no late shipments in its replacement');

  if (process.argv.includes('--faults')) {
    for (const service of ['api', 'mqtt', 'mongo']) {
      await command('reset');
      await waitState(state => assessReadiness(state).length === 0);
      await command('start');
      await waitState(state => state.simTimeMs > 1500);
      const runId = (await request('/api/demo/state')).runId;
      const started = Date.now();
      if (service === 'api') compose(['restart', 'api']);
      else {
        compose(['stop', service]);
        await sleep(2000);
        compose(['start', service]);
      }
      await waitState(state => state.runId === runId && assessReadiness(state).length === 0, 60000);
      const state = await request('/api/demo/state');
      if (state.status === 'running') await command('speed', { speed: 4 });
      await waitState(item => item.status === 'complete', 150000);
      const shipments = await request('/api/shipments');
      assert.equal(shipments.length, new Set(shipments.map(item => item.id)).size);
      assert.ok(shipments.every(item => item.status === 'delivered'));
      passed(`${service} interruption recovers and completes without duplicate shipments`, { recoveryAndCompletionMs: Date.now() - started });
    }
  }
  await command('reset');
  evidence.completedAt = new Date().toISOString(); evidence.status = 'passed';
} catch (error) {
  evidence.status = 'failed'; evidence.error = error.message;
  console.error(`INTEGRATION FAILED: ${error.message}`); process.exitCode = 1;
} finally {
  const directory = path.join(ROOT, 'artifacts', 'local');
  await fs.mkdir(directory, { recursive: true });
  await fs.writeFile(path.join(directory, 'integration-results.json'), JSON.stringify(evidence, null, 2) + '\n');
}
