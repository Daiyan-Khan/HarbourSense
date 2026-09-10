#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const PROJECT = 'harboursense-demo';
const WORKERS = ['manager', 'portsim', 'sensors', 'analyzer'];

export function parseOptions(args) {
  const options = { command: args[0] || 'start', apiPort: 8000, dashboardPort: 3000, yes: false };
  if (!['start', 'status', 'stop', 'reset', 'logs', 'config'].includes(options.command)) {
    throw new Error('Use start, status, stop, reset, logs, or config.');
  }
  for (let index = 1; index < args.length; index += 1) {
    const flag = args[index];
    if (flag === '--yes') options.yes = true;
    else if (flag === '--api-port' || flag === '--dashboard-port') {
      const value = args[++index];
      if (!/^\d+$/.test(value || '') || Number(value) < 1024 || Number(value) > 65535) {
        throw new Error(`${flag} requires a port between 1024 and 65535.`);
      }
      options[flag === '--api-port' ? 'apiPort' : 'dashboardPort'] = Number(value);
    } else throw new Error(`Unknown option: ${flag}`);
  }
  if (options.apiPort === options.dashboardPort) throw new Error('API and dashboard ports must differ.');
  return options;
}

export function composeArguments(args) {
  return ['compose', '--project-directory', ROOT, '--project-name', PROJECT,
    '--env-file', path.join(ROOT, 'config', 'demo.env'),
    '--file', path.join(ROOT, 'compose.demo.yml'), ...args];
}

export function dockerEnvironment(options, original = process.env) {
  // Configuration from an unrelated local/cloud session cannot select the demo DB.
  const env = { ...original };
  for (const key of Object.keys(env)) {
    if (/^(MONGO|MQTT|AWS_IOT|DEMO_|COMPOSE_)/.test(key)) delete env[key];
  }
  env.DEMO_API_PORT = String(options.apiPort);
  env.DEMO_DASHBOARD_PORT = String(options.dashboardPort);
  return env;
}

export function parseComposeStatus(output) {
  const value = output.trim();
  if (!value) return [];
  if (value.startsWith('[')) return JSON.parse(value);
  return value.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
}

export function assessReadiness(state, now = Date.now()) {
  if (!state?.enabled || state.mode !== 'demo' || !state.runId) {
    return ['API is not the isolated HarbourSense demo'];
  }
  return WORKERS.flatMap(name => {
    const service = state.services?.[name];
    const updated = Date.parse(service?.updatedAt || '');
    const age = Number.isFinite(service?.ageMs) ? service.ageMs : now - updated;
    if (service?.status !== 'ready') return [`${name}: ${service?.status || 'waiting'}`];
    if (!Number.isFinite(age) || age < -5000 || age > 15000) return [`${name}: heartbeat is stale`];
    if (service.mqttConnected !== true) return [`${name}: broker is not connected`];
    return [];
  });
}

function execute(args, options, { capture = false, timeout = 0 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', args, { cwd: ROOT, env: dockerEnvironment(options),
      stdio: capture ? ['ignore', 'pipe', 'pipe'] : 'inherit', windowsHide: true });
    let stdout = ''; let stderr = ''; let timedOut = false;
    child.stdout?.on('data', data => { stdout += data; });
    child.stderr?.on('data', data => { stderr += data; });
    const timer = timeout ? setTimeout(() => { timedOut = true; child.kill(); }, timeout) : null;
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('close', code => {
      clearTimeout(timer);
      if (timedOut) reject(new Error('Docker did not respond. Open Docker Desktop and resolve its startup error, then retry.'));
      else if (code !== 0) reject(new Error(stderr.trim() || `Docker exited with code ${code}.`));
      else resolve(stdout);
    });
  });
}

async function request(port, resource, init) {
  const response = await fetch(`http://127.0.0.1:${port}${resource}`, {
    ...init, signal: AbortSignal.timeout(6000),
  });
  if (!response.ok) throw new Error(`${resource} returned HTTP ${response.status}`);
  return response.json();
}

function portAvailable(port) {
  return new Promise(resolve => {
    const server = net.createServer();
    server.once('error', () => resolve(false));
    server.listen(port, '127.0.0.1', () => server.close(() => resolve(true)));
  });
}

async function checkPorts(options) {
  const records = parseComposeStatus(await execute(composeArguments(['ps', '--format', 'json']), options, { capture: true, timeout: 12000 }));
  const owned = new Set(records.flatMap(row => (row.Publishers || []).map(p => p.PublishedPort)));
  for (const port of [options.apiPort, options.dashboardPort]) {
    if (!owned.has(port) && !await portAvailable(port)) {
      throw new Error(`Port ${port} is already occupied outside this demo. Stop that service, or use --api-port and --dashboard-port to choose unused ports. No process was stopped.`);
    }
  }
}

async function waitReady(options) {
  const deadline = Date.now() + 120000;
  let previous = ''; let pending = ['API starting'];
  while (Date.now() < deadline) {
    try {
      const health = await request(options.apiPort, '/health/ready');
      if (health.status !== 'ready') throw new Error('Database or graph not ready');
      const state = await request(options.apiPort, '/api/demo/state');
      pending = assessReadiness(state);
      const dashboard = await fetch(`http://127.0.0.1:${options.dashboardPort}`, { signal: AbortSignal.timeout(3000) });
      if (!dashboard.ok) pending.push('dashboard starting');
      if (!pending.length) return state;
    } catch (error) { pending = [error.message]; }
    const next = pending.join('; ');
    if (next !== previous) { console.log(`Waiting: ${next}`); previous = next; }
    await new Promise(resolve => setTimeout(resolve, 1500));
  }
  throw new Error(`Demo did not become ready: ${pending.join('; ')}. Run npm run demo:logs to inspect the failing service. Data has been preserved.`);
}

export async function main(args = process.argv.slice(2)) {
  const options = parseOptions(args);
  if (Number(process.versions.node.split('.')[0]) < 24) throw new Error('Install Node.js 24 to use the supported demo commands.');
  if (options.command === 'config') {
    await execute(composeArguments(['config', '--quiet']), options); return;
  }
  if (options.command === 'reset' && !options.yes) {
    throw new Error('Reset replaces only the isolated demo run. To request it explicitly, run npm run demo:reset -- --yes.');
  }
  if (options.command === 'reset') {
    const state = await request(options.apiPort, '/api/demo/state');
    if (!state.enabled || state.mode !== 'demo') throw new Error('Reset refused: target API is not in demo mode.');
    const result = await request(options.apiPort, '/api/demo/reset', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ commandId: randomUUID() }),
    });
    console.log(`Demo reset acknowledged. Run ${result.state.runId}; other databases were not targeted.`); return;
  }
  try { await execute(['info', '--format', '{{.ServerVersion}}'], options, { capture: true, timeout: 12000 }); }
  catch (error) { throw new Error(`Docker is unavailable. Start Docker Desktop (Linux containers) and retry. ${error.message}`); }
  if (options.command === 'logs') { await execute(composeArguments(['logs', '--tail', '80']), options); return; }
  if (options.command === 'stop') {
    try {
      const state = await request(options.apiPort, '/api/demo/state');
      if (state.enabled && state.mode === 'demo' && state.status === 'running') {
        await request(options.apiPort, '/api/demo/pause', { method: 'POST',
          headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ commandId: randomUUID() }) });
      }
    } catch (error) {
      console.warn(`Could not acknowledge a pause before stopping: ${error.message}. Inspect the run after restart.`);
    }
    await execute(composeArguments(['stop']), options);
    console.log('HarbourSense demo stopped. Its database and other projects are preserved.'); return;
  }
  if (options.command === 'status') {
    await execute(composeArguments(['ps']), options);
    const state = await request(options.apiPort, '/api/demo/state');
    console.log(JSON.stringify({ runId: state.runId, status: state.status, scenario: state.scenarioId,
      services: state.services, pending: assessReadiness(state) }, null, 2)); return;
  }
  await checkPorts(options);
  await execute(composeArguments(['config', '--quiet']), options);
  console.log('Starting the isolated local demo. The first image build can take several minutes.');
  await execute(composeArguments(['up', '--build', '--detach', '--remove-orphans']), options);
  const state = await waitReady(options);
  console.log(`Ready: http://localhost:${options.dashboardPort}\nAPI: http://localhost:${options.apiPort}\nRun: ${state.runId} (${state.status})\nUse the dashboard to start a scenario. Existing runs are not reset by startup.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch(error => { console.error(`HarbourSense: ${error.message}`); process.exitCode = 1; });
}
