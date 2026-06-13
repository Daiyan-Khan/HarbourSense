#!/usr/bin/env node
/**
 * Bounded local smoke for HarbourSense Compose stack.
 * Usage: node scripts/smoke-local.mjs [--base-url http://localhost:8000] [--watch-shipment SECONDS] [--mqtt] [--mqtt-observe SECONDS]
 */

import { createRequire } from 'module';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import { fileURLToPath } from 'url';

const execFileAsync = promisify(execFile);
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const args = process.argv.slice(2);

function readArg(flag, fallback) {
  const index = args.indexOf(flag);
  if (index === -1 || index + 1 >= args.length) return fallback;
  return args[index + 1];
}

const baseUrl = (readArg('--base-url', process.env.SMOKE_BASE_URL || 'http://localhost:8000')).replace(/\/$/, '');
const watchSeconds = Number(readArg('--watch-shipment', '0'));
const runMqttSmoke = args.includes('--mqtt');
const mqttObserveSeconds = Number(readArg('--mqtt-observe', '0'));

async function requestJson(path) {
  const response = await fetch(`${baseUrl}${path}`);
  const body = await response.json().catch(() => ({}));
  return { status: response.status, body };
}

function fail(message) {
  console.error(`SMOKE FAIL: ${message}`);
  process.exit(1);
}

function pass(message) {
  console.log(`SMOKE OK: ${message}`);
}

async function checkHealth() {
  const live = await requestJson('/health/live');
  if (live.status !== 200 || live.body.status !== 'alive') {
    fail(`/health/live returned ${live.status} ${JSON.stringify(live.body)}`);
  }
  pass('/health/live');

  const ready = await requestJson('/health/ready');
  if (ready.status !== 200 || ready.body.status !== 'ready') {
    fail(`/health/ready returned ${ready.status} ${JSON.stringify(ready.body)}`);
  }
  pass(`/health/ready (${ready.body.graphNodes ?? '?'} graph nodes)`);
}

async function checkReadApis() {
  const graph = await requestJson('/api/graph');
  if (graph.status !== 200 || !graph.body.nodes || Object.keys(graph.body.nodes).length === 0) {
    fail(`/api/graph returned ${graph.status} with empty nodes`);
  }
  pass(`/api/graph (${Object.keys(graph.body.nodes).length} nodes)`);

  const edges = await requestJson('/api/edges');
  if (edges.status !== 200 || !Array.isArray(edges.body)) {
    fail(`/api/edges returned ${edges.status}`);
  }
  const deviceTypes = new Set(edges.body.map((edge) => edge.type));
  const requiredTypes = ['crane', 'truck_tempo', 'robot', 'forklift', 'truck_delivery'];
  const missingTypes = requiredTypes.filter((type) => !deviceTypes.has(type));
  if (missingTypes.length > 0) {
    fail(`/api/edges missing seeded device types: ${missingTypes.join(', ')} (re-run mongo seed)`);
  }
  pass(`/api/edges (${edges.body.length} devices; lifecycle types present)`);

  const sensors = await requestJson('/api/sensors');
  if (sensors.status !== 200 || !Array.isArray(sensors.body)) {
    fail(`/api/sensors returned ${sensors.status}`);
  }
  pass(`/api/sensors (${sensors.body.length} readings)`);

  const shipments = await requestJson('/api/shipments');
  if (shipments.status !== 200 || !Array.isArray(shipments.body)) {
    fail(`/api/shipments returned ${shipments.status}`);
  }
  pass(`/api/shipments (${shipments.body.length} records)`);

  const sensorAlerts = await requestJson('/api/alerts/sensor');
  if (sensorAlerts.status !== 200 || !Array.isArray(sensorAlerts.body)) {
    fail(`/api/alerts/sensor returned ${sensorAlerts.status}`);
  }
  pass(`/api/alerts/sensor (${sensorAlerts.body.length} unresolved)`);

  const maintenanceAlerts = await requestJson('/api/alerts/maintenance');
  if (maintenanceAlerts.status !== 200 || !Array.isArray(maintenanceAlerts.body)) {
    fail(`/api/alerts/maintenance returned ${maintenanceAlerts.status}`);
  }
  pass(`/api/alerts/maintenance (${maintenanceAlerts.body.length} unresolved)`);
}

async function loadMongoClient() {
  const candidates = [
    'mongodb',
    path.resolve(scriptDir, '../port-sim/node_modules/mongodb'),
  ];
  for (const candidate of candidates) {
    try {
      const mod = candidate === 'mongodb' ? await import('mongodb') : require(candidate);
      if (mod?.MongoClient) {
        return mod.MongoClient;
      }
    } catch {
      // try next candidate
    }
  }
  return null;
}

async function fetchRecentShipmentsViaDocker(dbName) {
  const container = process.env.SMOKE_MONGO_CONTAINER || 'harboursense-mongo-1';
  const evalScript = `JSON.stringify(db.shipments.find().sort({updatedAt:-1}).limit(5).toArray().map((doc)=>({id:doc.id,status:doc.status})))`;
  const { stdout } = await execFileAsync('docker', [
    'exec',
    container,
    'mongosh',
    dbName,
    '--quiet',
    '--eval',
    evalScript,
  ]);
  return JSON.parse(stdout.trim() || '[]');
}

async function fetchRecentShipmentsViaDriver(MongoClient, mongoUri, dbName) {
  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 5000 });
  await client.connect();
  try {
    return await client
      .db(dbName)
      .collection('shipments')
      .find({})
      .sort({ updatedAt: -1 })
      .limit(5)
      .toArray();
  } finally {
    await client.close();
  }
}

async function watchShipmentProgress() {
  const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017/port';
  const dbName = process.env.MONGO_DB_NAME || 'port';
  const MongoClient = await loadMongoClient();
  const useDocker = !MongoClient;

  async function readShipments() {
    if (useDocker) {
      return fetchRecentShipmentsViaDocker(dbName);
    }
    return fetchRecentShipmentsViaDriver(MongoClient, mongoUri, dbName);
  }

  const deadline = Date.now() + watchSeconds * 1000;
  const targetStatuses = ['arrived', 'offloaded', 'transported', 'storing', 'stored', 'delivered'];
  let lastSnapshot = '';

  if (useDocker) {
    console.log('SHIPMENT WATCH: using docker exec mongosh fallback');
  }

  while (Date.now() < deadline) {
    let docs;
    try {
      docs = await readShipments();
    } catch (error) {
      fail(`--watch-shipment could not read shipments (${error.message})`);
    }

    const snapshot = docs.map((doc) => `${doc.id}:${doc.status}`).join('|');
    if (snapshot !== lastSnapshot) {
      console.log('SHIPMENT WATCH:', docs.map((doc) => ({ id: doc.id, status: doc.status })));
      lastSnapshot = snapshot;
    }

    if (docs.some((doc) => doc.status === 'delivered')) {
      pass('at least one shipment reached delivered');
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 3000));
  }

  const finalDocs = await readShipments();
  const statuses = finalDocs.map((doc) => doc.status);
  const progressed = statuses.some(
    (status) => targetStatuses.indexOf(status) >= targetStatuses.indexOf('transported')
  );
  if (!progressed) {
    fail(`no shipment reached transported+ within ${watchSeconds}s (latest statuses: ${statuses.join(', ') || 'none'})`);
  }
  pass(`shipment watch ended without delivered; latest statuses: ${statuses.join(', ') || 'none'}`);
}

async function runBrokerSmoke() {
  const brokerScript = path.join(scriptDir, 'smoke-mqtt-broker.mjs');
  const brokerArgs = [brokerScript];
  if (mqttObserveSeconds > 0) {
    brokerArgs.push('--observe-stack', String(mqttObserveSeconds));
  }

  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, brokerArgs, {
      stdio: 'inherit',
      env: process.env,
    });
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`smoke-mqtt-broker.mjs exited with code ${code}`));
    });
  });
}

async function main() {
  console.log(`Running HarbourSense smoke against ${baseUrl}`);
  await checkHealth();
  await checkReadApis();
  if (watchSeconds > 0) {
    await watchShipmentProgress();
  }
  if (runMqttSmoke) {
    console.log('Running companion MQTT broker smoke');
    await runBrokerSmoke();
  }
  console.log('SMOKE PASS: bounded local checks complete');
}

main().catch((error) => fail(error.message));
