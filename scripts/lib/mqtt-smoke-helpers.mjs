/**
 * Shared helpers for HarbourSense broker-backed smoke checks.
 * Loads mqtt + contract helpers from port-sim without requiring a root package.json.
 */

import { createRequire } from 'module';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, '../..');
const portSimRoot = path.join(repoRoot, 'port-sim');
const require = createRequire(import.meta.url);

export function resolveBrokerUrl(hostArg, portArg) {
  const host = hostArg || process.env.MQTT_BROKER_HOST || 'localhost';
  const port = portArg || process.env.MQTT_BROKER_PORT || '1883';
  if (host.startsWith('mqtt://') || host.startsWith('mqtts://')) {
    return host;
  }
  return `mqtt://${host}:${port}`;
}

export function loadContract() {
  return require(path.join(portSimRoot, 'lib/mqtt-contract.js'));
}

export function loadFixture(name) {
  const fixturePath = path.join(portSimRoot, 'tests/fixtures', name);
  return JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
}

export async function loadMqttModule() {
  const candidates = [
    path.join(portSimRoot, 'node_modules/mqtt'),
    'mqtt',
  ];
  for (const candidate of candidates) {
    try {
      const mod = candidate.startsWith(path.sep) || candidate.includes(':')
        ? require(candidate)
        : await import(candidate);
      const connect = mod.connect || mod.default?.connect;
      if (connect) {
        return { connect };
      }
    } catch {
      // try next candidate
    }
  }
  throw new Error(
    'mqtt package not found; run npm install in HarbourSense/port-sim before broker smoke'
  );
}

export function connectClient(connect, brokerUrl, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const client = connect(brokerUrl, {
      connectTimeout: timeoutMs,
      reconnectPeriod: 0,
    });
    const timer = setTimeout(() => {
      client.end(true);
      reject(new Error(`MQTT connect timed out after ${timeoutMs}ms (${brokerUrl})`));
    }, timeoutMs);

    client.once('connect', () => {
      clearTimeout(timer);
      resolve(client);
    });
    client.once('error', (error) => {
      clearTimeout(timer);
      client.end(true);
      reject(error);
    });
  });
}

export function closeClient(client) {
  return new Promise((resolve) => {
    if (!client) {
      resolve();
      return;
    }
    client.end(false, {}, () => resolve());
  });
}

export function waitForTopicMessage(client, topic, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for MQTT message on ${topic}`));
    }, timeoutMs);

    function onMessage(receivedTopic, payload) {
      if (receivedTopic !== topic) {
        return;
      }
      cleanup();
      resolve(payload.toString());
    }

    function cleanup() {
      clearTimeout(timer);
      client.removeListener('message', onMessage);
    }

    client.on('message', onMessage);
    client.subscribe(topic, { qos: 0 }, (error) => {
      if (error) {
        cleanup();
        reject(error);
      }
    });
  });
}

export function publishJson(client, topic, payload) {
  return new Promise((resolve, reject) => {
    client.publish(topic, JSON.stringify(payload), { qos: 0 }, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

export function buildContractCases(contract) {
  const completion = loadFixture('completion-offload.json');
  const task = loadFixture('task-transport.json');
  const shipment = loadFixture('shipment-arrived.json');
  const sensor = loadFixture('sensor-data.json');
  const maintenance = loadFixture('maintenance-alert.json');
  const telemetry = loadFixture('crane-telemetry.json');

  return [
    {
      name: 'edge completion',
      topic: contract.completionTopic(completion.id),
      payload: completion,
      validate: contract.validateCompletionPayload,
    },
    {
      name: 'edge task',
      topic: contract.taskTopic('truck_tempo_1'),
      payload: task,
      validate: contract.validateTaskPayload,
    },
    {
      name: 'shipment status',
      topic: contract.shipmentTopic(shipment.id),
      payload: shipment,
      validate: contract.validateShipmentPayload,
    },
    {
      name: 'sensor data',
      topic: contract.sensorDataTopic(),
      payload: sensor,
      validate: contract.validateSensorDataPayload,
    },
    {
      name: 'maintenance alert',
      topic: contract.maintenanceAlertTopic(),
      payload: maintenance,
      validate: contract.validateMaintenanceAlertPayload,
    },
    {
      name: 'crane telemetry',
      topic: contract.craneTelemetryTopic(telemetry.craneId || 'crane001'),
      payload: telemetry,
      validate: contract.validateCraneTelemetryPayload,
    },
  ];
}

const STACK_TOPIC_PATTERNS = [
  /^harboursense\/shipments\/[^/]+$/,
  /^harboursense\/edge\/[^/]+\/(task|progress|completion)$/,
  /^harboursense\/sensor\/data$/,
  /^harboursense\/traffic\/update\/[^/]+$/,
];

export function isContractStackTopic(topic) {
  return STACK_TOPIC_PATTERNS.some((pattern) => pattern.test(topic));
}

export function observeStackTraffic(client, observeSeconds = 20) {
  const seen = new Map();

  return new Promise((resolve) => {
    const deadline = Date.now() + observeSeconds * 1000;

    function onMessage(topic) {
      if (!isContractStackTopic(topic)) {
        return;
      }
      seen.set(topic, (seen.get(topic) || 0) + 1);
    }

    client.on('message', onMessage);
    client.subscribe('harboursense/#', { qos: 0 }, () => {
      const poll = () => {
        if (seen.size > 0 || Date.now() >= deadline) {
          client.removeListener('message', onMessage);
          resolve([...seen.entries()].map(([topic, count]) => ({ topic, count })));
          return;
        }
        setTimeout(poll, 500);
      };
      poll();
    });
  });
}
