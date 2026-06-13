#!/usr/bin/env node
/**
 * Bounded Mosquitto smoke for HarbourSense MQTT contract topics.
 *
 * Usage:
 *   node scripts/smoke-mqtt-broker.mjs
 *   node scripts/smoke-mqtt-broker.mjs --broker-host localhost --broker-port 1883
 *   node scripts/smoke-mqtt-broker.mjs --observe-stack 20
 *
 * Requires: npm install in port-sim (mqtt dependency).
 * Does not require AWS IoT or running backend/portsim unless --observe-stack is set.
 */

import {
  buildContractCases,
  closeClient,
  connectClient,
  loadContract,
  loadMqttModule,
  observeStackTraffic,
  publishJson,
  resolveBrokerUrl,
  waitForTopicMessage,
} from './lib/mqtt-smoke-helpers.mjs';

const args = process.argv.slice(2);

function readArg(flag, fallback) {
  const index = args.indexOf(flag);
  if (index === -1 || index + 1 >= args.length) return fallback;
  return args[index + 1];
}

const brokerHost = readArg('--broker-host', null);
const brokerPort = readArg('--broker-port', null);
const brokerUrl = readArg('--broker', resolveBrokerUrl(brokerHost, brokerPort));
const timeoutMs = Number(readArg('--timeout-ms', process.env.MQTT_SMOKE_TIMEOUT_MS || '10000'));
const observeSeconds = Number(readArg('--observe-stack', '0'));

function fail(message) {
  console.error(`MQTT SMOKE FAIL: ${message}`);
  process.exit(1);
}

function pass(message) {
  console.log(`MQTT SMOKE OK: ${message}`);
}

async function checkBrokerEcho(client) {
  const echoTopic = 'harboursense/__smoke__/echo';
  const payload = { ping: 'harboursense-smoke', at: new Date().toISOString() };
  const receivePromise = waitForTopicMessage(client, echoTopic, timeoutMs);
  await publishJson(client, echoTopic, payload);
  const raw = await receivePromise;
  const parsed = JSON.parse(raw);
  if (parsed.ping !== payload.ping) {
    fail(`broker echo payload mismatch on ${echoTopic}`);
  }
  pass(`broker round-trip on ${echoTopic}`);
}

async function checkContractRoundTrips(client, contract) {
  for (const testCase of buildContractCases(contract)) {
    const validation = testCase.validate(testCase.payload);
    if (!validation.valid) {
      fail(`${testCase.name} fixture invalid before publish (${validation.missing.join(', ')})`);
    }

    const receivePromise = waitForTopicMessage(client, testCase.topic, timeoutMs);
    await publishJson(client, testCase.topic, testCase.payload);
    const raw = await receivePromise;
    const received = JSON.parse(raw);
    const receivedValidation = testCase.validate(received);
    if (!receivedValidation.valid) {
      fail(
        `${testCase.name} on ${testCase.topic} missing fields after round-trip: ${receivedValidation.missing.join(', ')}`
      );
    }
    pass(`${testCase.name} round-trip on ${testCase.topic}`);
  }
}

async function checkDeprecatedTopicDetection(contract) {
  const canonical = contract.completionTopic('crane001');
  const deprecated = `harboursense/edge/completion/crane001`;
  if (!contract.isCanonicalCompletionTopic(canonical)) {
    fail('canonical completion topic matcher failed');
  }
  if (!contract.isDeprecatedCompletionTopic(deprecated)) {
    fail('deprecated completion topic matcher failed');
  }
  pass('completion topic canonical/deprecated matchers');
}

async function maybeObserveStack(client) {
  if (observeSeconds <= 0) {
    return;
  }

  console.log(`MQTT SMOKE: observing live stack traffic for ${observeSeconds}s on harboursense/#`);
  const traffic = await observeStackTraffic(client, observeSeconds);
  if (traffic.length === 0) {
    fail(
      `--observe-stack saw no contract traffic within ${observeSeconds}s; start mqtt + backend + portsim first`
    );
  }
  pass(
    `live stack traffic observed (${traffic.length} topics: ${traffic.map((entry) => entry.topic).join(', ')})`
  );
}

async function main() {
  console.log(`Running HarbourSense MQTT broker smoke against ${brokerUrl}`);
  const contract = loadContract();
  await checkDeprecatedTopicDetection(contract);

  const { connect } = await loadMqttModule();
  let client;
  try {
    client = await connectClient(connect, brokerUrl, timeoutMs);
    pass(`connected to ${brokerUrl}`);

    await checkBrokerEcho(client);
    await checkContractRoundTrips(client, contract);
    await maybeObserveStack(client);
  } finally {
    await closeClient(client);
  }

  console.log('MQTT SMOKE PASS: broker-backed contract checks complete');
}

main().catch((error) => fail(error.message));
