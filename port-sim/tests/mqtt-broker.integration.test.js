const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');

const contract = require('../lib/mqtt-contract');

const fixturesDir = path.join(__dirname, 'fixtures');
const runBrokerSmoke = process.env.MQTT_BROKER_SMOKE === '1';
const timeoutMs = Number(process.env.MQTT_SMOKE_TIMEOUT_MS || '10000');

function loadFixture(name) {
  return JSON.parse(fs.readFileSync(path.join(fixturesDir, name), 'utf8'));
}

test('sensor, maintenance, and telemetry fixtures validate', () => {
  assert.equal(contract.validateSensorDataPayload(loadFixture('sensor-data.json')).valid, true);
  assert.equal(contract.validateMaintenanceAlertPayload(loadFixture('maintenance-alert.json')).valid, true);
  assert.equal(contract.validateCraneTelemetryPayload(loadFixture('crane-telemetry.json')).valid, true);
  assert.equal(contract.validateShipmentPayload(loadFixture('shipment-arrived.json')).valid, true);
});

test('topic helpers cover analyzer and sensor contracts', () => {
  assert.equal(contract.sensorDataTopic(), 'harboursense/sensor/data');
  assert.equal(contract.maintenanceAlertTopic(), 'harboursense/alerts/maintenance');
  assert.equal(contract.craneTelemetryTopic('crane001'), 'harboursense/telemetry/crane/crane001/raw');
});

test(
  'broker round-trip for contract fixtures',
  { skip: !runBrokerSmoke },
  async () => {
    const helpers = await import('../../scripts/lib/mqtt-smoke-helpers.mjs');
    const brokerUrl = helpers.resolveBrokerUrl(process.env.MQTT_BROKER_HOST, process.env.MQTT_BROKER_PORT);
    const { connect } = await helpers.loadMqttModule();
    const client = await helpers.connectClient(connect, brokerUrl, timeoutMs);
    try {
      for (const testCase of helpers.buildContractCases(contract)) {
        const receivePromise = helpers.waitForTopicMessage(client, testCase.topic, timeoutMs);
        await helpers.publishJson(client, testCase.topic, testCase.payload);
        const raw = await receivePromise;
        const received = JSON.parse(raw);
        const validation = testCase.validate(received);
        assert.equal(validation.valid, true, `${testCase.name} missing ${validation.missing.join(', ')}`);
      }
    } finally {
      await helpers.closeClient(client);
    }
  }
);
