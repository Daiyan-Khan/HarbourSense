const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');

const contract = require('../lib/mqtt-contract');

const fixturesDir = path.join(__dirname, 'fixtures');

function loadFixture(name) {
  return JSON.parse(fs.readFileSync(path.join(fixturesDir, name), 'utf8'));
}

test('completion topic uses canonical edge suffix shape', () => {
  assert.equal(contract.completionTopic('crane001'), 'harboursense/edge/crane001/completion');
  assert.equal(contract.isCanonicalCompletionTopic('harboursense/edge/crane001/completion'), true);
  assert.equal(contract.isDeprecatedCompletionTopic('harboursense/edge/completion/crane001'), true);
});

test('task and traffic topics match shared contract', () => {
  assert.equal(contract.taskTopic('truck_tempo_1'), 'harboursense/edge/truck_tempo_1/task');
  assert.equal(contract.routeTopic('truck_tempo_1'), 'harboursense/edge/truck_tempo_1/route');
  assert.equal(contract.trafficUpdateTopic('truck_tempo_1'), 'harboursense/traffic/update/truck_tempo_1');
  assert.equal(contract.shipmentTopic('shipment-42'), 'harboursense/shipments/shipment-42');
});

const COMPLETION_FIXTURES = [
  ['completion-offload.json', 'offload'],
  ['completion-transport.json', 'transport'],
  ['completion-store-load.json', 'store_load'],
  ['completion-delivery.json', 'delivery'],
];

for (const [filename, phase] of COMPLETION_FIXTURES) {
  test(`completion fixture ${filename} validates required fields`, () => {
    const payload = loadFixture(filename);
    const result = contract.validateCompletionPayload(payload);
    assert.equal(result.valid, true);
    assert.deepEqual(result.missing, []);
    assert.equal(payload.phase, phase);
  });
}

test('task fixture validates required fields', () => {
  const payload = loadFixture('task-transport.json');
  const result = contract.validateTaskPayload(payload);
  assert.equal(result.valid, true);
});

test('invalid completion payload reports missing fields', () => {
  const result = contract.validateCompletionPayload({ shipmentId: 's1', phase: 'offload' });
  assert.equal(result.valid, false);
  assert.ok(result.missing.includes('completedAt'));
});

test('sensor, shipment, maintenance, and telemetry fixtures validate', () => {
  assert.equal(contract.validateShipmentPayload(loadFixture('shipment-arrived.json')).valid, true);
  assert.equal(contract.validateSensorDataPayload(loadFixture('sensor-data.json')).valid, true);
  assert.equal(contract.validateMaintenanceAlertPayload(loadFixture('maintenance-alert.json')).valid, true);
  assert.equal(contract.validateCraneTelemetryPayload(loadFixture('crane-telemetry.json')).valid, true);
});

test('sensor and analyzer topic helpers match shared contract', () => {
  assert.equal(contract.sensorDataTopic(), 'harboursense/sensor/data');
  assert.equal(contract.maintenanceAlertTopic(), 'harboursense/alerts/maintenance');
  assert.equal(contract.craneTelemetryTopic('crane001'), 'harboursense/telemetry/crane/crane001/raw');
});
