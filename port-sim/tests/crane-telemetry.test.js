const test = require('node:test');
const assert = require('node:assert/strict');

const contract = require('../lib/mqtt-contract');
const telemetry = require('../lib/crane-telemetry');

function inRange(value, [min, max]) {
  return value >= min && value <= max;
}

test('buildCraneTelemetryPayload validates against mqtt contract', () => {
  const payload = telemetry.buildCraneTelemetryPayload('crane001', { active: false });
  const result = contract.validateCraneTelemetryPayload(payload);
  assert.equal(result.valid, true);
  assert.equal(payload.craneId, 'crane001');
  assert.equal(contract.craneTelemetryTopic('crane001'), 'harboursense/telemetry/crane/crane001/raw');
});

test('idle crane telemetry stays within idle training ranges', () => {
  for (let i = 0; i < 20; i += 1) {
    const payload = telemetry.buildCraneTelemetryPayload('crane002', { active: false });
    assert.ok(inRange(payload.motorTemp, telemetry.IDLE_RANGES.motorTemp));
    assert.ok(inRange(payload.vibration, telemetry.IDLE_RANGES.vibration));
    assert.ok(inRange(payload.energyUse, telemetry.IDLE_RANGES.energyUse));
  }
});

test('active crane telemetry stays within active training ranges', () => {
  for (let i = 0; i < 20; i += 1) {
    const payload = telemetry.buildCraneTelemetryPayload('crane003', { active: true });
    assert.ok(inRange(payload.motorTemp, telemetry.ACTIVE_RANGES.motorTemp));
    assert.ok(inRange(payload.vibration, telemetry.ACTIVE_RANGES.vibration));
    assert.ok(inRange(payload.energyUse, telemetry.ACTIVE_RANGES.energyUse));
  }
});

test('isCraneActive treats idle separately from working phases', () => {
  assert.equal(telemetry.isCraneActive('idle'), false);
  assert.equal(telemetry.isCraneActive('assigned'), true);
  assert.equal(telemetry.isCraneActive(undefined), false);
});
