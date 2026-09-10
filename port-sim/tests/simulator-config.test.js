const test = require('node:test');
const assert = require('node:assert/strict');

const {
  DEFAULT_SHIPMENT_INTERVALS_MS,
  getSimulatorSettings,
  parseShipmentIntervalsMs,
  pickShipmentIntervalMs,
} = require('../runtime-config');

test('parseShipmentIntervalsMs uses defaults when unset', () => {
  assert.deepEqual(parseShipmentIntervalsMs(undefined), DEFAULT_SHIPMENT_INTERVALS_MS);
  assert.deepEqual(parseShipmentIntervalsMs(''), DEFAULT_SHIPMENT_INTERVALS_MS);
});

test('parseShipmentIntervalsMs parses comma-separated positive integers', () => {
  assert.deepEqual(parseShipmentIntervalsMs('10000,20000'), [10000, 20000]);
});

test('parseShipmentIntervalsMs rejects invalid entries', () => {
  assert.throws(() => parseShipmentIntervalsMs('0,1000'), /positive integer/);
  assert.throws(() => parseShipmentIntervalsMs('abc'), /positive integer/);
});

test('pickShipmentIntervalMs returns a configured interval', () => {
  const interval = pickShipmentIntervalMs([15000, 45000]);
  assert.ok([15000, 45000].includes(interval));
});

test('getSimulatorSettings exposes shipment and tick defaults', () => {
  const settings = getSimulatorSettings({});
  assert.deepEqual(settings.shipmentIntervalMsList, DEFAULT_SHIPMENT_INTERVALS_MS);
  assert.equal(settings.craneTelemetryIntervalMs, 5000);
  assert.equal(settings.simProgressIntervalMs, 1000);
  assert.equal(settings.simTaskStepMs, 2000);
  assert.equal(settings.simLoopIdleDelayMs, 5000);
  assert.equal(settings.simLoopTickMs, 1000);
  assert.equal(settings.maxArrivalsPerDock, 2);
  assert.equal(settings.shipmentGenerationEnabled, true);
  assert.equal(settings.simDebug, false);
});

test('getSimulatorSettings reads env overrides', () => {
  const settings = getSimulatorSettings({
    SHIPMENT_INTERVALS_MS: '120000',
    CRANE_TELEMETRY_INTERVAL_MS: '8000',
    SIM_LOOP_TICK_MS: '500',
    SIM_TASK_STEP_MS: '1500',
    SIM_DEBUG: 'true',
    MAX_ARRIVALS_PER_DOCK: '3',
    SHIPMENT_GENERATION_ENABLED: 'false',
  });
  assert.deepEqual(settings.shipmentIntervalMsList, [120000]);
  assert.equal(settings.craneTelemetryIntervalMs, 8000);
  assert.equal(settings.simLoopTickMs, 500);
  assert.equal(settings.simTaskStepMs, 1500);
  assert.equal(settings.simDebug, true);
  assert.equal(settings.maxArrivalsPerDock, 3);
  assert.equal(settings.shipmentGenerationEnabled, false);
});
