const test = require('node:test');
const assert = require('node:assert/strict');

const {
  simDebug,
  componentDebugEnabled,
  debugEvent,
  simWarn,
  simError,
  formatStructuredLine,
} = require('../lib/sim-debug');

test('simDebug reads SIM_DEBUG master flag', () => {
  assert.equal(simDebug({ SIM_DEBUG: 'true' }), true);
  assert.equal(simDebug({ SIM_DEBUG: 'false' }), false);
  assert.equal(simDebug({}), false);
});

test('componentDebugEnabled falls back to SIM_DEBUG', () => {
  assert.equal(componentDebugEnabled('loop', { SIM_DEBUG: 'true' }), true);
  assert.equal(componentDebugEnabled('mqtt', { SIM_DEBUG: 'true' }), true);
  assert.equal(componentDebugEnabled('chain', { SIM_DEBUG: 'false' }), false);
});

test('componentDebugEnabled respects granular flags over fallback', () => {
  assert.equal(componentDebugEnabled('loop', { SIM_DEBUG: 'false', SIM_DEBUG_LOOP: 'true' }), true);
  assert.equal(componentDebugEnabled('mqtt', { SIM_DEBUG: 'true', SIM_DEBUG_MQTT: 'false' }), false);
  assert.equal(componentDebugEnabled('chain', { SIM_DEBUG: 'true', SIM_DEBUG_CHAIN: 'false' }), false);
});

test('debugEvent emits only when component flag enabled', () => {
  const lines = [];
  const original = console.log;
  console.log = (...args) => lines.push(args.join(' '));
  try {
    debugEvent({ SIM_DEBUG: 'false', SIM_DEBUG_MQTT: 'true' }, {
      component: 'mqtt',
      edgeId: 'crane_1',
      shipmentId: 'shipment_1',
      phase: 'offload',
      event: 'task_received',
    });
    debugEvent({ SIM_DEBUG: 'false', SIM_DEBUG_MQTT: 'false' }, {
      component: 'mqtt',
      edgeId: 'crane_2',
      event: 'hidden',
    });
    assert.equal(lines.length, 1);
    assert.match(lines[0], /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/);
    assert.match(lines[0], /DEBUG.*task_received/);
    assert.match(lines[0], /edge=crane_1/);
    assert.match(lines[0], /shipment=shipment_1/);
  } finally {
    console.log = original;
  }
});

test('formatStructuredLine prefixes ISO timestamp', () => {
  const line = formatStructuredLine('WARN', 'EMPTY_PATH_STALL', {
    component: 'loop',
    edgeId: 'truck_1',
    reason: 'NO_PATH',
  });
  assert.match(line, /^\d{4}-\d{2}-\d{2}T/);
  assert.match(line, /\[WARN\] EMPTY_PATH_STALL/);
  assert.match(line, /edge=truck_1/);
});

test('simWarn and simError include ISO timestamps', () => {
  const warnings = [];
  const errors = [];
  const originalWarn = console.warn;
  const originalError = console.error;
  console.warn = (...args) => warnings.push(args.join(' '));
  console.error = (...args) => errors.push(args.join(' '));
  try {
    simWarn('TEST_WARN', { component: 'loop', edgeId: 'e1' });
    simError('TEST_ERROR', { component: 'mqtt', edgeId: 'e2' });
    assert.match(warnings[0], /^\d{4}-\d{2}-\d{2}T/);
    assert.match(errors[0], /^\d{4}-\d{2}-\d{2}T/);
  } finally {
    console.warn = originalWarn;
    console.error = originalError;
  }
});

