const test = require('node:test');
const assert = require('node:assert/strict');

const { executeTask, completeTaskAndChain, resolveCompletionLocation } = require('../lib/task-executor');
const { TaskPhase } = require('../lib/edge-phases');
const { createSplitEdgeDb } = require('./test-db');

function createMockDb(initial = {}) {
  return createSplitEdgeDb(initial);
}
function createMockDevice() {
  const publishes = [];
  return {
    publishes,
    publish(topic, payload) {
      publishes.push({ topic, payload: JSON.parse(payload) });
    },
  };
}

test('executeTask persists progressToNext and never writes processing location', async () => {
  const edge = {
    id: 'forklift_1',
    type: 'forklift',
    taskPhase: TaskPhase.COMPLETING,
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: null,
    task: { phase: 'store_load', shipmentId: 'shipment_1', requiredPlace: 'B4' },
    shipmentId: 'shipment_1',
  };
  const db = createMockDb({ forklift_1: { ...edge } });
  const device = createMockDevice();

  await executeTask(
    'forklift_1',
    db,
    device,
    edge,
    { phase: 'store_load', shipmentId: 'shipment_1', requiredPlace: 'B4' },
    { steps: 3, simulatorSettings: { simTaskStepMs: 1 } },
  );

  const after = db.get('forklift_1');
  assert.equal(after.progressToNext, 0);
  assert.notEqual(after.currentLocation, 'processing');
  assert.equal(after.currentLocation, 'B4');
  assert.equal(after.taskPhase, TaskPhase.IDLE);

  const progressUpdates = device.publishes.filter((p) => p.topic.includes('/progress'));
  assert.ok(progressUpdates.length >= 3);
});

test('completeTaskAndChain uses resolveDestinationNode when finalNode is null', async () => {
  const edge = {
    id: 'crane_1',
    type: 'crane',
    taskPhase: TaskPhase.COMPLETING,
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: null,
    task: { phase: 'offload', shipmentId: 'shipment_2', requiredPlace: 'A1' },
    shipmentId: 'shipment_2',
  };
  const db = createMockDb({ crane_1: { ...edge } });
  const device = createMockDevice();

  await completeTaskAndChain(
    'crane_1',
    { phase: 'offload', shipmentId: 'shipment_2' },
    device,
    db,
    edge,
  );

  const after = db.get('crane_1');
  assert.equal(after.currentLocation, 'A1');
  assert.equal(after.progressToNext, 0);
  assert.equal(after.taskPhase, TaskPhase.IDLE);

  const completion = device.publishes.find((p) => p.topic.includes('/completion'));
  assert.equal(completion.payload.location, 'A1');
});

test('completeTaskAndChain includes assignmentEpoch in completion payload', async () => {
  const edge = {
    id: 'crane_1',
    type: 'crane',
    taskPhase: TaskPhase.COMPLETING,
    currentLocation: 'A1',
    assignmentEpoch: 42,
    routeRevision: 3,
    task: { phase: 'offload', shipmentId: 'shipment_2', assignmentEpoch: 42 },
    shipmentId: 'shipment_2',
  };
  const db = createMockDb({ crane_1: { ...edge } });
  const device = createMockDevice();

  await completeTaskAndChain(
    'crane_1',
    { phase: 'offload', shipmentId: 'shipment_2', assignmentEpoch: 42, routeRevision: 3 },
    device,
    db,
    edge,
  );

  const completion = device.publishes.find((p) => p.topic.includes('/completion'));
  assert.equal(completion.payload.assignmentEpoch, 42);
  assert.equal(completion.payload.routeRevision, 3);
});

test('resolveCompletionLocation prefers requiredPlace over stale edge location', () => {
  const location = resolveCompletionLocation(
    { phase: 'offload', shipmentId: 'shipment_3' },
    {
      currentLocation: 'B4',
      finalNode: null,
      task: { requiredPlace: 'A1', phase: 'offload' },
    },
  );
  assert.equal(location, 'A1');
});

test('completeTaskAndChain skips publish when phase is missing', async () => {
  const edge = {
    id: 'crane_2',
    type: 'crane',
    taskPhase: TaskPhase.COMPLETING,
    currentLocation: 'A1',
    shipmentId: 'shipment_4',
    task: { shipmentId: 'shipment_4' },
  };
  const db = createMockDb({ crane_2: { ...edge } });
  const device = createMockDevice();

  await completeTaskAndChain(
    'crane_2',
    { shipmentId: 'shipment_4' },
    device,
    db,
    edge,
  );

  assert.equal(device.publishes.length, 0);
  assert.equal(db.get('crane_2').taskPhase, TaskPhase.IDLE);
});

test('completeTaskAndChain resets to idle on epoch mismatch', async () => {
  const edge = {
    id: 'crane_3',
    type: 'crane',
    taskPhase: TaskPhase.COMPLETING,
    currentLocation: 'A1',
    assignmentEpoch: 10,
    shipmentId: 'shipment_5',
    task: { phase: 'offload', shipmentId: 'shipment_5', assignmentEpoch: 5 },
  };
  const db = createMockDb({ crane_3: { ...edge } });
  const device = createMockDevice();

  await completeTaskAndChain(
    'crane_3',
    { phase: 'offload', shipmentId: 'shipment_5', assignmentEpoch: 5 },
    device,
    db,
    edge,
  );

  assert.equal(device.publishes.length, 0);
  assert.equal(db.get('crane_3').taskPhase, TaskPhase.IDLE);
});

test('completeTaskAndChain publishes when task lives in assignment split only', async () => {
  const db = createSplitEdgeDb({
    robot_1: {
      type: 'robot',
      taskPhase: TaskPhase.COMPLETING,
      currentLocation: 'B4',
      shipmentId: 'shipment_1',
      assignmentEpoch: 7,
      routeRevision: 2,
      task: { phase: 'store_move', shipmentId: 'shipment_1', assignmentEpoch: 7, finalNode: 'B4' },
    },
  });
  const runtime = db.getRuntime('robot_1');
  delete runtime.task;
  delete runtime.shipmentId;
  const device = createMockDevice();

  await completeTaskAndChain(
    'robot_1',
    { phase: 'store_move', shipmentId: 'shipment_1', assignmentEpoch: 7 },
    device,
    db,
    runtime,
  );

  const completion = device.publishes.find((p) => p.topic.includes('/completion'));
  assert.ok(completion);
  assert.equal(completion.payload.phase, 'store_move');
  assert.equal(completion.payload.assignmentEpoch, 7);
});