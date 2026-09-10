const test = require('node:test');
const assert = require('node:assert/strict');

const { applyArrivalUpdate, touchesPhysicalState, updateEdgeInDB, updateEdgeState } = require('../lib/edge-state');
const { createSplitEdgeDb } = require('./test-db');

function createEdgeDb(initial = {}) {
  return createSplitEdgeDb(initial);
}

test('touchesPhysicalState detects location mutations', () => {
  assert.equal(touchesPhysicalState({ currentLocation: 'B2' }), true);
  assert.equal(touchesPhysicalState({ task: { phase: 'transport' } }), false);
});

test('applyArrivalUpdate increments stateRevision on success', async () => {
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      currentLocation: 'A1',
      routeRevision: 3,
      stateRevision: 1,
      activeHop: { from: 'A1', to: 'A2', startedAt: '2026-06-14T10:00:00.000Z', revision: 3 },
    },
  });

  await applyArrivalUpdate(db, 'truck_1', db.get('truck_1'), {
    currentLocation: 'A2',
    progressToNext: 0,
    path: ['A3'],
  });

  const after = db.get('truck_1');
  assert.equal(after.currentLocation, 'A2');
  assert.equal(after.stateRevision, 2);
});

test('applyArrivalUpdate returns applied false on CAS miss', async () => {
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      currentLocation: 'B2',
      routeRevision: 5,
      stateRevision: 4,
      activeHop: { from: 'B1', to: 'B2', startedAt: '2026-06-14T10:05:00.000Z', revision: 5 },
    },
  });

  const result = await applyArrivalUpdate(db, 'truck_1', {
    id: 'truck_1',
    currentLocation: 'B1',
    routeRevision: 3,
    activeHop: { startedAt: '2026-06-14T09:00:00.000Z' },
  }, {
    currentLocation: 'A1',
    progressToNext: 0,
    path: [],
  });

  assert.equal(result.applied, false);
  assert.equal(result.edge.currentLocation, 'B2');
});

test('applyArrivalUpdate CAS rejects stale hop', async () => {
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      currentLocation: 'B2',
      routeRevision: 5,
      stateRevision: 4,
      activeHop: { from: 'B1', to: 'B2', startedAt: '2026-06-14T10:05:00.000Z', revision: 5 },
    },
  });

  await applyArrivalUpdate(db, 'truck_1', {
    id: 'truck_1',
    currentLocation: 'B1',
    routeRevision: 3,
    activeHop: { startedAt: '2026-06-14T09:00:00.000Z' },
  }, {
    currentLocation: 'A1',
    progressToNext: 0,
    path: [],
  });

  const after = db.get('truck_1');
  assert.equal(after.currentLocation, 'B2');
  assert.equal(after.stateRevision, 4);
});

test('updateEdgeState strips stateRevision from payload when incrementing', async () => {
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      taskPhase: 'idle',
      stateRevision: 3,
      currentLocation: 'A1',
    },
  });

  await updateEdgeState(db, 'truck_1', {
    taskPhase: 'en_route_start',
    path: ['A2'],
    stateRevision: 3,
  });

  const after = db.get('truck_1');
  assert.equal(after.taskPhase, 'en_route_start');
  assert.equal(after.stateRevision, 4);
});

test('updateEdgeInDB increments stateRevision without conflicting $set', async () => {
  const db = createEdgeDb({
    crane_1: {
      id: 'crane_1',
      taskPhase: 'completing',
      stateRevision: 7,
      currentLocation: 'A1',
    },
  });

  await updateEdgeInDB({
    id: 'crane_1',
    taskPhase: 'idle',
    shipmentId: null,
    assignedShipment: null,
    currentLocation: 'A1',
    task: 'idle',
    path: [],
    remainingPath: [],
    pendingPath: [],
    nextNode: null,
    startNode: null,
    finalNode: null,
    eta: null,
    progressToNext: 0,
    journeyTime: null,
    stateRevision: 7,
  }, db);

  const after = db.get('crane_1');
  assert.equal(after.taskPhase, 'idle');
  assert.equal(after.stateRevision, 8);
});


test('pickup arrival survives split runtime persistence before destination completion', async () => {
  const { onArrival, resolveActiveLeg } = require('../lib/edge-workflow-engine');
  const db = createEdgeDb({ truck_1: {
    id: 'truck_1', currentLocation: 'A1', taskPhase: 'en_route_start',
    shipmentId: 'shipment_1', task: { phase: 'transport', startNode: 'A1', finalNode: 'B4' },
    startNode: 'A1', finalNode: 'B4', pickupCompleted: false,
  } });
  await onArrival('truck_1', db, db.get('truck_1'), 'A1');
  assert.equal(db.get('truck_1').pickupCompleted, true);
  assert.equal(resolveActiveLeg(db.get('truck_1')).kind, 'toDestination');
});
