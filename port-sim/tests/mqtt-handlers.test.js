const test = require('node:test');
const assert = require('node:assert/strict');

const {
  handleShipmentMessage,
  handleTaskMessage,
  handleRouteMessage,
  handleTrafficMessage,
  enqueueEdgeWork,
  resetEdgeQueuesForTests,
  canAcceptTaskRejectionReason,
  isStaleAssignmentEpoch,
  isStaleRouteRevision,
} = require('../lib/mqtt-handlers');
const { TaskPhase } = require('../lib/edge-phases');
const { createSplitEdgeDb } = require('./test-db');

function createShipmentsDb(initial = {}) {
  const store = new Map(Object.entries(initial));
  return {
    collection(name) {
      if (name !== 'shipments') throw new Error(`Unexpected collection ${name}`);
      return {
        async findOne(query) {
          const doc = store.get(query.id);
          return doc ? { ...doc } : null;
        },
        async updateOne(query, update, options = {}) {
          const current = store.get(query.id) || { id: query.id };
          if (update.$set) Object.assign(current, update.$set);
          store.set(query.id, current);
        },
      };
    },
    get(id) {
      return store.get(id);
    },
  };
}

test('handleShipmentMessage is publish-only and does not mutate Mongo', async () => {
  const db = createShipmentsDb({
    shipment_27: {
      id: 'shipment_27',
      status: 'delivered',
      assignedEdges: [{ edgeId: 'truck_1', phase: 'delivery', completedAt: '2026-06-12T10:00:00Z' }],
    },
  });

  await handleShipmentMessage('shipment_27', { status: 'arrived', currentNode: 'A1' }, db);

  const after = db.get('shipment_27');
  assert.equal(after.status, 'delivered');
  assert.equal(after.assignedEdges.length, 1);
});

test('isStaleAssignmentEpoch detects older epoch', () => {
  const edge = { assignmentEpoch: 200 };
  assert.equal(isStaleAssignmentEpoch(edge, 100), true);
  assert.equal(isStaleAssignmentEpoch(edge, 200), false);
  assert.equal(isStaleAssignmentEpoch(edge, 300), false);
});

test('isStaleRouteRevision detects older revision', () => {
  const edge = { routeRevision: 5 };
  assert.equal(isStaleRouteRevision(edge, 3), true);
  assert.equal(isStaleRouteRevision(edge, 5), false);
  assert.equal(isStaleRouteRevision(edge, 6), false);
});

test('handleTaskMessage rejects stale assignmentEpoch without mutation', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      taskPhase: TaskPhase.IDLE,
      currentLocation: 'B2',
      assignmentEpoch: 500,
      routeRevision: 10,
      path: [],
    },
  });

  await handleTaskMessage('truck_1', {
    phase: 'transport',
    shipmentId: 'shipment_1',
    assignmentEpoch: 100,
    routeRevision: 1,
    path: ['A1', 'A2'],
    startNode: 'A1',
    finalNode: 'B4',
  }, db);

  const after = db.get('truck_1');
  assert.equal(after.currentLocation, 'B2');
  assert.equal(after.taskPhase, TaskPhase.IDLE);
  assert.equal(after.assignmentEpoch, 500);
});

test('handleRouteMessage rejects stale routeRevision without mutation', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      taskPhase: TaskPhase.EN_ROUTE_START,
      currentLocation: 'B2',
      routeRevision: 10,
      path: ['B3'],
      progressToNext: 0,
    },
  });

  await handleRouteMessage('truck_1', {
    path: ['A1', 'A2', 'B2', 'B3'],
    routeRevision: 2,
  }, db, {});

  const after = db.get('truck_1');
  assert.equal(after.currentLocation, 'B2');
  assert.deepEqual(after.path, ['B3']);
  assert.equal(after.routeRevision, 10);
});

test('handleTaskMessage after arrival keeps currentLocation when stale task arrives', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    truck_1: {
      id: 'truck_1',
      taskPhase: TaskPhase.EN_ROUTE_START,
      currentLocation: 'B2',
      assignmentEpoch: 500,
      routeRevision: 10,
      shipmentId: 'shipment_1',
      path: ['B3'],
      task: { phase: 'transport', shipmentId: 'shipment_1' },
    },
  });

  await handleTaskMessage('truck_1', {
    phase: 'transport',
    shipmentId: 'shipment_1',
    assignmentEpoch: 100,
    path: ['A1', 'A2'],
    startNode: 'A1',
    finalNode: 'B4',
  }, db);

  assert.equal(db.get('truck_1').currentLocation, 'B2');
});

function createEdgeDb(initial = {}) {
  return createSplitEdgeDb(initial);
}

test('canAcceptTaskRejectionReason returns EDGE_BUSY for assigned edge', () => {
  const reason = canAcceptTaskRejectionReason(
    { taskPhase: TaskPhase.ASSIGNED, task: { phase: 'offload' }, shipmentId: 's1' },
    { phase: 'transport', shipmentId: 's2' },
  );
  assert.equal(reason, 'EDGE_BUSY');
});

test('enqueueEdgeWork serializes concurrent work per edge', async () => {
  resetEdgeQueuesForTests();
  const order = [];
  const edgeId = 'crane_serial';

  const first = enqueueEdgeWork(edgeId, async () => {
    order.push('first-start');
    await new Promise((resolve) => setTimeout(resolve, 20));
    order.push('first-end');
  });
  const second = enqueueEdgeWork(edgeId, async () => {
    order.push('second');
  });

  await Promise.all([first, second]);
  assert.deepEqual(order, ['first-start', 'first-end', 'second']);
});

test('enqueueEdgeWork reentrant calls from inside worker do not deadlock', async () => {
  resetEdgeQueuesForTests();
  const edgeId = 'crane_serial';
  let innerRan = false;

  await enqueueEdgeWork(edgeId, async () => {
    await enqueueEdgeWork(edgeId, async () => {
      innerRan = true;
    });
  });

  assert.equal(innerRan, true);
});

test('handleTaskMessage rejects busy edge without mutation', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    crane_busy: {
      id: 'crane_busy',
      taskPhase: TaskPhase.ASSIGNED,
      currentLocation: 'A1',
      task: { phase: 'transport', shipmentId: 'other' },
      shipmentId: 'other',
      path: ['A1', 'B1'],
    },
  });

  await handleTaskMessage('crane_busy', {
    phase: 'offload',
    shipmentId: 'shipment_99',
    startNode: 'A1',
    path: [],
  }, db);

  const after = db.get('crane_busy');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
  assert.equal(after.shipmentId, 'other');
});

test('handleTrafficMessage stashes pendingPath for active edge with path', async () => {
  const suggestions = {};
  const db = createEdgeDb({
    truck_active: {
      id: 'truck_active',
      taskPhase: TaskPhase.EN_ROUTE_START,
      currentLocation: 'B4',
      path: ['C4', 'D4'],
    },
  });

  await handleTrafficMessage('truck_active', {
    path: ['B4', 'C4', 'E4'],
    suggestedPath: ['B4', 'C4', 'E4'],
    eta: 12,
  }, db, suggestions);

  const after = db.get('truck_active');
  assert.deepEqual(after.path, ['C4', 'D4']);
  assert.deepEqual(suggestions.truck_active?.pendingPath, ['B4', 'C4', 'E4']);
  assert.equal(suggestions.truck_active?.eta, 12);
});

test('handleTrafficMessage updates path only for idle edge without path', async () => {
  const suggestions = {};
  const db = createEdgeDb({
    truck_idle: {
      id: 'truck_idle',
      taskPhase: TaskPhase.IDLE,
      currentLocation: 'A1',
      path: [],
    },
  });

  await handleTrafficMessage('truck_idle', {
    path: ['A1', 'B1', 'C1'],
    suggestedPath: ['A1', 'B1', 'C1'],
  }, db, suggestions);

  const after = db.get('truck_idle');
  assert.deepEqual(after.path, ['B1', 'C1']);
});

test('handleTrafficMessage does not overwrite assigned edge path', async () => {
  const suggestions = {};
  const db = createEdgeDb({
    robot_assigned: {
      id: 'robot_assigned',
      taskPhase: TaskPhase.ASSIGNED,
      currentLocation: 'B4',
      path: ['C4'],
    },
  });

  await handleTrafficMessage('robot_assigned', {
    path: ['B4', 'D4'],
    suggestedPath: ['B4', 'D4'],
  }, db, suggestions);

  assert.deepEqual(db.get('robot_assigned').path, ['C4']);
  assert.deepEqual(suggestions['robot_assigned'].pendingPath, ['B4', 'D4']);
});

test('handleTaskMessage assigns idle crane already at offload dock', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    crane_dock: {
      id: 'crane_dock',
      taskPhase: TaskPhase.IDLE,
      currentLocation: 'A1',
      path: [],
      routeRevision: 0,
    },
  });

  await handleTaskMessage('crane_dock', {
    phase: 'offload',
    shipmentId: 'shipment_1',
    requiredPlace: 'A1',
    destNode: 'A1',
    path: [],
    routeRevision: 1,
  }, db);

  const after = db.get('crane_dock');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
  assert.equal(after.nextNode, null);
  assert.deepEqual(after.path, []);
});

test('handleTaskMessage routes remote crane to requiredPlace', async () => {
  resetEdgeQueuesForTests();
  const db = createEdgeDb({
    crane002: {
      id: 'crane002',
      taskPhase: TaskPhase.IDLE,
      currentLocation: 'C3',
      path: [],
      routeRevision: 0,
    },
  });

  await handleTaskMessage('crane002', {
    phase: 'offload',
    shipmentId: 'shipment_2',
    requiredPlace: 'A1',
    destNode: 'A1',
    path: ['C3', 'B3', 'A3', 'A1'],
    routeRevision: 1,
  }, db);

  const after = db.get('crane002');
  assert.equal(after.taskPhase, TaskPhase.EN_ROUTE_START);
  assert.equal(after.nextNode, 'B3');
  assert.deepEqual(after.path, ['B3', 'A3', 'A1']);
});



test('delayed command for a completed epoch cannot restart a finished task', async () => {
  const db = createSplitEdgeDb({ crane_1: {
    id: 'crane_1', taskPhase: 'idle', currentLocation: 'A1', completedAssignmentEpoch: 7,
    assignmentEpoch: 7, shipmentId: 's1', task: { shipmentId: 's1', phase: 'offload' },
  } });
  const before = structuredClone(db.get('crane_1'));
  await handleTaskMessage('crane_1', { assignmentEpoch: 7, shipmentId: 's1', phase: 'offload', startNode: 'A1', finalNode: 'A1' }, db);
  assert.deepEqual(db.get('crane_1'), before);
});
