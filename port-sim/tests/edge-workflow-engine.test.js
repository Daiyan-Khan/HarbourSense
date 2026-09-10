const test = require('node:test');
const assert = require('node:assert/strict');

const { TaskPhase } = require('../lib/edge-phases');
const {
  resolveActiveLeg,
  resolveNextPhase,
  recoverPath,
  detectStall,
  acceptTask,
  buildArrivalUpdates,
  onPathEmpty,
  resolveTaskPhaseForAssignment,
  resetWorkflowEngineForTests,
} = require('../lib/edge-workflow-engine');

const graphMap = {
  A1: { id: 'A1', neighbors: { E: 'A2' } },
  A2: { id: 'A2', neighbors: { W: 'A1', E: 'A3' } },
  A3: { id: 'A3', neighbors: { W: 'A2', E: 'A4' } },
  A4: { id: 'A4', neighbors: { W: 'A3', S: 'B4' } },
  B4: { id: 'B4', neighbors: { N: 'A4' } },
};

test('resolveActiveLeg returns toPickup when transport pickup incomplete', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: false,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const leg = resolveActiveLeg(edge);
  assert.equal(leg.kind, 'toPickup');
  assert.equal(leg.targetNode, 'A1');
});

test('resolveActiveLeg returns toDestination after pickup completed', () => {
  const edge = {
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: true,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const leg = resolveActiveLeg(edge);
  assert.equal(leg.kind, 'toDestination');
  assert.equal(leg.targetNode, 'B4');
});

test('resolveActiveLeg returns stationary for store_move same node', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: 'B4',
    task: { phase: 'store_move', shipmentId: 's1' },
  };
  const leg = resolveActiveLeg(edge);
  assert.equal(leg.kind, 'stationary');
  assert.equal(leg.targetNode, 'B4');
});

test('resolveNextPhase does not shortcut transport at final before pickup', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: false,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  assert.equal(resolveNextPhase(edge), null);
});

test('resolveNextPhase shortcuts to completing at final after pickup', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: true,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const next = resolveNextPhase(edge);
  assert.equal(next.taskPhase, TaskPhase.COMPLETING);
  assert.equal(next.via, 'final_shortcut');
});

test('recoverPath promotes pendingPath toward pickup from warehouse', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    pendingPath: ['A4', 'A3', 'A2', 'A1'],
    progressToNext: 0,
    pickupCompleted: false,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const recovered = recoverPath(edge, graphMap);
  assert.ok(recovered?.path?.length > 0);
  assert.equal(recovered.path[0], 'A4');
  assert.equal(recovered.workflowLeg, 'toPickup');
});

test('detectStall returns PICKUP_LEG_MISSING not EMPTY_PATH_STALL at co-located warehouse', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    pickupCompleted: false,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const stall = detectStall(edge, 1);
  assert.equal(stall.code, 'PICKUP_LEG_MISSING');
});

test('acceptTask resets pickupCompleted on new task', () => {
  const edge = {
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: true,
    path: [],
    progressToNext: 0,
  };
  const updates = acceptTask(edge, {
    phase: 'transport',
    shipmentId: 's2',
    startNode: 'A1',
    finalNode: 'B4',
    path: ['A4', 'A3', 'A2', 'A1'],
  }, graphMap, { midTransit: false, atHopBoundary: true });
  assert.equal(updates.pickupCompleted, false);
  assert.equal(updates.workflowLeg, 'toPickup');
});

test('buildArrivalUpdates sets pickupCompleted on pickup arrival', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'A2',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: false,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const updates = buildArrivalUpdates(edge, 'A1', []);
  assert.equal(updates.pickupCompleted, true);
  assert.equal(updates.workflowLeg, 'toDestination');
});

test('onPathEmpty recovers pickup path instead of stalling at warehouse', async () => {
  resetWorkflowEngineForTests();
  const edge = {
    id: 'truck_tempo_1',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    pendingPath: ['A4', 'A3', 'A2', 'A1'],
    progressToNext: 0,
    pickupCompleted: false,
    shipmentId: 's1',
    task: { phase: 'transport', shipmentId: 's1' },
  };
  const store = new Map([['truck_tempo_1', { ...edge }]]);
  const db = {
    collection() {
      return {
        async findOne(query) {
          const doc = store.get(query.id);
          return doc ? { ...doc } : null;
        },
        async updateOne(query, update) {
          const current = store.get(query.id) || { id: query.id };
          if (update.$set) Object.assign(current, update.$set);
          store.set(query.id, current);
        },
      };
    },
  };

  const result = await onPathEmpty('truck_tempo_1', edge, db, graphMap);
  assert.equal(result.action, 'move');
  assert.ok(result.edge.path.length > 0);
});

test('resolveNextPhase assigns stationary store_move at same node', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: 'B4',
    path: [],
    task: { phase: 'store_move', shipmentId: 's1' },
  };
  const next = resolveNextPhase(edge);
  assert.equal(next.taskPhase, TaskPhase.ASSIGNED);
});

test('resolveTaskPhaseForAssignment assigns store_load at same node without en_route', () => {
  const edge = {
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: 'B4',
    pendingPath: ['B3', 'B2'],
  };
  const phase = resolveTaskPhaseForAssignment(
    edge,
    { phase: 'store_load', shipmentId: 'shipment_4', finalNode: 'B4', requiredPlace: 'B4' },
    [],
  );
  assert.equal(phase, TaskPhase.ASSIGNED);
});

test('acceptTask clears path for same-node store_load', () => {
  const edge = {
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: 'B4',
    path: [],
    pendingPath: ['B3', 'B2', 'C2'],
    progressToNext: 0,
  };
  const updates = acceptTask(
    edge,
    {
      phase: 'store_load',
      shipmentId: 'shipment_4',
      finalNode: 'B4',
      requiredPlace: 'B4',
      path: [],
    },
    graphMap,
    { midTransit: false, atHopBoundary: true },
  );
  assert.equal(updates.taskPhase, TaskPhase.ASSIGNED);
  assert.deepEqual(updates.path, []);
  assert.deepEqual(updates.pendingPath, []);
  assert.equal(updates.workflowLeg, 'stationary');
});

test('resolveNextPhase shortcuts same-node delivery at final to completing', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'E5',
    startNode: 'E5',
    finalNode: 'E5',
    path: [],
    task: { phase: 'delivery', shipmentId: 'shipment_4' },
  };
  const next = resolveNextPhase(edge);
  assert.equal(next.taskPhase, TaskPhase.COMPLETING);
  assert.equal(next.via, 'final_shortcut');
});

test('canShortcutEmptyPathAtFinal allows same-node delivery when edge.finalNode is null', () => {
  const { canShortcutEmptyPathAtFinal } = require('../lib/edge-phases');
  const edge = {
    currentLocation: 'E5',
    startNode: null,
    finalNode: null,
    task: { phase: 'delivery', finalNode: 'E5', startNode: 'E5' },
  };
  assert.equal(canShortcutEmptyPathAtFinal(edge), true);
});

test('recoverPath skips rebuild when same-node delivery at destination', () => {
  const edge = {
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'E5',
    startNode: null,
    finalNode: null,
    path: [],
    pendingPath: [],
    task: {
      phase: 'delivery',
      startNode: 'E5',
      finalNode: 'E5',
      path: ['E5', 'D5', 'C5', 'B5', 'B4', 'B5', 'C5', 'D5', 'E5'],
    },
  };
  const recovered = recoverPath(edge, graphMap);
  assert.equal(recovered, null);
});

test('acceptTask assigns same-node delivery at E5 without en_route', () => {
  const edge = {
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'E5',
    startNode: 'E5',
    finalNode: 'E5',
    path: [],
    pendingPath: ['E5'],
    progressToNext: 0,
  };
  const updates = acceptTask(
    edge,
    {
      phase: 'delivery',
      shipmentId: 'shipment_4',
      startNode: 'E5',
      finalNode: 'E5',
      pickupNode: 'E5',
      path: [],
    },
    graphMap,
    { midTransit: false, atHopBoundary: true },
  );
  assert.equal(updates.taskPhase, TaskPhase.ASSIGNED);
  assert.deepEqual(updates.path, []);
  assert.deepEqual(updates.pendingPath, []);
});


test('accepting transport at pickup records pickup before the first departing hop', () => {
  const updates = acceptTask({ currentLocation: 'A1', taskPhase: 'idle' },
    { shipmentId: 's1', phase: 'transport', startNode: 'A1', finalNode: 'B4', path: ['A1','A2','A3','A4','B4'], assignmentEpoch: 7 }, graphMap);
  assert.equal(updates.pickupCompleted, true);
  assert.equal(updates.acceptedAssignmentEpoch, 7);
  assert.equal(updates.workflowLeg, 'toDestination');
});
