const test = require('node:test');
const assert = require('node:assert/strict');

const { TaskPhase } = require('../lib/edge-phases');
const {
  resetExecutingGuard,
  resetMovingGuard,
  resetMovingGuardForTests,
  executingByEdge,
  movingByEdge,
  runSimulatedMovement,
  bootstrapClaimedTask,
  isStaleTransit,
} = require('../lib/edge-autonomous-loop');
const { handleTaskMessage } = require('../lib/mqtt-handlers');
const {
  transitionToAssigned,
  transitionToCompleting,
} = require('../lib/edge-phases');
const { updateEdgeState } = require('../lib/edge-state');
const { createSplitEdgeDb } = require('./test-db');

function createMockDb(initialEdges = {}) {
  const db = createSplitEdgeDb(initialEdges);
  const baseCollection = db.collection.bind(db);
  db.getEdge = (id) => db.get(id);
  db.collection = (name) => {
    if (name === 'edgeHistory' || name === 'graph') {
      return {
        async findOne() { return null; },
        async updateOne() {},
        async insertOne() {},
        find() { return { toArray: async () => [] }; },
      };
    }
    return baseCollection(name);
  };
  return db;
}

test('offload empty path at dock transitions en_route_start to assigned', async () => {
  const edge = {
    id: 'crane_1',
    type: 'crane',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'A1',
    path: [],
    task: { phase: 'offload', shipmentId: 'shipment_1' },
    shipmentId: 'shipment_1',
  };
  const db = createMockDb({ crane_1: edge });
  await updateEdgeState(db, 'crane_1', transitionToAssigned(edge));
  const after = db.getEdge('crane_1');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
});

test('empty path at finalNode transitions to completing', async () => {
  const edge = {
    id: 'truck_1',
    taskPhase: TaskPhase.ASSIGNED,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    shipmentId: 'shipment_2',
    task: { phase: 'transport', shipmentId: 'shipment_2' },
  };
  const db = createMockDb({ truck_1: edge });
  await updateEdgeState(db, 'truck_1', transitionToCompleting(edge));
  const after = db.getEdge('truck_1');
  assert.equal(after.taskPhase, TaskPhase.COMPLETING);
  assert.deepEqual(after.path, []);
});

test('task MQTT handler ignores busy edge without forced idle reset', async () => {
  const edge = {
    id: 'crane_1',
    taskPhase: TaskPhase.ASSIGNED,
    currentLocation: 'A1',
    task: { phase: 'transport', shipmentId: 'other' },
    shipmentId: 'other',
    path: ['A1', 'B1'],
  };
  const db = createMockDb({ crane_1: edge });
  const taskData = {
    phase: 'offload',
    shipmentId: 'shipment_99',
    task: { phase: 'offload', shipmentId: 'shipment_99' },
    startNode: 'A1',
    finalNode: 'A1',
    path: [],
  };

  await handleTaskMessage('crane_1', taskData, db);
  const after = db.getEdge('crane_1');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
  assert.equal(after.shipmentId, 'other');
});

test('task MQTT handler accepts idle edge and sets en_route_start', async () => {
  const edge = {
    id: 'crane_1',
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'A1',
    task: 'idle',
    shipmentId: null,
    startNode: null,
  };
  const db = createMockDb({ crane_1: edge });
  const taskData = {
    phase: 'offload',
    shipmentId: 'shipment_1',
    task: { phase: 'offload', shipmentId: 'shipment_1' },
    startNode: 'A1',
    finalNode: 'A1',
    path: [],
  };

  await handleTaskMessage('crane_1', taskData, db);
  const after = db.getEdge('crane_1');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
  assert.equal(after.shipmentId, 'shipment_1');
});

test('assigned offload with null finalNode but requiredPlace transitions to completing', async () => {
  const { handleAssignedPhase } = require('../lib/edge-autonomous-loop');
  const edge = {
    id: 'crane001',
    type: 'crane',
    taskPhase: TaskPhase.ASSIGNED,
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: null,
    path: [],
    shipmentId: 'shipment_24',
    task: {
      phase: 'offload',
      shipmentId: 'shipment_24',
      requiredPlace: 'A1',
      destNode: 'A1',
    },
  };
  const db = createMockDb({ crane001: edge });
  const device = { publish() {} };
  await handleAssignedPhase('crane001', edge, db, device, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('crane001');
  assert.equal(after.taskPhase, TaskPhase.COMPLETING);
});

test('resolveDestinationNode uses requiredPlace when finalNode is null', () => {
  const { resolveDestinationNode } = require('../lib/edge-phases');
  const dest = resolveDestinationNode({
    currentLocation: 'A1',
    finalNode: null,
    task: { requiredPlace: 'A1', phase: 'offload' },
  });
  assert.equal(dest, 'A1');
});

test('resetExecutingGuard clears in-memory guard', () => {
  executingByEdge.set('crane_1', true);
  resetExecutingGuard('crane_1');
  assert.equal(executingByEdge.has('crane_1'), false);
});

test('resetMovingGuard clears movement guard', () => {
  movingByEdge.set('truck_1', true);
  resetMovingGuard('truck_1');
  assert.equal(movingByEdge.has('truck_1'), false);
});

test('resetMovingGuardForTests clears all movement guards', () => {
  movingByEdge.set('truck_1', true);
  movingByEdge.set('truck_2', true);
  resetMovingGuardForTests();
  assert.equal(movingByEdge.size, 0);
});

test('runSimulatedMovement skips when movement already in progress', async () => {
  resetMovingGuardForTests();
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  resetEdgeQueuesForTests();

  let movementCalls = 0;
  const edge = {
    id: 'robot_guard',
    type: 'robot',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    path: ['C4', 'D4'],
    finalNode: 'D4',
    progressToNext: 0,
  };

  const db = {
    collection() {
      return {
        async findOne() {
          return { ...edge };
        },
        async updateOne(query, update) {
          if (update.$set) Object.assign(edge, update.$set);
          if (update.$pull) {
            const field = Object.keys(update.$pull)[0];
            edge[field] = (edge[field] || []).filter((v) => v !== update.$pull[field]);
          }
        },
        async insertOne() {},
      };
    },
  };

  movingByEdge.set('robot_guard', true);
  const moves = await runSimulatedMovement('robot_guard', edge, db, { publish() {} }, {
    simulatorSettings: { simProgressIntervalMs: 5 },
    suggestionsByEdge: {},
  });

  assert.equal(moves, 0);
  movingByEdge.delete('robot_guard');

  const first = runSimulatedMovement('robot_guard', edge, db, { publish() {} }, {
    simulatorSettings: { simProgressIntervalMs: 5 },
    suggestionsByEdge: { robot_guard: { eta: 0.05 } },
  });
  const second = runSimulatedMovement('robot_guard', edge, db, { publish() {} }, {
    simulatorSettings: { simProgressIntervalMs: 5 },
    suggestionsByEdge: {},
  });

  await Promise.all([first, second]);
  assert.equal(movingByEdge.has('robot_guard'), false);
});

test('handleEnRouteStartPhase warns on empty path stall', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const warnings = [];
  const original = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));
  try {
    const edge = {
      id: 'truck_stuck',
      taskPhase: TaskPhase.EN_ROUTE_START,
      currentLocation: 'B2',
      startNode: 'A1',
      finalNode: 'B4',
      path: [],
      shipmentId: 'shipment_stuck',
      task: { phase: 'transport', shipmentId: 'shipment_stuck' },
    };
    const db = createMockDb({ truck_stuck: edge });
    await handleEnRouteStartPhase('truck_stuck', edge, db, { publish() {} }, {
      simulatorSettings: { simLoopTickMs: 1 },
      suggestionsByEdge: {},
    });
    assert.ok(
      warnings.some((line) => line.includes('EMPTY_PATH_STALL') || line.includes('PICKUP_LEG_MISSING')),
    );
    assert.ok(warnings.some((line) => line.includes('shipment=shipment_stuck')));
    assert.equal(db.getEdge('truck_stuck').taskPhase, TaskPhase.EN_ROUTE_START);
  } finally {
    console.warn = original;
  }
});

test('handleAssignedPhase warns when empty path and not at work location', async () => {
  const { handleAssignedPhase } = require('../lib/edge-autonomous-loop');
  const warnings = [];
  const original = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));
  try {
    const edge = {
      id: 'truck_stuck2',
      taskPhase: TaskPhase.ASSIGNED,
      currentLocation: 'B2',
      startNode: 'A1',
      finalNode: 'B4',
      path: [],
      shipmentId: 'shipment_stuck2',
      task: { phase: 'transport', shipmentId: 'shipment_stuck2', requiredPlace: 'B4' },
    };
    const db = createMockDb({ truck_stuck2: edge });
    await handleAssignedPhase('truck_stuck2', edge, db, { publish() {} }, {
      simulatorSettings: { simLoopTickMs: 1 },
      suggestionsByEdge: {},
    });
    assert.ok(
      warnings.some((line) => line.includes('EMPTY_PATH_STALL') || line.includes('PICKUP_LEG_MISSING')),
    );
    assert.equal(db.getEdge('truck_stuck2').taskPhase, TaskPhase.ASSIGNED);
  } finally {
    console.warn = original;
  }
});

test('handleAssignedPhase resumes stale mid-transit with empty path from nextNode', async () => {
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  const { resetMovingGuardForTests, handleAssignedPhase } = require('../lib/edge-autonomous-loop');
  resetEdgeQueuesForTests();
  resetMovingGuardForTests();

  const edge = {
    id: 'truck_tempo_1',
    type: 'truck_tempo',
    taskPhase: TaskPhase.ASSIGNED,
    currentLocation: 'C5',
    startNode: 'C5',
    finalNode: 'B4',
    nextNode: 'B5',
    path: [],
    pendingPath: ['B5', 'B4'],
    progressToNext: 70,
    eta: 2,
    activeHop: { from: 'C5', to: 'B5', startedAt: new Date().toISOString(), revision: 0 },
    shipmentId: 'shipment_transport',
    task: { phase: 'transport', shipmentId: 'shipment_transport', requiredPlace: 'B4' },
  };

  const db = {
    collection(name) {
      if (name === 'edgeHistory') {
        return { async insertOne() {} };
      }
      return {
        async findOne(query) {
          return query.id === 'truck_tempo_1' ? { ...edge } : null;
        },
        async updateOne(query, update) {
          if (update.$set) Object.assign(edge, update.$set);
          if (update.$pull) {
            const field = Object.keys(update.$pull)[0];
            edge[field] = (edge[field] || []).filter((v) => v !== update.$pull[field]);
          }
        },
        async insertOne() {},
      };
    },
  };
  const device = { publish() {} };

  await handleAssignedPhase('truck_tempo_1', edge, db, device, {
    simulatorSettings: { simProgressIntervalMs: 5, simLoopTickMs: 1 },
    suggestionsByEdge: { truck_tempo_1: { eta: 0.05 } },
  });

  assert.equal(edge.currentLocation, 'B5');
  assert.equal(edge.progressToNext, 0);
  assert.equal(edge.nextNode, 'B4');
  assert.deepEqual(edge.path, ['B4']);
});

test('runSimulatedMovement clears orphaned movement guard after stale timeout', async () => {
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  const {
    resetMovingGuardForTests,
    runSimulatedMovement,
    movingByEdge,
    movementStartedAt,
  } = require('../lib/edge-autonomous-loop');
  resetEdgeQueuesForTests();
  resetMovingGuardForTests();

  const edge = {
    id: 'truck_orphan',
    type: 'truck_tempo',
    taskPhase: TaskPhase.ASSIGNED,
    currentLocation: 'C5',
    finalNode: 'B4',
    nextNode: 'B5',
    path: ['B5', 'B4'],
    progressToNext: 70,
    eta: 2,
    activeHop: { from: 'C5', to: 'B5', startedAt: new Date().toISOString(), revision: 0 },
    shipmentId: 'shipment_orphan',
    task: { phase: 'transport', shipmentId: 'shipment_orphan' },
  };

  const db = {
    collection() {
      return {
        async findOne() {
          return { ...edge };
        },
        async updateOne(query, update) {
          if (update.$set) Object.assign(edge, update.$set);
          if (update.$pull) {
            const field = Object.keys(update.$pull)[0];
            edge[field] = (edge[field] || []).filter((v) => v !== update.$pull[field]);
          }
        },
        async insertOne() {},
      };
    },
  };

  movingByEdge.set('truck_orphan', true);
  movementStartedAt.set('truck_orphan', Date.now() - 60_000);

  const moves = await runSimulatedMovement('truck_orphan', edge, db, { publish() {} }, {
    simulatorSettings: { simProgressIntervalMs: 5 },
    suggestionsByEdge: { truck_orphan: { eta: 0.05 } },
  });

  assert.equal(moves, 1);
  assert.equal(movingByEdge.has('truck_orphan'), false);
  assert.notEqual(edge.currentLocation, 'C5');
});

test('handleEnRouteStartPhase promotes pendingPath and starts movement', async () => {
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  const { resetMovingGuardForTests, handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetGraphCacheForTests } = require('../lib/movement-engine');
  resetEdgeQueuesForTests();
  resetMovingGuardForTests();
  resetGraphCacheForTests();

  const graphDocs = {
    C5: { id: 'C5', neighbors: { N: 'B5' } },
    B5: { id: 'B5', neighbors: { S: 'C5', N: 'B4' } },
    B4: { id: 'B4', neighbors: { S: 'B5' } },
  };

  const edge = {
    id: 'truck_tempo_1',
    type: 'truck_tempo',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'C5',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    pendingPath: ['B5', 'B4'],
    shipmentId: 'shipment_pending',
    task: { phase: 'transport', shipmentId: 'shipment_pending', finalNode: 'B4' },
  };

  const db = {
    collection(name) {
      if (name === 'graph') {
        return {
          async find() {
            return { toArray: async () => Object.values(graphDocs) };
          },
        };
      }
      if (name === 'edgeHistory') {
        return { async insertOne() {} };
      }
      return {
        async findOne(query) {
          return query.id === 'truck_tempo_1' ? { ...edge } : null;
        },
        async updateOne(query, update) {
          if (update.$set) Object.assign(edge, update.$set);
        },
        async insertOne() {},
      };
    },
  };

  const warnings = [];
  const original = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));

  try {
    await handleEnRouteStartPhase('truck_tempo_1', edge, db, { publish() {} }, {
      simulatorSettings: { simProgressIntervalMs: 5, simLoopTickMs: 1 },
      suggestionsByEdge: { truck_tempo_1: { eta: 0.05 } },
    });
    assert.ok(!warnings.some((line) => line.includes('EMPTY_PATH_STALL')));
    assert.ok(edge.path.length > 0 || edge.currentLocation !== 'C5');
  } finally {
    console.warn = original;
  }
});

test('handleEnRouteStartPhase promotes transport at pickup dock to assigned when path empty', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const edge = {
    id: 'truck_pickup',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    shipmentId: 'shipment_transport',
    task: { phase: 'transport', shipmentId: 'shipment_transport' },
  };
  const db = createMockDb({ truck_pickup: edge });
  await handleEnRouteStartPhase('truck_pickup', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('truck_pickup');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
});

test('handleEnRouteStartPhase at B4 warehouse promotes pendingPath toward pickup', async () => {
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  const { resetMovingGuardForTests, handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetGraphCacheForTests } = require('../lib/movement-engine');
  resetEdgeQueuesForTests();
  resetMovingGuardForTests();
  resetGraphCacheForTests();

  const graphDocs = {
    A1: { id: 'A1', neighbors: { E: 'A2' } },
    A2: { id: 'A2', neighbors: { W: 'A1', E: 'A3' } },
    A3: { id: 'A3', neighbors: { W: 'A2', E: 'A4' } },
    A4: { id: 'A4', neighbors: { W: 'A3', S: 'B4' } },
    B4: { id: 'B4', neighbors: { N: 'A4' } },
  };

  const edge = {
    id: 'truck_tempo_1',
    type: 'truck_tempo',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    nextNode: 'A4',
    path: [],
    pendingPath: ['A4', 'A3', 'A2', 'A1'],
    shipmentId: 'shipment_3',
    task: { phase: 'transport', shipmentId: 'shipment_3', requiredPlace: 'A1' },
  };

  const db = {
    collection(name) {
      if (name === 'graph') {
        return {
          async find() {
            return { toArray: async () => Object.values(graphDocs) };
          },
        };
      }
      if (name === 'edgeHistory') {
        return { async insertOne() {} };
      }
      return {
        async findOne(query) {
          return query.id === 'truck_tempo_1' ? { ...edge } : null;
        },
        async updateOne(query, update) {
          if (update.$set) Object.assign(edge, update.$set);
        },
        async insertOne() {},
      };
    },
  };

  const warnings = [];
  const original = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));

  try {
    await handleEnRouteStartPhase('truck_tempo_1', edge, db, { publish() {} }, {
      simulatorSettings: { simProgressIntervalMs: 5, simLoopTickMs: 1 },
      suggestionsByEdge: { truck_tempo_1: { eta: 0.05 } },
    });
    assert.ok(!warnings.some((line) => line.includes('EMPTY_PATH_STALL')));
    assert.notEqual(edge.currentLocation, 'B4');
  } finally {
    console.warn = original;
  }
});

test('handleEnRouteStartPhase does not shortcut transport at B4 before pickup to completing', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetWorkflowEngineForTests } = require('../lib/edge-workflow-engine');
  resetWorkflowEngineForTests();
  const edge = {
    id: 'truck_b4',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    shipmentId: 'shipment_3',
    task: { phase: 'transport', shipmentId: 'shipment_3' },
  };
  const db = createMockDb({ truck_b4: edge });
  await handleEnRouteStartPhase('truck_b4', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1, simTransportRecoveryMaxTicks: 3 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('truck_b4');
  assert.notEqual(after.taskPhase, TaskPhase.COMPLETING);
});

test('handleEnRouteStartPhase transitions transport to completing at final after pickup', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetWorkflowEngineForTests } = require('../lib/edge-workflow-engine');
  resetWorkflowEngineForTests();
  const edge = {
    id: 'truck_done',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    path: [],
    pickupCompleted: true,
    shipmentId: 'shipment_done',
    task: { phase: 'transport', shipmentId: 'shipment_done' },
  };
  const db = createMockDb({ truck_done: edge });
  await handleEnRouteStartPhase('truck_done', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('truck_done');
  assert.equal(after.taskPhase, TaskPhase.COMPLETING);
});

test('handleEnRouteStartPhase assigns robot store_move at same node', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetWorkflowEngineForTests } = require('../lib/edge-workflow-engine');
  resetWorkflowEngineForTests();
  const edge = {
    id: 'robot001',
    type: 'robot',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'B4',
    startNode: 'B4',
    finalNode: 'B4',
    path: [],
    shipmentId: 'shipment_store',
    task: { phase: 'store_move', shipmentId: 'shipment_store' },
  };
  const db = createMockDb({ robot001: edge });
  await handleEnRouteStartPhase('robot001', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('robot001');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
});

test('handleEnRouteStartPhase recovers stuck same-node delivery with stale task.path', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetWorkflowEngineForTests } = require('../lib/edge-workflow-engine');
  resetWorkflowEngineForTests();
  const edge = {
    id: 'truck_delivery_1',
    type: 'truck_delivery',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'E5',
    startNode: null,
    finalNode: null,
    path: [],
    pendingPath: [],
    shipmentId: 'shipment_4',
    task: {
      phase: 'delivery',
      shipmentId: 'shipment_4',
      startNode: 'E5',
      finalNode: 'E5',
      path: ['E5', 'D5', 'C5', 'B5', 'B4', 'B5', 'C5', 'D5', 'E5'],
    },
  };
  const db = createMockDb({ truck_delivery_1: edge });
  await handleEnRouteStartPhase('truck_delivery_1', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('truck_delivery_1');
  assert.equal(after.taskPhase, TaskPhase.COMPLETING);
});

test('handleEnRouteStartPhase completes same-node delivery at E5', async () => {
  const { handleEnRouteStartPhase } = require('../lib/edge-autonomous-loop');
  const { resetWorkflowEngineForTests } = require('../lib/edge-workflow-engine');
  resetWorkflowEngineForTests();
  const edge = {
    id: 'truck_delivery_1',
    type: 'truck_delivery',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'E5',
    startNode: 'E5',
    finalNode: 'E5',
    path: [],
    shipmentId: 'shipment_4',
    task: { phase: 'delivery', shipmentId: 'shipment_4' },
  };
  const db = createMockDb({ truck_delivery_1: edge });
  await handleEnRouteStartPhase('truck_delivery_1', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopTickMs: 1 },
    suggestionsByEdge: {},
  });
  const after = db.getEdge('truck_delivery_1');
  assert.equal(after.taskPhase, TaskPhase.COMPLETING);
});

test('handleIdlePhase bootstraps claimed backend assignment from Mongo task', async () => {
  const { handleIdlePhase, resetMovingGuardForTests } = require('../lib/edge-autonomous-loop');
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  resetMovingGuardForTests();
  resetEdgeQueuesForTests();
  const edge = {
    id: 'crane002',
    type: 'crane',
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'C1',
    shipmentId: 'shipment_2',
    assignedShipment: 'shipment_2',
    startNode: 'C1',
    finalNode: 'C1',
    pendingPath: [],
    task: {
      phase: 'offload',
      shipmentId: 'shipment_2',
      startNode: 'C1',
      finalNode: 'C1',
      requiredPlace: 'C1',
      assignmentEpoch: 1001,
    },
  };
  const db = createMockDb({ crane002: edge });
  await handleIdlePhase('crane002', edge, db, { publish() {} }, {
    simulatorSettings: { simLoopIdleDelayMs: 1 },
  });
  const after = db.getEdge('crane002');
  assert.equal(after.taskPhase, TaskPhase.ASSIGNED);
});

test('isStaleTransit returns false for in-progress hop within grace window', () => {
  const edge = {
    id: 'truck_1',
    taskPhase: TaskPhase.EN_ROUTE_START,
    currentLocation: 'A1',
    nextNode: 'A2',
    progressToNext: 40,
    eta: 10,
    path: ['A2', 'B2'],
    activeHop: {
      from: 'A1',
      to: 'A2',
      startedAt: new Date().toISOString(),
      revision: 1,
    },
    updatedAt: new Date(),
  };
  assert.equal(isStaleTransit(edge, 'truck_1', { simProgressIntervalMs: 1000 }), false);
});

test('bootstrapClaimedTask promotes idle claimed edge with pendingPath to en_route_start', async () => {
  const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');
  resetEdgeQueuesForTests();
  const edge = {
    id: 'truck_tempo_1',
    type: 'truck_tempo',
    taskPhase: TaskPhase.IDLE,
    currentLocation: 'B4',
    shipmentId: 'shipment_9',
    startNode: 'B4',
    finalNode: 'D2',
    pendingPath: ['B4', 'C4', 'D4', 'D3', 'D2'],
    assignmentEpoch: 42,
    task: {
      phase: 'transport',
      shipmentId: 'shipment_9',
      startNode: 'B4',
      finalNode: 'D2',
      assignmentEpoch: 42,
    },
  };
  const db = createMockDb({ truck_tempo_1: edge });
  const after = await bootstrapClaimedTask('truck_tempo_1', edge, db);
  assert.equal(after.taskPhase, TaskPhase.EN_ROUTE_START);
  assert.ok(Array.isArray(after.path));
});
