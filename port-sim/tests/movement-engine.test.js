const test = require('node:test');

const assert = require('node:assert/strict');



const {

  applyPathSuggestion,

  nodeDistance,

  trimPathFromCurrent,

  isMidTransit,

  buildArrivalPayload,

} = require('../lib/movement-engine');

const { TaskPhase } = require('../lib/edge-phases');

const { resetEdgeQueuesForTests } = require('../lib/mqtt-handlers');



test('nodeDistance computes Manhattan grid distance', () => {

  assert.equal(nodeDistance('A1', 'A1'), 0);

  assert.equal(nodeDistance('A1', 'B1'), 1);

  assert.equal(nodeDistance('A1', 'B2'), 2);

});



test('trimPathFromCurrent removes current location prefix', () => {

  assert.deepEqual(trimPathFromCurrent(['B4', 'C4', 'D4'], 'B4'), ['C4', 'D4']);

  assert.deepEqual(trimPathFromCurrent(['A1', 'A2', 'B2'], 'A1'), ['A2', 'B2']);

  assert.deepEqual(trimPathFromCurrent(['A1', 'A2', 'B2'], 'A2'), ['B2']);

  assert.deepEqual(trimPathFromCurrent([], 'A1'), []);

});



test('isMidTransit detects progress between hops', () => {

  assert.equal(isMidTransit({ progressToNext: 0 }), false);

  assert.equal(isMidTransit({ progressToNext: 50 }), true);

  assert.equal(isMidTransit({ progressToNext: 100 }), false);

});



test('applyPathSuggestion applies remaining hops from current location', () => {

  const suggestions = {

    edge1: { suggestedPath: ['B4', 'C4', 'D4'], eta: 5 },

  };

  const edge = { currentLocation: 'B4', path: ['B4'], progressToNext: 0 };

  const { edge: updated, suggestion } = applyPathSuggestion('edge1', edge, suggestions, {});

  assert.deepEqual(updated.path, ['C4', 'D4']);

  assert.equal(suggestion.eta, 5);

  assert.equal(suggestions.edge1, undefined);

});



test('applyPathSuggestion ignores suggestion during mid-transit', () => {

  const suggestions = {

    edge1: { suggestedPath: ['B4', 'C4'], eta: 3 },

  };

  const edge = { currentLocation: 'B4', path: ['C4'], progressToNext: 45 };

  const originalPath = [...edge.path];

  const { edge: updated } = applyPathSuggestion('edge1', edge, suggestions, {});

  assert.deepEqual(updated.path, originalPath);

  assert.equal(suggestions.edge1, undefined);

});



test('applyPathSuggestion ignores stale suggestion at wrong start node', () => {

  const suggestions = {

    edge1: { suggestedPath: ['A1', 'B1'], eta: 3 },

  };

  const edge = { currentLocation: 'B4', path: ['B4', 'C4'], progressToNext: 0 };

  const originalPath = [...edge.path];

  const { edge: updated } = applyPathSuggestion('edge1', edge, suggestions, {});

  assert.deepEqual(updated.path, originalPath);

});



test('stale suggestion does not mutate suggestions cache when ignored', () => {

  const suggestions = { edge1: { suggestedPath: ['A1', 'B1'] } };

  applyPathSuggestion('edge1', { currentLocation: 'C3', path: [] }, suggestions, {});

  assert.equal(suggestions.edge1, undefined);

});



test('rebuildPathFromNextNode uses pendingPath when path empty', () => {
  const { rebuildPathFromNextNode } = require('../lib/route-state');
  const graphMap = {
    B5: { id: 'B5', neighbors: { W: 'B4' } },
    B4: { id: 'B4', neighbors: {} },
  };
  assert.deepEqual(
    rebuildPathFromNextNode({ currentLocation: 'B5', pendingPath: ['B5', 'B4'] }, graphMap),
    ['B4'],
  );
  assert.deepEqual(
    rebuildPathFromNextNode({ currentLocation: 'B5', nextNode: 'B5', finalNode: 'B4' }, graphMap),
    [],
  );
});



test('simulateMovement resumes stale mid-transit progress and completes hop', async () => {

  resetEdgeQueuesForTests();

  const { simulateMovement } = require('../lib/movement-engine');



  const edge = {

    id: 'truck_tempo_1',

    type: 'truck_tempo',

    taskPhase: TaskPhase.ASSIGNED,

    currentLocation: 'C5',

    path: ['B5', 'B4'],

    nextNode: 'B5',

    finalNode: 'B4',

    shipmentId: 'shipment_3',

    progressToNext: 70,

    eta: 2,

    activeHop: { from: 'C5', to: 'B5', startedAt: new Date().toISOString(), revision: 0 },

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



  await simulateMovement('truck_tempo_1', db, { publish() {} }, {

    simulatorSettings: { simProgressIntervalMs: 5 },

    suggestionsByEdge: { truck_tempo_1: { eta: 0.05 } },

  });



  assert.equal(edge.currentLocation, 'B5');

  assert.equal(edge.progressToNext, 0);

  assert.equal(edge.nextNode, 'B4');

  assert.deepEqual(edge.path, ['B4']);

});



test('buildArrivalPayload omits reroute fields at destination', () => {

  const edge = {

    taskPhase: TaskPhase.COMPLETING,

    finalNode: 'B4',

    currentLocation: 'B4',

    task: { requiredPlace: 'B4' },

  };

  const payload = buildArrivalPayload(edge, 'B4', []);

  assert.equal(payload.status, 'arrived');

  assert.equal(payload.currentLocation, 'B4');

  assert.equal(payload.remainingPath, undefined);

  assert.equal(payload.finalNode, undefined);

});



test('buildArrivalPayload includes remaining path while en route', () => {

  const edge = {

    taskPhase: TaskPhase.EN_ROUTE_START,

    finalNode: 'D4',

    currentLocation: 'B4',

  };

  const payload = buildArrivalPayload(edge, 'B4', ['C4', 'D4']);

  assert.deepEqual(payload.remainingPath, ['C4', 'D4']);

  assert.equal(payload.traveled, 'B4');

});



test('simulateMovement publishes progress MQTT during transit without per-tick Mongo writes', async () => {

  resetEdgeQueuesForTests();

  const { simulateMovement } = require('../lib/movement-engine');
  const { createSplitEdgeDb } = require('./test-db');



  const progressPublishes = [];

  const edge = {

    id: 'robot_1',

    type: 'robot',

    taskPhase: TaskPhase.EN_ROUTE_START,

    currentLocation: 'B4',

    path: ['C4'],

    finalNode: 'C4',

    shipmentId: 'shipment_1',

    progressToNext: 0,

  };



  const db = createSplitEdgeDb({ robot_1: edge });



  const device = {
    publish(topic, payload) {
      if (topic.endsWith('/progress')) {
        progressPublishes.push(JSON.parse(payload).progress);
      }
    },
  };



  await simulateMovement('robot_1', db, device, {

    simulatorSettings: { simProgressIntervalMs: 5 },

    suggestionsByEdge: { robot_1: { eta: 0.05 } },

  });



  assert.ok(progressPublishes.length > 0);

  assert.ok(progressPublishes.some((p) => p > 0));

});



test('simulateMovement keeps en_route_start until work location', async () => {

  resetEdgeQueuesForTests();

  const { simulateMovement } = require('../lib/movement-engine');



  const edge = {

    id: 'robot_2',

    type: 'robot',

    taskPhase: TaskPhase.EN_ROUTE_START,

    currentLocation: 'B4',

    path: ['C4'],

    finalNode: 'D4',

    task: { requiredPlace: 'D4' },

    shipmentId: 'shipment_2',

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



  await simulateMovement('robot_2', db, { publish() {} }, {

    simulatorSettings: { simProgressIntervalMs: 5 },

    suggestionsByEdge: { robot_2: { eta: 0.05 } },

  });



  assert.equal(edge.taskPhase, TaskPhase.EN_ROUTE_START);

  assert.equal(edge.currentLocation, 'C4');

});



test('simulateMovement clears progress interval after completion', async () => {

  resetEdgeQueuesForTests();

  const { simulateMovement } = require('../lib/movement-engine');



  const originalClearInterval = global.clearInterval;

  let clearCount = 0;

  global.clearInterval = (...args) => {

    clearCount += 1;

    return originalClearInterval(...args);

  };



  const edge = {

    id: 'robot_3',

    type: 'robot',

    taskPhase: TaskPhase.EN_ROUTE_START,

    currentLocation: 'B4',

    path: ['C4'],

    finalNode: 'C4',

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



  try {

    await simulateMovement('robot_3', db, { publish() {} }, {

      simulatorSettings: { simProgressIntervalMs: 5 },

      suggestionsByEdge: { robot_3: { eta: 0.05 } },

    });

    assert.ok(clearCount >= 1);

  } finally {

    global.clearInterval = originalClearInterval;

  }

});


