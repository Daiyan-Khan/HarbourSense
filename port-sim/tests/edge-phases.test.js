const test = require('node:test');
const assert = require('node:assert/strict');

const {
  TaskPhase,
  IDLE_DEFAULTS,
  isNullSentinel,
  transitionToIdle,
  transitionToEn_routeStart,
  transitionToAssigned,
  transitionToCompleting,
  isRaceTolerantDuplicate,
} = require('../lib/edge-phases');

const idleEdge = { id: 'crane_1', taskPhase: TaskPhase.IDLE, task: 'idle', shipmentId: null };

test('transitionToIdle resets to canonical idle defaults', () => {
  const result = transitionToIdle({ id: 'x', taskPhase: TaskPhase.ASSIGNED });
  assert.equal(result.taskPhase, TaskPhase.IDLE);
  assert.deepEqual(result.path, []);
  assert.equal(result.shipmentId, null);
});

test('transitionToEn_routeStart accepts idle edge', () => {
  const taskData = {
    task: { phase: 'offload', shipmentId: 'shipment_1' },
    phase: 'offload',
    shipmentId: 'shipment_1',
    startNode: 'A1',
    finalNode: 'A1',
    path: [],
  };
  const result = transitionToEn_routeStart(idleEdge, taskData);
  assert.equal(result.taskPhase, TaskPhase.EN_ROUTE_START);
  assert.equal(result.shipmentId, 'shipment_1');
});

test('transitionToEn_routeStart rejects busy unrelated edge', () => {
  const busy = {
    ...idleEdge,
    taskPhase: TaskPhase.ASSIGNED,
    task: { phase: 'transport', shipmentId: 'shipment_2' },
    shipmentId: 'shipment_2',
  };
  assert.throws(
    () => transitionToEn_routeStart(busy, { phase: 'offload', shipmentId: 'shipment_1', task: {} }),
    /IDLE/,
  );
});

test('transitionToEn_routeStart accepts race-tolerant duplicate', () => {
  const racing = {
    ...idleEdge,
    taskPhase: TaskPhase.EN_ROUTE_START,
    task: { phase: 'offload', shipmentId: 'shipment_1' },
    shipmentId: 'shipment_1',
  };
  const taskData = { phase: 'offload', shipmentId: 'shipment_1', task: { phase: 'offload' } };
  assert.doesNotThrow(() => transitionToEn_routeStart(racing, taskData));
  assert.ok(isRaceTolerantDuplicate(racing, taskData));
});

test('transitionToAssigned requires en_route_start', () => {
  const enRoute = { ...idleEdge, taskPhase: TaskPhase.EN_ROUTE_START };
  assert.equal(transitionToAssigned(enRoute).taskPhase, TaskPhase.ASSIGNED);
  assert.throws(() => transitionToAssigned(idleEdge), /EN_ROUTE_START/);
});

test('transitionToCompleting requires assigned or en_route at destination', () => {
  const assigned = { ...idleEdge, taskPhase: TaskPhase.ASSIGNED, shipmentId: 's1' };
  const result = transitionToCompleting(assigned);
  assert.equal(result.taskPhase, TaskPhase.COMPLETING);
  assert.equal(result.shipmentId, 's1');
  assert.throws(() => transitionToCompleting(idleEdge), /COMPLETING/);
});

test('isNullSentinel recognizes legacy sentinels', () => {
  assert.ok(isNullSentinel('Null'));
  assert.ok(isNullSentinel(null));
  assert.ok(!isNullSentinel('A1'));
});

test('IDLE_DEFAULTS resets progressToNext to zero', () => {
  assert.equal(IDLE_DEFAULTS.progressToNext, 0);
  assert.deepEqual(IDLE_DEFAULTS.path, []);
  assert.equal(IDLE_DEFAULTS.taskPhase, TaskPhase.IDLE);
});

const {
  canTransitionToAssignedAtStartNode,
  isStationaryPhase,
} = require('../lib/edge-phases');

test('canTransitionToAssignedAtStartNode allows offload at dock', () => {
  const edge = {
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'A1',
    task: { phase: 'offload', shipmentId: 's1' },
  };
  assert.equal(canTransitionToAssignedAtStartNode(edge), true);
});

test('canTransitionToAssignedAtStartNode rejects transport pickup at dock', () => {
  const edge = {
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'B4',
    task: { phase: 'transport', shipmentId: 's1' },
  };
  assert.equal(canTransitionToAssignedAtStartNode(edge), false);
});

test('isStationaryPhase identifies offload and store phases', () => {
  assert.equal(isStationaryPhase('offload'), true);
  assert.equal(isStationaryPhase('transport'), false);
  assert.equal(isStationaryPhase('delivery'), false);
});

const {
  resolveDestinationNode,
  resolvePickupNode,
  hasCompletedPickupLeg,
  needsPickupLeg,
  isAtWorkLocation,
  canShortcutEmptyPathAtFinal,
} = require('../lib/edge-phases');

test('resolveDestinationNode for transport before pickup returns pickup node', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    task: { phase: 'transport', shipmentId: 's1', requiredPlace: 'A1' },
  };
  assert.equal(resolvePickupNode(edge), 'A1');
  assert.equal(hasCompletedPickupLeg(edge), false);
  assert.equal(needsPickupLeg(edge), true);
  assert.equal(resolveDestinationNode(edge), 'A1');
  assert.equal(isAtWorkLocation(edge), false);
});

test('resolveDestinationNode for transport at pickup returns warehouse', () => {
  const edge = {
    currentLocation: 'A1',
    startNode: 'A1',
    finalNode: 'B4',
    task: { phase: 'transport', shipmentId: 's1' },
  };
  assert.equal(needsPickupLeg(edge), false);
  assert.equal(resolveDestinationNode(edge), 'B4');
  assert.equal(isAtWorkLocation(edge), false);
});

test('canShortcutEmptyPathAtFinal rejects transport at warehouse before pickup', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    task: { phase: 'transport', shipmentId: 's1' },
  };
  assert.equal(canShortcutEmptyPathAtFinal(edge), false);
});

test('canShortcutEmptyPathAtFinal allows transport at warehouse after pickup completed', () => {
  const edge = {
    currentLocation: 'B4',
    startNode: 'A1',
    finalNode: 'B4',
    pickupCompleted: true,
    task: { phase: 'transport', shipmentId: 's1' },
  };
  assert.equal(hasCompletedPickupLeg(edge), true);
  assert.equal(canShortcutEmptyPathAtFinal(edge), true);
  assert.equal(isAtWorkLocation(edge), true);
});
