const test = require('node:test');
const assert = require('node:assert/strict');

const {
  trimPathFromCurrent,
  alignPathToLocation,
  pathAfterHop,
  isAtHopBoundary,
  rebuildPathFromNextNode,
  validatePathAdjacency,
} = require('../lib/route-state');

const dockGraph = {
  A1: { neighbors: { E: 'A2', S: 'B1' } },
  A2: { neighbors: { W: 'A1', S: 'B2' } },
  B1: { neighbors: { N: 'A1', E: 'B2' } },
  B2: { neighbors: { N: 'A2', W: 'B1', E: 'B3' } },
  B3: { neighbors: { W: 'B2', E: 'B4' } },
  B4: { neighbors: { W: 'B3' } },
};

test('pathAfterHop removes first hop atomically', () => {
  assert.deepEqual(pathAfterHop(['B2', 'C2', 'D1'], 'B2'), ['C2', 'D1']);
});

test('pathAfterHop always consumes completed hop even when head mismatches', () => {
  assert.deepEqual(pathAfterHop(['A1', 'A2', 'A1', 'B4'], 'A2'), ['A2', 'A1', 'B4']);
});

test('alignPathToLocation avoids A1-A2 reverse hop at A2 with stale full path', () => {
  assert.deepEqual(
    alignPathToLocation(['A1', 'A2', 'B1', 'B4'], 'A2', dockGraph),
    ['B1', 'B4'],
  );
});

test('alignPathToLocation preserves loop path continuation at A2', () => {
  assert.deepEqual(
    alignPathToLocation(['A1', 'A2', 'A1', 'B4'], 'A2', dockGraph),
    ['A1', 'B4'],
  );
});

test('alignPathToLocation keeps hop-relative path at A1', () => {
  assert.deepEqual(
    alignPathToLocation(['A1', 'A2', 'B4'], 'A1', dockGraph),
    ['A2', 'B4'],
  );
});

test('rebuildPathFromNextNode realigns task.path after path cleared mid-route', () => {
  const edge = {
    currentLocation: 'A2',
    path: [],
    task: { path: ['A1', 'A2', 'B2', 'B3', 'B4'] },
  };
  assert.deepEqual(rebuildPathFromNextNode(edge, dockGraph), ['B2', 'B3', 'B4']);
});

test('isAtHopBoundary false during mid-transit', () => {
  assert.equal(isAtHopBoundary({ progressToNext: 40, activeHop: { from: 'B1', to: 'B2' } }), false);
  assert.equal(isAtHopBoundary({ progressToNext: 0 }), true);
});

test('validatePathAdjacency rejects teleport hops', () => {
  const graph = {
    B1: { neighbors: { E: 'B2' } },
    B2: { neighbors: { E: 'C2' } },
  };
  assert.equal(validatePathAdjacency(['B1', 'C2'], graph), null);
  assert.deepEqual(validatePathAdjacency(['B1', 'B2', 'C2'], graph), ['B1', 'B2', 'C2']);
});

test('rebuildPathFromNextNode prefers pendingPath', () => {
  const graph = {};
  assert.deepEqual(
    rebuildPathFromNextNode({ currentLocation: 'B1', pendingPath: ['B1', 'B2', 'C2', 'D1'] }, graph),
    ['B2', 'C2', 'D1'],
  );
  assert.deepEqual(trimPathFromCurrent(['B1', 'B2', 'C2'], 'B1'), ['B2', 'C2']);
});


test('remaining route preserves a pickup detour that revisits the current node', () => {
  const graph = { B1: { neighbors: { N: 'A1', E: 'B2' } }, A1: { neighbors: { S: 'B1' } } };
  const { alignPathToLocation } = require('../lib/route-state');
  assert.deepEqual(alignPathToLocation(['A1', 'B1', 'B2'], 'B1', graph, true), ['A1', 'B1', 'B2']);
  assert.deepEqual(alignPathToLocation(['A1', 'B1', 'B2'], 'B1', graph), ['B2']);
});
