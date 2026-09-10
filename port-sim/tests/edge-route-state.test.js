const test = require('node:test');
const assert = require('node:assert/strict');

const {
  deriveNextNode,
  advancePathAfterArrival,
  promoteRouteAtBoundary,
} = require('../lib/edge-route-state');
const { pathAfterHop } = require('../lib/route-state');

test('deriveNextNode returns first hop or null', () => {
  assert.equal(deriveNextNode(['A2', 'B2', 'B4']), 'A2');
  assert.equal(deriveNextNode([]), null);
});

test('advancePathAfterArrival pops front hop only', () => {
  assert.deepEqual(advancePathAfterArrival(['A1', 'A2', 'B2', 'B4'], 'A1'), ['A2', 'B2', 'B4']);
  assert.deepEqual(advancePathAfterArrival(['A2', 'B2', 'B4'], 'A2'), ['B2', 'B4']);
});

test('pathAfterHop consumes hop target from aligned path head', () => {
  assert.deepEqual(pathAfterHop(['A2', 'A1', 'B4'], 'A2'), ['A1', 'B4']);
});

test('promoteRouteAtBoundary promotes pendingPath at hop boundary', () => {
  const edge = {
    currentLocation: 'B1',
    progressToNext: 0,
    pendingPath: ['B1', 'B2', 'C2', 'D1'],
    routeRevision: 2,
  };
  const promoted = promoteRouteAtBoundary(edge, {});
  assert.deepEqual(promoted.path, ['B2', 'C2', 'D1']);
  assert.equal(promoted.nextNode, 'B2');
  assert.deepEqual(promoted.pendingPath, []);
});

test('multi-hop path advances one node per arrival sequence', () => {
  let path = ['A1', 'A2', 'B2', 'B4'];
  const hops = [];
  while (path.length > 0) {
    const next = path[0];
    hops.push(next);
    path = advancePathAfterArrival(path, next);
  }
  assert.deepEqual(hops, ['A1', 'A2', 'B2', 'B4']);
});
