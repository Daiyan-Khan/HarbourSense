import {
  applyDevicePoll,
  buildDeviceDisplayMap,
  computeDisplayProgress,
  movementKey,
} from './deviceDisplayState';

describe('movementKey', () => {
  test('combines current location and next node', () => {
    expect(movementKey({ id: 'd1', currentLocation: 'A1', nextNode: 'A2' })).toBe('A1:A2');
  });
});

describe('applyDevicePoll', () => {
  const pollTs = 1000;

  test('creates initial overlay for new device', () => {
    const device = {
      id: 'd1',
      stateRevision: 1,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 20,
      eta: 10,
    };
    const overlay = applyDevicePoll(null, device, pollTs);
    expect(overlay).toEqual({
      stateRevision: 1,
      taskPhase: 'en_route_start',
      movementKey: 'A1:A2',
      baselineProgress: 20,
      serverProgress: 20,
      pollTimestamp: pollTs,
      eta: 10,
    });
  });

  test('resets baseline when stateRevision increases', () => {
    const prev = applyDevicePoll(null, {
      id: 'd1',
      stateRevision: 2,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 50,
      eta: 5,
    }, pollTs);

    const next = applyDevicePoll(prev, {
      id: 'd1',
      stateRevision: 3,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 10,
      eta: 8,
    }, pollTs + 500);

    expect(next.baselineProgress).toBe(10);
    expect(next.serverProgress).toBe(10);
    expect(next.stateRevision).toBe(3);
    expect(next.pollTimestamp).toBe(1500);
  });

  test('ignores stale poll with lower progress at same revision', () => {
    const prev = applyDevicePoll(null, {
      id: 'd1',
      stateRevision: 5,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 60,
      eta: 5,
    }, pollTs);

    const stale = applyDevicePoll(prev, {
      id: 'd1',
      stateRevision: 5,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 40,
      eta: 5,
    }, pollTs + 200);

    expect(stale).toBe(prev);
    expect(stale.serverProgress).toBe(60);
  });

  test('resets when movement key changes at same revision', () => {
    const prev = applyDevicePoll(null, {
      id: 'd1',
      stateRevision: 4,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 80,
      eta: 2,
    }, pollTs);

    const next = applyDevicePoll(prev, {
      id: 'd1',
      stateRevision: 4,
      taskPhase: 'en_route_start',
      currentLocation: 'A2',
      nextNode: 'A3',
      progressToNext: 5,
      eta: 10,
    }, pollTs + 100);

    expect(next.movementKey).toBe('A2:A3');
    expect(next.baselineProgress).toBe(5);
  });

  test('resets when taskPhase changes at same revision', () => {
    const prev = applyDevicePoll(null, {
      id: 'd1',
      stateRevision: 4,
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 80,
      eta: 2,
    }, pollTs);

    const next = applyDevicePoll(prev, {
      id: 'd1',
      stateRevision: 4,
      taskPhase: 'assigned',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 0,
      eta: 10,
    }, pollTs + 100);

    expect(next.taskPhase).toBe('assigned');
    expect(next.baselineProgress).toBe(0);
  });
});

describe('buildDeviceDisplayMap', () => {
  test('merges overlays for all live edges', () => {
    const edges = [
      { id: 'd1', stateRevision: 1, currentLocation: 'A1', nextNode: 'A2', progressToNext: 10, eta: 5 },
      { id: 'd2', stateRevision: 0, currentLocation: 'B1', nextNode: null, progressToNext: 0, eta: 0 },
    ];
    const map = buildDeviceDisplayMap({}, edges, 2000);
    expect(Object.keys(map)).toEqual(['d1', 'd2']);
    expect(map.d1.baselineProgress).toBe(10);
  });

  test('drops overlays for devices no longer present', () => {
    const prev = buildDeviceDisplayMap({}, [
      { id: 'd1', stateRevision: 1, currentLocation: 'A1', progressToNext: 0, eta: 0 },
      { id: 'd2', stateRevision: 1, currentLocation: 'B1', progressToNext: 0, eta: 0 },
    ], 1000);
    const next = buildDeviceDisplayMap(prev, [
      { id: 'd1', stateRevision: 2, currentLocation: 'A1', progressToNext: 0, eta: 0 },
    ], 2000);
    expect(Object.keys(next)).toEqual(['d1']);
  });
});

describe('computeDisplayProgress', () => {
  test('extrapolates from overlay baseline', () => {
    const overlay = {
      stateRevision: 1,
      movementKey: 'A1:A2',
      baselineProgress: 40,
      serverProgress: 40,
      pollTimestamp: 1000,
      eta: 10,
    };
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 40,
      eta: 10,
    };
    const result = computeDisplayProgress(overlay, device, 6000);
    expect(result).toBeCloseTo(70, 5);
  });
});
