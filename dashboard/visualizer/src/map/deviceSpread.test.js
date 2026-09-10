import { getLocationKey, computeSpreadOffsets } from './deviceSpread';

describe('getLocationKey', () => {
  test('idle device uses current location', () => {
    const device = { id: 'a', currentLocation: 'A1', progressToNext: 0 };
    expect(getLocationKey(device)).toBe('A1');
  });

  test('moving device uses travel edge key', () => {
    const device = {
      id: 'a',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 25,
    };
    expect(getLocationKey(device)).toBe('A1->A2');
  });
});

describe('computeSpreadOffsets', () => {
  test('single device at location has zero offset', () => {
    const devices = [{ id: 'a', currentLocation: 'A1', progressToNext: 0 }];
    const offsets = computeSpreadOffsets(devices);
    expect(offsets.get('a')).toEqual({ dx: 0, dy: 0 });
  });

  test('two devices at same node get different offsets', () => {
    const devices = [
      { id: 'b', currentLocation: 'A1', progressToNext: 0 },
      { id: 'a', currentLocation: 'A1', progressToNext: 0 },
    ];
    const offsets = computeSpreadOffsets(devices);
    const offsetA = offsets.get('a');
    const offsetB = offsets.get('b');
    expect(offsetA).toBeDefined();
    expect(offsetB).toBeDefined();
    expect(offsetA.dx !== offsetB.dx || offsetA.dy !== offsetB.dy).toBe(true);
  });

  test('sorts by device id for stable layout', () => {
    const devices = [
      { id: 'z', currentLocation: 'A1', progressToNext: 0 },
      { id: 'a', currentLocation: 'A1', progressToNext: 0 },
    ];
    const first = computeSpreadOffsets(devices);
    const second = computeSpreadOffsets([...devices].reverse());
    expect(first.get('a')).toEqual(second.get('a'));
    expect(first.get('z')).toEqual(second.get('z'));
  });

  test('co-located markers leave enough room for both device hit targets', () => {
    const offsets = [...computeSpreadOffsets([{ id: 'a', currentLocation: 'A1' }, { id: 'b', currentLocation: 'A1' }]).values()];
    expect(Math.hypot(offsets[0].dx - offsets[1].dx, offsets[0].dy - offsets[1].dy)).toBeGreaterThanOrEqual(36);
  });
});
