import { extrapolateDisplayProgress, isDeviceAnimating } from './deviceAnimation';

describe('extrapolateDisplayProgress', () => {
  const pollTs = 1000;

  test('idle device returns raw progress unchanged', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      progressToNext: 0,
      eta: 0,
    };
    expect(extrapolateDisplayProgress(device, pollTs, 2500)).toBe(0);
  });

  test('arrived device at 100 stays at 100', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A2',
      nextNode: 'A2',
      progressToNext: 100,
      eta: 5,
    };
    expect(extrapolateDisplayProgress(device, pollTs, 5000)).toBe(100);
  });

  test('extrapolates progress based on eta and elapsed time', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 40,
      eta: 10,
    };
    const result = extrapolateDisplayProgress(device, pollTs, 6000);
    expect(result).toBeCloseTo(70, 5);
  });

  test('caps extrapolated progress at 100', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 80,
      eta: 2,
    };
    const result = extrapolateDisplayProgress(device, pollTs, 5000);
    expect(result).toBe(100);
  });

  test('uses minimum eta of 0.1 to avoid division issues', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 50,
      eta: 0,
    };
    const result = extrapolateDisplayProgress(device, pollTs, 1100);
    expect(result).toBeGreaterThan(50);
    expect(result).toBeLessThanOrEqual(100);
  });
});

describe('isDeviceAnimating', () => {
  test('returns true for moving device with eta', () => {
    const device = {
      nextNode: 'A2',
      progressToNext: 30,
      eta: 5,
    };
    expect(isDeviceAnimating(device, 30)).toBe(true);
  });

  test('returns true for in-transit device without eta', () => {
    const device = {
      taskPhase: 'en_route_start',
      nextNode: 'A2',
      progressToNext: 30,
      eta: null,
    };
    expect(isDeviceAnimating(device, 30)).toBe(true);
  });
});
