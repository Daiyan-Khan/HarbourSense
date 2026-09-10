import { computeDevicePosition } from './devicePositions';
import { NODE_WIDTH, NODE_HEIGHT, DEVICE_SIZE } from './layout';

const positions = {
  A1: { x: 0, y: 0 },
  A2: { x: 250, y: 0 },
  B1: { x: 0, y: 200 },
};

describe('computeDevicePosition', () => {
  test('idle device centers on node', () => {
    const device = { id: 'd1', currentLocation: 'A1', progressToNext: 0 };
    const pos = computeDevicePosition(device, positions);
    expect(pos).toEqual({
      x: NODE_WIDTH / 2 - DEVICE_SIZE / 2,
      y: NODE_HEIGHT / 2 - DEVICE_SIZE / 2,
    });
  });

  test('idle device applies spread offset', () => {
    const device = { id: 'd1', currentLocation: 'A1', progressToNext: 0 };
    const pos = computeDevicePosition(device, positions, 0, { dx: 5, dy: -3 });
    expect(pos).toEqual({
      x: NODE_WIDTH / 2 - DEVICE_SIZE / 2 + 5,
      y: NODE_HEIGHT / 2 - DEVICE_SIZE / 2 - 3,
    });
  });

  test('moving device interpolates along horizontal edge anchors', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 50,
    };
    const pos = computeDevicePosition(device, positions, 50);

    const sourceX = NODE_WIDTH;
    const sourceY = NODE_HEIGHT / 2;
    const targetX = 250;
    const targetY = NODE_HEIGHT / 2;
    const midX = (sourceX + targetX) / 2 - DEVICE_SIZE / 2;
    const midY = sourceY - DEVICE_SIZE / 2;

    expect(pos.x).toBeCloseTo(midX, 5);
    expect(pos.y).toBeCloseTo(midY, 5);
  });

  test('moving device interpolates along vertical edge anchors', () => {
    const device = {
      id: 'd1',
      currentLocation: 'A1',
      nextNode: 'B1',
      progressToNext: 100,
    };
    const pos = computeDevicePosition(device, positions, 100);

    const targetX = NODE_WIDTH / 2 - DEVICE_SIZE / 2;
    const targetY = 200 - DEVICE_SIZE / 2;

    expect(pos.x).toBeCloseTo(targetX, 5);
    expect(pos.y).toBeCloseTo(targetY, 5);
  });

  test('returns zero when location is unknown', () => {
    const device = { id: 'd1', currentLocation: 'Z9', progressToNext: 0 };
    expect(computeDevicePosition(device, positions)).toEqual({ x: 0, y: 0 });
  });
});
