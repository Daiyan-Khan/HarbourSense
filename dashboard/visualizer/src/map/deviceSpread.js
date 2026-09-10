const MAX_SPREAD_PER_GROUP = 6;
const SPREAD_RADIUS = 38;

export function getLocationKey(device) {
  if (!device || !device.currentLocation) return null;

  const progress = device.progressToNext ?? 0;
  const nextNode = device.nextNode;

  if (progress > 0 && progress < 100 && nextNode && typeof nextNode === 'string') {
    return `${device.currentLocation}->${nextNode}`;
  }

  return device.currentLocation;
}

export function computeSpreadOffsets(devices) {
  const groups = new Map();

  (devices || []).forEach((device) => {
    const key = getLocationKey(device);
    if (!key) return;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(device);
  });

  const offsets = new Map();

  groups.forEach((groupDevices) => {
    const sorted = [...groupDevices].sort((a, b) => String(a.id).localeCompare(String(b.id)));

    sorted.forEach((device, index) => {
      if (sorted.length === 1) {
        offsets.set(device.id, { dx: 0, dy: 0 });
        return;
      }

      const groupSize = Math.min(sorted.length, MAX_SPREAD_PER_GROUP);
      const angle = ((index % MAX_SPREAD_PER_GROUP) * 2 * Math.PI) / groupSize;
      const baseRadius = groupSize <= 3 ? 23 : SPREAD_RADIUS;
      const radius = baseRadius * (1 + Math.floor(index / MAX_SPREAD_PER_GROUP));
      offsets.set(device.id, {
        dx: Math.round(Math.cos(angle) * radius),
        dy: Math.round(Math.sin(angle) * radius),
      });
    });
  });

  return offsets;
}
