import { extrapolateDisplayProgress, isDeviceAnimating } from '../map/deviceAnimation';

export function movementKey(device) {
  if (!device?.id) return '';
  return `${device.currentLocation ?? ''}:${device.nextNode ?? ''}`;
}

export function applyDevicePoll(prevOverlay, device, pollTimestamp) {
  const stateRevision = device?.stateRevision ?? 0;
  const taskPhase = device?.taskPhase ?? 'idle';
  const key = movementKey(device);
  const progress = device?.progressToNext ?? 0;
  const eta = device?.eta ?? 0;

  if (!prevOverlay) {
    return {
      stateRevision,
      taskPhase,
      movementKey: key,
      baselineProgress: progress,
      serverProgress: progress,
      pollTimestamp,
      eta,
    };
  }

  const revisionIncreased = stateRevision > prevOverlay.stateRevision;
  const keyChanged = key !== prevOverlay.movementKey;
  const phaseChanged = taskPhase !== prevOverlay.taskPhase;

  if (revisionIncreased || keyChanged || phaseChanged) {
    return {
      stateRevision,
      taskPhase,
      movementKey: key,
      baselineProgress: progress,
      serverProgress: progress,
      pollTimestamp,
      eta,
    };
  }

  if (progress >= prevOverlay.serverProgress) {
    return {
      ...prevOverlay,
      taskPhase,
      serverProgress: progress,
      baselineProgress: progress,
      pollTimestamp,
      eta: eta > 0 ? eta : prevOverlay.eta,
    };
  }

  return prevOverlay;
}

export function computeDisplayProgress(overlay, device, nowMs) {
  if (!device) return 0;
  if (!overlay) {
    return device.progressToNext ?? 0;
  }

  const deviceForExtrap = {
    ...device,
    progressToNext: overlay.baselineProgress,
    eta: overlay.eta,
  };

  return extrapolateDisplayProgress(
    deviceForExtrap,
    overlay.pollTimestamp,
    nowMs,
    overlay.eta,
  );
}

export function buildDeviceDisplayMap(prevMap, liveEdges, pollTimestamp) {
  const prev = prevMap || {};
  const next = { ...prev };
  const seenIds = new Set();

  (liveEdges || []).forEach((device) => {
    if (!device?.id) return;
    seenIds.add(device.id);
    next[device.id] = applyDevicePoll(prev[device.id], device, pollTimestamp);
  });

  Object.keys(next).forEach((id) => {
    if (!seenIds.has(id)) {
      delete next[id];
    }
  });

  return next;
}

export function hasAnimatingDevices(liveEdges, overlays, nowMs) {
  return (liveEdges || []).some((device) => {
    const overlay = overlays?.[device.id];
    const displayProgress = computeDisplayProgress(overlay, device, nowMs);
    return isDeviceAnimating(device, displayProgress);
  });
}
