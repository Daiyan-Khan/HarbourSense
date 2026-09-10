const DEFAULT_ETA_SEC = 5;
const IN_TRANSIT_PHASES = new Set(['en_route_start', 'assigned', 'relocating']);

function resolveEtaSeconds(device, overlayEta) {
  const deviceEta = device?.eta;
  if (typeof deviceEta === 'number' && deviceEta > 0) {
    return deviceEta;
  }
  if (typeof overlayEta === 'number' && overlayEta > 0) {
    return overlayEta;
  }
  return DEFAULT_ETA_SEC;
}

export function extrapolateDisplayProgress(device, pollTimestamp, nowMs, overlayEta = null) {
  const progressToNext = device?.progressToNext ?? 0;
  const nextNode = device?.nextNode;

  if (
    progressToNext <= 0
    || progressToNext >= 100
    || !nextNode
    || typeof nextNode !== 'string'
    || !pollTimestamp
  ) {
    return progressToNext;
  }

  const eta = resolveEtaSeconds(device, overlayEta);
  const elapsedSec = Math.max(0, (nowMs - pollTimestamp) / 1000);
  const rate = (100 - progressToNext) / Math.max(eta, 0.1);
  return Math.min(100, progressToNext + rate * elapsedSec);
}

export function isDeviceAnimating(device, displayProgress) {
  const progress = displayProgress ?? device?.progressToNext ?? 0;
  const nextNode = device?.nextNode;
  const taskPhase = device?.taskPhase || 'idle';
  const inTransit = IN_TRANSIT_PHASES.has(taskPhase);

  return (
    progress > 0
    && progress < 100
    && Boolean(nextNode)
    && (inTransit || (device?.eta ?? 0) > 0)
  );
}
