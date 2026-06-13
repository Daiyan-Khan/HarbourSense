/**
 * Synthetic crane telemetry aligned with edge-analyzer training ranges (shared-contract.md).
 */

const IDLE_RANGES = {
  motorTemp: [80, 85],
  vibration: [0.1, 0.35],
  energyUse: [100, 110],
};

const ACTIVE_RANGES = {
  motorTemp: [86, 90],
  vibration: [0.4, 0.6],
  energyUse: [112, 120],
};

function randomInRange([min, max]) {
  return min + Math.random() * (max - min);
}

function roundTelemetryValue(name, value) {
  if (name === 'motorTemp') {
    return Math.round(value * 10) / 10;
  }
  if (name === 'vibration') {
    return Math.round(value * 100) / 100;
  }
  return Math.round(value);
}

function isCraneActive(taskPhase) {
  return Boolean(taskPhase && taskPhase !== 'idle');
}

function buildCraneTelemetryPayload(craneId, { active = false } = {}) {
  const ranges = active ? ACTIVE_RANGES : IDLE_RANGES;
  return {
    craneId,
    motorTemp: roundTelemetryValue('motorTemp', randomInRange(ranges.motorTemp)),
    vibration: roundTelemetryValue('vibration', randomInRange(ranges.vibration)),
    energyUse: roundTelemetryValue('energyUse', randomInRange(ranges.energyUse)),
  };
}

module.exports = {
  ACTIVE_RANGES,
  IDLE_RANGES,
  buildCraneTelemetryPayload,
  isCraneActive,
};
