const COMPONENT_FLAGS = {
  loop: 'SIM_DEBUG_LOOP',
  mqtt: 'SIM_DEBUG_MQTT',
  chain: 'SIM_DEBUG_CHAIN',
};

function simDebug(env = process.env) {
  return String(env.SIM_DEBUG || '').trim().toLowerCase() === 'true';
}

function componentDebugEnabled(component, env = process.env) {
  const flagName = COMPONENT_FLAGS[component];
  if (flagName) {
    const granular = String(env[flagName] || '').trim().toLowerCase();
    if (granular === 'true') return true;
    if (granular === 'false') return false;
  }
  return simDebug(env);
}

function resolveEnv(firstArg) {
  if (firstArg && typeof firstArg === 'object') {
    const keys = ['SIM_DEBUG', ...Object.values(COMPONENT_FLAGS)];
    if (keys.some((key) => key in firstArg)) {
      return firstArg;
    }
  }
  return process.env;
}

function isoTimestamp() {
  return new Date().toISOString();
}

function formatStructuredLine(level, event, fields = {}) {
  const parts = [isoTimestamp(), `[${level}]`, event];
  if (fields.component) parts.push(`component=${fields.component}`);
  if (fields.edgeId) parts.push(`edge=${fields.edgeId}`);
  if (fields.shipmentId) parts.push(`shipment=${fields.shipmentId}`);
  if (fields.phase) parts.push(`phase=${fields.phase}`);
  if (fields.reason) parts.push(`reason=${fields.reason}`);
  if (fields.detail !== undefined) {
    parts.push(typeof fields.detail === 'string' ? fields.detail : JSON.stringify(fields.detail));
  }
  return parts.join(' ');
}

function debugEvent(envOrPayload, maybePayload) {
  let env;
  let payload;
  if (maybePayload !== undefined) {
    env = resolveEnv(envOrPayload);
    payload = maybePayload || {};
  } else {
    env = process.env;
    payload = envOrPayload || {};
  }

  const { component, edgeId, shipmentId, phase, event, detail } = payload;
  if (!componentDebugEnabled(component, env)) {
    return;
  }

  console.log(formatStructuredLine('DEBUG', event || 'event', {
    component,
    edgeId,
    shipmentId,
    phase,
    detail,
  }));
}

function simWarn(event, fields = {}) {
  console.warn(formatStructuredLine('WARN', event, fields));
}

function simError(event, fields = {}) {
  console.error(formatStructuredLine('ERROR', event, fields));
}

function debugLog(...args) {
  const env = resolveEnv(args[0]);
  if (args[0] && typeof args[0] === 'object' && ('SIM_DEBUG' in args[0] || Object.values(COMPONENT_FLAGS).some((key) => key in args[0]))) {
    args.shift();
  }
  if (simDebug(env)) {
    console.log(isoTimestamp(), ...args);
  }
}

function debugWarn(...args) {
  const env = resolveEnv(args[0]);
  if (args[0] && typeof args[0] === 'object' && ('SIM_DEBUG' in args[0] || Object.values(COMPONENT_FLAGS).some((key) => key in args[0]))) {
    args.shift();
  }
  if (simDebug(env)) {
    console.warn(isoTimestamp(), ...args);
  }
}

/** Structured hop trace for movement diagnosis (Phase 1+). */
function hopTrace(envOrPayload, maybePayload) {
  let env;
  let payload;
  if (maybePayload !== undefined) {
    env = resolveEnv(envOrPayload);
    payload = maybePayload || {};
  } else {
    env = process.env;
    payload = envOrPayload || {};
  }
  if (!simDebug(env)) {
    return;
  }
  const {
    edgeId,
    shipmentId,
    event,
    hopFrom,
    hopTo,
    routeRevision,
    progressToNext,
    pathLen,
    detail,
  } = payload;
  const parts = [
    isoTimestamp(),
    '[HOP]',
    event || 'hop_event',
    edgeId ? `edge=${edgeId}` : null,
    shipmentId ? `shipment=${shipmentId}` : null,
    hopFrom && hopTo ? `${hopFrom}->${hopTo}` : null,
    routeRevision !== undefined ? `rev=${routeRevision}` : null,
    progressToNext !== undefined ? `progress=${progressToNext}` : null,
    pathLen !== undefined ? `pathLen=${pathLen}` : null,
    detail !== undefined
      ? (typeof detail === 'string' ? detail : JSON.stringify(detail))
      : null,
  ].filter(Boolean);
  console.log(parts.join(' '));
}

module.exports = {
  simDebug,
  componentDebugEnabled,
  isoTimestamp,
  formatStructuredLine,
  debugEvent,
  simWarn,
  simError,
  debugLog,
  debugWarn,
  hopTrace,
};
