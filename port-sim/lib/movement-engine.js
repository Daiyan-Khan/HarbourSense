const { simulationSleep, simulationNow, currentContext } = require('./demo-runtime');
const { isAtWorkLocation } = require('./edge-phases');
const { updateEdgeState, applyArrivalUpdate, persistMovementProgress } = require('./edge-state');
const { enqueueEdgeWork } = require('./mqtt-handlers');
const { buildArrivalUpdates, onArrival, onPathEmpty, recoverPath } = require('./edge-workflow-engine');const { debugLog, debugWarn, hopTrace } = require('./sim-debug');
const { runtimeCollection, loadMergedEdge } = require('./edge-collections');
const {
  promoteRouteAtBoundary,
  deriveNextNode,
  advancePathAfterArrival,
} = require('./edge-route-state');
const {
  isMidTransit,
  isAtHopBoundary,
  buildActiveHop,
  loadGraphMap,
  alignPathToLocation,
  trimPathFromCurrent,
  isAdjacent,
  validatePathAdjacency,
  logPathRepairFailed,
  rebuildPathFromNextNode,
} = require('./route-state');

const NODE_BASE_DISTANCE = 100;
const EDGE_SPEEDS = {
  truck: 10,
  truck_tempo: 10,
  truck_delivery: 10,
  agv: 8,
  conveyor: 5,
  crane: 4,
  forklift: 4,
  robot: 6,
  unknown: 5,
};

let graphCache = null;
let graphCacheAt = 0;
const GRAPH_CACHE_MS = 30000;

const CLEARED_ROUTE_FIELDS = {
  path: [],
  nextNode: null,
  progressToNext: 0,
  eta: null,
  activeHop: null,
};

async function persistClearedRouteFields(edgeId, db, env = process.env) {
  await enqueueEdgeWork(edgeId, async () => {
    await updateEdgeState(db, edgeId, CLEARED_ROUTE_FIELDS, env);
  });
}

/**
 * Movement-engine resolves a hop path; workflow-engine owns recovery when empty.
 */
async function resolvePathForHop(edgeId, edge, db, graphMap, env, simulatorSettings, rawPathLen) {
  let working = { ...edge };
  working.path = alignPathToLocation(working.path || [], working.currentLocation, graphMap, true);

  if (!working.path || working.path.length === 0) {
    const emptyResult = await onPathEmpty(edgeId, working, db, graphMap, env, {
      persistFn: (fn) => enqueueEdgeWork(edgeId, fn),
      simulatorSettings,
    });
    if (emptyResult.action === 'transition' || emptyResult.action === 'force_idle') {
      return { edge: emptyResult.edge, phaseAdvanced: true, pathReady: false };
    }
    const recovered = recoverPath(working, graphMap);
    if (recovered?.path?.length) {
      working = { ...working, ...recovered };
      working.path = alignPathToLocation(working.path, working.currentLocation, graphMap, true);
      await enqueueEdgeWork(edgeId, async () => {
        await updateEdgeState(db, edgeId, {
          path: working.path,
          nextNode: working.nextNode ?? deriveNextNode(working.path),
          ...(recovered.progressToNext !== undefined ? { progressToNext: recovered.progressToNext } : {}),
          ...(recovered.eta !== undefined ? { eta: recovered.eta } : {}),
          ...(recovered.activeHop !== undefined ? { activeHop: recovered.activeHop } : {}),
        }, env);
      });
    } else if (shouldResumeActiveHop(working)) {
      const rebuilt = rebuildPathFromNextNode(working, graphMap);
      const trimmed = alignPathToLocation(rebuilt, working.currentLocation, graphMap);
      const path = trimmed.length > 0 ? trimmed : [working.nextNode];
      working = { ...working, path, nextNode: path[0] };
      await enqueueEdgeWork(edgeId, async () => {
        await updateEdgeState(db, edgeId, { path, nextNode: path[0] }, env);
      });
    } else if (rawPathLen > 0 || working.nextNode) {
      logPathRepairFailed(edgeId, working, env);
      await persistClearedRouteFields(edgeId, db, env);
      return { edge: working, phaseAdvanced: false, pathReady: false };
    } else {
      return { edge: working, phaseAdvanced: false, pathReady: false };
    }
  }

  let currentLocation = working.currentLocation;
  let nextNode = working.path[0];

  if (Object.keys(graphMap).length > 0 && !isAdjacent(graphMap, currentLocation, nextNode)) {
    const rebuilt = rebuildPathFromNextNode(working, graphMap);
    const trimmed = alignPathToLocation(rebuilt, currentLocation, graphMap);
    if (trimmed.length > 0 && isAdjacent(graphMap, currentLocation, trimmed[0])) {
      working.path = trimmed;
      nextNode = trimmed[0];
      await enqueueEdgeWork(edgeId, async () => {
        await updateEdgeState(db, edgeId, { path: trimmed, nextNode: trimmed[0] }, env);
      });
    } else {
      const validated = validatePathAdjacency(working.path, graphMap);
      if (!validated) {
        logPathRepairFailed(edgeId, working, env);
        await persistClearedRouteFields(edgeId, db, env);
        return { edge: working, phaseAdvanced: false, pathReady: false };
      }
      working.path = validated;
      nextNode = working.path[0];
    }
  }

  return { edge: working, phaseAdvanced: false, pathReady: true, nextNode, currentLocation };
}
async function getGraphMap(db) {
  const now = simulationNow();
  if (graphCache && now - graphCacheAt < GRAPH_CACHE_MS) {
    return graphCache;
  }
  graphCache = await loadGraphMap(db);
  graphCacheAt = now;
  return graphCache;
}

function resetGraphCacheForTests() {
  graphCache = null;
  graphCacheAt = 0;
}

function nodeDistance(a, b) {
  if (!a || typeof a !== 'string' || !b || typeof b !== 'string') {
    return 0;
  }
  const matchA = a.match(/([A-Z]+)([0-9]+)/);
  const matchB = b.match(/([A-Z]+)([0-9]+)/);
  if (!matchA || !matchB) {
    return 0;
  }
  const colA = matchA[1];
  const rowA = parseInt(matchA[2], 10);
  const colB = matchB[1];
  const rowB = parseInt(matchB[2], 10);
  return Math.abs(rowA - rowB) + Math.abs(colA.charCodeAt(0) - colB.charCodeAt(0));
}

function applyPathSuggestion(edgeId, edge, suggestionsByEdge, env = process.env, graphMap = {}) {
  const suggestion = suggestionsByEdge[edgeId] || {};
  if (!suggestion.suggestedPath || !Array.isArray(suggestion.suggestedPath) || suggestion.suggestedPath.length === 0) {
    return { edge, suggestion: {} };
  }

  if (!isAtHopBoundary(edge)) {
    debugWarn(
      env,
      `[DEBUG SIM] Queued path suggestion for ${edgeId}: mid-transit (progress=${edge.progressToNext})`,
    );
    edge.pendingPath = suggestion.suggestedPath;
    if (suggestion.routeRevision !== undefined) {
      edge.routeRevision = suggestion.routeRevision;
    }
    delete suggestionsByEdge[edgeId];
    return { edge, suggestion: {} };
  }

  const aligned = alignPathToLocation(suggestion.suggestedPath, edge.currentLocation, graphMap);
  const suggestedStart = suggestion.suggestedPath[0];
  const canApply = aligned.length > 0 && (
    suggestedStart === edge.currentLocation
    || suggestion.suggestedPath.includes(edge.currentLocation)
    || (Object.keys(graphMap).length > 0 && isAdjacent(graphMap, edge.currentLocation, aligned[0]))
  );
  if (canApply) {
    edge.path = aligned;
    debugLog(env, `[DEBUG SIM] Applied path suggestion for ${edgeId}: ${edge.path.join(' -> ')}`);
  } else {
    debugWarn(
      env,
      `[DEBUG SIM] Stashed stale path suggestion for ${edgeId}: could not align to ${edge.currentLocation}`,
    );
    edge.pendingPath = suggestion.suggestedPath;
  }
  delete suggestionsByEdge[edgeId];
  return { edge, suggestion };
}

function applyPendingPathAtBoundary(edge, graphMap = {}) {
  if (!isAtHopBoundary(edge)) {
    return edge;
  }
  const promoted = promoteRouteAtBoundary(edge, graphMap);
  if (promoted) {
    return { ...edge, ...promoted };
  }
  return edge;
}

function buildArrivalPayload(edge, arrivedNode, remainingPath) {
  const atDestination = remainingPath.length === 0 && edge && isAtWorkLocation(edge);
  const base = {
    currentLocation: arrivedNode,
    taskPhase: edge ? edge.taskPhase : 'unknown',
    routeRevision: edge?.routeRevision ?? 0,
  };
  if (atDestination) {
    return {
      ...base,
      status: 'arrived',
    };
  }
  return {
    ...base,
    remainingPath,
    finalNode: edge ? edge.finalNode : null,
    status: 'arrived',
    traveled: arrivedNode,
  };
}

function shouldResumeActiveHop(edge) {
  if (!edge.activeHop || !isMidTransit(edge)) {
    return false;
  }
  const hop = edge.activeHop;
  return hop.from === edge.currentLocation && hop.to === edge.nextNode;
}

async function simulateMovement(edgeId, db, device, options = {}) {
  const {
    suggestionsByEdge = {},
    simulatorSettings = {},
    env = process.env,
  } = options;

  debugLog(env, `[DEBUG SIM] Starting simulateMovement for ${edgeId}`);
  let edge = await runtimeCollection(db).findOne({ id: edgeId });
  if (!edge) {
    debugLog(env, `[DEBUG SIM] No edge found for ${edgeId} - skip`);
    return { moved: false };
  }

  const rawPathLen = edge.path ? edge.path.length : 0;
  const graphMap = await getGraphMap(db);

  edge = applyPendingPathAtBoundary(edge, graphMap);
  const { edge: edgeWithSuggestion, suggestion } = applyPathSuggestion(edgeId, edge, suggestionsByEdge, env, graphMap);
  edge = edgeWithSuggestion;

  if (edge.path && edge.path.length > 0 && (edge.pendingPath?.length || suggestion.suggestedPath)) {
    await enqueueEdgeWork(edgeId, async () => {
      await updateEdgeState(db, edgeId, {
        path: edge.path,
        pendingPath: edge.pendingPath || [],
      }, env);
    });
  }
  if (isMidTransit(edge) && !shouldResumeActiveHop(edge)) {
    debugLog(env, `[DEBUG SIM] Edge ${edgeId} mid-transit without resumable hop; deferring movement`);
    return { moved: false, edge };
  }

  edge.path = alignPathToLocation(edge.path || [], edge.currentLocation, graphMap, true);

  const pathResult = await resolvePathForHop(
    edgeId,
    edge,
    db,
    graphMap,
    env,
    simulatorSettings,
    rawPathLen,
  );
  if (pathResult.phaseAdvanced) {
    return { moved: false, edge: pathResult.edge, phaseAdvanced: true };
  }
  if (!pathResult.pathReady) {
    debugLog(env, `[DEBUG SIM] No path available for ${edgeId}`);
    return { moved: false, edge: pathResult.edge };
  }

  edge = pathResult.edge;
  let currentLocation = pathResult.currentLocation;
  let nextNode = pathResult.nextNode;

  if (currentLocation === nextNode) {    const remainingAfterSkip = advancePathAfterArrival(edge.path, nextNode);
    const skipUpdates = buildArrivalUpdates(edge, nextNode, remainingAfterSkip);
    await enqueueEdgeWork(edgeId, async () => {
      await updateEdgeState(db, edgeId, { ...skipUpdates, path: remainingAfterSkip }, env);
    });
    edge = await runtimeCollection(db).findOne({ id: edgeId });
    if (edge && edge.path.length === 0) {
      await onArrival(edgeId, db, edge, nextNode, env, (fn) => enqueueEdgeWork(edgeId, fn));
      return { moved: true, edge: await runtimeCollection(db).findOne({ id: edgeId }) };
    }
    nextNode = edge.path[0];
    currentLocation = edge.currentLocation;
  }

  const distance = nodeDistance(currentLocation, nextNode);
  const speedMultiplier = simulatorSettings.simSpeedMultiplier || 1;
  const distanceScale = simulatorSettings.simNodeDistanceScale ?? NODE_BASE_DISTANCE;
  const speed = (EDGE_SPEEDS[edge.type] || EDGE_SPEEDS.unknown) * speedMultiplier;
  const etaSeconds = Math.max(1, (distance * distanceScale) / speed);
  const suggestedEta = suggestion.eta ? parseFloat(suggestion.eta) : etaSeconds;
  const updateInterval = simulatorSettings.simProgressIntervalMs || 1000;
  const routeRevision = edge.routeRevision ?? 0;

  hopTrace(env, {
    edgeId,
    shipmentId: edge.shipmentId,
    event: 'hop_start',
    hopFrom: currentLocation,
    hopTo: nextNode,
    routeRevision,
    progressToNext: 0,
    pathLen: edge.path?.length ?? 0,
  });

  debugLog(
    env,
    `[DEBUG SIM] Edge ${edgeId} movement: ${currentLocation} -> ${nextNode} (ETA: ${suggestedEta}s)`,
  );

  const activeHop = buildActiveHop(currentLocation, nextNode, routeRevision);
  await enqueueEdgeWork(edgeId, async () => {
    await persistMovementProgress(db, edgeId, {
      nextNode,
      progressToNext: shouldResumeActiveHop(edge) ? edge.progressToNext : 0,
      eta: suggestedEta,
      activeHop,
    }, env);
  });
  const startTime = simulationNow();
  let intervalId = null;
  let hopTimedOut = false;
  const resumeProgress = shouldResumeActiveHop(edge) ? edge.progressToNext : 0;
  try {
    if (currentContext()) {
      // One awaited logical-time loop prevents timer accumulation on pause and
      // persists observed intermediate progress for snapshot/replay consumers.
      while (true) {
        await currentContext().checkpoint();
        const elapsed = (simulationNow() - startTime) / 1000;
        const ratio = Math.min(1, elapsed / suggestedEta);
        const progress = Math.min(100, resumeProgress + (100 - resumeProgress) * ratio);
        await persistMovementProgress(db, edgeId, {
          nextNode, progressToNext: progress, eta: Math.max(0, suggestedEta - elapsed), activeHop,
        }, env);
        await device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({
          id: edgeId, progress, remaining: Math.max(0, suggestedEta - elapsed),
          toNode: nextNode, currentLocation, routeRevision,
        }));
        if (ratio >= 1) break;
        await simulationSleep(Math.min(updateInterval, Math.max(1, (suggestedEta - elapsed) * 1000)));
      }
      hopTimedOut = true;
    } else {
    intervalId = setInterval(async () => {
      const elapsed = (simulationNow() - startTime) / 1000;
      const hopProgress = Math.min(100, Math.floor((elapsed / suggestedEta) * 100));
      const progress = Math.min(100, resumeProgress + Math.floor((100 - resumeProgress) * (hopProgress / 100)));
      const remaining = Math.max(0, Math.floor(suggestedEta - elapsed));
      await device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({
        id: edgeId,
        progress,
        remaining,
        toNode: nextNode,
        currentLocation,
        routeRevision,
      }));
      if (progress >= 100 && intervalId) {
        clearInterval(intervalId);
        intervalId = null;
      }
    }, updateInterval);

    await simulationSleep(suggestedEta * 1000);
    hopTimedOut = true;
    }
  } finally {
    if (intervalId) {
      clearInterval(intervalId);
    }
  }

  if (!hopTimedOut) {
    return { moved: false, edge };
  }
  const arrivedNode = nextNode;
  const pathHeadBefore = edge.path?.[0] ?? null;
  const locBefore = currentLocation;
  const remainingPath = advancePathAfterArrival(edge.path || [], arrivedNode);

  hopTrace(env, {
    edgeId,
    shipmentId: edge.shipmentId,
    event: 'hop_complete',
    hopFrom: currentLocation,
    hopTo: arrivedNode,
    routeRevision,
    progressToNext: 100,
    pathLen: remainingPath.length,
    pathHeadBefore,
    pathHeadAfter: remainingPath[0] ?? null,
    locBefore,
    locAfter: arrivedNode,
  });

  let arrivalApplied = false;
  try {
    const arrivalResult = await enqueueEdgeWork(edgeId, async () => applyArrivalUpdate(db, edgeId, edge, {
      ...buildArrivalUpdates(edge, arrivedNode, remainingPath),
      path: remainingPath,
    }, env));
    arrivalApplied = arrivalResult?.applied !== false;
    edge = arrivalResult?.edge ?? edge;
  } catch (err) {
    debugWarn(env, `[DEBUG SIM] Arrival failed for ${edgeId}: ${err.message}`);
    await enqueueEdgeWork(edgeId, async () => {
      await persistMovementProgress(db, edgeId, {
        progressToNext: 0,
        eta: null,
        activeHop: null,
      }, env);
    });
    return { moved: false, edge };
  }

  if (!arrivalApplied) {
    const fresh = edge || await runtimeCollection(db).findOne({ id: edgeId });
    const stillMidHop = fresh
      && fresh.currentLocation !== arrivedNode
      && (fresh.progressToNext ?? 0) > 0
      && (fresh.progressToNext ?? 0) < 100;
    if (stillMidHop) {
      debugWarn(env, `[DEBUG SIM] Arrival CAS miss for ${edgeId}; deferring traffic publish`);
      return { moved: false, edge: fresh };
    }
    edge = fresh;
  }

  if (!edge) {
    edge = await runtimeCollection(db).findOne({ id: edgeId });
  }
  if (!edge) {
    return { moved: true, edge: null };
  }  edge = applyPendingPathAtBoundary(edge, graphMap);
  if (edge?.pendingPath?.length && isAtHopBoundary(edge)) {
    const promoted = promoteRouteAtBoundary(edge, graphMap);
    if (promoted) {
      await enqueueEdgeWork(edgeId, async () => {
        await updateEdgeState(db, edgeId, promoted, env);
      });      edge = { ...edge, ...promoted };
    }
  }

  if (edge && edge.path.length === 0) {
    await onArrival(edgeId, db, edge, arrivedNode, env, (fn) => enqueueEdgeWork(edgeId, fn));
    edge = await runtimeCollection(db).findOne({ id: edgeId });
  }

  const finalRemaining = edge ? edge.path : remainingPath;
  const arrivalPayload = buildArrivalPayload(edge, arrivedNode, finalRemaining);
  await device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify(arrivalPayload));
  if (edge) {
    await db.collection('edgeHistory').insertOne({ ...arrivalPayload, timestamp: new Date() });
  }

  debugLog(env, `[DEBUG SIM] Edge ${edgeId} arrived at ${arrivedNode}`);
  return { moved: true, edge };
}

module.exports = {
  NODE_BASE_DISTANCE,
  EDGE_SPEEDS,
  nodeDistance,
  trimPathFromCurrent,
  alignPathToLocation,
  isMidTransit,
  rebuildPathFromNextNode,
  applyPathSuggestion,
  buildArrivalPayload,
  simulateMovement,
  getGraphMap,
  resetGraphCacheForTests,
  shouldResumeActiveHop,
  isAtHopBoundary,
};
