const {
  TaskPhase,
  transitionToAssigned,
  transitionToCompleting,
  transitionToIdle,
  isAtWorkLocation,
  canTransitionToAssignedAtStartNode,
  canShortcutEmptyPathAtFinal,
  resolvePickupNode,
  resolveFinalNode,
  needsPickupLeg,
  hasCompletedPickupLeg,
  isMobilePhase,
  extractWorkflowPhase,
  isNullSentinel,
  isStationaryPhase,
} = require('./edge-phases');
const { updateEdgeState } = require('./edge-state');
const { debugEvent, simWarn } = require('./sim-debug');
const { runtimeCollection, loadMergedEdge } = require('./edge-collections');
const {
  alignPathToLocation,
  isAdjacent,
  isMidTransit,
  isAtHopBoundary,
  rebuildPathFromNextNode,
} = require('./route-state');
const { promoteRouteAtBoundary, deriveNextNode } = require('./edge-route-state');

const CLEARED_ROUTE_FIELDS = {
  path: [],
  nextNode: null,
  progressToNext: 0,
  eta: null,
  activeHop: null,
};

const pathRecoveryFailures = new Map();

function clearRouteFields(extra = {}) {
  return { ...CLEARED_ROUTE_FIELDS, ...extra };
}

function resetPathRecoveryFailures(edgeId) {
  pathRecoveryFailures.delete(edgeId);
}

function bumpPathRecoveryFailure(edgeId, maxTicks = 10) {
  const count = (pathRecoveryFailures.get(edgeId) || 0) + 1;
  pathRecoveryFailures.set(edgeId, count);
  return { count, maxTicks, exceeded: count >= maxTicks };
}

/**
 * Explicit transport leg model replacing scattered pickupCompleted checks.
 * @returns {{ kind: 'toPickup'|'toDestination'|'stationary', targetNode: string|null }}
 */
function resolveActiveLeg(edge) {
  const phase = extractWorkflowPhase(edge);
  if (isMobilePhase(phase)) {
    const pickup = resolvePickupNode(edge);
    const finalNode = resolveFinalNode(edge);
    if (pickup && finalNode && pickup !== finalNode && !hasCompletedPickupLeg(edge)) {
      return { kind: 'toPickup', targetNode: pickup };
    }
    if (finalNode) {
      return { kind: 'toDestination', targetNode: finalNode };
    }
    if (pickup) {
      return { kind: 'toPickup', targetNode: pickup };
    }
    return { kind: 'stationary', targetNode: edge?.currentLocation || null };
  }
  const dest = resolveFinalNode(edge);
  if (!isNullSentinel(dest)) {
    return { kind: 'stationary', targetNode: dest };
  }
  const pickup = resolvePickupNode(edge);
  if (pickup) {
    return { kind: 'stationary', targetNode: pickup };
  }
  return { kind: 'stationary', targetNode: edge?.currentLocation || null };
}

/**
 * Single phase decision for empty-path / arrival contexts.
 * @returns {{ taskPhase: string, extra?: object }|null}
 */
function resolveNextPhase(edge, context = {}) {
  if (!edge) {
    return null;
  }
  const phase = edge.taskPhase;
  const arrivedNode = context.arrivedNode ?? edge.currentLocation;

  if (phase === TaskPhase.EN_ROUTE_START) {
    const workEdge = context.arrivedNode
      ? { ...edge, currentLocation: arrivedNode }
      : edge;
    const workflowPhase = extractWorkflowPhase(workEdge);
    if (isMobilePhase(workflowPhase) && canShortcutEmptyPathAtFinal(workEdge)) {
      return { taskPhase: TaskPhase.COMPLETING, via: 'final_shortcut' };
    }
    if (isAtWorkLocation(workEdge)) {
      return { taskPhase: TaskPhase.ASSIGNED };
    }
    if (canTransitionToAssignedAtStartNode(workEdge)) {
      return transitionToAssigned(workEdge);
    }
    return null;
  }

  if (phase === TaskPhase.ASSIGNED) {
    const workEdge = context.arrivedNode
      ? { ...edge, currentLocation: arrivedNode }
      : edge;
    if (isAtWorkLocation(workEdge)) {
      const workflowPhase = extractWorkflowPhase(workEdge);
      if (isStationaryPhase(workflowPhase) || canShortcutEmptyPathAtFinal(workEdge)) {
        return transitionToCompleting(workEdge);
      }
      return transitionToCompleting(workEdge);
    }
    return null;
  }

  return null;
}

async function applyPhaseTransition(db, edgeId, nextPhase, meta = {}, env = process.env) {
  const edge = await runtimeCollection(db).findOne({ id: edgeId });
  if (!edge) {
    return null;
  }

  let updates = { lastTransitionAt: new Date().toISOString() };
  const leg = resolveActiveLeg(edge);
  updates.workflowLeg = leg.kind;

  if (typeof nextPhase === 'object' && nextPhase !== null) {
    updates = { ...updates, ...nextPhase };
  } else if (nextPhase === TaskPhase.ASSIGNED) {
    updates.taskPhase = TaskPhase.ASSIGNED;
  } else if (nextPhase === TaskPhase.COMPLETING) {
    Object.assign(updates, transitionToCompleting(edge));
  } else if (nextPhase === TaskPhase.IDLE) {
    Object.assign(updates, transitionToIdle(edge));
  } else if (nextPhase === TaskPhase.EN_ROUTE_START) {
    updates.taskPhase = TaskPhase.EN_ROUTE_START;
  } else {
    updates.taskPhase = nextPhase;
  }

  if (meta.via) {
    updates.lastTransitionVia = meta.via;
  }

  await updateEdgeState(db, edgeId, updates, env);
  debugEvent(env, {
    component: 'workflow',
    edgeId,
    shipmentId: edge.shipmentId,
    phase: updates.taskPhase || edge.taskPhase,
    event: 'phase_transition',
    detail: { ...meta, workflowLeg: leg.kind },
  });
  return runtimeCollection(db).findOne({ id: edgeId });
}

/**
 * Unified path promotion/rebuild (pendingPath, task.path, nextNode repair).
 * @returns {object|null} partial edge updates when path recovered
 */
function recoverPath(edge, graphMap = {}) {
  if (!edge) {
    return null;
  }

  const activePathEmpty = !(edge.path && edge.path.length > 0);
  if (
    activePathEmpty
    && (canShortcutEmptyPathAtFinal(edge)
      || isAtWorkLocation(edge)
      || canTransitionToAssignedAtStartNode(edge))
  ) {
    return null;
  }

  if (isMidTransit(edge) && !isAtHopBoundary(edge)) {
    const trimmed = alignPathToLocation(edge.path || [], edge.currentLocation, graphMap, true);
    if (
      trimmed.length > 0
      && Object.keys(graphMap).length > 0
      && !isAdjacent(graphMap, edge.currentLocation, trimmed[0])
    ) {
      return clearRouteFields();
    }
    if (trimmed.length > 0) {
      return edge;
    }
    const rebuilt = rebuildPathFromNextNode(edge, graphMap);
    const aligned = alignPathToLocation(rebuilt, edge.currentLocation, graphMap);
    if (aligned.length > 0) {
      return {
        path: aligned,
        nextNode: deriveNextNode(aligned) ?? edge.nextNode,
        progressToNext: edge.progressToNext ?? 0,
        eta: edge.eta ?? null,
        activeHop: edge.activeHop ?? null,
      };
    }
    return null;
  }

  let path = edge.path && edge.path.length > 0
    ? alignPathToLocation(edge.path, edge.currentLocation, graphMap, true)
    : [];

  if (
    path.length > 0
    && Object.keys(graphMap).length > 0
    && !isAdjacent(graphMap, edge.currentLocation, path[0])
  ) {
    return clearRouteFields();
  }

  if (path.length === 0) {
    const promoted = promoteRouteAtBoundary(edge, graphMap);
    if (promoted?.path?.length) {
      const leg = resolveActiveLeg(edge);
      return { ...promoted, workflowLeg: leg.kind };
    }
  }

  if (path.length === 0) {
    const leg = resolveActiveLeg(edge);
    const routeSource = edge.pendingPath?.length
      ? edge.pendingPath
      : (edge.task && typeof edge.task === 'object' && edge.task.path?.length ? edge.task.path : null);
    if (leg.kind === 'toPickup' && leg.targetNode && routeSource?.length) {
      const pickupPath = rebuildPathFromNextNode(
        { ...edge, pendingPath: routeSource },
        graphMap,
      );
      if (pickupPath.length > 0) {
        const aligned = alignPathToLocation(pickupPath, edge.currentLocation, graphMap);
        if (aligned.length > 0) {
          return {
            path: aligned,
            nextNode: deriveNextNode(aligned),
            progressToNext: 0,
            eta: null,
            activeHop: null,
            workflowLeg: 'toPickup',
          };
        }
      }
    }

    const rebuilt = rebuildPathFromNextNode(edge, graphMap);
    if (rebuilt.length > 0) {
      path = alignPathToLocation(rebuilt, edge.currentLocation, graphMap);
      if (path.length > 0) {
        return {
          path,
          nextNode: deriveNextNode(path),
          progressToNext: 0,
          eta: null,
          activeHop: null,
          workflowLeg: leg.kind,
        };
      }
    }
  } else if (path.length !== (edge.path || []).length || path[0] !== edge.path?.[0]) {
    const leg = resolveActiveLeg(edge);
    return {
      path,
      nextNode: deriveNextNode(path),
      workflowLeg: leg.kind,
    };
  }

  return path.length > 0 ? edge : null;
}

/**
 * Stall detection with leg-aware codes for backend diagnostics.
 */
function detectStall(edge, tickCount = 0) {
  if (!edge) {
    return null;
  }
  const pathLen = edge.path ? edge.path.length : 0;
  if (pathLen > 0 || edge.nextNode) {
    return null;
  }

  const leg = resolveActiveLeg(edge);
  if (leg.kind === 'toPickup' && needsPickupLeg(edge)) {
    return {
      code: 'PICKUP_LEG_MISSING',
      leg: leg.kind,
      tickCount,
      loc: edge.currentLocation,
      targetNode: leg.targetNode,
    };
  }

  if (isAtWorkLocation(edge)) {
    return null;
  }

  if (canTransitionToAssignedAtStartNode(edge)) {
    return null;
  }

  if (canShortcutEmptyPathAtFinal(edge)) {
    return null;
  }

  if (edge.taskPhase === TaskPhase.IDLE && edge.shipmentId) {
    return {
      code: 'MQTT_TASK_NEVER_APPLIED',
      leg: leg.kind,
      tickCount,
      loc: edge.currentLocation,
    };
  }

  return {
    code: 'EMPTY_PATH_STALL',
    leg: leg.kind,
    tickCount,
    loc: edge.currentLocation,
    startNode: edge.startNode,
    finalNode: edge.finalNode,
  };
}

function resolveTaskPhaseForAssignment(edge, taskData, trimmedPath) {
  const workflowPhase = taskData.phase || extractWorkflowPhase({ ...edge, task: taskData });
  const workEdge = {
    ...edge,
    finalNode: taskData.finalNode || edge.finalNode,
    startNode: taskData.startNode || edge.startNode,
    task: { ...taskData, phase: workflowPhase },
    pickupCompleted: false,
  };
  if (trimmedPath.length > 0) {
    return TaskPhase.EN_ROUTE_START;
  }
  const startNode = taskData.startNode || edge.startNode;
  const finalNode = taskData.finalNode || edge.finalNode;
  const sameNodeMobile = isMobilePhase(workflowPhase)
    && !isNullSentinel(startNode)
    && startNode === finalNode;
  if (isStationaryPhase(workflowPhase) && isAtWorkLocation(workEdge)) {
    return TaskPhase.ASSIGNED;
  }
  if (sameNodeMobile && isAtWorkLocation(workEdge)) {
    return TaskPhase.ASSIGNED;
  }
  if (isAtWorkLocation(workEdge)) {
    return TaskPhase.ASSIGNED;
  }
  if (canTransitionToAssignedAtStartNode(workEdge)) {
    return TaskPhase.ASSIGNED;
  }
  if (
    trimmedPath.length === 0
    && !isStationaryPhase(workflowPhase)
    && (edge.pendingPath?.length || taskData.path?.length)
  ) {
    return TaskPhase.EN_ROUTE_START;
  }
  if (isStationaryPhase(workflowPhase)) {
    return TaskPhase.ASSIGNED;
  }
  return TaskPhase.EN_ROUTE_START;
}

function buildArrivalUpdates(edge, arrivedNode, remainingPath = []) {
  const updates = {
    currentLocation: arrivedNode,
    nextNode: deriveNextNode(remainingPath),
    progressToNext: 0,
    eta: null,
    activeHop: null,
  };
  const pickup = resolvePickupNode(edge);
  const phase = extractWorkflowPhase(edge);
  if (isMobilePhase(phase) && pickup && arrivedNode === pickup) {
    updates.pickupCompleted = true;
    updates.workflowLeg = 'toDestination';
  }
  const workEdge = { ...edge, ...updates };
  updates.workflowLeg = resolveActiveLeg(workEdge).kind;
  const next = resolveNextPhase(workEdge, { arrivedNode });
  if (next && !next.via) {
    Object.assign(updates, next);
    updates.lastTransitionAt = new Date().toISOString();
  }
  return updates;
}

/**
 * Build Mongo updates for MQTT task acceptance.
 */
function acceptTask(edge, taskData, graphMap = {}, options = {}) {
  const { midTransit = false, atHopBoundary = true } = options;
  const existingTask = (typeof edge.task === 'object' && edge.task !== null) ? edge.task : {};
  const mergedTask = { ...existingTask, ...taskData };
  let startNode = edge.startNode;
  let shipmentId = edge.shipmentId;
  if (isNullSentinel(startNode)) {
    startNode = taskData.startNode || 'A1';
  }
  if (isNullSentinel(shipmentId)) {
    shipmentId = taskData.shipmentId || null;
  }

  const incomingPath = taskData.path || edge.pendingPath || [];
  const routeRevision = taskData.routeRevision ?? (edge.routeRevision ?? 0) + 1;
  const sameAssignment = (
    (taskData.assignmentEpoch == null || edge.acceptedAssignmentEpoch === taskData.assignmentEpoch)
    && edge.shipmentId === (taskData.shipmentId || edge.shipmentId)
    && extractWorkflowPhase({ ...edge, task: mergedTask }) === extractWorkflowPhase({ task: taskData })
  );
  const pickupCompleted = (sameAssignment && edge.pickupCompleted === true)
    || (atHopBoundary && !midTransit && edge.currentLocation === (taskData.startNode || startNode));

  const leg = resolveActiveLeg({
    ...edge,
    startNode,
    finalNode: taskData.finalNode || edge.finalNode,
    task: mergedTask,
    pickupCompleted,
  });

  const updates = {
    task: mergedTask,
    shipmentId,
    finalNode: taskData.finalNode || edge.finalNode,
    startNode,
    assignedShipment: taskData.shipmentId,
    routeRevision,
    pickupCompleted,
    acceptedAssignmentEpoch: taskData.assignmentEpoch ?? null,
    workflowLeg: leg.kind === 'toPickup' ? 'toPickup' : (leg.kind === 'toDestination' ? 'toDestination' : 'stationary'),
    lastTransitionAt: new Date().toISOString(),
    updatedAt: new Date(),
  };

  if (midTransit || !atHopBoundary) {
    updates.pendingPath = incomingPath.length > 0 ? incomingPath : (edge.pendingPath || []);
    if (edge.taskPhase === TaskPhase.IDLE) {
      updates.taskPhase = TaskPhase.EN_ROUTE_START;
    }
    return updates;
  }

  const mergedEdge = {
    ...edge,
    ...updates,
    pendingPath: incomingPath.length > 0 ? incomingPath : (edge.pendingPath || []),
    pickupCompleted,
  };
  const promoted = promoteRouteAtBoundary(mergedEdge, graphMap);
  if (promoted?.path?.length) {
    Object.assign(updates, promoted);
    updates.taskPhase = resolveTaskPhaseForAssignment(mergedEdge, taskData, promoted.path);
    return updates;
  }

  const trimmedPath = alignPathToLocation(incomingPath, edge.currentLocation, graphMap);
  const workflowPhase = taskData.phase || extractWorkflowPhase(mergedEdge);
  const workEdgeForLocation = {
    ...mergedEdge,
    task: mergedTask,
    pickupCompleted,
  };
  const atStationaryWork = isStationaryPhase(workflowPhase) && isAtWorkLocation(workEdgeForLocation);
  const startForSameNode = taskData.startNode || mergedEdge.startNode;
  const finalForSameNode = taskData.finalNode || mergedEdge.finalNode;
  const atSameNodeMobile = isMobilePhase(workflowPhase)
    && !isNullSentinel(startForSameNode)
    && startForSameNode === finalForSameNode
    && isAtWorkLocation(workEdgeForLocation);
  if (atStationaryWork || atSameNodeMobile) {
    updates.pendingPath = [];
    updates.path = [];
    updates.nextNode = null;
    updates.progressToNext = 0;
    updates.activeHop = null;
    updates.workflowLeg = 'stationary';
    updates.taskPhase = TaskPhase.ASSIGNED;
    return updates;
  }
  updates.pendingPath = trimmedPath.length > 0 ? incomingPath : (edge.pendingPath || []);
  updates.path = trimmedPath;
  updates.activeHop = null;
  updates.progressToNext = 0;
  updates.nextNode = deriveNextNode(trimmedPath);
  updates.taskPhase = resolveTaskPhaseForAssignment(edge, taskData, trimmedPath);
  if (trimmedPath.length === 0 && updates.pendingPath.length > 0) {
    updates.nextNode = null;
  }
  return updates;
}

function buildTaskAcceptUpdates(edge, taskData, graphMap = {}) {
  return acceptTask(edge, taskData, graphMap, {
    midTransit: isMidTransit(edge),
    atHopBoundary: isAtHopBoundary(edge),
  });
}

/**
 * Phase transitions when path is empty after arrival or during loop tick.
 * @returns {Promise<object|null>} refreshed edge or null
 */
async function onArrival(edgeId, db, edge, arrivedNode, env = process.env, persistFn = null) {
  const persist = persistFn || ((fn) => fn());
  const pickup = resolvePickupNode(edge);
  const phase = extractWorkflowPhase(edge);
  const pickupUpdates = {};
  if (isMobilePhase(phase) && pickup && arrivedNode === pickup) {
    pickupUpdates.pickupCompleted = true;
    pickupUpdates.workflowLeg = 'toDestination';
  }

  const workEdge = { ...edge, ...pickupUpdates, currentLocation: arrivedNode };
  let next = resolveNextPhase(workEdge, { arrivedNode, trigger: 'arrival' });

  if (next?.via === 'final_shortcut') {
    await persist(async () => applyPhaseTransition(
      db,
      edgeId,
      transitionToCompleting(edge),
      { via: 'final_shortcut', trigger: 'arrival' },
      env,
    ));
    return runtimeCollection(db).findOne({ id: edgeId });
  }

  if (next) {
    const updates = { ...pickupUpdates, ...next };
    await persist(async () => applyPhaseTransition(db, edgeId, updates, { trigger: 'arrival' }, env));
    return runtimeCollection(db).findOne({ id: edgeId });
  }

  if (Object.keys(pickupUpdates).length > 0) {
    await persist(async () => updateEdgeState(db, edgeId, pickupUpdates, env));
    return runtimeCollection(db).findOne({ id: edgeId });
  }

  return edge;
}

/**
 * Handle empty path during autonomous loop tick.
 * @returns {Promise<{ action: 'move'|'transition'|'stall'|'force_idle'|'noop', edge?: object }>}
 */
async function onPathEmpty(edgeId, edge, db, graphMap, env = process.env, options = {}) {
  const {
    persistFn = null,
    simulatorSettings = {},
    tickCount = 0,
  } = options;
  const persist = persistFn || ((fn) => fn());
  const maxTicks = simulatorSettings.simTransportRecoveryMaxTicks || 10;

  const next = resolveNextPhase(edge, { trigger: 'empty_path' });
  if (next?.via === 'final_shortcut') {
    await persist(async () => applyPhaseTransition(
      db,
      edgeId,
      transitionToCompleting(edge),
      { via: 'final_shortcut' },
      env,
    ));
    return { action: 'transition', edge: await runtimeCollection(db).findOne({ id: edgeId }) };
  }

  if (next) {
    resetPathRecoveryFailures(edgeId);
    await persist(async () => applyPhaseTransition(db, edgeId, next, { trigger: 'empty_path' }, env));
    return { action: 'transition', edge: await runtimeCollection(db).findOne({ id: edgeId }) };
  }

  const recovered = recoverPath(edge, graphMap);
  if (recovered?.path?.length) {
    resetPathRecoveryFailures(edgeId);
    await persist(async () => updateEdgeState(db, edgeId, recovered, env));
    debugEvent(env, {
      component: 'workflow',
      edgeId,
      shipmentId: edge.shipmentId,
      phase: edge.taskPhase,
      event: 'path_recovered',
      detail: { path: recovered.path, leg: recovered.workflowLeg },
    });
    const fresh = await runtimeCollection(db).findOne({ id: edgeId });
    return { action: 'move', edge: fresh };
  }

  if (recovered && typeof recovered === 'object' && recovered.path === undefined && !recovered.path?.length) {
    const cleared = clearRouteFields();
    if (recovered.path !== undefined || Object.keys(recovered).some((k) => CLEARED_ROUTE_FIELDS[k] !== undefined)) {
      await persist(async () => updateEdgeState(db, edgeId, cleared, env));
    }
  }

  const stall = detectStall(edge, tickCount);
  if (stall && stall.code === 'PICKUP_LEG_MISSING') {
    const routeSource = edge.pendingPath?.length
      ? edge.pendingPath
      : (edge.task && typeof edge.task === 'object' && edge.task.path?.length ? edge.task.path : null);
    if (routeSource?.length) {
      const pickupPath = rebuildPathFromNextNode(
        { ...edge, pendingPath: routeSource },
        graphMap,
      );
    const aligned = alignPathToLocation(pickupPath, edge.currentLocation, graphMap);
    if (aligned.length > 0) {
      resetPathRecoveryFailures(edgeId);
      await persist(async () => updateEdgeState(db, edgeId, {
        path: aligned,
        nextNode: deriveNextNode(aligned),
        progressToNext: 0,
        eta: null,
        activeHop: null,
        workflowLeg: 'toPickup',
      }, env));
      return { action: 'move', edge: await runtimeCollection(db).findOne({ id: edgeId }) };
    }
    }
  }

  const { exceeded } = bumpPathRecoveryFailure(edgeId, maxTicks);
  if (exceeded) {
    simWarn('TRANSPORT_PATH_RECOVERY_FAILED', {
      component: 'workflow',
      edgeId,
      shipmentId: edge.shipmentId,
      phase: edge.taskPhase,
      reason: 'MAX_RECOVERY_TICKS',
      detail: { loc: edge.currentLocation, stall },
    });
    await persist(async () => applyPhaseTransition(db, edgeId, TaskPhase.IDLE, { reason: 'MAX_RECOVERY_TICKS' }, env));
    resetPathRecoveryFailures(edgeId);
    return { action: 'force_idle', edge: await runtimeCollection(db).findOne({ id: edgeId }) };
  }

  if (stall) {
    simWarn(stall.code, {
      component: 'workflow',
      edgeId,
      shipmentId: edge.shipmentId,
      phase: edge.taskPhase,
      reason: stall.code,
      detail: stall,
    });
    debugEvent(env, {
      component: 'workflow',
      edgeId,
      shipmentId: edge.shipmentId,
      phase: edge.taskPhase,
      event: 'empty_path_stall',
      detail: stall,
    });
  }

  return { action: 'stall', edge };
}

function resetWorkflowEngineForTests() {
  pathRecoveryFailures.clear();
}

module.exports = {
  resolveActiveLeg,
  resolveNextPhase,
  applyPhaseTransition,
  recoverPath,
  detectStall,
  acceptTask,
  buildTaskAcceptUpdates,
  buildArrivalUpdates,
  onPathEmpty,
  onArrival,
  resolveTaskPhaseForAssignment,
  clearRouteFields,
  resetPathRecoveryFailures,
  bumpPathRecoveryFailure,
  resetWorkflowEngineForTests,
  pathRecoveryFailures,
  CLEARED_ROUTE_FIELDS,
};
