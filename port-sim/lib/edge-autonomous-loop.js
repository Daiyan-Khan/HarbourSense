const { simulationSleep, simulationNow, currentContext } = require('./demo-runtime');
const {

  TaskPhase,

  transitionToIdle,

  transitionToCompleting,

  isCanonicalPhase,

  extractTaskData,

  isMobilePhase,

  extractWorkflowPhase,

  isAtWorkLocation,

  isStationaryPhase,

  resolvePickupNode,

  canShortcutEmptyPathAtFinal,

} = require('./edge-phases');

const { updateEdgeState } = require('./edge-state');

const { simulateMovement, isMidTransit, getGraphMap } = require('./movement-engine');

const {

  alignPathToLocation,

  isAtHopBoundary,

  isAdjacent,

} = require('./route-state');

const { deriveNextNode } = require('./edge-route-state');

const { enqueueEdgeWork, handleTaskMessage } = require('./mqtt-handlers');
const { runtimeCollection, assignmentCollection, loadMergedEdge } = require('./edge-collections');

const { executeTask } = require('./task-executor');

const { debugEvent, simWarn } = require('./sim-debug');

const {

  recoverPath,

  onPathEmpty,

  clearRouteFields,

  resetPathRecoveryFailures,

  resetWorkflowEngineForTests,

  pathRecoveryFailures,

  CLEARED_ROUTE_FIELDS,

  applyPhaseTransition,

  acceptTask,

} = require('./edge-workflow-engine');



const executingByEdge = new Map();

const movingByEdge = new Map();

const movementStartedAt = new Map();



function hasOrphanedNextNode(edge, edgeId) {

  if (!edge?.nextNode) {

    return false;

  }

  const progress = edge.progressToNext ?? 0;

  if (progress > 0 && progress < 100) {

    return false;

  }

  const pathLen = edge.path ? edge.path.length : 0;

  if (pathLen > 0) {

    return false;

  }

  if (movingByEdge.get(edgeId)) {

    return false;

  }

  return true;

}



function hasClaimedTask(edge) {

  const shipmentId = edge?.shipmentId || edge?.assignedShipment;

  if (!shipmentId) {

    return false;

  }

  const task = edge?.task;

  if (!task || task === 'idle') {

    return false;

  }

  if (typeof task === 'string') {

    return task !== 'idle';

  }

  return Boolean(task.phase || task.shipmentId);

}



function buildTaskDataFromEdge(edge) {
  if (typeof edge.task === 'object' && edge.task !== null) {
    return {
      ...edge.task,
      phase: edge.task.phase || edge.task.task,
      shipmentId: edge.shipmentId || edge.task.shipmentId,
      path: edge.task.path || edge.pendingPath || [],
      assignmentEpoch: edge.assignmentEpoch ?? edge.task.assignmentEpoch,
      routeRevision: edge.routeRevision ?? edge.task.routeRevision,
      startNode: edge.startNode ?? edge.task.startNode,
      finalNode: edge.finalNode ?? edge.task.finalNode,
    };
  }
  return {
    phase: edge.task,
    shipmentId: edge.shipmentId,
    path: edge.pendingPath || [],
    assignmentEpoch: edge.assignmentEpoch,
    routeRevision: edge.routeRevision,
    startNode: edge.startNode,
    finalNode: edge.finalNode,
  };
}

async function bootstrapClaimedTask(edgeId, edge, db, env = process.env) {

  const merged = await loadMergedEdge(db, edgeId) || edge;

  if (!hasClaimedTask(merged)) {

    return merged;

  }

  const taskData = buildTaskDataFromEdge(merged);

  await handleTaskMessage(edgeId, taskData, db);

  let fresh = await loadMergedEdge(db, edgeId);

  if (fresh?.taskPhase === TaskPhase.IDLE && hasClaimedTask(fresh)) {
    const graphMap = await getGraphMap(db);
    const updates = acceptTask(fresh, taskData, graphMap, {
      midTransit: isMidTransit(fresh),
      atHopBoundary: isAtHopBoundary(fresh),
    });
    await queuedUpdateEdgeState(db, edgeId, updates, env);
    fresh = await loadMergedEdge(db, edgeId);

    if (fresh?.taskPhase !== TaskPhase.IDLE) {
      debugEvent(env, {
        component: 'loop',
        edgeId,
        shipmentId: fresh.shipmentId,
        phase: fresh.taskPhase,
        event: 'bootstrap_direct_accept',
        detail: { loc: fresh.currentLocation },
      });
    }
  }

  if (fresh?.taskPhase === TaskPhase.IDLE && hasClaimedTask(fresh)) {

    simWarn('CLAIMED_BUT_IDLE', {

      component: 'loop',

      edgeId,

      shipmentId: fresh.shipmentId,

      phase: extractWorkflowPhase(fresh),

      reason: 'BOOTSTRAP_FAILED',

      detail: { loc: fresh.currentLocation },

    });

    await queuedUpdateEdgeState(db, edgeId, { stallCode: 'MQTT_TASK_NEVER_APPLIED' }, env);

  }

  return fresh || edge;

}



async function tryAdvanceAtWorkSite(edgeId, edge, db, env = process.env) {

  if (!edge || (edge.path && edge.path.length > 0)) {

    return edge;

  }

  const phase = edge.taskPhase;

  if (phase !== TaskPhase.EN_ROUTE_START && phase !== TaskPhase.ASSIGNED) {

    return edge;

  }

  const workflowPhase = extractWorkflowPhase(edge);

  const pickup = resolvePickupNode(edge);

  if (

    isMobilePhase(workflowPhase)

    && pickup

    && edge.currentLocation === pickup

    && !edge.pickupCompleted

  ) {

    await queuedUpdateEdgeState(db, edgeId, {

      pickupCompleted: true,

      workflowLeg: 'toDestination',

    }, env);

    edge = await runtimeCollection(db).findOne({ id: edgeId }) || edge;

    const graphMap = await getGraphMap(db);

    const recovered = recoverPath(edge, graphMap);

    if (recovered?.path?.length) {

      await queuedUpdateEdgeState(db, edgeId, recovered, env);

      return runtimeCollection(db).findOne({ id: edgeId });

    }

  }

  if (!isAtWorkLocation(edge)) {

    return edge;

  }

  if (isStationaryPhase(workflowPhase) || canShortcutEmptyPathAtFinal(edge)) {

    await applyPhaseTransition(

      db,

      edgeId,

      transitionToCompleting(edge),

      { via: 'work_site_advance', trigger: 'loop_tick' },

      env,

    );

    return runtimeCollection(db).findOne({ id: edgeId });

  }

  return edge;

}



async function queuedUpdateEdgeState(db, edgeId, updates, env = process.env) {

  await enqueueEdgeWork(edgeId, () => updateEdgeState(db, edgeId, updates, env));

  return runtimeCollection(db).findOne({ id: edgeId });

}



function resetExecutingGuard(edgeId) {

  executingByEdge.delete(edgeId);

}



function resetMovingGuard(edgeId) {

  movingByEdge.delete(edgeId);

  movementStartedAt.delete(edgeId);

}



function hasActiveRoute(edge) {

  return Boolean((edge.path && edge.path.length > 0) || edge.nextNode);

}



function edgeUpdatedAtMs(edge) {
  const context = currentContext();
  if (context && edge?.demoUpdatedSimMs != null) return context.state.startedWallMs + edge.demoUpdatedSimMs;
  if (!edge?.updatedAt) {
    return 0;
  }
  if (edge.updatedAt instanceof Date) {
    return edge.updatedAt.getTime();
  }
  const parsed = new Date(edge.updatedAt).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasBrokenTransitPath(edge) {
  const progress = edge.progressToNext ?? 0;
  if (progress <= 0 || progress >= 100) {
    return false;
  }
  return !(edge.path && edge.path.length > 0);
}

function isHopWithinGrace(edge, edgeId, simulatorSettings = {}) {
  const progress = edge.progressToNext ?? 0;
  if (progress <= 0 || progress >= 100) {
    return false;
  }

  const graceMs = (simulatorSettings.simProgressIntervalMs || 1000) * 2;
  const etaMs = Math.max(1000, ((edge.eta ?? 5) + 2) * 1000);

  if (edge.activeHop?.startedAt) {
    const hopStart = new Date(edge.activeHop.startedAt).getTime();
    if (Number.isFinite(hopStart) && simulationNow() - hopStart < etaMs + graceMs) {
      return true;
    }
  }

  const updatedAt = edgeUpdatedAtMs(edge);
  if (updatedAt && simulationNow() - updatedAt < graceMs) {
    return true;
  }

  if (movingByEdge.get(edgeId)) {
    return true;
  }

  const startedAt = movementStartedAt.get(edgeId);
  if (startedAt && simulationNow() - startedAt < etaMs + graceMs) {
    return true;
  }

  return false;
}

function isMidHopProgress(edge) {
  const progress = edge?.progressToNext ?? 0;
  return progress > 0 && progress < 100;
}

function isStaleTransit(edge, edgeId, simulatorSettings = {}) {

  if (hasOrphanedNextNode(edge, edgeId)) {

    return true;

  }

  if (!isMidTransit(edge) || !hasActiveRoute(edge)) {

    return false;

  }

  if (hasBrokenTransitPath(edge)) {

    return true;

  }

  if (movingByEdge.get(edgeId)) {

    const startedAt = movementStartedAt.get(edgeId);

    if (!startedAt) {

      return true;

    }

    const etaMs = Math.max(1000, ((edge.eta ?? 5) + 2) * 1000);

    const graceMs = (simulatorSettings.simProgressIntervalMs || 1000) * 2;

    return simulationNow() - startedAt > etaMs + graceMs;

  }

  if (isHopWithinGrace(edge, edgeId, simulatorSettings)) {

    return false;

  }

  return true;

}



async function recoverStaleTransitPath(edgeId, edge, db, env = process.env) {

  if (hasOrphanedNextNode(edge, edgeId)) {

    const graphMap = await getGraphMap(db);

    const recovered = recoverPath(edge, graphMap);

    if (recovered?.path?.length) {

      await queuedUpdateEdgeState(db, edgeId, recovered, env);

      debugEvent(env, {

        component: 'loop',

        edgeId,

        shipmentId: edge.shipmentId,

        phase: edge.taskPhase,

        event: 'orphan_next_node_rebuilt',

        detail: { nextNode: edge.nextNode, path: recovered.path, loc: edge.currentLocation },

      });

      return runtimeCollection(db).findOne({ id: edgeId });

    }

    await queuedUpdateEdgeState(db, edgeId, clearRouteFields(), env);

    debugEvent(env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: edge.taskPhase,

      event: 'orphan_next_node_cleared',

      detail: { nextNode: edge.nextNode, loc: edge.currentLocation },

    });

    edge = await runtimeCollection(db).findOne({ id: edgeId });

  }

  if (isMidHopProgress(edge) && !hasBrokenTransitPath(edge)) {

    return edge;

  }

  if (edge.path && edge.path.length > 0) {

    return edge;

  }

  const graphMap = await getGraphMap(db);

  const recovered = recoverPath(edge, graphMap);

  if (recovered?.path?.length) {

    await queuedUpdateEdgeState(db, edgeId, recovered, env);

    debugEvent(env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: edge.taskPhase,

      event: 'stale_transit_path_rebuilt',

      detail: { path: recovered.path },

    });

    return runtimeCollection(db).findOne({ id: edgeId });

  }

  return edge;

}



/**

 * Promote pendingPath / task.path into active path before movement or stall checks.

 * Returns refreshed edge document when path was updated.

 */

async function ensureActivePath(edgeId, edge, db, env = process.env) {

  if (!edge) {

    return edge;

  }



  const graphMap = await getGraphMap(db);

  const trimmedCurrent = alignPathToLocation(edge.path || [], edge.currentLocation, graphMap, true);

  const pathHeadInvalid = (

    trimmedCurrent.length > 0

    && Object.keys(graphMap).length > 0

    && !isAdjacent(graphMap, edge.currentLocation, trimmedCurrent[0])

  );



  if (isMidTransit(edge) && !isAtHopBoundary(edge)) {

    if (!pathHeadInvalid) {

      return edge;

    }

    await queuedUpdateEdgeState(db, edgeId, clearRouteFields(), env);

    edge = {

      ...edge,

      ...CLEARED_ROUTE_FIELDS,

    };

    debugEvent(env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: edge.taskPhase,

      event: 'corrupt_transit_reset',

      detail: { loc: edge.currentLocation },

    });

  }



  let path = edge.path && edge.path.length > 0

    ? alignPathToLocation(edge.path, edge.currentLocation, graphMap, true)

    : [];



  if (

    path.length > 0

    && Object.keys(graphMap).length > 0

    && !isAdjacent(graphMap, edge.currentLocation, path[0])

  ) {

    debugEvent(env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: edge.taskPhase,

      event: 'stale_path_cleared_non_adjacent',

      detail: { loc: edge.currentLocation, pathHead: path[0] },

    });

    await queuedUpdateEdgeState(db, edgeId, clearRouteFields(), env);

    edge = { ...edge, ...CLEARED_ROUTE_FIELDS };

    path = [];

  }



  const recovered = recoverPath(path.length > 0 ? { ...edge, path } : edge, graphMap);

  if (recovered?.path?.length) {

    await queuedUpdateEdgeState(db, edgeId, recovered, env);

    debugEvent(env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: edge.taskPhase,

      event: 'path_recovered_before_move',

      detail: { path: recovered.path, leg: recovered.workflowLeg },

    });

    return runtimeCollection(db).findOne({ id: edgeId });

  }



  if (recovered && recovered !== edge && !recovered.path?.length && Object.keys(recovered).includes('path')) {

    await queuedUpdateEdgeState(db, edgeId, recovered, env);

    return runtimeCollection(db).findOne({ id: edgeId });

  }



  if (path.length > 0 && (path.length !== (edge.path || []).length || path[0] !== edge.path?.[0])) {

    await queuedUpdateEdgeState(db, edgeId, {

      path,

      nextNode: deriveNextNode(path),

    }, env);

    return runtimeCollection(db).findOne({ id: edgeId });

  }



  return edge;

}



function resolveLoopOptions(loopOptions = {}) {

  if (loopOptions.simulatorSettings || loopOptions.suggestionsByEdge) {

    return loopOptions;

  }

  return { simulatorSettings: loopOptions, suggestionsByEdge: {} };

}



function warnEmptyPathStall(edgeId, edge, phase, env = process.env) {

  simWarn('EMPTY_PATH_STALL', {

    component: 'loop',

    edgeId,

    shipmentId: edge.shipmentId,

    phase,

    reason: 'NO_PATH_NOT_AT_WORK',

    detail: {

      loc: edge.currentLocation,

      startNode: edge.startNode,

      finalNode: edge.finalNode,

      pathLen: edge.path ? edge.path.length : 0,

    },

  });

  debugEvent(env, {

    component: 'loop',

    edgeId,

    shipmentId: edge.shipmentId,

    phase,

    event: 'empty_path_stall',

    detail: { loc: edge.currentLocation },

  });

}



async function runSimulatedMovement(edgeId, edge, db, device, loopOptions) {

  const { simulatorSettings, suggestionsByEdge } = resolveLoopOptions(loopOptions);



  if (movingByEdge.get(edgeId)) {

    if (edge && isStaleTransit(edge, edgeId, simulatorSettings)) {

      simWarn('STALE_MOVEMENT_GUARD', {

        component: 'loop',

        edgeId,

        shipmentId: edge.shipmentId,

        phase: edge.taskPhase,

        reason: 'CLEARING_ORPHANED_GUARD',

        detail: {

          progress: edge.progressToNext,

          eta: edge.eta,

          loc: edge.currentLocation,

        },

      });

      resetMovingGuard(edgeId);

    } else {

      debugEvent(process.env, {

        component: 'loop',

        edgeId,

        event: 'movement_skipped_already_running',

      });

      return 0;

    }

  }



  movingByEdge.set(edgeId, true);

  movementStartedAt.set(edgeId, simulationNow());

  try {

    await simulateMovement(edgeId, db, device, { suggestionsByEdge, simulatorSettings });

    return 1;

  } finally {

    movingByEdge.delete(edgeId);

    movementStartedAt.delete(edgeId);

  }

}



async function handleIdlePhase(edgeId, edge, db, device, loopOptions) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  if (edge.path && edge.path.length > 0) {

    await queuedUpdateEdgeState(db, edgeId, { path: [], remainingPath: [] });

    debugEvent(process.env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: TaskPhase.IDLE,

      event: 'stale_path_cleared',

    });

  }

  if (edge.assignmentEpoch != null && edge.completedAssignmentEpoch === edge.assignmentEpoch) {
    await simulationSleep(simulatorSettings.simLoopIdleDelayMs);
    return;
  }

  if (hasClaimedTask(edge)) {

    const bootstrapped = await bootstrapClaimedTask(edgeId, edge, db);

    if (bootstrapped?.taskPhase !== TaskPhase.IDLE) {

      return;

    }

  }

  await simulationSleep(simulatorSettings.simLoopIdleDelayMs);

}



async function handleMobileEmptyPath(edgeId, edge, db, device, loopOptions) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  const graphMap = await getGraphMap(db);

  const tickCount = pathRecoveryFailures.get(edgeId) || 0;

  const result = await onPathEmpty(edgeId, edge, db, graphMap, process.env, {

    persistFn: (fn) => enqueueEdgeWork(edgeId, fn),

    simulatorSettings,

    tickCount,

  });



  if (result.action === 'move' && result.edge?.path?.length) {

    return runSimulatedMovement(edgeId, result.edge, db, device, loopOptions);

  }

  if (result.action === 'stall') {

    warnEmptyPathStall(edgeId, result.edge || edge, edge.taskPhase);

  }

  return 0;

}



async function tryAdvanceEmptyPathPhase(edgeId, edge, db, loopOptions) {

  if (!edge || (edge.path && edge.path.length > 0)) {

    return null;

  }

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  const graphMap = await getGraphMap(db);

  const result = await onPathEmpty(edgeId, edge, db, graphMap, process.env, {

    persistFn: (fn) => enqueueEdgeWork(edgeId, fn),

    simulatorSettings,

    tickCount: pathRecoveryFailures.get(edgeId) || 0,

  });

  if (result.action === 'transition' || result.action === 'force_idle') {

    return result.edge;

  }

  return null;

}



async function handleEnRouteStartPhase(edgeId, edge, db, device, loopOptions) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  if (isStaleTransit(edge, edgeId, simulatorSettings)) {

    edge = await recoverStaleTransitPath(edgeId, edge, db) || edge;

    debugEvent(process.env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: TaskPhase.EN_ROUTE_START,

      event: 'stale_transit_resume',

      detail: { progress: edge.progressToNext, pathLen: edge.path?.length ?? 0 },

    });

  }



  const advanced = await tryAdvanceEmptyPathPhase(edgeId, edge, db, loopOptions);

  if (advanced) {

    return 0;

  }



  edge = await tryAdvanceAtWorkSite(edgeId, edge, db) || edge;

  if (edge.taskPhase === TaskPhase.COMPLETING) {

    return 0;

  }



  edge = await ensureActivePath(edgeId, edge, db) || edge;



  const pathLen = edge.path ? edge.path.length : 0;

  if (pathLen > 0 || (isStaleTransit(edge, edgeId, simulatorSettings) && edge.nextNode)) {

    resetPathRecoveryFailures(edgeId);

    return runSimulatedMovement(edgeId, edge, db, device, loopOptions);

  }



  return handleMobileEmptyPath(edgeId, edge, db, device, loopOptions);

}



async function handleAssignedPhase(edgeId, edge, db, device, loopOptions) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  if (isStaleTransit(edge, edgeId, simulatorSettings)) {

    edge = await recoverStaleTransitPath(edgeId, edge, db) || edge;

    debugEvent(process.env, {

      component: 'loop',

      edgeId,

      shipmentId: edge.shipmentId,

      phase: TaskPhase.ASSIGNED,

      event: 'stale_transit_resume',

      detail: { progress: edge.progressToNext, pathLen: edge.path?.length ?? 0 },

    });

  }



  const advancedAssigned = await tryAdvanceEmptyPathPhase(edgeId, edge, db, loopOptions);

  if (advancedAssigned) {

    return 0;

  }



  edge = await tryAdvanceAtWorkSite(edgeId, edge, db) || edge;

  if (edge.taskPhase === TaskPhase.COMPLETING) {

    return 0;

  }



  edge = await ensureActivePath(edgeId, edge, db) || edge;



  const pathLen = edge.path ? edge.path.length : 0;

  if (pathLen > 0 || (isStaleTransit(edge, edgeId, simulatorSettings) && edge.nextNode)) {

    resetPathRecoveryFailures(edgeId);

    return runSimulatedMovement(edgeId, edge, db, device, loopOptions);

  }



  return handleMobileEmptyPath(edgeId, edge, db, device, loopOptions);

}



async function handleCompletingPhase(edgeId, db, device, loopOptions) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  if (executingByEdge.get(edgeId)) {

    debugEvent(process.env, {

      component: 'loop',

      edgeId,

      event: 'execute_skipped_already_running',

    });

    return 0;

  }



  const freshEdge = await loadMergedEdge(db, edgeId);

  const taskData = extractTaskData(freshEdge);

  if (!freshEdge || !taskData) {

    simWarn('COMPLETING_NO_TASK', {

      component: 'loop',

      edgeId,

      shipmentId: freshEdge?.shipmentId,

      phase: TaskPhase.COMPLETING,

      reason: 'MISSING_TASK',

    });

    await queuedUpdateEdgeState(db, edgeId, transitionToIdle(freshEdge || { id: edgeId }));

    return 0;

  }



  executingByEdge.set(edgeId, true);

  try {

    const steps = freshEdge.path && freshEdge.path.length ? freshEdge.path.length : 3;

    await executeTask(edgeId, db, device, freshEdge, taskData, {

      steps,

      simulatorSettings,

    });

  } finally {

    executingByEdge.delete(edgeId);

  }



  await simulationSleep(simulatorSettings.simCompletingDelayMs);

  return 0;

}



async function edgeAutonomousLoop(edgeId, db, device, loopOptions = {}) {

  const { simulatorSettings } = resolveLoopOptions(loopOptions);

  let moveCount = 0;



  debugEvent(process.env, {

    component: 'loop',

    edgeId,

    event: 'loop_started',

  });



  while (true) {

    let movesThisTick = 0;

    await enqueueEdgeWork(edgeId, async () => {

      const edge = await loadMergedEdge(db, edgeId);

      if (!edge) {

        return;

      }



      const state = edge.taskPhase;

      const pathLen = edge.path ? edge.path.length : 0;

      debugEvent(process.env, {

        component: 'loop',

        edgeId,

        shipmentId: edge.shipmentId,

        phase: state,

        event: 'loop_tick',

        detail: { pathLen, loc: edge.currentLocation, workflowLeg: edge.workflowLeg },

      });



      if (state === TaskPhase.IDLE) {

        await handleIdlePhase(edgeId, edge, db, device, loopOptions);

      } else if (state === TaskPhase.EN_ROUTE_START) {

        movesThisTick = await handleEnRouteStartPhase(edgeId, edge, db, device, loopOptions);

      } else if (state === TaskPhase.ASSIGNED) {

        movesThisTick = await handleAssignedPhase(edgeId, edge, db, device, loopOptions);

      } else if (state === TaskPhase.COMPLETING) {

        movesThisTick = await handleCompletingPhase(edgeId, db, device, loopOptions);

      } else if (!isCanonicalPhase(state)) {

        simWarn('UNKNOWN_PHASE', {

          component: 'loop',

          edgeId,

          shipmentId: edge.shipmentId,

          phase: state,

          reason: 'FORCING_IDLE',

        });

        await queuedUpdateEdgeState(db, edgeId, transitionToIdle(edge));

      }

    });



    moveCount += movesThisTick;

    if (moveCount > 100) {

      simWarn('MAX_MOVES_REACHED', {

        component: 'loop',

        edgeId,

        reason: 'RESET_TO_IDLE',

      });

      const fresh = await runtimeCollection(db).findOne({ id: edgeId });

      await queuedUpdateEdgeState(db, edgeId, transitionToIdle(fresh || { id: edgeId }));

      moveCount = 0;

    }



    const edgeExists = await runtimeCollection(db).findOne({ id: edgeId }, { projection: { id: 1 } });

    if (!edgeExists) {

      await simulationSleep(simulatorSettings.simEdgeMissingDelayMs);

      continue;

    }

    await simulationSleep(simulatorSettings.simLoopTickMs);

  }

}



function resetExecutingEdgesForTests() {

  executingByEdge.clear();

}



function resetMovingGuardForTests() {

  movingByEdge.clear();

  movementStartedAt.clear();

  resetWorkflowEngineForTests();

}



module.exports = {

  edgeAutonomousLoop,

  resetExecutingGuard,

  resetMovingGuard,

  resetExecutingEdgesForTests,

  resetMovingGuardForTests,

  executingByEdge,

  movingByEdge,

  movementStartedAt,

  handleIdlePhase,

  handleEnRouteStartPhase,

  handleAssignedPhase,

  handleCompletingPhase,

  warnEmptyPathStall,

  runSimulatedMovement,

  isStaleTransit,

  recoverStaleTransitPath,

  hasActiveRoute,

  hasOrphanedNextNode,

  ensureActivePath,

  clearRouteFields,

  pathRecoveryFailures,

  bootstrapClaimedTask,

  tryAdvanceAtWorkSite,

  hasClaimedTask,

};


