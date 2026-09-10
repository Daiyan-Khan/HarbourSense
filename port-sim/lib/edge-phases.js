const TaskPhase = Object.freeze({
  IDLE: 'idle',
  EN_ROUTE_START: 'en_route_start',
  ASSIGNED: 'assigned',
  COMPLETING: 'completing',
});

const NULL_SENTINELS = new Set([null, undefined, 'Null', 'null', 'None', 'undefined', '']);

function isNullSentinel(value) {
  return NULL_SENTINELS.has(value);
}

const IDLE_DEFAULTS = {
  task: 'idle',
  taskPhase: TaskPhase.IDLE,
  path: [],
  remainingPath: [],
  pendingPath: [],
  nextNode: null,
  finalNode: null,
  startNode: null,
  eta: null,
  progressToNext: 0,
  activeHop: null,
  routeRevision: 0,
  journeyTime: null,
  shipmentId: null,
  assignedShipment: null,
  taskCompletionTime: 0,
  priority: null,
  pickupCompleted: false,
  updatedAt: new Date().toISOString(),
};

function transitionToIdle(currentEdge) {
  return { ...IDLE_DEFAULTS, taskPhase: TaskPhase.IDLE };
}

function transitionToEn_routeStart(currentEdge, taskData) {
  if (
    !currentEdge
    || (currentEdge.taskPhase !== TaskPhase.IDLE
      && !(
        currentEdge.taskPhase === TaskPhase.EN_ROUTE_START
        && currentEdge.task?.phase === taskData.phase
        && currentEdge.shipmentId === taskData.shipmentId
      ))
  ) {
    throw new Error(
      `Can only assign task from IDLE (or matching en_route_start race). Current: ${currentEdge?.taskPhase}`,
    );
  }

  return {
    task: taskData.task,
    taskPhase: TaskPhase.EN_ROUTE_START,
    startNode: taskData.startNode || null,
    finalNode: taskData.finalNode || null,
    path: taskData.path || [],
    eta: null,
    journeyTime: 0,
    shipmentId: taskData.shipmentId || null,
  };
}

function transitionToAssigned(currentEdge) {
  if (!currentEdge || currentEdge.taskPhase !== TaskPhase.EN_ROUTE_START) {
    throw new Error('Can only transition to ASSIGNED from EN_ROUTE_START');
  }
  return { taskPhase: TaskPhase.ASSIGNED };
}

function transitionToCompleting(currentEdge) {
  if (
    !currentEdge
    || (currentEdge.taskPhase !== TaskPhase.ASSIGNED
      && currentEdge.taskPhase !== TaskPhase.EN_ROUTE_START)
  ) {
    throw new Error('Can only transition to COMPLETING from ASSIGNED or EN_ROUTE_START at destination');
  }
  return {
    taskPhase: TaskPhase.COMPLETING,
    nextNode: null,
    finalNode: null,
    eta: null,
    journeyTime: null,
    path: [],
    shipmentId: currentEdge.shipmentId,
  };
}

function isRaceTolerantDuplicate(currentEdge, taskData) {
  if (!currentEdge || !taskData) return false;
  const phase = taskData.phase || taskData.task?.phase;
  return (
    currentEdge.taskPhase === TaskPhase.EN_ROUTE_START
    && currentEdge.task?.phase === phase
    && currentEdge.shipmentId === taskData.shipmentId
  );
}

function extractTaskData(edge) {
  if (!edge || !edge.task) return null;
  if (typeof edge.task === 'string') {
    return edge.task === 'idle' ? null : { phase: edge.task, shipmentId: edge.shipmentId };
  }
  return {
    phase: edge.task.phase || edge.task.task,
    shipmentId: edge.task.shipmentId || edge.shipmentId,
    ...edge.task,
  };
}

function isCanonicalPhase(phase) {
  return Object.values(TaskPhase).includes(phase);
}

const MOBILE_PHASES = new Set(['transport', 'delivery']);

function isMobilePhase(phase) {
  return MOBILE_PHASES.has(phase);
}

function resolvePickupNode(edge) {
  if (!edge) return null;
  const task = typeof edge.task === 'object' && edge.task !== null ? edge.task : {};
  const pickup = edge.startNode || task.pickupNode || task.requiredPlace;
  return isNullSentinel(pickup) ? null : pickup;
}

function resolveFinalNode(edge) {
  if (!edge) return null;
  if (!isNullSentinel(edge.finalNode)) {
    return edge.finalNode;
  }
  const task = typeof edge.task === 'object' && edge.task !== null ? edge.task : {};
  const fromTask = task.destNode || task.finalNode;
  if (!isNullSentinel(fromTask)) {
    return fromTask;
  }
  return null;
}

function hasCompletedPickupLeg(edge) {
  if (!edge) return true;
  const pickup = resolvePickupNode(edge);
  if (!pickup || pickup === resolveFinalNode(edge)) {
    return true;
  }
  return edge.pickupCompleted === true;
}

function needsPickupLeg(edge) {
  const pickup = resolvePickupNode(edge);
  if (!pickup || pickup === resolveFinalNode(edge)) {
    return false;
  }
  if (edge.pickupCompleted === true) {
    return false;
  }
  return edge.currentLocation !== pickup;
}

/** Work location when finalNode is unset (e.g. crane offload at dock). */
function resolveDestinationNode(edge) {
  if (!edge) return null;
  const phase = extractWorkflowPhase(edge);
  if (isMobilePhase(phase)) {
    const pickup = resolvePickupNode(edge);
    const finalNode = resolveFinalNode(edge);
    if (needsPickupLeg(edge)) {
      return pickup;
    }
    if (finalNode) {
      return finalNode;
    }
    return pickup || edge.currentLocation || null;
  }
  if (!isNullSentinel(edge.finalNode)) {
    return edge.finalNode;
  }
  const task = typeof edge.task === 'object' && edge.task !== null ? edge.task : {};
  const fromTask = task.destNode || task.finalNode;
  if (!isNullSentinel(fromTask)) {
    return fromTask;
  }
  if (!isNullSentinel(task.requiredPlace)) {
    return task.requiredPlace;
  }
  if (!isNullSentinel(edge.startNode)) {
    return edge.startNode;
  }
  return edge.currentLocation || null;
}

function canShortcutEmptyPathAtFinal(edge) {
  const finalNode = resolveFinalNode(edge);
  if (!edge || !finalNode || edge.currentLocation !== finalNode) {
    return false;
  }
  const pickup = resolvePickupNode(edge);
  if (pickup && pickup === finalNode) {
    return true;
  }
  const phase = extractWorkflowPhase(edge);
  if (isMobilePhase(phase)) {
    return hasCompletedPickupLeg(edge);
  }
  return true;
}

function isAtWorkLocation(edge) {
  const phase = extractWorkflowPhase(edge);
  if (isMobilePhase(phase)) {
    const finalNode = resolveFinalNode(edge);
    if (!finalNode || edge.currentLocation !== finalNode) {
      return false;
    }
    return hasCompletedPickupLeg(edge);
  }
  const dest = resolveDestinationNode(edge);
  return Boolean(dest && edge.currentLocation === dest);
}

const STATIONARY_PHASES = new Set(['offload', 'store_move', 'store_load', 'maintenance']);

function extractWorkflowPhase(edge) {
  const task = extractTaskData(edge);
  return task?.phase || null;
}

function isStationaryPhase(phase) {
  return STATIONARY_PHASES.has(phase);
}

/** True when empty path at startNode should transition to assigned (same-node / stationary work). */
function canTransitionToAssignedAtStartNode(edge) {
  const startNode = resolvePickupNode(edge);
  const finalNode = resolveFinalNode(edge);
  if (!edge || !startNode || edge.currentLocation !== startNode) {
    return false;
  }
  if (finalNode && startNode === finalNode) {
    return true;
  }
  const phase = extractWorkflowPhase(edge);
  return isStationaryPhase(phase);
}

module.exports = {
  TaskPhase,
  NULL_SENTINELS,
  IDLE_DEFAULTS,
  isNullSentinel,
  transitionToIdle,
  transitionToEn_routeStart,
  transitionToAssigned,
  transitionToCompleting,
  isRaceTolerantDuplicate,
  extractTaskData,
  isCanonicalPhase,
  resolveDestinationNode,
  resolvePickupNode,
  resolveFinalNode,
  hasCompletedPickupLeg,
  needsPickupLeg,
  isAtWorkLocation,
  MOBILE_PHASES,
  isMobilePhase,
  canShortcutEmptyPathAtFinal,
  STATIONARY_PHASES,
  extractWorkflowPhase,
  isStationaryPhase,
  canTransitionToAssignedAtStartNode,
};
