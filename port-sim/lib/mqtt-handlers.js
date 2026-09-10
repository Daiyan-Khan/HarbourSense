const { AsyncLocalStorage } = require('async_hooks');
const { TaskPhase, transitionToCompleting, isAtWorkLocation } = require('./edge-phases');
const { updateEdgeState } = require('./edge-state');
const {
  buildTaskAcceptUpdates,
  resolveNextPhase,
  applyPhaseTransition,
} = require('./edge-workflow-engine');
const { debugLog, debugEvent, simWarn, hopTrace } = require('./sim-debug');
const { runtimeCollection, loadMergedEdge } = require('./edge-collections');
const { isMidTransit, isAtHopBoundary, alignPathToLocation, loadGraphMap } = require('./route-state');
const { promoteRouteAtBoundary, deriveNextNode } = require('./edge-route-state');

const edgeQueues = new Map();
const edgeQueueDepth = new Map();
const edgeWorkContext = new AsyncLocalStorage();

function enqueueEdgeWork(edgeId, fn) {
  if (edgeWorkContext.getStore() === edgeId) {
    return Promise.resolve().then(() => fn());
  }

  const previous = edgeQueues.get(edgeId) || Promise.resolve();
  const next = previous
    .catch((err) => {
      simWarn('EDGE_QUEUE_ERROR', {
        component: 'mqtt',
        edgeId,
        reason: err?.message || String(err),
      });
    })
    .then(async () => {
      edgeQueueDepth.set(edgeId, (edgeQueueDepth.get(edgeId) || 0) + 1);
      try {
        return await edgeWorkContext.run(edgeId, () => fn());
      } finally {
        const nextDepth = (edgeQueueDepth.get(edgeId) || 1) - 1;
        if (nextDepth <= 0) {
          edgeQueueDepth.delete(edgeId);
        } else {
          edgeQueueDepth.set(edgeId, nextDepth);
        }
      }
    })
    .catch((err) => {
      simWarn('EDGE_QUEUE_TASK_FAILED', {
        component: 'mqtt',
        edgeId,
        reason: err?.message || String(err),
      });
      throw err;
    });
  edgeQueues.set(edgeId, next);
  return next;
}

function resetEdgeQueuesForTests() {
  edgeQueues.clear();
  edgeQueueDepth.clear();
}

function taskMatchesEdge(edge, taskData) {
  if (!edge || !taskData) {
    return false;
  }
  const edgePhase = typeof edge.task === 'object' && edge.task !== null
    ? edge.task.phase
    : null;
  const incomingPhase = taskData.phase || taskData.task?.phase;
  const edgeShipment = edge.shipmentId || edge.assignedShipment;
  return edgePhase === incomingPhase && edgeShipment === taskData.shipmentId;
}

function canAcceptTask(edge, taskData) {
  if (edge.taskPhase === TaskPhase.IDLE) {
    return true;
  }
  if (
    edge.taskPhase === TaskPhase.EN_ROUTE_START
    && taskMatchesEdge(edge, taskData)
  ) {
    return true;
  }
  if (
    (edge.taskPhase === TaskPhase.ASSIGNED || edge.taskPhase === TaskPhase.COMPLETING)
    && taskMatchesEdge(edge, taskData)
  ) {
    return true;
  }
  return false;
}

function canAcceptTaskRejectionReason(edge, taskData) {
  if (canAcceptTask(edge, taskData)) {
    return null;
  }
  if (edge.taskPhase === TaskPhase.EN_ROUTE_START) {
    return 'EN_ROUTE_MISMATCH';
  }
  return 'EDGE_BUSY';
}

function parseRevision(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isStaleAssignmentEpoch(edge, incomingEpoch) {
  if (incomingEpoch == null) {
    return false;
  }
  const edgeEpoch = parseRevision(edge?.assignmentEpoch);
  const incoming = parseRevision(incomingEpoch);
  return incoming > 0 && edgeEpoch > 0 && incoming < edgeEpoch;
}

function isStaleRouteRevision(edge, incomingRevision) {
  if (incomingRevision == null) {
    return false;
  }
  const edgeRev = parseRevision(edge?.routeRevision);
  const incoming = parseRevision(incomingRevision);
  return incoming > 0 && edgeRev > 0 && incoming < edgeRev;
}

async function handleTaskMessage(edgeId, taskData, db) {
  const edge = await loadMergedEdge(db, edgeId);
  if (!edge) {
    console.warn(`No edge found for ${edgeId}; skipping task assignment.`);
    return;
  }

  if (taskData.assignmentEpoch != null && edge.completedAssignmentEpoch === taskData.assignmentEpoch) return;

  if (isStaleAssignmentEpoch(edge, taskData.assignmentEpoch)) {
    simWarn('STALE_TASK_REJECTED', {
      component: 'mqtt',
      edgeId,
      shipmentId: taskData.shipmentId,
      phase: taskData.phase,
      reason: 'STALE_ASSIGNMENT_EPOCH',
      detail: {
        incomingEpoch: taskData.assignmentEpoch,
        edgeEpoch: edge.assignmentEpoch,
        loc: edge.currentLocation,
      },
    });
    return;
  }

  debugEvent(process.env, {
    component: 'mqtt',
    edgeId,
    shipmentId: taskData.shipmentId,
    phase: taskData.phase,
    event: 'task_received',
    detail: { edgePhase: edge.taskPhase, loc: edge.currentLocation },
  });

  if (!canAcceptTask(edge, taskData)) {
    const reason = canAcceptTaskRejectionReason(edge, taskData);
    simWarn('TASK_REJECTED', {
      component: 'mqtt',
      edgeId,
      shipmentId: taskData.shipmentId,
      phase: taskData.phase,
      reason,
      detail: {
        edgePhase: edge.taskPhase,
        edgeShipmentId: edge.shipmentId,
        edgeTaskPhase: edge.task?.phase,
      },
    });
    return;
  }

  if (taskMatchesEdge(edge, taskData) && edge.taskPhase !== TaskPhase.IDLE) {
    const mergedTask = {
      ...(typeof edge.task === 'object' && edge.task !== null ? edge.task : {}),
      ...taskData,
    };
    const workEdge = {
      ...edge,
      task: mergedTask,
      startNode: taskData.startNode || edge.startNode,
      finalNode: taskData.finalNode || edge.finalNode,
    };
    const activePathEmpty = !(edge.path && edge.path.length > 0) && !(edge.pendingPath && edge.pendingPath.length);
    if (activePathEmpty) {
      const next = resolveNextPhase(workEdge, { trigger: 'duplicate_task' });
      if (next?.via === 'final_shortcut') {
        await applyPhaseTransition(
          db,
          edgeId,
          transitionToCompleting(workEdge),
          { via: 'final_shortcut', trigger: 'duplicate_task' },
        );
        return;
      }
      if (next?.taskPhase === TaskPhase.ASSIGNED && edge.taskPhase === TaskPhase.EN_ROUTE_START) {
        await applyPhaseTransition(db, edgeId, next, { trigger: 'duplicate_task' });
        return;
      }
      if (edge.taskPhase === TaskPhase.ASSIGNED && isAtWorkLocation(workEdge)) {
        await applyPhaseTransition(
          db,
          edgeId,
          transitionToCompleting(workEdge),
          { trigger: 'duplicate_task' },
        );
        return;
      }
      if (isAtWorkLocation(workEdge)) {
        return;
      }
    }
  }

  const graphMap = await loadGraphMap(db);
  const midTransit = isMidTransit(edge);
  const incomingPath = taskData.path || edge.pendingPath || [];
  const shipmentId = taskData.shipmentId || edge.shipmentId;

  hopTrace(process.env, {
    edgeId,
    shipmentId,
    event: midTransit ? 'task_queued_pending_path' : 'task_accepted',
    routeRevision: taskData.routeRevision ?? (edge.routeRevision ?? 0) + 1,
    progressToNext: edge.progressToNext,
    pathLen: incomingPath.length,
  });

  const updates = buildTaskAcceptUpdates(edge, taskData, graphMap);
  await updateEdgeState(db, edgeId, updates);
  debugEvent(process.env, {
    component: 'mqtt',
    edgeId,
    shipmentId,
    phase: taskData.phase,
    event: midTransit ? 'task_stashed_pending_path' : 'task_accepted',
    detail: {
      path: updates.path,
      pendingPath: updates.pendingPath,
      nextNode: updates.nextNode,
      workflowLeg: updates.workflowLeg,
    },
  });
}

function isActiveMovingEdge(edge) {
  return edge && (
    edge.taskPhase === TaskPhase.EN_ROUTE_START
    || edge.taskPhase === TaskPhase.ASSIGNED
  );
}

async function handleRouteMessage(edgeId, routeData, db, suggestionsByEdge) {
  if (!routeData.path || !Array.isArray(routeData.path) || routeData.path.length === 0) {
    return;
  }

  const edge = await loadMergedEdge(db, edgeId);
  if (!edge) {
    return;
  }

  const routeRevision = routeData.routeRevision ?? (edge.routeRevision ?? 0) + 1;
  if (isStaleRouteRevision(edge, routeData.routeRevision)) {
    simWarn('STALE_ROUTE_REJECTED', {
      component: 'mqtt',
      edgeId,
      shipmentId: edge.shipmentId,
      reason: 'STALE_ROUTE_REVISION',
      detail: {
        incomingRevision: routeData.routeRevision,
        edgeRevision: edge.routeRevision,
        loc: edge.currentLocation,
      },
    });
    return;
  }

  const graphMap = await loadGraphMap(db);
  const trimmed = alignPathToLocation(routeData.path, edge.currentLocation, graphMap);

  hopTrace(process.env, {
    edgeId,
    shipmentId: edge.shipmentId,
    event: 'route_command_received',
    routeRevision,
    progressToNext: edge.progressToNext,
    pathLen: routeData.path.length,
  });

  if (isMidTransit(edge) || !isAtHopBoundary(edge) || (isActiveMovingEdge(edge) && edge.path?.length > 0)) {
    if (suggestionsByEdge) {
      suggestionsByEdge[edgeId] = {
        ...(suggestionsByEdge[edgeId] || {}),
        pendingPath: routeData.path,
        routeRevision,
        ...(routeData.eta !== undefined ? { eta: routeData.eta } : {}),
      };
    }
    debugEvent(process.env, {
      component: 'mqtt',
      edgeId,
      event: 'route_stashed_pending',
      detail: { routeRevision, pathLen: routeData.path.length },
    });
    return;
  }

  if (routeData.eta !== undefined && suggestionsByEdge) {
    suggestionsByEdge[edgeId] = {
      ...(suggestionsByEdge[edgeId] || {}),
      eta: routeData.eta,
      routeRevision,
    };
  }

  await updateEdgeState(db, edgeId, {
    path: trimmed,
    routeRevision,
    nextNode: deriveNextNode(trimmed),
    progressToNext: 0,
    activeHop: null,
    updatedAt: new Date(),
  });
  debugEvent(process.env, {
    component: 'mqtt',
    edgeId,
    event: 'route_applied',
    detail: { path: trimmed, routeRevision },
  });
}

async function handleTrafficMessage(edgeId, trafficData, db, suggestionsByEdge) {
  if (trafficData.status === 'arrived') {
    return;
  }

  if (trafficData.path || trafficData.suggestedPath) {
    await handleRouteMessage(edgeId, {
      path: trafficData.suggestedPath || trafficData.path,
      routeRevision: trafficData.routeRevision,
      eta: trafficData.eta,
    }, db, suggestionsByEdge);
    return;
  }

  if (!trafficData.eta) {
    return;
  }

  suggestionsByEdge[edgeId] = {
    ...suggestionsByEdge[edgeId],
    eta: trafficData.eta,
  };
}

async function handleShipmentMessage(shipmentId, shipmentData, db) {
  // Phase 8: backend is sole Mongo writer for shipments; sim publish-only via generator.
  debugEvent(process.env, {
    component: 'mqtt',
    shipmentId,
    event: 'shipment_mqtt_ignored',
    detail: { status: shipmentData?.status, reason: 'BACKEND_SINGLE_WRITER' },
  });
}

function createMqttMessageHandler(db, device, suggestionsByEdge = {}) {
  return async function onMessage(topic, payload) {
    try {
      const taskMatch = topic.match(/^harboursense\/edge\/([^/]+)\/task$/);
      const routeMatch = topic.match(/^harboursense\/edge\/([^/]+)\/route$/);

      if (taskMatch) {
        const edgeId = taskMatch[1];
        const taskData = JSON.parse(payload.toString());
        await enqueueEdgeWork(edgeId, () => handleTaskMessage(edgeId, taskData, db));
      } else if (routeMatch) {
        const edgeId = routeMatch[1];
        const routeData = JSON.parse(payload.toString());
        await enqueueEdgeWork(edgeId, () => handleRouteMessage(edgeId, routeData, db, suggestionsByEdge));
      }
    } catch (err) {
      console.error('Error processing MQTT message:', err);
    }
  };
}

module.exports = {
  canAcceptTask,
  canAcceptTaskRejectionReason,
  taskMatchesEdge,
  isStaleAssignmentEpoch,
  isStaleRouteRevision,
  handleTaskMessage,
  handleRouteMessage,
  handleTrafficMessage,
  handleShipmentMessage,
  createMqttMessageHandler,
  enqueueEdgeWork,
  resetEdgeQueuesForTests,
  edgeQueues,
  isActiveMovingEdge,
};
