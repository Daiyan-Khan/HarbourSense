const { simulationSleep, simulationNow, currentContext } = require('./demo-runtime');
const { TaskPhase, transitionToIdle, resolveDestinationNode } = require('./edge-phases');
const { updateEdgeState, updateEdgeInDB } = require('./edge-state');
const { debugEvent, simWarn, simError } = require('./sim-debug');
const { runtimeCollection, loadMergedEdge } = require('./edge-collections');

async function persistTaskProgress(db, edgeId, { progressToNext, eta, nextNode }, env = process.env) {
  await updateEdgeState(db, edgeId, { progressToNext, eta, nextNode }, env);
}

async function resetEdgeAfterFailedCompletion(db, edgeId, edge, env = process.env) {
  const base = edge || { id: edgeId };
  await updateEdgeState(db, edgeId, transitionToIdle(base), env);
}

async function executeTask(edgeId, db, device, edge, taskData, options = {}) {
  const {
    steps = 5,
    simulatorSettings = {},
    env = process.env,
  } = options;

  if (!edge || !taskData) {
    simError('EXECUTE_MISSING_DATA', {
      component: 'chain',
      edgeId,
      reason: 'FORCING_IDLE',
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, edge, env);
    return;
  }

  const stepMs = simulatorSettings.simTaskStepMs || 2000;
  debugEvent(env, {
    component: 'chain',
    edgeId,
    shipmentId: taskData.shipmentId || edge.shipmentId,
    phase: taskData.phase,
    event: 'execute_started',
    detail: { steps: stepMs },
  });

  await updateEdgeState(db, edgeId, { taskPhase: TaskPhase.COMPLETING }, env);
  edge.taskPhase = TaskPhase.COMPLETING;

  const workNode = resolveDestinationNode(edge) || edge.currentLocation;

  try {
    for (let step = 1; step <= steps; step += 1) {
      await simulationSleep(stepMs);
      const remaining = steps - step;
      const progress = Math.floor((step / steps) * 100);

      await device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({
        id: edgeId,
        remaining,
        progress,
        phase: taskData.phase,
        shipmentId: taskData.shipmentId || edge.shipmentId,
        currentLocation: workNode,
      }));

      await persistTaskProgress(db, edgeId, {
        progressToNext: progress,
        eta: remaining,
        nextNode: workNode,
      }, env);
      debugEvent(env, {
        component: 'chain',
        edgeId,
        shipmentId: taskData.shipmentId || edge.shipmentId,
        phase: taskData.phase,
        event: 'execute_progress',
        detail: { progress, workNode },
      });
    }

    await completeTaskAndChain(edgeId, taskData, device, db, edge, env);
    debugEvent(env, {
      component: 'chain',
      edgeId,
      shipmentId: taskData.shipmentId || edge.shipmentId,
      phase: taskData.phase,
      event: 'execute_done',
    });
  } catch (err) {
    simWarn('EXECUTE_FAILED', {
      component: 'chain',
      edgeId,
      shipmentId: taskData.shipmentId || edge.shipmentId,
      reason: err?.message || String(err),
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, edge, env);
    throw err;
  } finally {
    const fresh = await runtimeCollection(db).findOne({ id: edgeId });
    if (fresh?.taskPhase === TaskPhase.COMPLETING) {
      await resetEdgeAfterFailedCompletion(db, edgeId, fresh, env);
    }
  }
}

function resolveCompletionLocation(taskData, freshEdge) {
  return taskData.finalNode
    || taskData.requiredPlace
    || taskData.destNode
    || taskData.startNode
    || resolveDestinationNode(freshEdge)
    || freshEdge.currentLocation
    || null;
}

async function completeTaskAndChain(edgeId, taskData, device, db, edge, env = process.env) {
  const shipmentId = taskData.shipmentId || edge.shipmentId;
  const phase = taskData.phase || taskData.task;
  if (!shipmentId) {
    simWarn('CHAIN_SKIPPED', {
      component: 'chain',
      edgeId,
      reason: 'NO_SHIPMENT_ID',
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, edge, env);
    return;
  }
  if (!phase) {
    simWarn('CHAIN_SKIPPED', {
      component: 'chain',
      edgeId,
      shipmentId,
      reason: 'NO_PHASE',
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, edge, env);
    return;
  }

  const mergedEdge = (await loadMergedEdge(db, edgeId)) || edge;
  const freshEdge = (await runtimeCollection(db).findOne({ id: edgeId })) || mergedEdge;
  const taskEpoch = (
    taskData.assignmentEpoch
    ?? taskData.task?.assignmentEpoch
    ?? mergedEdge.assignmentEpoch
    ?? mergedEdge.task?.assignmentEpoch
  );
  const edgeEpoch = mergedEdge.assignmentEpoch ?? mergedEdge.task?.assignmentEpoch;
  if (taskEpoch != null && edgeEpoch != null && Number(edgeEpoch) !== Number(taskEpoch)) {
    simWarn('COMPLETION_STALE_EPOCH', {
      component: 'chain',
      edgeId,
      shipmentId,
      phase,
      reason: 'EPOCH_MISMATCH',
      detail: { edgeEpoch, taskEpoch },
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, freshEdge, env);
    return;
  }
  const edgeShipment = mergedEdge.shipmentId || mergedEdge.assignedShipment;
  if (edgeShipment && edgeShipment !== shipmentId) {
    simWarn('COMPLETION_SUPERSEDED', {
      component: 'chain',
      edgeId,
      shipmentId,
      phase,
      reason: 'EDGE_REASSIGNED',
      detail: { edgeShipment },
    });
    await resetEdgeAfterFailedCompletion(db, edgeId, freshEdge, env);
    return;
  }
  const completionLocation = resolveCompletionLocation(taskData, mergedEdge);
  const completionPayload = {
    id: edgeId,
    location: completionLocation,
    phase,
    status: 'completed',
    shipmentId,
    completedAt: new Date(simulationNow()).toISOString(),
    assignmentEpoch: taskEpoch ?? mergedEdge.assignmentEpoch ?? null,
    routeRevision: taskData.routeRevision ?? mergedEdge.routeRevision ?? null,
  };

  // Publish before clearing edge assignment so backend can process while work is still attributable.
  await device.publish(`harboursense/edge/${edgeId}/completion`, JSON.stringify(completionPayload));
  debugEvent(env, {
    component: 'chain',
    edgeId,
    shipmentId,
    phase,
    event: 'completion_published',
    detail: completionPayload,
  });

  edge.completedAssignmentEpoch = taskEpoch ?? null;
  edge.taskPhase = TaskPhase.IDLE;
  edge.shipmentId = null;
  edge.assignedShipment = null;
  edge.currentLocation = completionLocation;
  edge.task = 'idle';
  edge.path = [];
  edge.remainingPath = [];
  edge.nextNode = null;
  edge.startNode = null;
  edge.finalNode = null;
  edge.eta = null;
  edge.progressToNext = 0;
  edge.journeyTime = null;
  await updateEdgeInDB({ ...mergedEdge, ...edge, id: edgeId }, db, env);
}

module.exports = {
  executeTask,
  completeTaskAndChain,
  persistTaskProgress,
  resolveCompletionLocation,
  resetEdgeAfterFailedCompletion,
};
