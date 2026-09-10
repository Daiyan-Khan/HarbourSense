const { debugLog } = require('./sim-debug');
const { runtimeCollection, loadMergedEdge } = require('./edge-collections');

const PHYSICAL_STATE_FIELDS = new Set([
  'currentLocation',
  'progressToNext',
  'path',
  'taskPhase',
  'nextNode',
  'activeHop',
  'routeRevision',
]);

const RUNTIME_WRITE_FIELDS = new Set([
  'currentLocation',
  'progressToNext',
  'path',
  'remainingPath',
  'taskPhase',
  'nextNode',
  'activeHop',
  'routeRevision',
  'eta',
  'journeyTime',
  'taskCompletionTime',
  'workflowLeg',
  'pickupCompleted',
  'acceptedAssignmentEpoch',
  'completedAssignmentEpoch',
  'stallCode',
  'lastTransitionAt',
  'debugEvent',
  'task',
  'shipmentId',
  'assignedShipment',
  'startNode',
  'finalNode',
]);

function touchesPhysicalState(updates) {
  return Object.keys(updates).some((key) => PHYSICAL_STATE_FIELDS.has(key));
}

function stripServerManagedFields(updates) {
  const payload = { ...updates };
  delete payload.stateRevision;
  delete payload.pendingPath;
  delete payload.assignmentEpoch;
  return payload;
}

function runtimeOnlyPayload(updates) {
  const payload = stripServerManagedFields({ ...updates });
  for (const key of Object.keys(payload)) {
    if (!RUNTIME_WRITE_FIELDS.has(key) && key !== 'updatedAt') {
      delete payload[key];
    }
  }
  return payload;
}

async function updateEdgeState(db, edgeId, updates, env = process.env) {
  const col = runtimeCollection(db);
  const edgeBefore = await col.findOne({ id: edgeId });
  const payload = runtimeOnlyPayload({ ...updates, updatedAt: new Date() });
  const mongoUpdate = touchesPhysicalState(updates)
    ? { $set: payload, $inc: { stateRevision: 1 } }
    : { $set: payload };
  await col.updateOne({ id: edgeId }, mongoUpdate, { upsert: true });
  const edgeAfter = await col.findOne({ id: edgeId });
  const mergedBefore = edgeBefore ? await loadMergedEdge(db, edgeId) : null;
  const mergedAfter = edgeAfter ? await loadMergedEdge(db, edgeId) : null;

  if (mergedAfter && mergedAfter.task !== 'awaiting task') {
    const shipmentId = mergedAfter.shipmentId ?? mergedBefore?.shipmentId ?? null;
    if (mergedBefore && mergedAfter && mergedBefore.task !== mergedAfter.task) {
      const oldTaskStr = typeof mergedBefore.task === 'string'
        ? mergedBefore.task
        : JSON.stringify({ phase: mergedBefore.task?.phase, shipmentId: mergedBefore.task?.shipmentId });
      const newTaskStr = typeof mergedAfter.task === 'string'
        ? mergedAfter.task
        : JSON.stringify({ phase: mergedAfter.task?.phase, shipmentId: mergedAfter.task?.shipmentId });
      debugLog(env, `Edge ${edgeId} task changed: ${oldTaskStr} -> ${newTaskStr} shipment=${shipmentId ?? 'null'}`);
    }
    if (mergedBefore && mergedBefore.taskPhase !== mergedAfter.taskPhase) {
      debugLog(
        env,
        `Edge ${edgeId} phase changed: ${mergedBefore.taskPhase} -> ${mergedAfter.taskPhase} shipment=${shipmentId ?? 'null'}`,
      );
    }
    if (mergedBefore && mergedBefore.currentLocation !== mergedAfter.currentLocation) {
      debugLog(
        env,
        `Edge ${edgeId} location changed: ${mergedBefore.currentLocation} -> ${mergedAfter.currentLocation} shipment=${shipmentId ?? 'null'}`,
      );
    }
    if (mergedBefore && mergedBefore.shipmentId !== mergedAfter.shipmentId) {
      debugLog(
        env,
        `Edge ${edgeId} shipment changed: ${mergedBefore.shipmentId} -> ${mergedAfter.shipmentId}`,
      );
    }
  }

  return mergedAfter || edgeAfter;
}

async function applyArrivalUpdate(db, edgeId, edge, arrivalUpdates, env = process.env) {
  const col = runtimeCollection(db);
  const filter = { id: edgeId };
  if (edge?.activeHop?.startedAt) {
    filter['activeHop.startedAt'] = edge.activeHop.startedAt;
  }
  const payload = runtimeOnlyPayload({ ...arrivalUpdates, updatedAt: new Date() });
  const result = await col.updateOne(
    filter,
    { $set: payload, $inc: { stateRevision: 1 } },
  );
  const current = await loadMergedEdge(db, edgeId);
  const modifiedCount = result?.modifiedCount ?? 0;
  if (modifiedCount === 0) {
    debugLog(env, `[DEBUG SIM] Arrival CAS miss for ${edgeId} at ${arrivalUpdates.currentLocation}`);
    return { edge: current, applied: false };
  }
  return { edge: current, applied: true };
}

async function persistMovementProgress(db, edgeId, fields, env = process.env) {
  await updateEdgeState(db, edgeId, fields, env);
}

async function updateEdgeInDB(edge, db, env = process.env) {
  const col = runtimeCollection(db);
  await col.updateOne(
    { id: edge.id },
    {
      $set: runtimeOnlyPayload({
        taskPhase: edge.taskPhase,
        shipmentId: edge.shipmentId,
        assignedShipment: edge.assignedShipment,
        currentLocation: edge.currentLocation,
        task: edge.task,
        path: edge.path,
        remainingPath: edge.remainingPath,
        routeRevision: edge.routeRevision ?? 0,
        activeHop: edge.activeHop ?? null,
        nextNode: edge.nextNode,
        startNode: edge.startNode,
        finalNode: edge.finalNode,
        eta: edge.eta,
        progressToNext: edge.progressToNext ?? 0,
        journeyTime: edge.journeyTime,
        updatedAt: new Date(),
      }),
      $inc: { stateRevision: 1 },
    },
    { upsert: true },
  );
  debugLog(
    env,
    `[CHAIN] Updated edge ${edge.id} in DB: phase=${edge.taskPhase}, location=${edge.currentLocation}, shipment=${edge.shipmentId ?? 'null'}`,
  );
}

module.exports = {
  updateEdgeState,
  applyArrivalUpdate,
  persistMovementProgress,
  updateEdgeInDB,
  touchesPhysicalState,
  stripServerManagedFields,
  runtimeOnlyPayload,
};
