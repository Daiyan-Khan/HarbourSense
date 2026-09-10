const RUNTIME_COLLECTION = 'edgeRuntime';
const ASSIGNMENT_COLLECTION = 'edgeAssignments';
const LEGACY_COLLECTION = 'edgeDevices';

const ASSIGNMENT_FIELDS = new Set([
  'shipmentId',
  'assignedShipment',
  'task',
  'pendingPath',
  'routeRevision',
  'assignmentEpoch',
  'startNode',
  'finalNode',
  'claimedAt',
]);

const RUNTIME_FIELDS = new Set([
  'type',
  'roles',
  'speed',
  'capacity',
  'currentLocation',
  'taskPhase',
  'path',
  'remainingPath',
  'nextNode',
  'progressToNext',
  'eta',
  'activeHop',
  'stateRevision',
  'workflowLeg',
  'pickupCompleted',
  'acceptedAssignmentEpoch',
  'completedAssignmentEpoch',
  'stallCode',
  'lastTransitionAt',
  'debugEvent',
  'journeyTime',
  'taskCompletionTime',
]);

function hasOpenAssignment(assignment) {
  if (!assignment) return false;
  return Boolean(assignment.shipmentId || assignment.assignedShipment);
}

function splitEdgeDocument(doc) {
  const edgeId = doc.id;
  if (!edgeId) {
    throw new Error('edge document requires id');
  }

  const runtime = { id: edgeId };
  const assignment = { id: edgeId };

  for (const [key, value] of Object.entries(doc)) {
    if (key === 'id') continue;
    if (ASSIGNMENT_FIELDS.has(key)) {
      assignment[key] = value;
    } else if (RUNTIME_FIELDS.has(key) || key === 'updatedAt') {
      runtime[key] = value;
      if (key === 'updatedAt') {
        assignment[key] = value;
      }
    } else {
      runtime[key] = value;
    }
  }

  if (!hasOpenAssignment(assignment)) {
    const kept = { id: edgeId };
    for (const key of ['routeRevision', 'pendingPath', 'assignmentEpoch']) {
      if (assignment[key] !== undefined) {
        kept[key] = assignment[key];
      }
    }
    return { assignment: kept, runtime };
  }
  return { assignment, runtime };
}

function mergeEdgeSnapshot(assignment, runtime) {
  if (!runtime) {
    throw new Error('runtime document is required');
  }

  const merged = { ...runtime };
  merged.id = runtime.id || assignment?.id;

  if (assignment) {
    for (const key of ASSIGNMENT_FIELDS) {
      if (key in assignment) {
        merged[key] = assignment[key];
      }
    }
  }

  if (!hasOpenAssignment(assignment)) {
    if (merged.shipmentId === undefined) merged.shipmentId = null;
    if (merged.assignedShipment === undefined) merged.assignedShipment = null;
    if (merged.taskPhase === 'idle') {
      merged.task = merged.task ?? 'idle';
    }
  }

  if (merged.stateRevision == null) merged.stateRevision = 0;
  if (merged.routeRevision == null) {
    merged.routeRevision = assignment?.routeRevision ?? 0;
  }
  if (merged.progressToNext == null) merged.progressToNext = 0;
  return merged;
}

function runtimeCollection(db) {
  return db.collection(RUNTIME_COLLECTION);
}

function assignmentCollection(db) {
  return db.collection(ASSIGNMENT_COLLECTION);
}

async function loadMergedEdge(db, edgeId) {
  const runtime = await runtimeCollection(db).findOne({ id: edgeId });
  if (!runtime) {
    return db.collection(LEGACY_COLLECTION).findOne({ id: edgeId });
  }
  const assignment = await assignmentCollection(db).findOne({ id: edgeId });
  return mergeEdgeSnapshot(assignment, runtime);
}

async function loadMergedEdges(db) {
  const runtimeCount = await runtimeCollection(db).countDocuments({});
  if (runtimeCount === 0) {
    const legacy = await db.collection(LEGACY_COLLECTION).find({}).toArray();
    if (legacy.length > 0) return legacy;
  }

  const assignments = await assignmentCollection(db).find({}).toArray();
  const byId = new Map(assignments.map((doc) => [doc.id, doc]));
  const runtimes = await runtimeCollection(db).find({}).toArray();
  return runtimes.map((runtime) => mergeEdgeSnapshot(byId.get(runtime.id), runtime));
}

function runtimeDocumentFromSeed(edge) {
  const { assignment, runtime } = (() => {
    const a = { id: edge.id };
    const r = { id: edge.id };
    for (const [key, value] of Object.entries(edge)) {
      if (key === 'id') continue;
      if (ASSIGNMENT_FIELDS.has(key)) {
        a[key] = value;
      } else {
        r[key] = value;
      }
    }
    return { assignment: a, runtime: r };
  })();
  if (!hasOpenAssignment(assignment)) {
    return { runtime, assignment: { id: edge.id } };
  }
  return { runtime, assignment };
}

module.exports = {
  RUNTIME_COLLECTION,
  ASSIGNMENT_COLLECTION,
  LEGACY_COLLECTION,
  ASSIGNMENT_FIELDS,
  RUNTIME_FIELDS,
  splitEdgeDocument,
  mergeEdgeSnapshot,
  runtimeCollection,
  assignmentCollection,
  loadMergedEdge,
  loadMergedEdges,
  runtimeDocumentFromSeed,
  hasOpenAssignment,
};
