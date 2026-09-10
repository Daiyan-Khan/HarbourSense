const { simWarn } = require('./sim-debug');

function normalizeNodeId(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  return String(value).trim().replace(/-/g, '');
}

function neighborIds(graphMap, nodeId) {
  if (!graphMap || !nodeId) {
    return [];
  }
  const node = graphMap[nodeId];
  if (!node || !node.neighbors) {
    return [];
  }
  return Object.values(node.neighbors)
    .map((n) => normalizeNodeId(n))
    .filter(Boolean);
}

function isAdjacent(graphMap, fromNode, toNode) {
  if (!fromNode || !toNode || fromNode === toNode) {
    return fromNode === toNode;
  }
  return neighborIds(graphMap, fromNode).includes(normalizeNodeId(toNode));
}

function validatePathAdjacency(path, graphMap) {
  if (!path || !Array.isArray(path) || path.length === 0) {
    return null;
  }
  const normalized = path.map((n) => normalizeNodeId(n)).filter(Boolean);
  if (normalized.length === 0) {
    return null;
  }
  if (normalized.length === 1) {
    return normalized;
  }
  for (let i = 0; i < normalized.length - 1; i += 1) {
    if (!isAdjacent(graphMap, normalized[i], normalized[i + 1])) {
      return null;
    }
  }
  return normalized;
}

function isMidTransit(edge) {
  const progress = edge?.progressToNext ?? 0;
  return progress > 0 && progress < 100;
}

function isAtHopBoundary(edge) {
  if (!edge) {
    return false;
  }
  if (edge.activeHop && isMidTransit(edge)) {
    return false;
  }
  return !isMidTransit(edge);
}

function buildActiveHop(fromNode, toNode, routeRevision) {
  return {
    from: fromNode,
    to: toNode,
    startedAt: new Date().toISOString(),
    revision: routeRevision ?? 0,
  };
}

async function loadGraphMap(db) {
  try {
    const col = db.collection('graph');
    if (!col || typeof col.find !== 'function') {
      return {};
    }
    const docs = await col.find({}).toArray();
    const map = {};
    for (const doc of docs) {
      if (doc.id) {
        map[doc.id] = doc;
      } else if (doc.nodes && typeof doc.nodes === 'object') {
        Object.assign(map, doc.nodes);
      }
    }
    return map;
  } catch {
    return {};
  }
}

function rebuildPathFromNextNode(edge, graphMap = {}) {
  const repaired = repairPath(edge, graphMap);
  if (repaired && repaired.length > 0) {
    return repaired;
  }
  if (edge?.path && Array.isArray(edge.path) && edge.path.length > 0) {
    return alignPathToLocation(edge.path, edge.currentLocation, graphMap);
  }
  return [];
}

function repairPath(edge, graphMap) {
  const hasGraph = graphMap && Object.keys(graphMap).length > 0;
  if (edge.pendingPath && Array.isArray(edge.pendingPath) && edge.pendingPath.length > 0) {
    const trimmed = alignPathToLocation(edge.pendingPath, edge.currentLocation, graphMap);
    if (!hasGraph) {
      return trimmed.length > 0 ? trimmed : null;
    }
    const validated = validatePathAdjacency(trimmed, graphMap);
    if (validated && validated.length > 0) {
      return validated;
    }
  }
  const taskPath = edge.task && typeof edge.task === 'object' ? edge.task.path : null;
  if (taskPath && Array.isArray(taskPath) && taskPath.length > 0) {
    const trimmed = alignPathToLocation(taskPath, edge.currentLocation, graphMap);
    if (!hasGraph) {
      return trimmed.length > 0 ? trimmed : null;
    }
    const validated = validatePathAdjacency(trimmed, graphMap);
    if (validated && validated.length > 0) {
      return validated;
    }
  }
  return null;
}

/**
 * Align a backend-style path (may include current node and prior hops) to remaining
 * hops from currentLocation. Matches python-backend trim_path_from_current, then
 * drops the current node prefix so path[0] is always the next hop target.
 */
function alignPathToLocation(path, currentLocation, graphMap = {}, hopRelative = false) {
  if (!path || !Array.isArray(path) || path.length === 0) {
    return [];
  }
  const current = normalizeNodeId(currentLocation);
  if (!current) {
    return [];
  }

  const normalized = path.map((n) => normalizeNodeId(n)).filter(Boolean);
  if (normalized.length === 0) {
    return [];
  }

  let aligned;
  if (normalized[0] === current) {
    aligned = [...normalized];
  } else if (hopRelative && Object.keys(graphMap || {}).length > 0 && isAdjacent(graphMap, current, normalized[0])) {
    // A remaining route may revisit this node after a required pickup.
    aligned = [...normalized];
  } else if (normalized.includes(current)) {
    aligned = normalized.slice(normalized.indexOf(current));
  } else if (Object.keys(graphMap || {}).length > 0 && isAdjacent(graphMap, current, normalized[0])) {
    aligned = [...normalized];
  } else if (Object.keys(graphMap || {}).length === 0 && !normalized.includes(current)) {
    // Hop-relative path without graph context (path[0] is next hop target).
    aligned = [...normalized];
  } else {
    return [];
  }

  while (aligned.length > 0 && aligned[0] === current) {
    aligned.shift();
  }

  return aligned;
}

function trimPathFromCurrent(path, currentLocation, graphMap = {}) {
  return alignPathToLocation(path, currentLocation, graphMap);
}

function promotePendingPath(edge, graphMap = {}) {
  if (!edge.pendingPath || !Array.isArray(edge.pendingPath) || edge.pendingPath.length === 0) {
    return null;
  }
  const trimmed = alignPathToLocation(edge.pendingPath, edge.currentLocation, graphMap);
  return {
    path: trimmed,
    pendingPath: [],
    routeRevision: edge.routeRevision ?? 0,
  };
}

function pathAfterHop(currentPath, arrivedNode) {
  if (!currentPath || !Array.isArray(currentPath) || currentPath.length === 0) {
    return [];
  }
  // Movement always completes a hop to path[0]; consume it even if stale head mismatches.
  return currentPath.slice(1);
}

function logPathRepairFailed(edgeId, edge, env = process.env) {
  simWarn('PATH_REPAIR_FAILED', {
    component: 'movement',
    edgeId,
    shipmentId: edge?.shipmentId,
    reason: 'NO_VALID_PATH',
    detail: {
      loc: edge?.currentLocation,
      finalNode: edge?.finalNode,
      pendingLen: edge?.pendingPath?.length ?? 0,
    },
  });
}

module.exports = {
  normalizeNodeId,
  neighborIds,
  isAdjacent,
  validatePathAdjacency,
  isMidTransit,
  isAtHopBoundary,
  buildActiveHop,
  loadGraphMap,
  repairPath,
  alignPathToLocation,
  trimPathFromCurrent,
  promotePendingPath,
  pathAfterHop,
  logPathRepairFailed,
  rebuildPathFromNextNode,
};
