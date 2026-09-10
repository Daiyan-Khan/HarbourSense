const {
  alignPathToLocation,
  trimPathFromCurrent,
  pathAfterHop,
  isMidTransit,
  isAtHopBoundary,
  validatePathAdjacency,
  isAdjacent,
} = require('./route-state');

function isInTransit(edge) {
  return isMidTransit(edge);
}

function deriveNextNode(path) {
  if (!path || !Array.isArray(path) || path.length === 0) {
    return null;
  }
  return path[0];
}

function advancePathAfterArrival(path, arrivedNode) {
  return pathAfterHop(path, arrivedNode);
}

/**
 * Promote pendingPath (or task.path) to active path at a hop boundary.
 * Returns partial edge updates or null when nothing to promote.
 */
function promoteRouteAtBoundary(edge, graphMap = {}) {
  if (!edge || !isAtHopBoundary(edge)) {
    return null;
  }

  let sourcePath = [];
  let routeRevision = edge.routeRevision ?? 0;

  if (edge.pendingPath && Array.isArray(edge.pendingPath) && edge.pendingPath.length > 0) {
    sourcePath = edge.pendingPath;
  } else if (edge.task && typeof edge.task === 'object' && Array.isArray(edge.task.path) && edge.task.path.length > 0) {
    sourcePath = edge.task.path;
    routeRevision = edge.task.routeRevision ?? routeRevision;
  } else if (edge.path && Array.isArray(edge.path) && edge.path.length > 0) {
    const trimmed = alignPathToLocation(edge.path, edge.currentLocation, graphMap, true);
    return {
      path: trimmed,
      nextNode: deriveNextNode(trimmed),
    };
  } else {
    return null;
  }

  let path = alignPathToLocation(sourcePath, edge.currentLocation, graphMap);
  if (Object.keys(graphMap).length > 0) {
    const validated = validatePathAdjacency(path, graphMap);
    if (validated) {
      path = validated;
    } else if (path.length > 0) {
      return null;
    }
  }

  if (path.length === 0) {
    return null;
  }

  return {
    path,
    pendingPath: [],
    routeRevision,
    nextNode: deriveNextNode(path),
    progressToNext: 0,
    activeHop: null,
  };
}

/**
 * Resolve active path for movement: trim current path or promote pending at boundary.
 */
function resolveActivePath(edge, graphMap = {}) {
  if (!edge) {
    return { path: [], nextNode: null };
  }

  let path = edge.path && edge.path.length > 0
    ? alignPathToLocation(edge.path, edge.currentLocation, graphMap, true)
    : [];

  if (
    path.length > 0
    && Object.keys(graphMap).length > 0
    && !isAdjacent(graphMap, edge.currentLocation, path[0])
  ) {
    path = [];
  }

  if (path.length === 0 && isAtHopBoundary(edge)) {
    const promoted = promoteRouteAtBoundary(edge, graphMap);
    if (promoted?.path?.length) {
      return { path: promoted.path, nextNode: promoted.nextNode, promoted };
    }
  }

  return {
    path,
    nextNode: deriveNextNode(path),
    promoted: null,
  };
}

module.exports = {
  isInTransit,
  deriveNextNode,
  advancePathAfterArrival,
  promoteRouteAtBoundary,
  resolveActivePath,
  alignPathToLocation,
  trimPathFromCurrent,
};
