export const NODE_WIDTH = 150;
export const NODE_HEIGHT = 100;
export const DEVICE_SIZE = 30;

export function undirectedEdgeKey(a, b) {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function computeSideMidpoints(srcPos, tgtPos) {
  let sourceX;
  let sourceY;
  let targetX;
  let targetY;

  if (srcPos.y === tgtPos.y) {
    if (srcPos.x <= tgtPos.x) {
      sourceX = srcPos.x + NODE_WIDTH;
      sourceY = srcPos.y + NODE_HEIGHT / 2;
      targetX = tgtPos.x;
      targetY = tgtPos.y + NODE_HEIGHT / 2;
    } else {
      sourceX = srcPos.x;
      sourceY = srcPos.y + NODE_HEIGHT / 2;
      targetX = tgtPos.x + NODE_WIDTH;
      targetY = tgtPos.y + NODE_HEIGHT / 2;
    }
  } else if (srcPos.x === tgtPos.x) {
    if (srcPos.y <= tgtPos.y) {
      sourceX = srcPos.x + NODE_WIDTH / 2;
      sourceY = srcPos.y + NODE_HEIGHT;
      targetX = tgtPos.x + NODE_WIDTH / 2;
      targetY = tgtPos.y;
    } else {
      sourceX = srcPos.x + NODE_WIDTH / 2;
      sourceY = srcPos.y;
      targetX = tgtPos.x + NODE_WIDTH / 2;
      targetY = tgtPos.y + NODE_HEIGHT;
    }
  } else {
    sourceX = srcPos.x + NODE_WIDTH / 2;
    sourceY = srcPos.y + NODE_HEIGHT / 2;
    targetX = tgtPos.x + NODE_WIDTH / 2;
    targetY = tgtPos.y + NODE_HEIGHT / 2;
  }

  return { sourceX, sourceY, targetX, targetY };
}

export function getEdgeAnchors(nodeA, nodeB, positions) {
  const srcPos = positions[nodeA];
  const tgtPos = positions[nodeB];
  if (!srcPos || !tgtPos) {
    return null;
  }

  const { sourceX, sourceY, targetX, targetY } = computeSideMidpoints(srcPos, tgtPos);
  return {
    sourceAnchor: { x: sourceX, y: sourceY },
    targetAnchor: { x: targetX, y: targetY },
  };
}

export function gridLayout(rawNodes) {
  const nodePositions = {};
  rawNodes.forEach((node, index) => {
    const prefix = node.id.match(/[A-Z]+/)?.[0] || 'A';
    const suffix = parseInt(node.id.match(/[0-9]+/)?.[0] || String(index + 1), 10);
    let row = 0;
    for (let i = 0; i < prefix.length; i++) {
      row = row * 26 + (prefix.charCodeAt(i) - 'A'.charCodeAt(0) + 1);
    }
    row -= 1;
    const col = suffix - 1;
    nodePositions[node.id] = {
      x: col * (NODE_WIDTH + 100),
      y: row * (NODE_HEIGHT + 100),
    };
  });
  return nodePositions;
}

export function buildGraphEdges(rawNodes, pos, activeEdgeKeys = new Set()) {
  const seen = new Set();
  const graphEdges = [];

  rawNodes.forEach((node) => {
    Object.values(node.neighbors || {}).forEach((nbrId) => {
      const key = undirectedEdgeKey(node.id, nbrId);
      if (seen.has(key)) return;
      seen.add(key);

      const srcPos = pos[node.id];
      const tgtPos = pos[nbrId];
      if (!srcPos || !tgtPos) return;

      const { sourceX, sourceY, targetX, targetY } = computeSideMidpoints(srcPos, tgtPos);
      const isActive = activeEdgeKeys.has(key);

      graphEdges.push({
        id: key,
        source: node.id,
        target: nbrId,
        type: 'side',
        data: {
          sourceX,
          sourceY,
          targetX,
          targetY,
          active: isActive,
        },
      });
    });
  });

  return graphEdges;
}

export function getActiveTravelEdgeKeys(devices) {
  const keys = new Set();
  (devices || []).forEach((device) => {
    const progress = device.progressToNext ?? 0;
    const nextNode = device.nextNode;
    const currentLocation = device.currentLocation;
    if (
      progress > 0
      && progress < 100
      && nextNode
      && typeof nextNode === 'string'
      && currentLocation
    ) {
      keys.add(undirectedEdgeKey(currentLocation, nextNode));
    }
  });
  return keys;
}
