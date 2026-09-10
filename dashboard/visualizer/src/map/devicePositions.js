import { getEdgeAnchors, NODE_WIDTH, NODE_HEIGHT, DEVICE_SIZE } from './layout';

function nodeCenterTopLeft(nodePos) {
  return {
    x: nodePos.x + NODE_WIDTH / 2 - DEVICE_SIZE / 2,
    y: nodePos.y + NODE_HEIGHT / 2 - DEVICE_SIZE / 2,
  };
}

function anchorToTopLeft(anchor) {
  return {
    x: anchor.x - DEVICE_SIZE / 2,
    y: anchor.y - DEVICE_SIZE / 2,
  };
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

export function computeDevicePosition(
  device,
  nodePositions,
  displayProgress = null,
  spreadOffset = { dx: 0, dy: 0 },
) {
  if (!device || !device.currentLocation) return { x: 0, y: 0 };

  const startNode = device.currentLocation;
  const endNode = device.nextNode;
  const progress = displayProgress ?? device.progressToNext ?? 0;

  const startPos = nodePositions[startNode];
  if (!startPos) return { x: 0, y: 0 };

  if (progress <= 0 || !endNode || typeof endNode !== 'string') {
    const base = nodeCenterTopLeft(startPos);
    return {
      x: base.x + spreadOffset.dx,
      y: base.y + spreadOffset.dy,
    };
  }

  const endPos = nodePositions[endNode];
  if (!endPos) return { x: 0, y: 0 };

  const anchors = getEdgeAnchors(startNode, endNode, nodePositions);
  if (!anchors) return { x: 0, y: 0 };

  const progressRatio = Math.min(1, Math.max(0, progress / 100));
  const anchorX = lerp(anchors.sourceAnchor.x, anchors.targetAnchor.x, progressRatio);
  const anchorY = lerp(anchors.sourceAnchor.y, anchors.targetAnchor.y, progressRatio);
  const base = anchorToTopLeft({ x: anchorX, y: anchorY });

  return {
    x: base.x + spreadOffset.dx,
    y: base.y + spreadOffset.dy,
  };
}
