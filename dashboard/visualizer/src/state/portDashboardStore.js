import {
  gridLayout,
  buildGraphEdges,
  getActiveTravelEdgeKeys,
  NODE_WIDTH,
  NODE_HEIGHT,
} from '../map/layout';
import { computeSpreadOffsets } from '../map/deviceSpread';
import { computeDevicePosition } from '../map/devicePositions';
import { normalizeEdgeDevice } from '../viewModels/edgeDevice';
import { mapPortStateSnapshot } from '../viewModels/diagnostics';
import {
  buildDeviceDisplayMap,
  computeDisplayProgress,
  hasAnimatingDevices,
} from './deviceDisplayState';

export const initialPanelErrors = {
  shipments: null,
  sensorAlerts: null,
  maintenanceAlerts: null,
  portState: null,
};

export const initialState = {
  source: { kind: 'live', status: 'connecting', scenarios: [], positionMs: 0, speed: 1 },
  timeline: [],
  telemetryHistory: [],
  graph: {
    status: 'loading',
    rawNodes: [],
    graphNodes: [],
    positions: {},
    staticEdges: [],
    error: null,
  },
  live: {
    edges: [],
    lastFastPollAt: null,
    error: null,
  },
  panels: {
    shipments: [],
    sensorAlerts: [],
    maintenanceAlerts: [],
    sensors: [],
    craneTelemetry: [],
    maintenanceHistory: [],
    maintenanceTasks: [],
    portState: null,
    errors: { ...initialPanelErrors },
    lastSlowPollAt: null,
  },
  ui: {
    selectedDeviceId: null,
    sensorNodeId: null,
    selectedShipmentId: null,
  },
  display: {
    overlays: {},
    frameNow: Date.now(),
  },
};

export function portDashboardReducer(state, action) {
  switch (action.type) {
    case 'SOURCE_STATUS':
      return { ...state, source: { ...state.source, ...action.payload } };
    case 'TIMELINE_SET':
      return { ...state, timeline: action.payload || [] };
    case 'DATA_RESET':
      return { ...initialState, graph: action.payload?.keepGraph ? state.graph : initialState.graph, source: state.source };
    case 'DATA_SNAPSHOT': {
      const { timestamp, graph, edges, ...panels } = action.payload;
      let next = state;
      if (graph) {
        const built = buildGraphFromResponse(graph.nodes);
        next = portDashboardReducer(next, built ? { type: 'GRAPH_LOADED', payload: built } : { type: 'GRAPH_EMPTY' });
      }
      if (edges) next = portDashboardReducer(next, { type: 'FAST_POLL_OK', payload: { edges, pollTimestamp: timestamp } });
      next = portDashboardReducer(next, { type: 'SLOW_POLL_OK', payload: { ...panels, errors: { ...initialPanelErrors }, lastSlowPollAt: timestamp } });
      return next;
    }
    case 'GRAPH_LOADING':
      return {
        ...state,
        graph: {
          ...state.graph,
          status: 'loading',
          error: null,
        },
      };

    case 'GRAPH_LOADED': {
      const { rawNodes, positions, graphNodes, staticEdges } = action.payload;
      return {
        ...state,
        graph: {
          status: 'ready',
          rawNodes,
          graphNodes,
          positions,
          staticEdges,
          error: null,
        },
      };
    }

    case 'GRAPH_EMPTY':
      return {
        ...state,
        graph: {
          status: 'empty',
          rawNodes: [],
          graphNodes: [],
          positions: {},
          staticEdges: [],
          error: null,
        },
      };

    case 'GRAPH_ERROR':
      return {
        ...state,
        graph: {
          ...state.graph,
          status: 'error',
          error: action.payload,
        },
      };

    case 'FAST_POLL_OK': {
      const { edges, pollTimestamp } = action.payload;
      const overlays = buildDeviceDisplayMap(state.display.overlays, edges, pollTimestamp);
      return {
        ...state,
        live: {
          edges,
          lastFastPollAt: pollTimestamp,
          error: null,
        },
        display: {
          ...state.display,
          overlays,
          frameNow: pollTimestamp,
        },
      };
    }

    case 'EDGE_STREAM_PATCH': {
      const incoming = Array.isArray(action.payload.edges) ? action.payload.edges : [];
      const pollTimestamp = Date.now();
      const byId = new Map(state.live.edges.map((edge) => [edge.id, edge]));
      for (const edge of incoming) {
        if (edge?.id) {
          byId.set(edge.id, edge);
        }
      }
      const edges = Array.from(byId.values());
      const overlays = buildDeviceDisplayMap(state.display.overlays, edges, pollTimestamp);
      return {
        ...state,
        live: {
          edges,
          lastFastPollAt: pollTimestamp,
          error: null,
        },
        display: {
          ...state.display,
          overlays,
          frameNow: pollTimestamp,
        },
      };
    }

    case 'EDGE_STREAM_SNAPSHOT': {
      const edges = Array.isArray(action.payload.edges) ? action.payload.edges : [];
      const pollTimestamp = Date.now();
      const overlays = buildDeviceDisplayMap(state.display.overlays, edges, pollTimestamp);
      return {
        ...state,
        live: {
          edges,
          lastFastPollAt: pollTimestamp,
          error: null,
        },
        display: {
          ...state.display,
          overlays,
          frameNow: pollTimestamp,
        },
      };
    }

    case 'FAST_POLL_ERR':
      return {
        ...state,
        live: {
          ...state.live,
          error: action.payload.message,
        },
      };

    case 'SLOW_POLL_OK':
      return {
        ...state,
        telemetryHistory: action.payload.sensors || action.payload.craneTelemetry
          ? [...state.telemetryHistory, { at: action.payload.lastSlowPollAt, sensors: action.payload.sensors || [], craneTelemetry: action.payload.craneTelemetry || [] }].slice(-120)
          : state.telemetryHistory,
        panels: {
          ...state.panels,
          ...action.payload,
          errors: action.payload.errors,
          lastSlowPollAt: action.payload.lastSlowPollAt,
        },
        display: {
          ...state.display,
          frameNow: action.payload.lastSlowPollAt ?? state.display.frameNow,
        },
      };

    case 'TICK_FRAME':
      return {
        ...state,
        display: {
          ...state.display,
          frameNow: action.payload,
        },
      };

    case 'SELECT_DEVICE':
      return {
        ...state,
        ui: {
          ...state.ui,
          selectedDeviceId: action.payload,
          sensorNodeId: null,
          selectedShipmentId: null,
        },
      };

    case 'SELECT_SENSOR_NODE':
      return {
        ...state,
        ui: {
          ...state.ui,
          sensorNodeId: action.payload,
          selectedDeviceId: null,
          selectedShipmentId: null,
        },
      };

    case 'SELECT_SHIPMENT':
      return { ...state, ui: { selectedDeviceId: null, sensorNodeId: null, selectedShipmentId: action.payload } };

    default:
      return state;
  }
}

export function buildGraphFromResponse(nodesObject) {
  const rawNodes = Object.values(nodesObject || {});
  if (rawNodes.length === 0) {
    return null;
  }

  const positions = gridLayout(rawNodes);
  const graphNodes = rawNodes.map((n) => ({
    id: n.id,
    type: 'port',
    zIndex: 1,
    data: {
      label: n.name || n.label || n.id,
      nodeType: n.type || 'unknown',
    },
    position: positions[n.id],
    ariaLabel: `${n.id}, ${(n.type || 'unknown').replace(/_/g, ' ')}`,
    style: {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    },
  }));

  return {
    rawNodes,
    positions,
    graphNodes,
    staticEdges: buildGraphEdges(rawNodes, positions),
  };
}

export function selectFlowEdges(state) {
  const { graph, live } = state;
  if (graph.status !== 'ready' || graph.rawNodes.length === 0) {
    return graph.staticEdges;
  }
  const activeKeys = getActiveTravelEdgeKeys(live.edges);
  return buildGraphEdges(graph.rawNodes, graph.positions, activeKeys);
}

export function selectDeviceNodes(state) {
  const { live, graph, display } = state;
  const { edges, lastFastPollAt } = live;
  const { positions } = graph;
  const { overlays, frameNow } = display;

  const edgesWithDisplay = (edges || []).map((raw) => {
    const overlay = overlays[raw.id];
    const displayProgress = state.source?.kind === 'replay' || ['paused', 'complete'].includes(state.source?.status) ? raw.progressToNext ?? 0 : computeDisplayProgress(overlay, raw, frameNow);
    return { ...raw, progressToNext: displayProgress };
  });

  const spreadOffsets = computeSpreadOffsets(edgesWithDisplay);

  return (edges || []).map((raw) => {
    const overlay = overlays[raw.id];
    const displayProgress = state.source?.kind === 'replay' || ['paused', 'complete'].includes(state.source?.status) ? raw.progressToNext ?? 0 : computeDisplayProgress(overlay, raw, frameNow);
    const normalized = normalizeEdgeDevice({
      ...raw,
      progressToNext: displayProgress,
    });
    const spread = spreadOffsets.get(raw.id) || { dx: 0, dy: 0 };
    const position = computeDevicePosition(raw, positions, displayProgress, spread);

    return {
      id: `dev-${raw.id}`,
      type: 'device',
      position,
      zIndex: 10,
      draggable: false,
      ariaLabel: `${normalized.id}, ${normalized.statusLabel}, at ${normalized.location || 'unknown location'}`,
      data: {
        device: normalized,
        displayProgress,
        spread,
        pollTick: lastFastPollAt,
      },
    };
  });
}

export function selectFlowNodes(state) {
  const graphNodes = state.graph.graphNodes || [];
  const deviceNodes = selectDeviceNodes(state);
  return [...graphNodes, ...deviceNodes];
}

export function selectPortStateView(state) {
  return mapPortStateSnapshot(state.panels.portState);
}

export function selectHasAnimatingDevices(state) {
  return hasAnimatingDevices(
    state.live.edges,
    state.display.overlays,
    state.display.frameNow,
  );
}

export function selectSelectedDevice(state) {
  if (!state.ui.selectedDeviceId) return null;
  return state.live.edges.find((d) => d.id === state.ui.selectedDeviceId) || null;
}

export function selectNodeSensors(state) {
  const { sensorNodeId } = state.ui;
  if (!sensorNodeId) return [];
  return state.panels.sensors.filter((s) => s.node === sensorNodeId);
}

export function extractApiError(error, fallback) {
  return error?.response?.data?.detail?.message
    || error?.response?.data?.detail
    || error?.message
    || fallback;
}
