import axios from 'axios';
import { getApiBaseUrl } from '../apiConfig';
import { buildGraphFromResponse, extractApiError } from '../state/portDashboardStore';

const PANEL_ENDPOINTS = {
  sensors: '/api/sensors', shipments: '/api/shipments', sensorAlerts: '/api/alerts/sensor',
  maintenanceAlerts: '/api/alerts/maintenance', portState: '/api/port-state',
  craneTelemetry: '/api/telemetry/cranes',
  maintenanceHistory: '/api/alerts/maintenance?unresolved_only=false',
  maintenanceTasks: '/api/maintenance/tasks',
};

export function createLiveSource({ emit, client = axios, EventSourceClass = window.EventSource }) {
  const baseUrl = getApiBaseUrl();
  let disposed = false;
  let stream = null;
  let graphReady = false;
  let graphBusy = false;
  let fastBusy = false;
  let slowBusy = false;
  let demoBusy = false;
  let tick = 0;
  let interval;
  let graphRetryAt = 0;
  let graphDelay = 1000;
  let streamRetryAt = 0;
  let streamDelay = 1000;
  let streamLastMessageAt = 0;
  let demoSupported = true;
  let selectedScenario = null;
  let scenarios = [];
  let currentRunId = null;
  let currentSequence = -1;
  const retiredRuns = new Set();
  let pendingCommand = null;
  let generation = 0;

  const safeEmit = (action) => { if (!disposed) emit(action); };
  const get = (path) => client.get(`${baseUrl}${path}`, { timeout: 8000 });
  const publishDemo = (state) => {
    if (disposed || retiredRuns.has(state.runId)) return;
    if (currentRunId === state.runId && Number.isFinite(state.sequence) && state.sequence < currentSequence) return;
    if (currentRunId && currentRunId !== state.runId) {
      retiredRuns.add(currentRunId);
      currentSequence = -1;
      generation += 1;
      safeEmit({ type: 'DATA_RESET', payload: { keepGraph: true } });
    }
    currentRunId = state.runId;
    currentSequence = state.sequence ?? currentSequence;
    scenarios = state.scenarios || scenarios;
    if (!selectedScenario || ['running', 'paused', 'complete'].includes(state.status)) selectedScenario = state.scenarioId || selectedScenario;
    safeEmit({ type: 'SOURCE_STATUS', payload: {
      kind: state.enabled ? 'demo' : 'live', ...state,
      positionMs: state.simTimeMs || 0, controlsEnabled: state.enabled,
      selectedScenarioId: selectedScenario,
      scenario: scenarios.find((item) => item.id === selectedScenario),
      error: state.error?.message || null,
    } });
    safeEmit({ type: 'TIMELINE_SET', payload: state.timeline || [] });
  };

  const loadGraph = async () => {
    if (graphBusy || disposed) return;
    graphBusy = true;
    try {
      const response = await get('/api/graph');
      if (disposed) return;
      const graph = buildGraphFromResponse(response.data.nodes);
      graphReady = Boolean(graph);
      safeEmit(graph ? { type: 'GRAPH_LOADED', payload: graph } : { type: 'GRAPH_EMPTY' });
      graphDelay = 1000;
    } catch (error) {
      safeEmit({ type: 'GRAPH_ERROR', payload: extractApiError(error, 'Unable to load port graph') });
      graphRetryAt = Date.now() + graphDelay;
      graphDelay = Math.min(15000, graphDelay * 2);
    } finally { graphBusy = false; }
  };

  const pollFast = async () => {
    if (fastBusy || disposed) return;
    fastBusy = true;
    const requestGeneration = generation;
    try {
      const response = await get('/api/edges');
      if (requestGeneration !== generation) return;
      safeEmit({ type: 'FAST_POLL_OK', payload: { edges: Array.isArray(response.data) ? response.data : [], pollTimestamp: Date.now() } });
    } catch (error) {
      safeEmit({ type: 'FAST_POLL_ERR', payload: { pollTimestamp: Date.now(), message: extractApiError(error, 'Unable to refresh device data') } });
    } finally { fastBusy = false; }
  };

  const pollSlow = async () => {
    if (slowBusy || disposed) return;
    slowBusy = true;
    const requestGeneration = generation;
    try {
      const entries = Object.entries(PANEL_ENDPOINTS);
      const responses = await Promise.allSettled(entries.map(([, path]) => get(path)));
      if (disposed || requestGeneration !== generation) return;
      const payload = { errors: {}, lastSlowPollAt: Date.now() };
      responses.forEach((response, index) => {
        const key = entries[index][0];
        if (response.status === 'fulfilled') {
          payload[key] = key === 'portState' ? response.value.data
            : Array.isArray(response.value.data) ? response.value.data : [];
          payload.errors[key] = null;
        } else {
          payload.errors[key] = extractApiError(response.reason, `Unable to load ${key}`);
        }
      });
      safeEmit({ type: 'SLOW_POLL_OK', payload });
    } finally { slowBusy = false; }
  };

  const pollDemo = async () => {
    if (!demoSupported || demoBusy || disposed) return;
    demoBusy = true;
    try {
      const response = await get('/api/demo/state');
      if (!disposed) publishDemo(response.data);
    } catch (error) {
      if (error?.response?.status === 404) {
        demoSupported = false;
        safeEmit({ type: 'SOURCE_STATUS', payload: { kind: 'live', controlsEnabled: false } });
      }
    } finally { demoBusy = false; }
  };

  const connectStream = () => {
    if (stream || !EventSourceClass || disposed) return;
    try {
      stream = new EventSourceClass(`${baseUrl}/api/edges/stream`);
      stream.onopen = () => {
        streamDelay = 1000;
        streamLastMessageAt = Date.now();
        safeEmit({ type: 'SOURCE_STATUS', payload: { transport: 'SSE + snapshot reconciliation' } });
        pollFast();
        pollSlow();
      };
      stream.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (!['snapshot', 'patch'].includes(message.type) || !Array.isArray(message.edges)) return;
          if (message.runId === currentRunId && Number.isFinite(message.demo?.sequence) && message.demo.sequence < currentSequence) return;
          if (message.demo) publishDemo(message.demo);
          if (currentRunId && message.runId && message.runId !== currentRunId) return;
          streamLastMessageAt = Date.now();
          safeEmit({ type: message.type === 'snapshot' ? 'EDGE_STREAM_SNAPSHOT' : 'EDGE_STREAM_PATCH', payload: { edges: message.edges } });
        } catch {
          safeEmit({ type: 'SOURCE_STATUS', payload: { transport: 'Recovering invalid stream data' } });
          pollFast();
        }
      };
      stream.onerror = () => {
        stream?.close();
        stream = null;
        streamRetryAt = Date.now() + streamDelay;
        streamDelay = Math.min(15000, streamDelay * 2);
        safeEmit({ type: 'SOURCE_STATUS', payload: { transport: 'Polling · stream reconnecting' } });
        pollFast();
      };
    } catch {
      stream = null;
      streamRetryAt = Date.now() + 15000;
    }
  };

  const refresh = async () => {
    if (disposed) return;
    tick += 1;
    if (!graphReady && Date.now() >= graphRetryAt) await loadGraph();
    if (!graphReady || disposed) return;
    if (Date.now() >= streamRetryAt) connectStream();
    // Reconcile even an open stream: an open socket alone is not evidence of telemetry.
    if (tick % 3 === 1 || Date.now() - streamLastMessageAt > 15000) pollFast();
    if (tick % 9 === 1) pollSlow();
    if (tick % 3 === 1) pollDemo();
  };

  return {
    start: () => {
      safeEmit({ type: 'SOURCE_STATUS', payload: { kind: 'live', status: 'connecting', transport: 'Connecting', controlsEnabled: false } });
      refresh();
      interval = window.setInterval(refresh, 1000);
    },
    retry: () => { graphRetryAt = 0; demoSupported = true; loadGraph(); pollFast(); pollSlow(); pollDemo(); },
    choose: (id) => {
      selectedScenario = id;
      safeEmit({ type: 'SOURCE_STATUS', payload: { selectedScenarioId: id, scenario: scenarios.find((item) => item.id === id) } });
    },
    command: async (action, value) => {
      if (pendingCommand) return;
      // Retrying an uncertain HTTP response uses the same command identity.
      const commandId = window.crypto?.randomUUID?.() || 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (char) => { const value = Math.floor(Math.random() * 16); return (char === 'x' ? value : (value & 3) | 8).toString(16); });
      const body = { commandId, ...(action === 'start' ? { scenarioId: selectedScenario || 'normal' } : {}), ...(action === 'speed' ? { speed: Number(value) } : {}) };
      pendingCommand = commandId;
      safeEmit({ type: 'SOURCE_STATUS', payload: { commandPending: true, commandError: null } });
      try {
        let response;
        try { response = await client.post(`${baseUrl}/api/demo/${action}`, body, { timeout: 15000 }); }
        catch (error) {
          if (error.response) throw error;
          response = await client.post(`${baseUrl}/api/demo/${action}`, body, { timeout: 15000 });
        }
        if (disposed) return;
        publishDemo(response.data.state);
        pollFast();
        pollSlow();
      } catch (error) {
        safeEmit({ type: 'SOURCE_STATUS', payload: { commandError: extractApiError(error, 'Control could not be acknowledged. Retry the action.') } });
      } finally {
        pendingCommand = null;
        safeEmit({ type: 'SOURCE_STATUS', payload: { commandPending: false } });
      }
    },
    dispose: () => { disposed = true; generation += 1; stream?.close(); window.clearInterval(interval); },
  };
}
