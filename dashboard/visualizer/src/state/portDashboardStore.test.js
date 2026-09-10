import {
  initialState,
  portDashboardReducer,
  buildGraphFromResponse,
  selectFlowEdges,
  selectDeviceNodes,
} from './portDashboardStore';

describe('portDashboardReducer', () => {
  test('FAST_POLL_OK updates edges and display overlays', () => {
    const pollTimestamp = 5000;
    const edges = [{
      id: 'd1',
      stateRevision: 2,
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 30,
      eta: 10,
    }];

    const next = portDashboardReducer(initialState, {
      type: 'FAST_POLL_OK',
      payload: { edges, pollTimestamp },
    });

    expect(next.live.edges).toEqual(edges);
    expect(next.live.error).toBeNull();
    expect(next.display.frameNow).toBe(pollTimestamp);
    expect(next.display.overlays.d1.baselineProgress).toBe(30);
    expect(next.display.overlays.d1.stateRevision).toBe(2);
  });

  test('FAST_POLL_ERR preserves edges and sets error', () => {
    const stateWithEdges = portDashboardReducer(initialState, {
      type: 'FAST_POLL_OK',
      payload: {
        edges: [{ id: 'd1', currentLocation: 'A1', progressToNext: 0, eta: 0 }],
        pollTimestamp: 1000,
      },
    });

    const next = portDashboardReducer(stateWithEdges, {
      type: 'FAST_POLL_ERR',
      payload: { pollTimestamp: 2000, message: 'edges down' },
    });

    expect(next.live.edges).toHaveLength(1);
    expect(next.live.error).toBe('edges down');
  });

  test('SLOW_POLL_OK updates all panel slices atomically', () => {
    const next = portDashboardReducer(initialState, {
      type: 'SLOW_POLL_OK',
      payload: {
        sensors: [{ id: 's1', node: 'A1' }],
        shipments: [{ id: 'sh1', status: 'arrived' }],
        sensorAlerts: [{ id: 'a1' }],
        maintenanceAlerts: [{ assetId: 'c1' }],
        portState: { pending_counts: {} },
        errors: {
          shipments: null,
          sensorAlerts: null,
          maintenanceAlerts: null,
          portState: null,
        },
        lastSlowPollAt: 9000,
      },
    });

    expect(next.panels.shipments).toHaveLength(1);
    expect(next.panels.sensors).toHaveLength(1);
    expect(next.panels.portState).toEqual({ pending_counts: {} });
    expect(next.panels.lastSlowPollAt).toBe(9000);
  });

  test('GRAPH_LOADED sets ready graph state', () => {
    const graphData = buildGraphFromResponse({
      A1: { id: 'A1', type: 'dock', neighbors: {} },
    });
    const next = portDashboardReducer(initialState, {
      type: 'GRAPH_LOADED',
      payload: graphData,
    });
    expect(next.graph.status).toBe('ready');
    expect(next.graph.graphNodes).toHaveLength(1);
    expect(next.graph.staticEdges).toEqual([]);
  });
});

describe('selectors', () => {
  test('selectFlowEdges highlights active travel from live edges', () => {
    let state = portDashboardReducer(initialState, {
      type: 'GRAPH_LOADED',
      payload: buildGraphFromResponse({
        A1: { id: 'A1', type: 'dock', neighbors: { E: 'A2' } },
        A2: { id: 'A2', type: 'warehouse', neighbors: { W: 'A1' } },
      }),
    });

    state = portDashboardReducer(state, {
      type: 'FAST_POLL_OK',
      payload: {
        edges: [{
          id: 'd1',
          currentLocation: 'A1',
          nextNode: 'A2',
          progressToNext: 50,
          eta: 5,
        }],
        pollTimestamp: Date.now(),
      },
    });

    const flowEdges = selectFlowEdges(state);
    expect(flowEdges.some((e) => e.data?.active)).toBe(true);
  });

  test('selectDeviceNodes produces react-flow device nodes', () => {
    let state = portDashboardReducer(initialState, {
      type: 'GRAPH_LOADED',
      payload: buildGraphFromResponse({
        A1: { id: 'A1', type: 'dock', neighbors: {} },
      }),
    });

    state = portDashboardReducer(state, {
      type: 'FAST_POLL_OK',
      payload: {
        edges: [{
          id: 'crane001',
          type: 'crane',
          taskPhase: 'idle',
          currentLocation: 'A1',
          progressToNext: 0,
          eta: 0,
        }],
        pollTimestamp: Date.now(),
      },
    });

    const nodes = selectDeviceNodes(state);
    expect(nodes).toHaveLength(1);
    expect(nodes[0].id).toBe('dev-crane001');
    expect(nodes[0].type).toBe('device');
  });
});
