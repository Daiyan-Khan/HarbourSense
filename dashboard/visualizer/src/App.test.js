import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import axios from 'axios';
import App from './App';
import { PortDashboardProvider } from './state/PortDashboardProvider';

jest.mock('axios');

function renderApp() {
  return render(
    <PortDashboardProvider>
      <App />
    </PortDashboardProvider>,
  );
}

jest.mock('reactflow', () => {
  const React = require('react');

  const useFlowState = (initial) => {
    const [state, setState] = React.useState(initial);
    const onChange = React.useCallback(() => {}, []);
    return [state, setState, onChange];
  };

  return {
    __esModule: true,
    ReactFlowProvider: ({ children }) => <>{children}</>,
    useReactFlow: () => ({ fitView: jest.fn() }),
    useNodesState: useFlowState,
    useEdgesState: useFlowState,
    default: ({
      nodes = [],
      nodeTypes = {},
      onNodesChange,
      onEdgesChange,
      children,
    }) => (
      <div
        data-testid="react-flow"
        data-has-nodes-change={typeof onNodesChange === 'function' ? 'true' : 'false'}
        data-has-edges-change={typeof onEdgesChange === 'function' ? 'true' : 'false'}
      >
        {nodes.map((node) => {
          const NodeComponent = nodeTypes[node.type];
          if (NodeComponent) {
            return <NodeComponent key={node.id} data={node.data} />;
          }
          return (
            <div key={node.id}>
              {typeof node.data?.label === 'string' ? node.data.label : node.id}
            </div>
          );
        })}
        {children}
      </div>
    ),
    Background: () => <div data-testid="flow-background" />,
    Controls: () => <div data-testid="flow-controls" />,
    BaseEdge: () => null,
    Handle: () => null,
    Position: { Top: 'top', Bottom: 'bottom' },
  };
});

const graphResponse = {
  nodes: {
    A1: { id: 'A1', type: 'dock', neighbors: { E: 'A2' } },
    A2: { id: 'A2', type: 'warehouse', neighbors: { W: 'A1' } },
  },
};

const edgesResponse = [
  {
    id: 'truck_tempo_1',
    type: 'truck_tempo',
    task: 'idle',
    taskPhase: 'idle',
    currentLocation: 'A1',
    nextNode: null,
    progressToNext: 0,
    eta: 0,
  },
  {
    id: 'crane001',
    type: 'crane',
    taskPhase: 'assigned',
    task: { task: 'offload', shipmentId: 'shipment_1' },
    shipmentId: 'shipment_1',
    currentLocation: 'C3',
    progressToNext: 0,
    eta: 0,
  },
];

const sensorsResponse = [
  { id: 'temp-1', type: 'temperature', node: 'A1', reading: 24.5, timestamp: '2026-06-12T10:00:00Z' },
];

const shipmentsResponse = [
  {
    id: 'shipment_1',
    status: 'offloaded',
    arrivalNode: 'A1',
    currentNode: 'C3',
    destination: 'D2',
    createdAt: '2026-06-12T09:55:00Z',
    updatedAt: '2026-06-12T10:00:00Z',
    assignedEdges: [{ edgeId: 'crane001', phase: 'offload', completedAt: '2026-06-12T09:55:00Z' }],
  },
];

const sensorAlertsResponse = [
  {
    id: 'alert-1',
    type: 'temperature',
    node: 'A1',
    severity: 'high',
    timestamp: '2026-06-12T10:05:00Z',
  },
];

const maintenanceAlertsResponse = [
  {
    assetId: 'crane001',
    alertType: 'PREDICTIVE_MAINTENANCE_REQUIRED',
    reason: 'Anomalous motor telemetry detected by EdgeAnalyzer.',
    timestamp: '2026-06-12T10:06:00Z',
  },
];

const portStateResponse = {
  shipment_diagnostics: [
    {
      shipmentId: 'shipment_1',
      status: 'offloaded',
      nextPhase: 'transport',
      blockerCode: 'QUEUED_TRANSPORT',
      blockerMessage: 'Waiting for idle truck',
      assignedIncomplete: [],
    },
  ],
  idle_edge_diagnostics: [
    {
      edgeId: 'truck_tempo_1',
      whyIdle: 'NO_TASK',
      detail: 'No pending transport assignment',
    },
  ],
  pending_counts: {
    pending_offloaded: 1,
  },
  idle_by_type: { truck_tempo: 1, crane: 0 },
  queue_counts: { transportQueued: 1 },
};

beforeEach(() => {
  process.env.REACT_APP_API_BASE_URL = 'http://localhost:8000';
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: graphResponse });
    if (url.endsWith('/api/edges')) return Promise.resolve({ data: edgesResponse });
    if (url.endsWith('/api/sensors')) return Promise.resolve({ data: sensorsResponse });
    if (url.endsWith('/api/shipments')) return Promise.resolve({ data: shipmentsResponse });
    if (url.endsWith('/api/alerts/sensor')) return Promise.resolve({ data: sensorAlertsResponse });
    if (url.endsWith('/api/alerts/maintenance')) return Promise.resolve({ data: maintenanceAlertsResponse });
    if (url.endsWith('/api/port-state')) return Promise.resolve({ data: portStateResponse });
    if (url.endsWith('/api/telemetry/cranes')) return Promise.resolve({ data: [] });
    if (url.endsWith('/api/demo/state')) return Promise.reject({ response: { status: 404 } });
    return Promise.reject(new Error(`Unexpected URL ${url}`));
  });
});

afterEach(() => {
  jest.clearAllMocks();
});

test('renders HarbourSense dashboard with fleet and shipments', async () => {
  renderApp();

  expect(screen.getByRole('heading', { name: /HarbourSense Smart Port/i })).toBeInTheDocument();

  await waitFor(() => expect(screen.getAllByText('A1')[0]).toBeInTheDocument());
  expect(screen.getAllByText('A2')[0]).toBeInTheDocument();

  await waitFor(() => expect(screen.getByLabelText(/Edge fleet/i)).toBeInTheDocument());
  fireEvent.click(screen.getByText('Shipment details & backlog'));
  expect(screen.getByLabelText(/Next shipment arrival/i)).toBeInTheDocument();
  expect(screen.getAllByText('crane001').length).toBeGreaterThanOrEqual(1);
  expect(screen.getByRole('region', { name: 'Shipments' })).toBeInTheDocument();
  expect(screen.getAllByText('shipment_1').length).toBeGreaterThan(0);
  expect(screen.getByLabelText(/Sensor alerts/i)).toBeInTheDocument();
  expect(screen.getByLabelText(/Maintenance alerts/i)).toBeInTheDocument();

  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/graph', { timeout: 8000 });
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/edges', { timeout: 8000 });
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/port-state', { timeout: 8000 });
});

test('shows port-state diagnostics on fleet and shipment panels', async () => {
  renderApp();

  await waitFor(() => expect(screen.getByLabelText(/Edge fleet/i)).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText(/Why idle: No assignment/i)).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText(/Next phase: Transport/i)).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText(/Waiting for: Waiting for idle truck/i)).toBeInTheDocument());
  expect(screen.getByLabelText(/Port backlog/i)).toBeInTheDocument();
});

test('device modal shows contract-aligned fields without N/A for zero progress', async () => {
  renderApp();

  await waitFor(() => expect(screen.getByLabelText(/Device crane001/i)).toBeInTheDocument());
  fireEvent.click(screen.getByLabelText(/Device crane001/i));

  await waitFor(() => expect(screen.getByRole('dialog', { name: /Device details/i })).toBeInTheDocument());
  const dialog = screen.getByRole('dialog', { name: /Device details/i });
  expect(dialog).toHaveTextContent('offload');
  expect(dialog).toHaveTextContent('shipment_1');
  expect(dialog).toHaveTextContent('0%');
  expect(dialog).toHaveTextContent('0s');
  expect(dialog).not.toHaveTextContent('N/A');
});

test('shows graph error when API graph request fails', async () => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.reject(new Error('network down'));
    return Promise.resolve({ data: [] });
  });

  renderApp();

  await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent(/Graph unavailable/i));
  expect(screen.getByRole('alert')).toHaveTextContent(/network down/i);
});

test('shows empty graph message when no nodes are returned', async () => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: { nodes: {} } });
    return Promise.resolve({ data: [] });
  });

  renderApp();

  await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/Port graph is empty/i));
});

test('shows live data warning when polling fails after graph load', async () => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: graphResponse });
    if (url.endsWith('/api/edges')) return Promise.reject(new Error('edges unavailable'));
    if (url.endsWith('/api/sensors')) return Promise.resolve({ data: sensorsResponse });
    if (url.endsWith('/api/shipments')) return Promise.resolve({ data: shipmentsResponse });
    if (url.endsWith('/api/alerts/sensor')) return Promise.resolve({ data: sensorAlertsResponse });
    if (url.endsWith('/api/alerts/maintenance')) return Promise.resolve({ data: maintenanceAlertsResponse });
    if (url.endsWith('/api/port-state')) return Promise.resolve({ data: portStateResponse });
    return Promise.reject(new Error(`Unexpected URL ${url}`));
  });

  renderApp();

  await waitFor(() => expect(screen.getAllByText('A1')[0]).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText(/Live data warning/i)).toHaveTextContent(/edges unavailable/i));
});

test('passes polled nodes directly to React Flow', async () => {
  renderApp();

  await waitFor(() => expect(screen.getByTestId('react-flow')).toBeInTheDocument());
  await waitFor(() => expect(screen.getAllByText('A1')[0]).toBeInTheDocument());
  await waitFor(() => expect(screen.getByRole('button', { name: /Device truck_tempo_1/i })).toBeInTheDocument());

  const reactFlow = screen.getByTestId('react-flow');
  expect(reactFlow).toHaveAttribute('data-has-nodes-change', 'false');
  expect(reactFlow).toHaveAttribute('data-has-edges-change', 'false');
});

test('replay renders through the shared dashboard without HTTP API or hidden EventSource calls', async () => {
  const previousFetch = window.fetch;
  const previousEventSource = window.EventSource;
  const eventSourceSpy = jest.fn();
  window.EventSource = eventSourceSpy;
  const recording = {
    schemaVersion: 1, recordedAt: '2026-01-01T00:00:00Z', durationMs: 1000,
    scenario: { id: 'test', title: 'Test-only playback' },
    initialSnapshot: { graph: graphResponse, edges: edgesResponse, sensors: [], shipments: [], sensorAlerts: [], maintenanceAlerts: [], portState: {} }, frames: [],
  };
  window.fetch = jest.fn(async (url) => ({ ok: true, json: async () => url.endsWith('index.json')
    ? { schemaVersion: 1, scenarios: [{ id: 'test', title: 'Test-only playback', file: 'test.json' }] }
    : recording }));
  try {
    const { unmount } = render(<PortDashboardProvider dataSource="replay"><App /></PortDashboardProvider>);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Play scenario' })).toBeEnabled());
    expect(screen.getByText('Recorded simulation')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Play scenario' }));
    expect(screen.getByRole('button', { name: 'Pause scenario' })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: 'Pause scenario' }));
    expect(screen.getByRole('button', { name: 'Resume scenario' })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: 'Reset scenario' }));
    expect(screen.getByRole('button', { name: 'Play scenario' })).toBeEnabled();
    expect(axios.get).not.toHaveBeenCalled();
    expect(eventSourceSpy).not.toHaveBeenCalled();
    expect(window.fetch.mock.calls.map(([url]) => url)).toEqual(['/replays/index.json', '/replays/test.json']);
    unmount();
  } finally { window.fetch = previousFetch; window.EventSource = previousEventSource; }
});

