import { render, screen, waitFor } from '@testing-library/react';
import axios from 'axios';
import App from './App';

jest.mock('axios');

jest.mock('reactflow', () => ({
  __esModule: true,
  default: ({ nodes = [], children }) => (
    <div data-testid="react-flow">
      {nodes.map((node) => (
        <div key={node.id}>{typeof node.data?.label === 'string' ? node.data.label : node.id}</div>
      ))}
      {children}
    </div>
  ),
  Background: () => <div data-testid="flow-background" />,
  Controls: () => <div data-testid="flow-controls" />,
  BaseEdge: () => null,
}));

const graphResponse = {
  nodes: {
    A1: { id: 'A1', type: 'dock', neighbors: { E: 'A2' } },
    A2: { id: 'A2', type: 'warehouse', neighbors: { W: 'A1' } },
  },
};

const edgesResponse = [
  {
    id: 'truck-1',
    type: 'truck_tempo',
    currentLocation: 'A1',
    nextNode: 'A2',
    progressToNext: 50,
    task: { phase: 'transport', shipmentId: 'shipment-1' },
  },
];

const sensorsResponse = [
  { id: 'temp-1', type: 'temperature', node: 'A1', reading: 24.5 },
];

const shipmentsResponse = [
  {
    id: 'shipment-1',
    status: 'transported',
    currentNode: 'A1',
    updatedAt: '2026-06-12T10:00:00Z',
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

beforeEach(() => {
  process.env.REACT_APP_API_BASE_URL = 'http://localhost:8000';
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: graphResponse });
    if (url.endsWith('/api/edges')) return Promise.resolve({ data: edgesResponse });
    if (url.endsWith('/api/sensors')) return Promise.resolve({ data: sensorsResponse });
    if (url.endsWith('/api/shipments')) return Promise.resolve({ data: shipmentsResponse });
    if (url.endsWith('/api/alerts/sensor')) return Promise.resolve({ data: sensorAlertsResponse });
    if (url.endsWith('/api/alerts/maintenance')) return Promise.resolve({ data: maintenanceAlertsResponse });
    return Promise.reject(new Error(`Unexpected URL ${url}`));
  });
});

afterEach(() => {
  jest.clearAllMocks();
});

test('renders HarbourSense graph and fetches live API data', async () => {
  render(<App />);

  expect(screen.getByText(/HarbourSense Smart Port/i)).toBeInTheDocument();

  await waitFor(() => expect(screen.getByText('A1 (dock)')).toBeInTheDocument());
  expect(screen.getByText('A2 (warehouse)')).toBeInTheDocument();
  await waitFor(() => expect(screen.getByText('🚚')).toBeInTheDocument());

  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/graph');
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/edges');
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/sensors');
  await waitFor(() => expect(screen.getByLabelText(/Active shipments/i)).toBeInTheDocument());
  expect(screen.getByText(/shipment-1/i)).toBeInTheDocument();
  expect(screen.getByText(/transported/i)).toBeInTheDocument();
  expect(screen.getByLabelText(/Sensor alerts/i)).toBeInTheDocument();
  expect(screen.getByText(/temperature/i)).toBeInTheDocument();
  expect(screen.getByLabelText(/Maintenance alerts/i)).toBeInTheDocument();
  expect(screen.getByText(/crane001/i)).toBeInTheDocument();
});

test('shows graph error when API graph request fails', async () => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.reject(new Error('network down'));
    return Promise.resolve({ data: [] });
  });

  render(<App />);

  await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent(/Graph unavailable/i));
  expect(screen.getByRole('alert')).toHaveTextContent(/network down/i);
});

test('shows empty graph message when no nodes are returned', async () => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: { nodes: {} } });
    return Promise.resolve({ data: [] });
  });

  render(<App />);

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
    return Promise.reject(new Error(`Unexpected URL ${url}`));
  });

  render(<App />);

  await waitFor(() => expect(screen.getByText('A1 (dock)')).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText(/Live data warning/i)).toHaveTextContent(/edges unavailable/i));
});
