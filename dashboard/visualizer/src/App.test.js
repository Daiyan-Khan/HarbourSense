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
  { id: 'temp-1', type: 'temperature', node: 'A1', reading: 24.5 },
];

beforeEach(() => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/graph')) return Promise.resolve({ data: graphResponse });
    if (url.endsWith('/api/edges')) return Promise.resolve({ data: edgesResponse });
    if (url.endsWith('/api/sensors')) return Promise.resolve({ data: sensorsResponse });
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
  expect(screen.getByText('🚚')).toBeInTheDocument();

  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/graph');
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/edges');
  expect(axios.get).toHaveBeenCalledWith('http://localhost:8000/api/sensors');
});
