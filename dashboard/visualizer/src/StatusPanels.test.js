import { render, screen, fireEvent } from '@testing-library/react';
import {
  EdgeFleetPanel,
  ShipmentsPanel,
  SensorAlertsPanel,
  NextArrivalPanel,
  PortBacklogPanel,
} from './StatusPanels';

const idleDevice = {
  id: 'truck_tempo_1',
  type: 'truck_tempo',
  task: 'idle',
  taskPhase: 'idle',
  currentLocation: 'A1',
};

const activeDevice = {
  id: 'crane001',
  type: 'crane',
  taskPhase: 'assigned',
  task: { phase: 'offload', shipmentId: 'shipment_1' },
  shipmentId: 'shipment_1',
  currentLocation: 'C3',
  progressToNext: 0,
  eta: 0,
};

const activeShipment = {
  id: 'shipment_1',
  status: 'offloaded',
  arrivalNode: 'A1',
  currentNode: 'C3',
  destination: 'D2',
  updatedAt: '2026-06-12T10:00:00Z',
  assignedEdges: [{ edgeId: 'crane001', phase: 'offload', completedAt: '2026-06-12T09:55:00Z' }],
};

const completingDevice = {
  id: 'robot001',
  type: 'robot',
  taskPhase: 'completing',
  currentLocation: 'B4',
};

describe('StatusPanels', () => {
  test('EdgeFleetPanel shows summary chips and device rows', () => {
    const onSelect = jest.fn();
    render(
      <EdgeFleetPanel
        devices={[idleDevice, activeDevice]}
        selectedDeviceId={null}
        onSelectDevice={onSelect}
      />
    );

    expect(screen.getByLabelText(/Edge fleet/i)).toBeInTheDocument();
    expect(screen.getByText('crane001')).toBeInTheDocument();
    expect(screen.getByText('Assigned')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText(/Device crane001/i));
    expect(onSelect).toHaveBeenCalledWith(activeDevice);
  });

  test('EdgeFleetPanel shows idle reason from diagnostics', () => {
    render(
      <EdgeFleetPanel
        devices={[idleDevice]}
        idleEdgeDiagnosticsById={{
          truck_tempo_1: {
            subline: 'No assignment: No pending transport',
          },
        }}
      />,
    );

    expect(screen.getByText(/Why idle: No assignment/i)).toBeInTheDocument();
  });

  test('EdgeFleetPanel filters devices by phase', () => {
    render(
      <EdgeFleetPanel
        devices={[idleDevice, activeDevice, completingDevice]}
      />,
    );

    expect(screen.getByText('truck_tempo_1')).toBeInTheDocument();
    expect(screen.getByText('crane001')).toBeInTheDocument();
    expect(screen.getByText('robot001')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Filter edges by phase/i), {
      target: { value: 'idle' },
    });

    expect(screen.getByText('truck_tempo_1')).toBeInTheDocument();
    expect(screen.queryByText('crane001')).not.toBeInTheDocument();
    expect(screen.queryByText('robot001')).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Filter edges by phase/i), {
      target: { value: 'active' },
    });

    expect(screen.queryByText('truck_tempo_1')).not.toBeInTheDocument();
    expect(screen.getByText('crane001')).toBeInTheDocument();
    expect(screen.queryByText('robot001')).not.toBeInTheDocument();
  });

  test('ShipmentsPanel shows lifecycle stepper for active shipment', () => {
    render(<ShipmentsPanel shipments={[activeShipment]} />);

    expect(screen.getByRole('region', { name: 'Shipments' })).toBeInTheDocument();
    expect(screen.getByText('shipment_1')).toBeInTheDocument();
    const lifecycle = screen.getByLabelText(/Shipment lifecycle/i);
    expect(lifecycle).toBeInTheDocument();
    expect(lifecycle).toHaveTextContent('Offloaded');
    expect(screen.getByText(/crane001/i)).toBeInTheDocument();
  });

  test('ShipmentsPanel shows blocker diagnostics on shipment card', () => {
    render(
      <ShipmentsPanel
        shipments={[
          {
            id: 'shipment_2',
            status: 'arrived',
            arrivalNode: 'A1',
            destination: 'D2',
            updatedAt: '2026-06-12T10:00:00Z',
            assignedEdges: [{ edgeId: 'crane_1', phase: 'offload' }],
          },
        ]}
        shipmentDiagnosticsById={{
          shipment_2: {
            nextPhaseLabel: 'Offload',
            blockerMessage: 'No idle crane near A1 (0 idle crane)',
            hasBlocker: true,
            assignedIncomplete: [{ edgeId: 'crane_1', phase: 'offload' }],
          },
        }}
      />,
    );

    expect(screen.getByText(/Next phase: Offload/i)).toBeInTheDocument();
    expect(screen.getByText(/Waiting for: No idle crane near A1/i)).toBeInTheDocument();
    expect(screen.getByText(/crane_1 \(offload\)/i)).toHaveClass('assigned-edge--incomplete');
  });

  test('PortBacklogPanel shows pending summary and expandable details', () => {
    render(
      <PortBacklogPanel
        pendingBacklog={[
          { key: 'pending_arrived', label: 'Awaiting offload', count: 2 },
        ]}
        queueCounts={{ transportQueued: 1 }}
      />,
    );

    expect(screen.getByLabelText(/Port backlog/i)).toBeInTheDocument();
    expect(screen.getByText(/2/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /Show details/i }));
    expect(screen.getByText(/Awaiting offload/i)).toBeInTheDocument();
    expect(screen.getByText(/transportQueued: 1/i)).toBeInTheDocument();
  });

  test('ShipmentsPanel shows error state', () => {
    render(<ShipmentsPanel shipments={[]} error="Unable to load shipments" />);
    expect(screen.getByRole('alert')).toHaveTextContent(/Unable to load shipments/i);
  });

  test('ShipmentsPanel orders active shipments FIFO and filters by status', () => {
    render(
      <ShipmentsPanel
        shipments={[
          {
            id: 'shipment_3',
            status: 'arrived',
            createdAt: '2026-06-12T10:04:00Z',
            updatedAt: '2026-06-12T10:10:00Z',
          },
          {
            id: 'shipment_1',
            status: 'offloaded',
            createdAt: '2026-06-12T10:00:00Z',
            updatedAt: '2026-06-12T10:09:00Z',
          },
          {
            id: 'shipment_2',
            status: 'arrived',
            createdAt: '2026-06-12T10:02:00Z',
            updatedAt: '2026-06-12T10:08:00Z',
          },
        ]}
      />,
    );

    const cards = screen.getAllByText(/shipment_\d/);
    expect(cards.map((node) => node.textContent)).toEqual([
      'shipment_1',
      'shipment_2',
      'shipment_3',
    ]);

    fireEvent.change(screen.getByLabelText(/Filter shipments by status/i), {
      target: { value: 'arrived' },
    });

    const arrivedCards = screen.getAllByText(/shipment_\d/);
    expect(arrivedCards.map((node) => node.textContent)).toEqual([
      'shipment_2',
      'shipment_3',
    ]);
    expect(screen.queryByText('shipment_1')).not.toBeInTheDocument();
  });

  test('SensorAlertsPanel shows empty state', () => {
    render(<SensorAlertsPanel alerts={[]} />);
    expect(screen.getByText(/No unresolved sensor alerts/i)).toBeInTheDocument();
  });

  test('NextArrivalPanel shows countdown and dock queue', () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-06-12T10:04:30Z'));

    render(
      <NextArrivalPanel
        shipments={[
          {
            id: 'shipment_2',
            status: 'arrived',
            arrivalNode: 'A1',
            createdAt: '2026-06-12T10:04:00Z',
          },
          {
            id: 'shipment_1',
            status: 'delivered',
            arrivalNode: 'A1',
            createdAt: '2026-06-12T10:00:00Z',
          },
        ]}
      />,
    );

    expect(screen.getByLabelText(/Next shipment arrival/i)).toBeInTheDocument();
    expect(screen.getByText('3m 30s')).toBeInTheDocument();
    expect(screen.getAllByText(/shipment_2/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/At dock now/i)).toBeInTheDocument();

    jest.useRealTimers();
  });
});
