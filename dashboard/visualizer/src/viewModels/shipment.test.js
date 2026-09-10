import {
  deriveEffectiveStatus,
  normalizeShipment,
  partitionShipments,
  sortShipmentsFifo,
  filterShipmentsByStatus,
  statusToStepIndex,
  formatAssignedEdges,
  getNextArrivalForecast,
  formatCountdown,
  computeAverageArrivalIntervalMs,
} from './shipment';

describe('shipment view model', () => {
  test('maps status to lifecycle step index', () => {
    expect(statusToStepIndex('arrived')).toBe(0);
    expect(statusToStepIndex('transported')).toBe(2);
    expect(statusToStepIndex('transporting')).toBe(2);
    expect(statusToStepIndex('delivered')).toBe(5);
  });

  test('normalizes active shipment', () => {
    const s = normalizeShipment({
      id: 'shipment_1',
      status: 'transported',
      arrivalNode: 'A1',
      currentNode: 'B4',
      destination: 'D2',
      warehouseAssigned: 'D2',
      updatedAt: '2026-06-12T10:00:00Z',
      assignedEdges: [
        { edgeId: 'crane001', phase: 'offload', completedAt: '2026-06-12T09:00:00Z' },
        { edgeId: 'truck_tempo_1', phase: 'transport' },
      ],
      transportQueued: true,
    });
    expect(s.isActive).toBe(true);
    expect(s.stepIndex).toBe(2);
    expect(s.routeLabel).toContain('A1');
    expect(s.assignedEdges).toHaveLength(2);
    expect(s.assignedEdges[0].completed).toBe(true);
    expect(s.queueFlags).toContain('Waiting transport');
  });

  test('hides stale offload queue flag after offload completes', () => {
    const s = normalizeShipment({
      id: 'shipment_7',
      status: 'offloaded',
      offloadQueued: true,
      transportQueued: true,
      assignedEdges: [
        { edgeId: 'crane001', phase: 'offload', completedAt: '2026-06-12T09:00:00Z' },
      ],
    });
    expect(s.queueFlags).not.toContain('Waiting offload');
    expect(s.queueFlags).toContain('Waiting transport');
  });

  test('marks delivered as completed', () => {
    const s = normalizeShipment({ id: 'shipment_2', status: 'delivered' });
    expect(s.isActive).toBe(false);
    expect(s.stepIndex).toBe(5);
  });

  test('derives effectiveStatus ahead of regressed Mongo status', () => {
    const s = normalizeShipment({
      id: 'shipment_27',
      status: 'arrived',
      assignedEdges: [
        { edgeId: 'truck_d', phase: 'delivery', completedAt: '2026-06-12T10:00:00Z' },
      ],
    });
    expect(s.effectiveStatus).toBe('delivered');
    expect(s.stepIndex).toBe(5);
    expect(s.isActive).toBe(false);
  });

  test('deriveEffectiveStatus respects monotonic rank', () => {
    expect(
      deriveEffectiveStatus({
        status: 'transported',
        assignedEdges: [{ edgeId: 'c', phase: 'offload', completedAt: 't' }],
      }),
    ).toBe('transported');
  });

  test('partitions active and completed', () => {
    const parts = partitionShipments([
      { id: 'a', status: 'arrived' },
      { id: 'b', status: 'delivered' },
    ]);
    expect(parts.active).toHaveLength(1);
    expect(parts.completed).toHaveLength(1);
  });

  test('sorts shipments FIFO by createdAt oldest first', () => {
    const sorted = sortShipmentsFifo([
      normalizeShipment({
        id: 'shipment_3',
        status: 'arrived',
        createdAt: '2026-06-12T10:04:00Z',
      }),
      normalizeShipment({
        id: 'shipment_1',
        status: 'arrived',
        createdAt: '2026-06-12T10:00:00Z',
      }),
      normalizeShipment({
        id: 'shipment_2',
        status: 'offloaded',
        createdAt: '2026-06-12T10:02:00Z',
      }),
    ]);

    expect(sorted.map((s) => s.id)).toEqual(['shipment_1', 'shipment_2', 'shipment_3']);
  });

  test('partitionShipments keeps FIFO order within active and completed', () => {
    const parts = partitionShipments([
      { id: 'shipment_3', status: 'arrived', createdAt: '2026-06-12T10:04:00Z' },
      { id: 'shipment_1', status: 'delivered', createdAt: '2026-06-12T10:00:00Z' },
      { id: 'shipment_2', status: 'offloaded', createdAt: '2026-06-12T10:02:00Z' },
    ]);

    expect(parts.active.map((s) => s.id)).toEqual(['shipment_2', 'shipment_3']);
    expect(parts.completed.map((s) => s.id)).toEqual(['shipment_1']);
  });

  test('filterShipmentsByStatus uses effective status', () => {
    const filtered = filterShipmentsByStatus([
      { id: 'a', status: 'arrived' },
      {
        id: 'b',
        status: 'arrived',
        assignedEdges: [{ edgeId: 'c', phase: 'offload', completedAt: 't' }],
      },
      { id: 'c', status: 'delivered' },
    ], 'offloaded');

    expect(filtered.map((s) => s.id)).toEqual(['b']);
  });

  test('formats legacy string assigned edges', () => {
    const edges = formatAssignedEdges(['crane001']);
    expect(edges[0].edgeId).toBe('crane001');
    expect(edges[0].completed).toBe(false);
  });

  test('estimates next arrival from recent shipment creation gaps', () => {
    const now = new Date('2026-06-12T10:05:00Z').getTime();
    const shipments = [
      { id: 'shipment_3', status: 'arrived', arrivalNode: 'A1', createdAt: '2026-06-12T10:04:00Z' },
      { id: 'shipment_2', status: 'offloaded', arrivalNode: 'A1', createdAt: '2026-06-12T10:02:00Z' },
      { id: 'shipment_1', status: 'delivered', arrivalNode: 'A1', createdAt: '2026-06-12T10:00:00Z' },
    ];

    expect(computeAverageArrivalIntervalMs(shipments)).toBe(120000);

    const forecast = getNextArrivalForecast(shipments, now);
    expect(forecast.hasData).toBe(true);
    expect(forecast.latestArrival.id).toBe('shipment_3');
    expect(forecast.secondsUntil).toBe(60);
    expect(forecast.awaitingAtDock).toHaveLength(1);
    expect(formatCountdown(90)).toBe('1m 30s');
    expect(formatCountdown(0)).toBe('Any moment now');
  });
});
