import {
  normalizeEdgeDevice,
  resolveWorkflowPhase,
  resolveShipmentId,
  summarizeFleet,
  filterEdgesByPhase,
} from './edgeDevice';

describe('edgeDevice view model', () => {
  test('normalizes idle seed device with string task', () => {
    const doc = {
      id: 'truck_tempo_1',
      type: 'truck_tempo',
      task: 'idle',
      taskPhase: 'idle',
      currentLocation: 'A1',
      progressToNext: 0,
      eta: 0,
    };
    const d = normalizeEdgeDevice(doc);
    expect(d.taskPhase).toBe('idle');
    expect(d.workflowPhase).toBeNull();
    expect(d.shipmentId).toBeNull();
    expect(d.progressPct).toBe(0);
    expect(d.etaSec).toBe(0);
    expect(d.statusLabel).toBe('Idle');
  });

  test('resolves workflow phase from task.phase', () => {
    const doc = {
      task: { phase: 'transport', shipmentId: 'shipment_1' },
      taskPhase: 'assigned',
      shipmentId: 'shipment_1',
    };
    expect(resolveWorkflowPhase(doc)).toBe('transport');
    expect(resolveShipmentId(doc)).toBe('shipment_1');
  });

  test('resolves workflow phase from task.task assignment path', () => {
    const doc = {
      task: { task: 'offload', shipmentId: 'shipment_2' },
      taskPhase: 'assigned',
    };
    expect(resolveWorkflowPhase(doc)).toBe('offload');
    expect(resolveShipmentId(doc)).toBe('shipment_2');
  });

  test('hides stale shipment and phase on idle devices', () => {
    const doc = {
      task: { phase: 'transport', shipmentId: 'shipment_75' },
      taskPhase: 'idle',
      shipmentId: 'shipment_75',
    };
    expect(resolveWorkflowPhase(doc)).toBeNull();
    expect(resolveShipmentId(doc)).toBeNull();
  });

  test('treats zero progress and eta as valid not missing', () => {
    const doc = {
      id: 'crane001',
      type: 'crane',
      taskPhase: 'assigned',
      currentLocation: 'C3',
      progressToNext: 0,
      eta: 0,
    };
    const d = normalizeEdgeDevice(doc);
    expect(d.progressPct).toBe(0);
    expect(d.etaSec).toBe(0);
    expect(d.isMoving).toBe(false);
  });

  test('detects moving device', () => {
    const doc = {
      id: 'truck_tempo_1',
      taskPhase: 'en_route_start',
      currentLocation: 'A1',
      nextNode: 'A2',
      progressToNext: 45,
    };
    const d = normalizeEdgeDevice(doc);
    expect(d.isMoving).toBe(true);
    expect(d.progressPct).toBe(45);
  });

  test('summarizeFleet counts phases', () => {
    const summary = summarizeFleet([
      { id: 'a', taskPhase: 'idle' },
      { id: 'b', taskPhase: 'assigned' },
      { id: 'c', taskPhase: 'completing' },
    ]);
    expect(summary.total).toBe(3);
    expect(summary.idle).toBe(1);
    expect(summary.completing).toBe(1);
    expect(summary.active).toBe(2);
  });

  test('filterEdgesByPhase filters by phase', () => {
    const devices = [
      { id: 'a', taskPhase: 'idle' },
      { id: 'b', taskPhase: 'assigned' },
      { id: 'c', taskPhase: 'en_route_start' },
      { id: 'd', taskPhase: 'completing' },
      { id: 'e', taskPhase: 'relocating' },
    ];

    expect(filterEdgesByPhase(devices, 'all').map((d) => d.id)).toEqual([
      'a', 'b', 'c', 'd', 'e',
    ]);
    expect(filterEdgesByPhase(devices, 'idle').map((d) => d.id)).toEqual(['a']);
    expect(filterEdgesByPhase(devices, 'completing').map((d) => d.id)).toEqual(['d']);
    expect(filterEdgesByPhase(devices, 'active').map((d) => d.id)).toEqual([
      'b', 'c', 'e',
    ]);
  });
});
