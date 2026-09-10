import {
  formatPhaseLabel,
  formatWhyIdleLabel,
  mapAssignedIncomplete,
  mapIdleEdgeDiagnostic,
  mapPendingCounts,
  mapPortStateSnapshot,
  mapShipmentDiagnostic,
} from './diagnostics';

describe('diagnostics view model', () => {
  test('mapShipmentDiagnostic maps blocker fields and incomplete edges', () => {
    const mapped = mapShipmentDiagnostic({
      shipmentId: 'shipment_1',
      status: 'arrived',
      nextPhase: 'offload',
      blockerCode: 'NO_IDLE_DEVICE',
      blockerMessage: 'No idle crane near A1 (0 idle crane)',
      requiredDeviceType: 'crane',
      idleOfType: 0,
      assignedIncomplete: [{ edgeId: 'crane_1', phase: 'offload' }],
    });

    expect(mapped).toMatchObject({
      shipmentId: 'shipment_1',
      nextPhaseLabel: 'Offload',
      blockerMessage: 'No idle crane near A1 (0 idle crane)',
      hasBlocker: true,
    });
    expect(mapped.assignedIncomplete).toHaveLength(1);
    expect(mapped.assignedIncomplete[0].isIncomplete).toBe(true);
  });

  test('mapShipmentDiagnostic treats AT_DOCK_OK as non-blocker', () => {
    const mapped = mapShipmentDiagnostic({
      shipmentId: 'shipment_2',
      blockerCode: 'AT_DOCK_OK',
      blockerMessage: 'At dock',
    });
    expect(mapped.hasBlocker).toBe(false);
  });

  test('mapIdleEdgeDiagnostic builds subline from whyIdle and detail', () => {
    const mapped = mapIdleEdgeDiagnostic({
      edgeId: 'crane_1',
      whyIdle: 'NO_TASK',
      detail: 'No pending offload for crane_1',
    });

    expect(mapped.whyIdleLabel).toBe('No assignment');
    expect(mapped.subline).toBe('No assignment: No pending offload for crane_1');
  });

  test('mapPendingCounts filters zero buckets and labels known keys', () => {
    const rows = mapPendingCounts({
      pending_arrived: 2,
      pending_offloaded: 0,
      pending_transported: 1,
    });

    expect(rows).toEqual([
      { key: 'pending_arrived', label: 'Awaiting offload', count: 2 },
      { key: 'pending_transported', label: 'Awaiting store move', count: 1 },
    ]);
  });

  test('mapPortStateSnapshot indexes diagnostics by id', () => {
    const view = mapPortStateSnapshot({
      shipment_diagnostics: [
        {
          shipmentId: 'shipment_1',
          nextPhase: 'transport',
          blockerCode: 'QUEUED_TRANSPORT',
          blockerMessage: 'Transport queued',
        },
      ],
      idle_edge_diagnostics: [
        { edgeId: 'truck_1', whyIdle: 'WAITING_MONITOR', detail: 'Monitor cycle pending' },
      ],
      pending_counts: { pending_offloaded: 1 },
      idle_by_type: { crane: 2 },
      queue_counts: { transportQueued: 1 },
    });

    expect(view.shipmentDiagnosticsById.shipment_1.blockerMessage).toBe('Transport queued');
    expect(view.idleEdgeDiagnosticsById.truck_1.whyIdleLabel).toBe('Waiting for monitor');
    expect(view.pendingBacklog[0].count).toBe(1);
    expect(view.idleByType.crane).toBe(2);
    expect(view.queueCounts.transportQueued).toBe(1);
    expect(view.hasData).toBe(true);
  });

  test('format helpers fall back for unknown codes', () => {
    expect(formatPhaseLabel('custom_phase')).toBe('custom phase');
    expect(formatWhyIdleLabel('CUSTOM_IDLE')).toBe('custom idle');
    expect(mapAssignedIncomplete(null)).toEqual([]);
  });

  test('mapPortStateSnapshot returns empty view for invalid input', () => {
    const view = mapPortStateSnapshot(null);
    expect(view.hasData).toBe(false);
    expect(view.shipmentDiagnostics).toEqual([]);
  });
});
