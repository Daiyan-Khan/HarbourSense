const PHASE_LABELS = {
  offload: 'Offload',
  transport: 'Transport',
  store_move: 'Store move',
  store_load: 'Store load',
  delivery: 'Delivery',
};

const WHY_IDLE_LABELS = {
  NO_TASK: 'No assignment',
  WAITING_MONITOR: 'Waiting for monitor',
  STALE_PATH_CLEARED: 'Stale path cleared',
  COMPLETION_PENDING: 'Awaiting completion ack',
};

const PENDING_BUCKET_LABELS = {
  pending_arrived: 'Awaiting offload',
  pending_offloaded: 'Awaiting transport',
  pending_transported: 'Awaiting store move',
  pending_storing: 'Awaiting store load',
  pending_stored: 'Awaiting delivery',
};

const NON_BLOCKER_CODES = new Set(['NONE', 'AT_DOCK_OK']);

export function formatPhaseLabel(phase) {
  if (!phase) return null;
  return PHASE_LABELS[phase] || String(phase).replace(/_/g, ' ');
}

export function formatWhyIdleLabel(whyIdle) {
  if (!whyIdle) return null;
  return WHY_IDLE_LABELS[whyIdle] || String(whyIdle).replace(/_/g, ' ').toLowerCase();
}

export function mapAssignedIncomplete(entries) {
  if (!Array.isArray(entries)) return [];
  return entries.map((entry) => {
    const edgeId = entry.edgeId || entry.edge_id || 'device';
    const phase = entry.phase || 'unknown';
    return {
      edgeId,
      phase,
      label: `${edgeId} (${phase}) — in progress`,
      isIncomplete: true,
    };
  });
}

export function mapShipmentDiagnostic(raw) {
  if (!raw) return null;

  const shipmentId = raw.shipmentId || raw.shipment_id;
  if (!shipmentId) return null;

  const nextPhase = raw.nextPhase || raw.next_phase || null;
  const blockerCode = raw.blockerCode || raw.blocker_code || null;
  const blockerMessage = raw.blockerMessage || raw.blocker_message || null;

  return {
    shipmentId,
    status: raw.status || null,
    nextPhase,
    nextPhaseLabel: formatPhaseLabel(nextPhase),
    blockerCode,
    blockerMessage,
    requiredDeviceType: raw.requiredDeviceType || raw.required_device_type || null,
    idleOfType: raw.idleOfType ?? raw.idle_of_type ?? null,
    assignedIncomplete: mapAssignedIncomplete(
      raw.assignedIncomplete || raw.assigned_incomplete,
    ),
    hasBlocker: Boolean(blockerMessage)
      && (!blockerCode || !NON_BLOCKER_CODES.has(blockerCode)),
  };
}

export function mapIdleEdgeDiagnostic(raw) {
  if (!raw) return null;

  const edgeId = raw.edgeId || raw.edge_id;
  if (!edgeId) return null;

  const whyIdle = raw.whyIdle || raw.why_idle || null;
  const whyIdleLabel = formatWhyIdleLabel(whyIdle);
  const detail = raw.detail || null;

  return {
    edgeId,
    whyIdle,
    whyIdleLabel,
    detail,
    subline: detail && whyIdleLabel
      ? `${whyIdleLabel}: ${detail}`
      : whyIdleLabel || detail,
  };
}

export function mapPendingCounts(pendingCounts) {
  if (!pendingCounts || typeof pendingCounts !== 'object') return [];

  return Object.entries(pendingCounts)
    .filter(([, count]) => count > 0)
    .map(([key, count]) => ({
      key,
      label: PENDING_BUCKET_LABELS[key] || key.replace(/_/g, ' '),
      count,
    }));
}

export function indexByKey(items, key) {
  return (items || []).reduce((acc, item) => {
    if (item?.[key]) {
      acc[item[key]] = item;
    }
    return acc;
  }, {});
}

export function mapPortStateSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    return {
      shipmentDiagnostics: [],
      shipmentDiagnosticsById: {},
      idleEdgeDiagnostics: [],
      idleEdgeDiagnosticsById: {},
      pendingBacklog: [],
      idleByType: {},
      queueCounts: {},
      hasData: false,
    };
  }

  const shipmentDiagnostics = (snapshot.shipment_diagnostics || snapshot.shipmentDiagnostics || [])
    .map(mapShipmentDiagnostic)
    .filter(Boolean);

  const idleEdgeDiagnostics = (snapshot.idle_edge_diagnostics || snapshot.idleEdgeDiagnostics || [])
    .map(mapIdleEdgeDiagnostic)
    .filter(Boolean);

  return {
    shipmentDiagnostics,
    shipmentDiagnosticsById: indexByKey(shipmentDiagnostics, 'shipmentId'),
    idleEdgeDiagnostics,
    idleEdgeDiagnosticsById: indexByKey(idleEdgeDiagnostics, 'edgeId'),
    pendingBacklog: mapPendingCounts(snapshot.pending_counts || snapshot.pendingCounts),
    idleByType: snapshot.idle_by_type || snapshot.idleByType || {},
    queueCounts: snapshot.queue_counts || snapshot.queueCounts || {},
    hasData: true,
  };
}
