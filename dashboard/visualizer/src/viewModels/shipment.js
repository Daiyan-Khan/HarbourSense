export const LIFECYCLE_STEPS = [
  { key: 'arrived', label: 'Arrived' },
  { key: 'offloaded', label: 'Offloaded' },
  { key: 'transported', label: 'Transported' },
  { key: 'storing', label: 'Storing' },
  { key: 'stored', label: 'Stored' },
  { key: 'delivered', label: 'Delivered' },
];

const STATUS_ALIASES = {
  transporting: 'transported',
};

const PHASE_TO_STATUS = {
  offload: 'offloaded',
  transport: 'transported',
  store_move: 'storing',
  store_load: 'stored',
  delivery: 'delivered',
};

export function deriveEffectiveStatus(doc) {
  const rawStatus = doc?.status || 'arrived';
  let effectiveStatus = STATUS_ALIASES[rawStatus] || rawStatus;
  let effectiveRank = statusToStepIndex(effectiveStatus);

  for (const entry of doc?.assignedEdges || []) {
    if (typeof entry === 'object' && entry?.completedAt && entry?.phase) {
      const implied = PHASE_TO_STATUS[entry.phase];
      if (implied && statusToStepIndex(implied) > effectiveRank) {
        effectiveRank = statusToStepIndex(implied);
        effectiveStatus = implied;
      }
    }
  }
  return effectiveStatus;
}

export function statusToStepIndex(status) {
  const normalized = STATUS_ALIASES[status] || status;
  const idx = LIFECYCLE_STEPS.findIndex((s) => s.key === normalized);
  return idx >= 0 ? idx : 0;
}

export function formatAssignedEdges(assignedEdges) {
  if (!Array.isArray(assignedEdges) || assignedEdges.length === 0) {
    return [];
  }
  return assignedEdges.map((entry) => {
    if (typeof entry === 'string') {
      return { edgeId: entry, phase: 'unknown', completed: false, label: entry };
    }
    const completed = Boolean(entry.completedAt);
    const phase = entry.phase || 'unknown';
    const edgeId = entry.edgeId || 'device';
    return {
      edgeId,
      phase,
      completed,
      assignedAt: entry.assignedAt || null,
      completedAt: entry.completedAt || null,
      label: `${edgeId} (${phase})${completed ? ' ✓' : ''}`,
    };
  });
}

function isPhaseComplete(assignedEdges, phase) {
  return (assignedEdges || []).some(
    (entry) => typeof entry === 'object'
      && entry?.phase === phase
      && Boolean(entry?.completedAt),
  );
}

export function getQueueFlags(doc) {
  const flags = [];
  const assigned = doc?.assignedEdges || [];
  if (doc?.offloadQueued && !isPhaseComplete(assigned, 'offload')) {
    flags.push('Waiting offload');
  }
  if (doc?.transportQueued && !isPhaseComplete(assigned, 'transport')) {
    flags.push('Waiting transport');
  }
  if (doc?.storeQueued && !isPhaseComplete(assigned, 'store_load')) {
    flags.push('Waiting store');
  }
  if (doc?.deliveryStatus === 'pending' || doc?.deliveryStatus === 'assigned') {
    flags.push(`Delivery: ${doc.deliveryStatus}`);
  }
  return flags;
}

export function normalizeShipment(doc) {
  if (!doc) return null;

  const status = doc.status || 'unknown';
  const effectiveStatus = deriveEffectiveStatus(doc);
  const stepIndex = statusToStepIndex(effectiveStatus);
  const isActive = effectiveStatus !== 'delivered';

  return {
    raw: doc,
    id: doc.id,
    status,
    effectiveStatus,
    stepIndex,
    isActive,
    currentStep: LIFECYCLE_STEPS[stepIndex] || { key: effectiveStatus, label: effectiveStatus },
    arrivalNode: doc.arrivalNode ?? null,
    currentNode: doc.currentNode ?? doc.arrivalNode ?? null,
    destination: doc.destination ?? null,
    warehouseAssigned: doc.warehouseAssigned ?? null,
    createdAt: doc.createdAt ?? null,
    updatedAt: doc.updatedAt ?? null,
    assignedEdges: formatAssignedEdges(doc.assignedEdges),
    queueFlags: getQueueFlags(doc),
    routeLabel: [doc.arrivalNode, doc.currentNode, doc.destination]
      .filter(Boolean)
      .filter((v, i, arr) => arr.indexOf(v) === i)
      .join(' → ') || '—',
  };
}

export function compareShipmentsFifo(a, b) {
  const aMs = parseCreatedAtMs(a?.createdAt);
  const bMs = parseCreatedAtMs(b?.createdAt);
  if (aMs != null && bMs != null && aMs !== bMs) return aMs - bMs;
  if (aMs == null && bMs != null) return 1;
  if (aMs != null && bMs == null) return -1;
  return String(a?.id ?? '').localeCompare(String(b?.id ?? ''));
}

export function sortShipmentsFifo(shipments) {
  return [...(shipments || [])].sort(compareShipmentsFifo);
}

export const STATUS_FILTER_OPTIONS = [
  { value: 'all', label: 'All statuses' },
  ...LIFECYCLE_STEPS.map((step) => ({ value: step.key, label: step.label })),
];

export function filterShipmentsByStatus(shipments, statusFilter) {
  if (!statusFilter || statusFilter === 'all') return shipments || [];
  return (shipments || []).filter(
    (doc) => deriveEffectiveStatus(doc) === statusFilter,
  );
}

export function partitionShipments(shipments) {
  const normalized = (shipments || []).map(normalizeShipment).filter(Boolean);
  return {
    active: sortShipmentsFifo(normalized.filter((s) => s.isActive)),
    completed: sortShipmentsFifo(normalized.filter((s) => !s.isActive)),
  };
}

const DEFAULT_ARRIVAL_INTERVAL_MS = 60000;

export function parseCreatedAtMs(value) {
  if (!value) return null;
  const ms = new Date(value).getTime();
  return Number.isNaN(ms) ? null : ms;
}

export function computeAverageArrivalIntervalMs(shipments, fallback = DEFAULT_ARRIVAL_INTERVAL_MS) {
  const times = (shipments || [])
    .map((s) => parseCreatedAtMs(s.createdAt))
    .filter((t) => t != null)
    .sort((a, b) => b - a);

  if (times.length < 2) return fallback;

  const gaps = [];
  for (let i = 0; i < Math.min(5, times.length - 1); i += 1) {
    gaps.push(times[i] - times[i + 1]);
  }
  const avg = gaps.reduce((sum, gap) => sum + gap, 0) / gaps.length;
  return avg > 0 ? Math.round(avg) : fallback;
}

export function formatCountdown(seconds) {
  if (seconds == null) return '—';
  if (seconds <= 0) return 'Any moment now';
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) return `${secs}s`;
  return `${mins}m ${secs.toString().padStart(2, '0')}s`;
}

export function getNextArrivalForecast(shipments, nowMs = Date.now()) {
  const list = Array.isArray(shipments) ? shipments : [];
  const withCreated = list
    .map((doc) => ({ doc, createdMs: parseCreatedAtMs(doc.createdAt) }))
    .filter((entry) => entry.createdMs != null)
    .sort((a, b) => b.createdMs - a.createdMs);

  const latestEntry = withCreated[0] || null;
  const avgIntervalMs = computeAverageArrivalIntervalMs(list);

  const scheduledFromLatest = latestEntry?.doc?.scheduledNextAt
    ? parseCreatedAtMs(latestEntry.doc.scheduledNextAt)
    : null;

  const awaitingAtDock = list
    .filter((s) => s.status === 'arrived' || s.status === 'waiting')
    .sort(
      (a, b) => (parseCreatedAtMs(b.createdAt) || 0) - (parseCreatedAtMs(a.createdAt) || 0),
    )
    .map(normalizeShipment);

  if (!latestEntry) {
    return {
      hasData: false,
      latestArrival: null,
      estimatedNextAtMs: null,
      secondsUntil: null,
      averageIntervalMs: avgIntervalMs,
      awaitingAtDock,
      isOverdue: false,
      usesScheduledNextAt: false,
    };
  }

  const estimatedNextAtMs = scheduledFromLatest ?? (latestEntry.createdMs + avgIntervalMs);
  const secondsUntil = Math.ceil((estimatedNextAtMs - nowMs) / 1000);

  return {
    hasData: true,
    latestArrival: normalizeShipment(latestEntry.doc),
    estimatedNextAtMs,
    secondsUntil,
    averageIntervalMs: avgIntervalMs,
    awaitingAtDock,
    isOverdue: secondsUntil <= 0,
    usesScheduledNextAt: scheduledFromLatest != null,
  };
}
