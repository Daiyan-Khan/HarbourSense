const TASK_PHASE_LABELS = {
  idle: 'Idle',
  en_route_start: 'En route',
  assigned: 'Assigned',
  completing: 'Completing',
  relocating: 'Relocating',
};

const TASK_PHASE_TONES = {
  idle: 'idle',
  en_route_start: 'active',
  assigned: 'active',
  completing: 'completing',
  relocating: 'active',
};

export function resolveWorkflowPhase(doc) {
  if ((doc?.taskPhase || 'idle') === 'idle') {
    return null;
  }
  const task = doc?.task;
  if (task && typeof task === 'object') {
    return task.phase || task.task || null;
  }
  if (typeof task === 'string' && task !== 'idle') {
    return task;
  }
  return null;
}

export function resolveShipmentId(doc) {
  if (!doc || (doc.taskPhase || 'idle') === 'idle') {
    return null;
  }
  const task = doc.task;
  const fromTask = task && typeof task === 'object' ? task.shipmentId : null;
  return doc.shipmentId ?? doc.assignedShipment ?? fromTask ?? null;
}

export function getTaskPhaseLabel(taskPhase) {
  return TASK_PHASE_LABELS[taskPhase] || taskPhase || 'Unknown';
}

export function getTaskPhaseTone(taskPhase) {
  return TASK_PHASE_TONES[taskPhase] || 'idle';
}

export function getDeviceTypeIcon(type) {
  const t = (type || '').toLowerCase();
  if (t.includes('truck')) return '🚚';
  if (t.includes('conveyor')) return '⬭';
  if (t.includes('robot')) return '🤖';
  if (t.includes('crane')) return '🏗️';
  if (t.includes('forklift')) return '🛒';
  if (t.includes('agv')) return '🤖';
  return '⚙️';
}

export function getTaskPhaseBorderColor(taskPhase) {
  switch (taskPhase) {
    case 'en_route_start':
    case 'assigned':
    case 'relocating':
      return '#2563eb';
    case 'completing':
      return '#d97706';
    default:
      return '#64748b';
  }
}

export function normalizeEdgeDevice(doc) {
  if (!doc) {
    return null;
  }

  const taskPhase = doc.taskPhase || 'idle';
  const workflowPhase = resolveWorkflowPhase(doc);
  const shipmentId = resolveShipmentId(doc);
  const progressPct = doc.progressToNext ?? 0;
  const etaSec = doc.eta ?? 0;
  const nextNode = doc.nextNode ?? null;
  const isMoving = progressPct > 0 && progressPct < 100 && Boolean(nextNode);

  return {
    raw: doc,
    id: doc.id,
    type: doc.type || 'unknown',
    desc: doc.desc || null,
    taskPhase,
    workflowPhase,
    shipmentId,
    location: doc.currentLocation ?? null,
    nextNode,
    finalNode: doc.finalNode ?? null,
    progressPct,
    etaSec,
    journeyTimeSec: doc.journeyTime ?? null,
    taskCompletionTimeSec: doc.taskCompletionTime ?? null,
    priority: doc.prio ?? doc.priority ?? null,
    isMoving,
    statusLabel: getTaskPhaseLabel(taskPhase),
    statusTone: getTaskPhaseTone(taskPhase),
    typeIcon: getDeviceTypeIcon(doc.type),
    borderColor: getTaskPhaseBorderColor(taskPhase),
  };
}

export function summarizeFleet(devices) {
  const normalized = (devices || []).map(normalizeEdgeDevice).filter(Boolean);
  const idle = normalized.filter((d) => d.taskPhase === 'idle').length;
  const completing = normalized.filter((d) => d.taskPhase === 'completing').length;
  const active = normalized.length - idle;
  return { total: normalized.length, idle, active, completing, devices: normalized };
}

export const EDGE_PHASE_FILTER_OPTIONS = [
  { value: 'all', label: 'All phases' },
  { value: 'idle', label: 'Idle' },
  { value: 'active', label: 'Active' },
  { value: 'completing', label: 'Completing' },
];

export function filterEdgesByPhase(devices, phaseFilter) {
  if (!phaseFilter || phaseFilter === 'all') return devices || [];
  return (devices || []).filter((doc) => {
    const taskPhase = doc?.taskPhase || 'idle';
    if (phaseFilter === 'idle') return taskPhase === 'idle';
    if (phaseFilter === 'completing') return taskPhase === 'completing';
    if (phaseFilter === 'active') {
      return taskPhase !== 'idle' && taskPhase !== 'completing';
    }
    return true;
  });
}
