export const TELEMETRY_STALE_MS = 15000;
export const PANELS_STALE_MS = 30000;

export function telemetryTimestamp(device) {
  const value = device?.lastTelemetryAt ?? device?.lastUpdated ?? device?.timestamp ?? device?.updatedAt;
  if (value == null) return null;
  const timestamp = typeof value === 'number' ? value : Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

export function selectDataHealth(state, wallNow = Date.now()) {
  const source = state.source || {};
  if (source.kind === 'replay') {
    return { label: ({ idle: 'Ready to play', running: 'Playing', paused: 'Paused', complete: 'Complete', failed: 'Failed' })[source.status] || 'Loading',
      tone: source.status === 'failed' ? 'error' : 'ok', ageLabel: 'Recorded time · browser-local session' };
  }
  if (source.status === 'failed') return { label: 'Failed', tone: 'error', ageLabel: source.error || 'The scenario stopped with an error' };
  if (state.graph.status === 'error') {
    return { label: 'Reconnecting', tone: 'warn', ageLabel: 'Retrying the local API' };
  }
  if (state.graph.status !== 'ready') return { label: 'Connecting', tone: 'warn', ageLabel: 'Waiting for port data' };
  if (source.status === 'paused') return { label: 'Paused', tone: 'ok', ageLabel: 'Simulation clock paused' };
  if (source.status === 'complete') return { label: 'Complete', tone: 'ok', ageLabel: 'Scenario finished' };
  if (state.live.error) return { label: 'Reconnecting', tone: 'warn', ageLabel: 'Latest snapshot retained' };
  const timestamps = state.live.edges.map(telemetryTimestamp).filter((value) => value !== null);
  const lastTelemetryAt = timestamps.length ? Math.max(...timestamps) : null;
  if (lastTelemetryAt == null) return { label: 'Awaiting telemetry', tone: 'warn', ageLabel: 'No timestamped device telemetry received' };
  const age = Math.max(0, wallNow - lastTelemetryAt);
  if (age > TELEMETRY_STALE_MS) return { label: 'Stale', tone: 'warn', ageLabel: `Device telemetry ${Math.floor(age / 1000)}s old` };
  const panelsStale = !state.panels.lastSlowPollAt || wallNow - state.panels.lastSlowPollAt > PANELS_STALE_MS || Object.values(state.panels.errors).some(Boolean);
  return { label: panelsStale ? 'Partial data' : 'Healthy', tone: panelsStale ? 'warn' : 'ok', ageLabel: `Device telemetry ${Math.floor(age / 1000)}s old` };
}
