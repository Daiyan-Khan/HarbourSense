import { selectDataHealth } from './freshness';
import { initialState } from '../state/portDashboardStore';
import { deriveOverview } from '../OperationsPanels';

const at = Date.parse('2026-01-01T00:00:00Z');
const state = { ...initialState, graph: { ...initialState.graph, status: 'ready' }, live: { edges: [{ id: 'truck', taskPhase: 'idle', updatedAt: new Date(at).toISOString() }], lastFastPollAt: at, error: null }, panels: { ...initialState.panels, lastSlowPollAt: at } };

test('successful polls cannot conceal stopped telemetry and new telemetry recovers health', () => {
  expect(selectDataHealth(state, at + 5000).label).toBe('Healthy');
  expect(selectDataHealth({ ...state, live: { ...state.live, lastFastPollAt: at + 20000 } }, at + 20000).label).toBe('Stale');
  expect(selectDataHealth({ ...state, live: { ...state.live, edges: [{ ...state.live.edges[0], updatedAt: new Date(at + 20000).toISOString() }] } }, at + 21000).label).toBe('Healthy');
});

test('missing timestamps and paused replay are distinct from healthy telemetry', () => {
  expect(selectDataHealth({ ...state, live: { ...state.live, edges: [{ id: 'truck' }] } }, at).label).toBe('Awaiting telemetry');
  expect(selectDataHealth({ ...state, source: { kind: 'replay', status: 'paused' } }, at + 999999).label).toBe('Paused');
});

test('slow panels have their own freshness threshold', () => {
  const freshDevices = { ...state, live: { ...state.live, edges: [{ updatedAt: at + 35000 }] } };
  expect(selectDataHealth(freshDevices, at + 35000).label).toBe('Partial data');
});

test('overview distinguishes unknown deadlines from zero and counts only source-labeled critical alerts', () => {
  expect(deriveOverview(initialState).map((metric) => metric.value)).toEqual(['—', '—', '—', '—']);
  const metrics = deriveOverview({ ...state, panels: { ...state.panels, shipments: [{ id: 'one', status: 'delivered' }], maintenanceAlerts: [{ alertType: 'maintenance' }], sensorAlerts: [{ severity: 'high' }, { severity: 'high', resolved: true }] } });
  expect(metrics.map((metric) => metric.value)).toEqual([1, 0, '—', 1]);
});
