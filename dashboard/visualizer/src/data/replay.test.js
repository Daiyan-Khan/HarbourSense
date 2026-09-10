import { assetUrl, createReplaySource, replayAt, validateRecording } from './replay';
import { initialState, portDashboardReducer } from '../state/portDashboardStore';

// Deliberately tiny test-only fixture. Public recordings are exported from the real pipeline.
export const testRecording = {
  schemaVersion: 1, scenario: { id: 'test-normal', title: 'Test normal' },
  recordedAt: '2026-01-01T00:00:00.000Z', durationMs: 2000,
  initialSnapshot: {
    graph: { nodes: { A1: { id: 'A1', type: 'dock', neighbors: {} } } },
    edges: [{ id: 'truck-test', currentLocation: 'A1', taskPhase: 'idle' }],
    sensors: [], shipments: [], sensorAlerts: [], maintenanceAlerts: [], portState: {},
  },
  frames: [
    { atMs: 1000, snapshot: { shipments: [{ id: 'shipment-test', status: 'arrived' }] }, events: [{ id: 'event-test', atMs: 1000, type: 'arrival', message: 'Test shipment arrives' }] },
    { atMs: 2000, snapshot: { shipments: [{ id: 'shipment-test', status: 'delivered' }] }, events: [{ id: 'done-test', atMs: 2000, type: 'delivered', message: 'Test complete' }] },
  ],
};

const manifest = { schemaVersion: 1, scenarios: [{ id: 'test-normal', title: 'Test normal', file: 'test-normal.json' }] };
const fetchJson = async (url) => url.endsWith('/index.json') ? manifest : testRecording;

beforeEach(() => { jest.useFakeTimers(); });
afterEach(() => { jest.useRealTimers(); delete process.env.PUBLIC_URL; });

test('requires a supported, ordered recording and confines asset URLs to its Pages subpath', () => {
  process.env.PUBLIC_URL = '/HarbourSense';
  expect(assetUrl('normal.json')).toBe('/HarbourSense/replays/normal.json');
  expect(() => assetUrl('https://backend.example/recording.json')).toThrow();
  expect(() => assetUrl('../private.json')).toThrow();
  expect(() => validateRecording({ ...testRecording, schemaVersion: 2 })).toThrow();
  expect(() => validateRecording({ ...testRecording, frames: [...testRecording.frames].reverse() })).toThrow();
});

test('seeking never exposes future events and reconstructs state without regressing the fixture', () => {
  expect(replayAt(testRecording, 999).events).toEqual([]);
  expect(replayAt(testRecording, 1500).snapshot.shipments[0].status).toBe('arrived');
  expect(replayAt(testRecording, 9000).snapshot.shipments[0].status).toBe('delivered');
  expect(replayAt(testRecording, 0).snapshot.shipments).toEqual([]);
  expect(testRecording.initialSnapshot.shipments).toEqual([]);
});

test('pause freezes the recorded clock, speed changes are continuous, completion and reset reconstruct state', async () => {
  let now = 0;
  let state = initialState;
  const source = createReplaySource({ fetchJson, now: () => now, emit: (action) => { state = portDashboardReducer(state, action); } });
  await source.start();
  expect(state.source.status).toBe('idle');
  expect(state.graph.status).toBe('ready');
  await source.command('start');
  now = 800; jest.advanceTimersByTime(80);
  await source.command('pause');
  expect(state.source.positionMs).toBe(800);
  const frozenClock = state.display.frameNow;
  now = 10000; jest.advanceTimersByTime(4000);
  expect(state.display.frameNow).toBe(frozenClock);
  await source.command('speed', 2);
  await source.command('resume');
  now = 10400; jest.advanceTimersByTime(80);
  expect(state.source.positionMs).toBe(1600);
  expect(state.panels.shipments[0].status).toBe('arrived');
  now = 11000; jest.advanceTimersByTime(80);
  expect(state.source.status).toBe('complete');
  expect(state.panels.shipments[0].status).toBe('delivered');
  await source.command('reset');
  expect(state.source.status).toBe('idle');
  expect(state.source.positionMs).toBe(0);
  expect(state.panels.shipments).toEqual([]);
  expect(state.timeline).toEqual([]);
  source.dispose();
});

test('visitors have independent controls and disposal ends playback', async () => {
  const a = []; const b = [];
  const sourceA = createReplaySource({ fetchJson, emit: (action) => a.push(action) });
  const sourceB = createReplaySource({ fetchJson, emit: (action) => b.push(action) });
  await sourceA.start(); await sourceB.start();
  await sourceA.command('start');
  expect(a.filter((item) => item.type === 'SOURCE_STATUS').at(-1).payload.status).toBe('running');
  expect(b.filter((item) => item.type === 'SOURCE_STATUS').at(-1).payload.status).toBe('idle');
  sourceA.dispose(); sourceB.dispose();
  const count = a.length;
  jest.advanceTimersByTime(3000);
  expect(a).toHaveLength(count);
});

test('an absent catalogue reports a clear error and never falls back to live requests', async () => {
  const emit = jest.fn();
  const fetchEmpty = jest.fn(async () => ({ schemaVersion: 1, scenarios: [] }));
  const source = createReplaySource({ fetchJson: fetchEmpty, emit });
  await source.start();
  expect(emit).toHaveBeenCalledWith(expect.objectContaining({ type: 'GRAPH_ERROR', payload: expect.stringContaining('No verified scenario recordings') }));
  expect(fetchEmpty).toHaveBeenCalledTimes(1);
  expect(fetchEmpty.mock.calls[0][0]).toBe('/replays/index.json');
  source.dispose();
});

test('a superseded scenario download cannot mutate the selected run', async () => {
  let resolveOld;
  const other = { ...testRecording, scenario: { id: 'second', title: 'Second' } };
  const events = [];
  const source = createReplaySource({ emit: (action) => events.push(action), fetchJson: async (url) => {
    if (url.endsWith('index.json')) return { ...manifest, scenarios: [...manifest.scenarios, { id: 'second', title: 'Second', file: 'second.json' }] };
    if (url.endsWith('second.json')) return other;
    return new Promise((resolve) => { resolveOld = resolve; });
  } });
  const starting = source.start();
  await Promise.resolve();
  await source.choose('second');
  resolveOld(testRecording);
  await starting;
  expect(events.filter((event) => event.type === 'SOURCE_STATUS').at(-1).payload.scenario.id).toBe('second');
  source.dispose();
});
