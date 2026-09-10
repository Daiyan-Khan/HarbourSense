import { createLiveSource } from './live';

const graph = { nodes: { A1: { id: 'A1', type: 'dock', neighbors: {} } } };
const flush = async () => { for (let index = 0; index < 10; index += 1) await Promise.resolve(); };

function mockClient() {
  return { get: jest.fn(async (url) => {
    if (url.endsWith('/api/graph')) return { data: graph };
    if (url.endsWith('/api/demo/state')) return { data: { enabled: false, mode: 'live', status: 'idle', sequence: 0 } };
    return { data: [] };
  }), post: jest.fn() };
}

beforeEach(() => { jest.useFakeTimers(); process.env.REACT_APP_API_BASE_URL = 'http://localhost:8000'; });
afterEach(() => { jest.useRealTimers(); });

test('initial graph failures retry automatically and resume device and panel reads', async () => {
  const client = mockClient();
  client.get.mockRejectedValueOnce(new Error('API not yet ready'));
  const emit = jest.fn();
  const source = createLiveSource({ emit, client, EventSourceClass: null });
  source.start(); await flush();
  expect(emit).toHaveBeenCalledWith({ type: 'GRAPH_ERROR', payload: 'API not yet ready' });
  jest.advanceTimersByTime(2000); await flush();
  expect(emit).toHaveBeenCalledWith(expect.objectContaining({ type: 'GRAPH_LOADED' }));
  jest.advanceTimersByTime(2000); await flush();
  expect(emit).toHaveBeenCalledWith(expect.objectContaining({ type: 'FAST_POLL_OK' }));
  source.dispose();
});

test('an interrupted event stream reconnects and reconciles an HTTP snapshot', async () => {
  const streams = [];
  class TestEventSource { constructor() { this.close = jest.fn(); streams.push(this); } }
  const client = mockClient();
  const emit = jest.fn();
  const source = createLiveSource({ emit, client, EventSourceClass: TestEventSource });
  source.start(); await flush();
  streams[0].onopen(); await flush();
  streams[0].onerror(); await flush();
  expect(streams[0].close).toHaveBeenCalled();
  jest.advanceTimersByTime(2000); await flush();
  expect(streams).toHaveLength(2);
  streams[1].onmessage({ data: JSON.stringify({ type: 'snapshot', edges: [{ id: 'reconciled' }] }) });
  expect(emit).toHaveBeenCalledWith({ type: 'EDGE_STREAM_SNAPSHOT', payload: { edges: [{ id: 'reconciled' }] } });
  source.dispose();
  expect(streams[1].close).toHaveBeenCalled();
});

test('an open but silent stream does not disable HTTP reconciliation', async () => {
  let stream;
  class TestEventSource { constructor() { stream = this; this.close = jest.fn(); } }
  const client = mockClient();
  const source = createLiveSource({ emit: jest.fn(), client, EventSourceClass: TestEventSource });
  source.start(); await flush(); stream.onopen(); await flush();
  const before = client.get.mock.calls.filter(([url]) => url.endsWith('/api/edges')).length;
  for (let index = 0; index < 18; index += 1) { jest.advanceTimersByTime(1000); await flush(); }
  expect(client.get.mock.calls.filter(([url]) => url.endsWith('/api/edges')).length).toBeGreaterThan(before);
  source.dispose();
});

test('an uncertain control is retried with one stable command identity, then acknowledged', async () => {
  const client = mockClient();
  const acknowledged = { runId: 'new', sequence: 2, enabled: true, scenarioId: 'normal', status: 'paused', speed: 1, simTimeMs: 1000, timeline: [] };
  client.post.mockRejectedValueOnce(new Error('connection interrupted after send')).mockResolvedValueOnce({ data: { acknowledged: true, state: acknowledged } });
  const emit = jest.fn();
  const source = createLiveSource({ emit, client, EventSourceClass: null });
  await source.command('pause');
  expect(client.post).toHaveBeenCalledTimes(2);
  expect(client.post.mock.calls[0][1]).toEqual(client.post.mock.calls[1][1]);
  expect(client.post.mock.calls[0][1].commandId).toMatch(/^[a-f0-9-]{36}$/i);
  expect(emit).toHaveBeenCalledWith(expect.objectContaining({ type: 'SOURCE_STATUS', payload: expect.objectContaining({ status: 'paused' }) }));
  source.dispose();
});

test('disposing suppresses late HTTP results', async () => {
  let resolve;
  const client = mockClient();
  client.get.mockImplementationOnce(() => new Promise((done) => { resolve = done; }));
  const emit = jest.fn();
  const source = createLiveSource({ emit, client, EventSourceClass: null });
  source.start(); source.dispose();
  const count = emit.mock.calls.length;
  resolve({ data: graph }); await flush();
  expect(emit).toHaveBeenCalledTimes(count);
});

test('choosing an idle scenario survives background polling until start is acknowledged', async () => {
  const client = mockClient();
  const demoState = { enabled: true, runId: 'one', sequence: 1, status: 'idle', scenarioId: 'normal', scenarios: [{ id: 'normal', title: 'Normal' }, { id: 'congestion', title: 'Congestion' }] };
  client.get.mockImplementation(async (url) => ({ data: url.endsWith('/api/graph') ? graph : url.endsWith('/api/demo/state') ? demoState : [] }));
  client.post.mockResolvedValue({ data: { state: { ...demoState, runId: 'two', sequence: 2, status: 'running', scenarioId: 'congestion' } } });
  const emit = jest.fn();
  const source = createLiveSource({ emit, client, EventSourceClass: null });
  source.start(); await flush(); source.choose('congestion');
  jest.advanceTimersByTime(3000); await flush();
  await source.command('start');
  expect(client.post.mock.calls[0][1].scenarioId).toBe('congestion');
  source.dispose();
});
