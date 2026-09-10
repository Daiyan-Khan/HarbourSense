export const REPLAY_SCHEMA_VERSION = 1;

export function assetUrl(file) {
  // Filenames are confined to the deployed recording directory, including Pages subpaths.
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*\.json$/.test(file)) {
    throw new Error('Invalid recording filename');
  }
  return `${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/replays/${file}`;
}

export function validateRecording(recording) {
  if (recording?.schemaVersion !== REPLAY_SCHEMA_VERSION
    || !recording.scenario?.id
    || !Number.isFinite(Date.parse(recording.recordedAt))
    || !Number.isFinite(recording.durationMs) || recording.durationMs <= 0
    || !recording.initialSnapshot?.graph?.nodes
    || !Array.isArray(recording.frames)) {
    throw new Error('This recording is incomplete or uses an unsupported format.');
  }
  let previous = -1;
  const validateSnapshot = (snapshot) => {
    for (const key of ['edges', 'sensors', 'shipments', 'sensorAlerts', 'maintenanceAlerts', 'craneTelemetry', 'maintenanceHistory', 'maintenanceTasks']) {
      if (snapshot[key] != null && !Array.isArray(snapshot[key])) throw new Error(`Recording ${key} must be a list.`);
    }
  };
  validateSnapshot(recording.initialSnapshot);
  for (const frame of recording.frames) {
    if (!Number.isFinite(frame.atMs) || frame.atMs < previous
      || frame.atMs < 0 || frame.atMs > recording.durationMs || !frame.snapshot) {
      throw new Error('Recording frames must be ordered within the playback duration.');
    }
    validateSnapshot(frame.snapshot);
    if (frame.events != null && !Array.isArray(frame.events)) throw new Error('Recording events must be a list.');
    for (const event of frame.events || []) {
      if (!event.id || !Number.isFinite(event.atMs) || event.atMs < 0 || event.atMs > recording.durationMs) throw new Error('Recording events require stable IDs and valid recorded times.');
    }
    previous = frame.atMs;
  }
  return recording;
}

export function replayAt(recording, elapsedMs) {
  const positionMs = Math.min(recording.durationMs, Math.max(0, elapsedMs));
  let snapshot = recording.initialSnapshot;
  let snapshotAtMs = 0;
  const events = [...(recording.events || []).filter((event) => event.atMs <= positionMs)];
  for (const frame of recording.frames) {
    if (frame.atMs > positionMs) break;
    snapshot = { ...snapshot, ...frame.snapshot };
    snapshotAtMs = frame.atMs;
    events.push(...(frame.events || []));
  }
  return { snapshot, snapshotAtMs, events: [...new Map(events.filter((event) => event.atMs <= positionMs).map((event) => [event.id, event])).values()].sort((a, b) => a.atMs - b.atMs), positionMs };
}

export function createReplaySource({ emit, fetchJson = async (url) => {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Recording request failed (${response.status})`);
  return response.json();
}, now = () => performance.now() }) {
  let disposed = false;
  let generation = 0;
  let recording = null;
  let scenarios = [];
  let positionMs = 0;
  let anchorMs = 0;
  let anchorPositionMs = 0;
  let speed = 1;
  let status = 'loading';
  let timer = null;
  let lastSnapshotAt = null;

  const publish = (extra = {}) => {
    if (disposed) return;
    emit({ type: 'SOURCE_STATUS', payload: {
      kind: 'replay', status, transport: 'Browser playback', scenarios,
      scenario: recording?.scenario, durationMs: recording?.durationMs || 0,
      recordedAt: recording?.recordedAt, provenance: recording?.provenance,
      positionMs, speed, error: null, ...extra,
    } });
    if (!recording) return;
    const frame = replayAt(recording, positionMs);
    const recordedClock = Date.parse(recording.recordedAt);
    if (frame.snapshotAtMs !== lastSnapshotAt) {
      emit({ type: 'DATA_SNAPSHOT', payload: {
        ...frame.snapshot, timestamp: recordedClock + frame.snapshotAtMs,
      } });
      lastSnapshotAt = frame.snapshotAtMs;
    }
    emit({ type: 'TIMELINE_SET', payload: frame.events });
    emit({ type: 'TICK_FRAME', payload: recordedClock + positionMs });
  };

  const advance = () => {
    if (!recording || status !== 'running') return;
    positionMs = Math.min(recording.durationMs, anchorPositionMs + (now() - anchorMs) * speed);
    if (positionMs >= recording.durationMs) status = 'complete';
    publish();
  };

  const choose = async (id) => {
    const scenario = scenarios.find((item) => item.id === id);
    if (!scenario) return;
    const requestGeneration = ++generation;
    recording = null;
    positionMs = 0;
    status = 'loading';
    lastSnapshotAt = null;
    emit({ type: 'DATA_RESET' });
    publish();
    try {
      const loaded = validateRecording(await fetchJson(assetUrl(scenario.file)));
      if (loaded.scenario.id !== scenario.id) throw new Error('The recording does not match the selected scenario.');
      if (disposed || requestGeneration !== generation) return;
      recording = loaded;
      status = 'idle';
      speed = 1;
      publish();
    } catch (error) {
      if (disposed || requestGeneration !== generation) return;
      status = 'failed';
      emit({ type: 'GRAPH_ERROR', payload: error.message });
      publish({ error: error.message });
    }
  };

  const start = async () => {
    window.clearInterval(timer);
    publish();
    try {
      const manifest = await fetchJson(assetUrl('index.json'));
      if (disposed) return;
      if (manifest?.schemaVersion !== REPLAY_SCHEMA_VERSION || !Array.isArray(manifest.scenarios)) {
        throw new Error('The scenario catalogue uses an unsupported format.');
      }
      scenarios = manifest.scenarios;
      if (!scenarios.length) throw new Error('No verified scenario recordings have been published yet.');
      await choose(scenarios[0].id);
      if (!disposed) timer = window.setInterval(advance, 80);
    } catch (error) {
      if (disposed) return;
      status = 'failed';
      emit({ type: 'GRAPH_ERROR', payload: error.message });
      publish({ error: error.message });
    }
  };

  return {
    start,
    retry: start,
    choose,
    command: async (action, value) => {
      if (!recording) return;
      if (action === 'speed') {
        advance();
        if (![0.5, 1, 2, 4].includes(Number(value))) return;
        speed = Number(value);
      } else if (action === 'pause') {
        advance();
        if (status === 'running') status = 'paused';
      } else if (action === 'reset') {
        positionMs = 0;
        speed = 1;
        status = 'idle';
        lastSnapshotAt = null;
        emit({ type: 'DATA_RESET' });
      } else if (action === 'start' || action === 'resume') {
        if (status === 'complete') {
          positionMs = 0;
          lastSnapshotAt = null;
          emit({ type: 'DATA_RESET' });
        }
        status = 'running';
      }
      anchorPositionMs = positionMs;
      anchorMs = now();
      publish();
    },
    dispose: () => {
      disposed = true;
      generation += 1;
      window.clearInterval(timer);
    },
  };
}
