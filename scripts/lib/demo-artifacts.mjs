import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

export const SCENARIOS = ['normal', 'congestion', 'crane-fault'];

export function normalizeBasePath(value = '/HarbourSense') {
  if (!/^\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_-]*\/?$/.test(value)) {
    throw new Error('The deployment base must be a path such as /HarbourSense or /.');
  }
  return value.replace(/\/$/, '');
}

export function validateRecording(recording, expectedId) {
  if (recording?.schemaVersion !== 1 || recording.scenario?.id !== expectedId
      || !Number.isFinite(Date.parse(recording.recordedAt))
      || !Number.isFinite(recording.durationMs) || recording.durationMs <= 0
      || !recording.initialSnapshot?.graph?.nodes || !recording.frames?.length) {
    throw new Error(`${expectedId}: incomplete recording.`);
  }
  if (recording.provenance?.synthetic !== true || recording.provenance?.recorded !== true
      || !recording.provenance.runId || recording.provenance.terminalState !== 'complete') {
    throw new Error(`${expectedId}: only completed real pipeline recordings with synthetic inputs can be published.`);
  }
  let previous = -1;
  for (const frame of recording.frames) {
    if (!Number.isFinite(frame.atMs) || frame.atMs < previous || frame.atMs > recording.durationMs
        || !frame.snapshot || !Array.isArray(frame.events)) {
      throw new Error(`${expectedId}: invalid frame ordering or snapshot.`);
    }
    previous = frame.atMs;
  }
  const shipments = recording.frames.at(-1).snapshot.shipments;
  if (!shipments?.length || shipments.some(item => item.status !== 'delivered')) {
    throw new Error(`${expectedId}: final recording has unfinished shipments.`);
  }
  const final = recording.frames.at(-1).snapshot;
  if (!final.edges?.length || final.edges.some(edge => (edge.taskPhase || edge.status) !== 'idle'
      || edge.shipmentId || edge.assignedShipment || (edge.task && edge.task !== 'idle'))) {
    throw new Error(`${expectedId}: devices still have unfinished assignments.`);
  }
  const snapshots = recording.frames.map(frame => frame.snapshot);
  if (expectedId === 'crane-fault') {
    const scoredFault = snapshots.some(snapshot => snapshot.craneTelemetry?.some(sample =>
      Number.isFinite(sample.analysis?.anomalyScore) && sample.analysis.anomalous === true));
    if (!scoredFault || !final.maintenanceHistory?.length
        || !final.maintenanceTasks?.some(task => task.status === 'completed')) {
      throw new Error('crane-fault: recorded model scoring, alert history and completed repair are required.');
    }
  }
  if (expectedId === 'congestion') {
    const fault = recording.provenance.scenarioInput?.faults?.find(item => item.type === 'occupancy');
    const observed = fault && snapshots.some(snapshot => snapshot.sensors?.some(sensor =>
      sensor.node === fault.node && sensor.type === 'occupancy' && sensor.reading >= fault.reading));
    const alternative = fault && recording.frames.some(frame => frame.atMs >= fault.fromMs && frame.atMs < fault.untilMs
      && frame.snapshot.edges?.some(edge => edge.type === 'truck_tempo'
        && (edge.pendingPath?.length ? edge.pendingPath : edge.path)?.length > 2
        && !(edge.pendingPath?.length ? edge.pendingPath : edge.path).includes(fault.node)));
    if (!observed || !alternative) throw new Error('congestion: captured bottleneck input and an alternative transport route are required.');
  }
  return recording;
}

export async function validateReplayDirectory(directory) {
  const manifest = JSON.parse(await fs.readFile(path.join(directory, 'index.json'), 'utf8'));
  if (manifest.schemaVersion !== 1 || !Array.isArray(manifest.scenarios)
      || manifest.scenarios.length !== SCENARIOS.length
      || new Set(manifest.scenarios.map(item => item.id)).size !== SCENARIOS.length) {
    throw new Error('The replay manifest must contain the three distinct supported scenarios.');
  }
  const recordings = [];
  for (const entry of manifest.scenarios) {
    if (!SCENARIOS.includes(entry.id) || !/^[a-zA-Z0-9][a-zA-Z0-9._-]*\.json$/.test(entry.file)) {
      throw new Error('The replay manifest contains an unsupported scenario or unsafe filename.');
    }
    const bytes = await fs.readFile(path.join(directory, entry.file));
    if (!entry.sha256 || createHash('sha256').update(bytes).digest('hex') !== entry.sha256) {
      throw new Error(`${entry.id}: recording hash does not match the manifest.`);
    }
    recordings.push(validateRecording(JSON.parse(bytes), entry.id));
  }
  return { manifest, recordings };
}

export async function walkFiles(directory) {
  const result = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`Publish directory contains a symbolic link: ${entry.name}`);
    if (entry.isDirectory()) result.push(...await walkFiles(file));
    else result.push(file);
  }
  return result;
}
