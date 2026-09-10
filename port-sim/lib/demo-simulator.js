/* Scenario inputs drive the existing task, motion, sensor and MQTT processing. */
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');
const { getMongoSettings, getSimulatorSettings } = require('../runtime-config');
const { edgeAutonomousLoop, resetExecutingEdgesForTests, resetMovingGuardForTests } = require('./edge-autonomous-loop');
const { createMqttMessageHandler, resetEdgeQueuesForTests } = require('./mqtt-handlers');
const { resetGraphCacheForTests } = require('./movement-engine');
const { DemoContext, DemoRunEnded, validateDemoSettings, logicalMs, storage, wallSleep } = require('./demo-runtime');

const specs = JSON.parse(fs.readFileSync(path.join(process.env.DEMO_SPEC_DIR || path.resolve(__dirname, '../..', 'demo'), 'scenarios.json'), 'utf8')).scenarios;

function healthyCrane(seed, tick) {
  const delta = Math.sin((tick + seed) * 1.618);
  return { motorTemp: +(85 + delta).toFixed(2), vibration: +(0.3 + delta * .04).toFixed(3), energyUse: +(110 + delta * 2).toFixed(2) };
}

async function scheduledShipments(context, spec, device) {
  for (const scheduled of spec.shipments) {
    const remaining = scheduled.atMs - logicalMs(await context.checkpoint());
    if (remaining > 0) await context.sleep(remaining);
    await device.publish(`harboursense/shipments/${scheduled.id}`, JSON.stringify({
      id: scheduled.id, arrivalNode: scheduled.arrivalNode, currentNode: scheduled.arrivalNode,
      destination: scheduled.destination, status: 'arrived', assignedEdges: [],
      createdAt: new Date(context.state.startedWallMs + scheduled.atMs).toISOString(),
      scheduledNextAt: null, eventId: `${context.runId}:shipment:${scheduled.id}`,
    }));
  }
}

async function craneTelemetry(context, spec, device) {
  let lastTick = -1;
  while (true) {
    const state = await context.checkpoint();
    const at = logicalMs(state);
    const tick = Math.floor(at / 2000);
    if (tick !== lastTick) {
      const cranes = await context.db.collection('edgeRuntime').find({ type: 'crane' }).toArray();
      for (const [index, crane] of cranes.entries()) {
        const fault = index === 0 ? spec.faults.find((item) => item.type === 'crane' && at >= item.fromMs && at < item.untilMs) : null;
        const values = fault ? { motorTemp: fault.motorTemp, vibration: fault.vibration, energyUse: fault.energyUse } : healthyCrane(spec.seed, tick + index);
        await device.publish(`harboursense/telemetry/crane/${crane.id}/raw`, JSON.stringify({
          craneId: crane.id, ...values, eventId: `${context.runId}:crane:${crane.id}:${tick}`,
          scenarioLabel: fault ? 'injected-fault' : 'healthy',
        }));
      }
      lastTick = tick;
    }
    await context.sleep(250);
  }
}

async function sensorTelemetry(context, spec, device) {
  // Four documented instruments keep the guided run legible; the canonical
  // fixture remains available in full for other workloads.
  const sensors = (await context.db.collection('sensorList').find({}).toArray()).filter((sensor) => ['S001', 'S002', 'S004', 'S005'].includes(sensor.id));
  const normal = { temperature: 25, vibration: 2, humidity: 45, occupancy: 25, motion: 1, speed: 8 };
  let lastTick = -1;
  while (true) {
    const state = await context.checkpoint();
    const at = logicalMs(state);
    const tick = Math.floor(at / 4000);
    if (tick !== lastTick) {
      const occupancyInputs = spec.faults.filter((fault) => fault.type === 'occupancy');
      const inputs = occupancyInputs.map((fault) => ({ id: `demo-occupancy-${fault.node}`, type: 'occupancy', node: fault.node, reading: at >= fault.fromMs && at < fault.untilMs ? fault.reading : 25 }));
      inputs.push(...sensors.map((sensor) => ({ ...sensor, reading: normal[sensor.type] ?? 5 })));
      for (const input of inputs) {
        const eventId = `${context.runId}:sensor:${input.id}:${tick}`;
        const payload = { ...input, timestamp: new Date(), eventId, runId: context.runId,
          scenarioId: spec.id, simulatedTimeMs: at };
        delete payload._id;
        await context.db.collection('sensorData').updateOne({ _id: eventId }, { $setOnInsert: payload }, { upsert: true });
        await device.publish('harboursense/sensor/data', JSON.stringify(payload));
      }
      lastTick = tick;
    }
    await context.sleep(250);
  }
}

async function runSession(context) {
  const spec = specs.find((item) => item.id === context.state.scenarioId);
  if (!spec) throw new Error('Unknown demo scenario');
  const broker = `mqtt://${process.env.MQTT_BROKER_HOST || 'localhost'}:${process.env.MQTT_BROKER_PORT || '1883'}`;
  const client = mqtt.connect(broker, { clientId: `demo-${context.role}-${context.runId}`, clean: false, reconnectPeriod: 500 });
  context.mqtt = client;
  client.on('error', () => {}); // Connection health is reported through the worker heartbeat.
  const device = { publish: context.publish.bind(context) };
  const workers = [];
  const tracked = (promise) => { promise.catch(() => {}); return promise; };
  const onMessage = createMqttMessageHandler(context.db, device, {});
  let messageChain = Promise.resolve();
  const receive = (topic, payload) => {
    messageChain = messageChain.catch(() => {}).then(() => storage.run(context, async () => {
      const data = JSON.parse(payload.toString());
      if (!await context.accept(data)) return;
      await onMessage(topic, payload);
      await context.acknowledge(data);
    })).catch((error) => { if (!(error instanceof DemoRunEnded)) console.error('Demo command delivery:', error.message); });
  };
  try {
    await new Promise((resolve) => {
      if (client.connected) resolve();
      else client.once('connect', resolve);
    });
    if (context.role === 'portsim') {
      client.on('message', receive);
      client.on('connect', () => client.subscribe(['harboursense/edge/+/task', 'harboursense/edge/+/route'], { qos: 1 }));
      await new Promise((resolve, reject) => client.subscribe(['harboursense/edge/+/task', 'harboursense/edge/+/route'], { qos: 1 }, (error) => error ? reject(error) : resolve()));
    }
    await context.heartbeat('ready');
    while (['complete', 'failed'].includes((await context.refresh()).status)) await wallSleep(250);
    workers.push(tracked(context.flushOutbox()));
    if (context.role === 'portsim') {
      resetExecutingEdgesForTests();
      resetMovingGuardForTests();
      resetEdgeQueuesForTests();
      resetGraphCacheForTests();
      const settings = { ...getSimulatorSettings(), shipmentGenerationEnabled: false,
        simSpeedMultiplier: 1, simNodeDistanceScale: 20, simProgressIntervalMs: 250,
        simTaskStepMs: 750, simLoopIdleDelayMs: 250, simLoopTickMs: 100, simCompletingDelayMs: 100 };
      const edges = await context.db.collection('edgeRuntime').find({}).toArray();
      for (const edge of edges) workers.push(tracked(edgeAutonomousLoop(edge.id, context.db, device, { simulatorSettings: settings, suggestionsByEdge: {} })));
      workers.push(tracked(scheduledShipments(context, spec, device)), tracked(craneTelemetry(context, spec, device)));
    } else {
      workers.push(tracked(sensorTelemetry(context, spec, device)));
    }
    await Promise.all(workers);
  } finally {
    context.stopped = true;
    client.removeListener('message', receive);
    client.end(true);
    await Promise.allSettled(workers);
  }
}

async function main(role) {
  const settings = getMongoSettings();
  validateDemoSettings(settings);
  const mongo = new MongoClient(settings.uri, { serverSelectionTimeoutMS: 2000 });
  await mongo.connect();
  const base = mongo.db(settings.databaseName);
  while (true) {
    let context;
    let session;
    let outcome;
    try {
      const state = await base.collection('_demoControl').findOne({ _id: 'active' });
      if (!state || state.status === 'resetting') { await wallSleep(250); continue; }
      context = new DemoContext(base, state, role);
      session = storage.run(context, () => runSession(context)).then(() => { outcome = null; }, (error) => { outcome = error; });
      while (true) {
        const current = await context.refresh();
        if (outcome !== undefined) {
          if (outcome && !(outcome instanceof DemoRunEnded)) throw outcome;
          if (!['complete', 'failed', 'resetting'].includes(current.status)) break;
        }
        await context.heartbeat(outcome !== undefined ? 'stopped' : context.mqtt?.connected ? 'ready' : 'starting');
        await wallSleep(300);
      }
    } catch (error) {
      if (!(error instanceof DemoRunEnded)) {
        console.error(`Demo ${role} reconnecting:`, error.message);
        if (context) await context.heartbeat('starting', error.name).catch(() => {});
        await wallSleep(500);
      }
    } finally {
      if (context) { context.stopped = true; context.mqtt?.end(true); }
      if (session) await Promise.race([session, wallSleep(2000)]);
    }
  }
}

module.exports = { main, healthyCrane, scheduledShipments };
