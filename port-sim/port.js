if (process.env.DEMO_MODE === 'true') {
  require('./lib/demo-simulator').main('portsim').catch((error) => { console.error(error.message); process.exitCode = 1; });
} else {
const fs = require('fs');
const { MongoClient } = require('mongodb');
const {
  createMongoClient,
  createMqttDevice,
  getMongoSettings,
  getMqttBrokerLabel,
  getSimulatorSettings,
} = require('./runtime-config');
const {
  craneTelemetryTopic,
  validateCraneTelemetryPayload,
} = require('./lib/mqtt-contract');
const {
  buildCraneTelemetryPayload,
  isCraneActive,
} = require('./lib/crane-telemetry');
const { TaskPhase, IDLE_DEFAULTS } = require('./lib/edge-phases');
const { edgeAutonomousLoop } = require('./lib/edge-autonomous-loop');
const { createMqttMessageHandler } = require('./lib/mqtt-handlers');
const { generateShipmentsPeriodically } = require('./lib/shipment-generator');
const { runtimeCollection, runtimeDocumentFromSeed } = require('./lib/edge-collections');

const mongoSettings = getMongoSettings();
const simulatorSettings = getSimulatorSettings();
const device = createMqttDevice('port_simulator');
const mqttBrokerLabel = getMqttBrokerLabel();
const client = createMongoClient(MongoClient, mongoSettings);

const suggestionsByEdge = {};

if (simulatorSettings.simLogToFile) {
  const logStream = fs.createWriteStream('log.txt', { flags: 'a' });
  const originalConsoleLog = console.log;
  const originalConsoleError = console.error;
  console.log = function logWithFile(...args) {
    originalConsoleLog.apply(console, args);
    logStream.write(`${new Date().toISOString()} ${args.join(' ')}\n`);
  };
  console.error = function errorWithFile(...args) {
    originalConsoleError.apply(console, args);
    logStream.write(`${new Date().toISOString()} [ERROR] ${args.join(' ')}\n`);
  };
}

async function publishCraneTelemetryPeriodically(db) {
  console.log(
    `Crane telemetry publisher interval=${simulatorSettings.craneTelemetryIntervalMs}ms`,
  );

  while (true) {
    try {
      const cranes = await runtimeCollection(db).find({ type: 'crane' }).toArray();
      for (const crane of cranes) {
        const payload = buildCraneTelemetryPayload(crane.id, {
          active: isCraneActive(crane.taskPhase),
        });
        const validation = validateCraneTelemetryPayload(payload);
        if (!validation.valid) {
          console.warn(`Skipping invalid crane telemetry for ${crane.id}`);
          continue;
        }
        device.publish(craneTelemetryTopic(crane.id), JSON.stringify(payload));
      }
    } catch (err) {
      console.error('Crane telemetry publish error:', err);
    }
    await new Promise((resolve) => setTimeout(resolve, simulatorSettings.craneTelemetryIntervalMs));
  }
}

async function runPortSimulation() {
  await client.connect();
  console.log(`Connected to MongoDB database '${mongoSettings.databaseName}'`);
  const db = client.db(mongoSettings.databaseName);

  const edgesCount = await runtimeCollection(db).countDocuments();
  if (edgesCount === 0) {
    console.log('No edgeRuntime found; inserting defaults...');
    const defaultEdges = [
      { id: 'crane_1', type: 'crane', currentLocation: 'A1', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'crane_2', type: 'crane', currentLocation: 'A1', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_tempo_1', type: 'truck_tempo', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_tempo_2', type: 'truck_tempo', currentLocation: 'D2', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_delivery_1', type: 'truck_delivery', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot001', type: 'robot', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot_2', type: 'robot', currentLocation: 'D2', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'forklift_1', type: 'forklift', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'forklift_2', type: 'forklift', currentLocation: 'D2', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
    ];
    const runtimeDocs = [];
    const assignmentDocs = [];
    for (const edge of defaultEdges) {
      const { runtime, assignment } = runtimeDocumentFromSeed(edge);
      runtimeDocs.push(runtime);
      assignmentDocs.push(assignment);
    }
    await runtimeCollection(db).insertMany(runtimeDocs);
    await db.collection('edgeAssignments').insertMany(assignmentDocs);
    console.log('Inserted 9 default edge runtime docs.');
  }

  const graphNodes = await db.collection('graph').find({}).toArray();
  const docks = graphNodes.filter((node) => node.type === 'dock').map((node) => node.id);
  const warehouses = graphNodes.filter((node) => node.type === 'warehouse').map((node) => node.id);
  if (docks.length < 1) docks.push('A1');
  if (warehouses.length < 1) warehouses.push('C5');
  console.log(`Graph: ${docks.length} docks, ${warehouses.length} warehouses`);

  generateShipmentsPeriodically(db, docks, warehouses, device, simulatorSettings);

  const loopOptions = {
    suggestionsByEdge,
    simulatorSettings,
  };

  device.on('connect', async () => {
    console.log(`Connected to ${mqttBrokerLabel}`);
    const topics = [
      'harboursense/edge/+/task',
      'harboursense/edge/+/route',
    ];
    topics.forEach((topic) => {
      device.subscribe(topic);
      console.log(`Subscribed to ${topic}`);
    });

    const edges = await runtimeCollection(db).find().toArray();
    if (edges.length) {
      console.log(`Starting simulation for ${edges.length} edges...`);
      edges.forEach((edge) => edgeAutonomousLoop(edge.id, db, device, loopOptions));
    }

    publishCraneTelemetryPeriodically(db);
  });

  device.on('error', (err) => console.error('MQTT error:', err));
  device.on('message', createMqttMessageHandler(db, device, suggestionsByEdge));
}

runPortSimulation().catch(console.error);

}
