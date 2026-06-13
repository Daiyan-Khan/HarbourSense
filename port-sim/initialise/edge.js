const { MongoClient } = require('mongodb');
const { createMqttDevice, getMongoSettings, getMqttBrokerLabel } = require('../runtime-config');

const device = createMqttDevice('edge_simulator');
console.log(`Edge simulator MQTT target: ${getMqttBrokerLabel()}`);

async function edgeAutonomousLoop(edgeId) {
  const mongoSettings = getMongoSettings();
  const client = new MongoClient(mongoSettings.uri, {
    serverSelectionTimeoutMS: mongoSettings.serverSelectionTimeoutMS,
  });
  await client.connect();
  const db = client.db(mongoSettings.databaseName);
  const edgesCol = db.collection('edges');

  // Subscribe to MQTT for task updates (e.g., new route assignments)
  device.on('connect', () => {
    console.log('Connected to AWS IoT for edge');
    device.subscribe(`harboursense/edge/${edgeId}/task`);
  });
  device.on('message', async (topic, payload) => {
    const taskData = JSON.parse(payload.toString());
    console.log(`Received task update for ${edgeId}:`, taskData);
    // Update DB with new task/nextNode/eta
    await edgesCol.updateOne({ id: edgeId }, { $set: { task: taskData.task, nextNode: taskData.nextNode, eta: taskData.eta } });
    // Trigger immediate movement simulation
    await simulateMovement(edgeId, db);
  });

  while (true) {
    const edge = await edgesCol.findOne({ id: edgeId });
    if (!edge) {
      console.log(`Edge ${edgeId} not found, stopping.`);
      break;
    }
    if (!edge.nextNode || !edge.eta || edge.task === 'idle') {
      console.log(`Edge ${edgeId} idle or no route, checking again in 5s...`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    await simulateMovement(edgeId, db);
  }
  await client.close();
}

async function simulateMovement(edgeId, db) {
  const edge = await db.collection('edges').findOne({ id: edgeId });
  const etaMs = edge.eta * 3600 * 1000 / 3600; // Speed up for demo
  console.log(`Edge ${edgeId} traveling to ${edge.nextNode} for ${edge.eta} hours...`);
  await new Promise(resolve => setTimeout(resolve, etaMs));

  const newLocation = edge.nextNode;
  const graphDoc = await db.collection('graph').findOne();
  const neighbors = graphDoc?.nodes?.[newLocation]?.neighbors || {};
  let bestNode = null;
  let bestEta = Infinity;
  for (const [neighbor, dist] of Object.entries(neighbors)) {
    const speed = edge.speed || 10;
    const eta = dist / speed;
    if (eta < bestEta) {
      bestEta = eta;
      bestNode = neighbor;
    }
  }
  if (!bestNode && Object.keys(neighbors).length > 0) {
    bestNode = Object.keys(neighbors)[0];
    bestEta = neighbors[bestNode] / (edge.speed || 10);
  }

  await db.collection('edges').updateOne(
    { id: edgeId },
    { $set: { currentLocation: newLocation, nextNode: bestNode, eta: bestEta } }  // Updated to use currentLocation
  );
  console.log(`Edge ${edgeId} arrived at ${newLocation}. New next: ${bestNode}, ETA: ${bestEta.toFixed(2)} hours.`);

  // NEW: Publish location update to MQTT for real-time feeds
  const updatePayload = { id: edgeId, currentLocation: newLocation, nextNode: bestNode, eta: bestEta };
  device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(updatePayload));
}

module.exports = { edgeAutonomousLoop };

// To run for a specific edge: node edge.js truck001
if (require.main === module) {
  const edgeId = process.argv[2];
  if (!edgeId) {
    console.error('Provide edge ID as argument, e.g., node edge.js truck001');
    process.exit(1);
  }
  edgeAutonomousLoop(edgeId);
}
