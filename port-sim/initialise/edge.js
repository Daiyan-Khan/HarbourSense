const { MongoClient } = require('mongodb');
const awsIot = require('aws-iot-device-sdk');
const path = require('path');

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';
const device = awsIot.device({
  keyPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),
  certPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, 'certs/AmazonRootCA1.pem'),
  clientId: 'edge_simulator',
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com'
});

async function edgeAutonomousLoop(edgeId) {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');
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
