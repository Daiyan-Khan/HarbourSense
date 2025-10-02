const { MongoClient } = require('mongodb');
const awsIot = require('aws-iot-device-sdk');
const path = require('path');
const { edgeAutonomousLoop } = require('./edge.js');  // Import edge logic (your first snippet)

// MongoDB URI and AWS IoT setup
const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';
const device = awsIot.device({
  keyPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),
  certPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, 'certs/AmazonRootCA1.pem'),
  clientId: 'port_simulator',  // Unified client ID
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com'
});

async function runPortSimulation() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  // AWS IoT connection handlers
  device.on('connect', () => console.log('Connected to AWS IoT Core'));
  device.on('error', (err) => console.error('AWS IoT error:', err));

  // Run sensor simulation (adapted from sensors.js)
  const sensorsCol = db.collection('sensorList');
  const dataCol = db.collection('sensorData');
  const sensors = await sensorsCol.find().toArray();
  if (sensors.length === 0) {
    console.log('No sensors found.');
  } else {
    console.log(`Starting simulation for ${sensors.length} sensors...`);
    sensors.forEach(sensor => simulateSensor(sensor, device, dataCol));
  }

  // Run edge simulation (from your port.js example, with added history storage)
  const edges = await db.collection('edges').find().toArray();
  if (edges.length === 0) {
    console.log('No edges found.');
  } else {
    console.log(`Starting simulation for ${edges.length} edges...`);
    const promises = edges.map(edge => edgeAutonomousLoop(edge.id, db, device));  // Pass db and device for sharing
    await Promise.all(promises);
  }

  // No close() to keep running indefinitely
}

// Modified simulateSensor function (from sensors.js, using shared device)
function simulateSensor(sensor, device, dataCol) {
  const postData = async () => {
    const reading = generateReading(sensor.type);
    const payload = {
      id: sensor.id,
      type: sensor.type,
      node: sensor.node,
      reading: reading,
      timestamp: new Date()
    };
    device.publish('harboursense/sensor/data', JSON.stringify(payload));
    await dataCol.insertOne(payload);
    console.log(`Inserted sensor data for ${sensor.id}`);
  };

  let intervalMs;
  switch (sensor.type) {
    case 'motion':
    case 'occupancy':
      intervalMs = Math.random() * (10000 - 5000) + 5000;
      break;
    case 'temperature':
    case 'humidity':
      intervalMs = Math.random() * (60000 - 30000) + 30000;
      break;
    default:
      intervalMs = Math.random() * (30000 - 10000) + 10000;
  }
  postData();
  setInterval(postData, intervalMs);
}

// generateReading function (unchanged from sensors.js)
function generateReading(type) {
  switch (type) {
    case 'temperature': return (Math.random() * 40 - 10).toFixed(2);
    case 'humidity': return (Math.random() * 100).toFixed(2);
    case 'vibration': return (Math.random() * 10).toFixed(2);
    case 'occupancy': return Math.floor(Math.random() * 101);
    case 'motion': return Math.random() < 0.5 ? 'detected' : 'none';
    default: return (Math.random() * 100).toFixed(2);
  }
}

// Updated edgeAutonomousLoop (modified to use shared db/device and store history)
async function edgeAutonomousLoop(edgeId, db, device) {
  const edgesCol = db.collection('edges');
  const historyCol = db.collection('edgeHistory');  // New collection for history

  device.subscribe(`harboursense/edge/${edgeId}/task`);

  device.on('message', async (topic, payload) => {
    const taskData = JSON.parse(payload.toString());
    await edgesCol.updateOne({ id: edgeId }, { $set: { task: taskData.task, nextNode: taskData.nextNode, eta: taskData.eta, destinationNode: taskData.destinationNode, taskCompletionTime: taskData.taskCompletionTime } });
    await simulateMovement(edgeId, db, device);
  });

  while (true) {
    const edge = await edgesCol.findOne({ id: edgeId });
    if (!edge || !edge.nextNode || !edge.eta || edge.task === 'idle') {
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }
    await simulateMovement(edgeId, db, device);
  }
}

// Updated simulateMovement with intelligent publishing (final-node, next-node, journey-time, eta)
async function simulateMovement(edgeId, db, device) {
  const edgesCol = db.collection('edges');
  const edge = await edgesCol.findOne({ id: edgeId });

  // Intelligent: Fetch suggested path/eta from traffic-analyzer's DB (assumes /analyze_traffic ran recently)
  const trafficData = await db.trafficData.findOne({}, { sort: { timestamp: -1 } });
  const suggestion = trafficData?.suggestions.find(s => s.edgeId === edgeId) || {};
  const suggestedNextNode = suggestion.suggestedNextNode || edge.nextNode;
  const suggestedEta = suggestion.eta || edge.eta;

  // Calculate journey time (cumulative eta + task time)
  const journeyTime = (edge.journeyTime || 0) + suggestedEta + (edge.taskCompletionTime || 0);

  // Publish intelligent info to MQTT and update DB
  const payload = {
    id: edgeId,
    currentLocation: edge.currentLocation,
    nextNode: suggestedNextNode,
    finalNode: edge.destinationNode,
    eta: suggestedEta,
    journeyTime: journeyTime
  };
  device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(payload));
  await edgesCol.updateOne({ id: edgeId }, { $set: { ...payload, nextNode: suggestedNextNode, eta: suggestedEta, journeyTime } });
  await db.collection('edgeHistory').insertOne({ ...payload, timestamp: new Date() });

  // Simulate movement with eta delay
  const etaMs = suggestedEta * 1000;
  await new Promise(resolve => setTimeout(resolve, etaMs));

  // Update location post-movement
  await edgesCol.updateOne({ id: edgeId }, { $set: { currentLocation: suggestedNextNode } });
  console.log(`Edge ${edgeId} moved intelligently to ${suggestedNextNode}`);
}

runPortSimulation().catch(console.error);
