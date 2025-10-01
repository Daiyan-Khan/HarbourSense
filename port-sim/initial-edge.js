// initial-edge.js - Simulates initial edge device data and stores in MongoDB

const { MongoClient } = require('mongodb');
const { graphData } = require('./graph.js');  // Your graph loader for valid nodes/routes
const devices = require('./edge.json');  // Load devices from edge.json

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';  // Your URI

// Helper: Get random node or route
function getRandomLocation(type) {
  const randomNode = graphData[Math.floor(Math.random() * graphData.length)].id;
  if (type === 'crane' || type === 'conveyor') return randomNode;  // Static at nodes
  // For mobile: 50% chance of route (e.g., 'A2-A3')
  if (Math.random() > 0.5) {
    const neighbors = graphData.find(n => n.id === randomNode)?.neighbors || {};
    const dir = Object.keys(neighbors)[0];  // Random direction
    return dir ? `${randomNode}-${neighbors[dir]}` : randomNode;
  }
  return randomNode;
}

// Generate initial edges from loaded devices
const initialEdges = devices.map(device => {
  const type = device.id.match(/^[a-z]+/) ? device.id.match(/^[a-z]+/)[0] : 'unknown';
  const location = getRandomLocation(type);
  const task = device.roles[Math.floor(Math.random() * device.roles.length)] || 'idle';
  const priority = Math.floor(Math.random() * 10) + 1;  // 1-10
  return { id: device.id, location, task, priority, type };
});

async function storeInitialEdges() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('port');
    const edgesColl = db.collection('edges');
    await edgesColl.deleteMany({});  // Reset for fresh start
    const result = await edgesColl.insertMany(initialEdges);
    console.log(`Inserted ${result.insertedCount} initial edges.`);
  } catch (error) {
    console.error('Error storing initial edges:', error);
  } finally {
    await client.close();
  }
}

// Run the script
storeInitialEdges().catch(console.error);

module.exports = { initialEdges };  // For use in other files
