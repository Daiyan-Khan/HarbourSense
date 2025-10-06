// initial-edge.js - Simulates initial edge device data and stores in MongoDB

const { MongoClient } = require('mongodb');
const { graphData } = require('./graph.js');  // Your graph loader for valid nodes/routes
const devices = require('./test-edge.json');  // Load devices from test-edge.json

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';

// Helper: Get random node from graph
function getRandomNode() {
  return graphData[Math.floor(Math.random() * graphData.length)].id;
}

// Helper: Remove consecutive duplicate nodes from a path array
function removeConsecutiveDuplicates(path) {
  if (path.length < 2) return path;
  const uniquePath = [path[0]];
  for (let i = 1; i < path.length; i++) {
    if (path[i] !== path[i - 1]) {
      uniquePath.push(path[i]);
    } else {
      console.log(`Removed duplicate consecutive node ${path[i]} from path`);
    }
  }
  if (uniquePath.length > 1 && uniquePath[0] === uniquePath[uniquePath.length - 1]) {
    uniquePath.pop(); // Avoid loop
  }
  return uniquePath;
}

// Helper: Get a random valid path (startNode, next, final) based on graph
function getRandomPath(type) {
  // For static types, no path - just a random start
  if (type === 'crane' || type === 'conveyor') {
    const start = getRandomNode();
    return { startNode: start, next: "null", final: "null", path: [] };
  }

  let pathObj;
  do {
    const startNode = getRandomNode();
    const neighbors = graphData.find(n => n.id === startNode)?.neighbors || {};
    const directions = Object.keys(neighbors);
    if (!directions.length) {
      return { startNode, next: "null", final: "null", path: [] };
    }

    const nextDir = directions[Math.floor(Math.random() * directions.length)];
    const nextNode = neighbors[nextDir];
    const finalNeighbors = graphData.find(n => n.id === nextNode)?.neighbors || {};
    const finalDirs = Object.keys(finalNeighbors) || [];

    let finalNode = "null";
    for (let i = 0; i < finalDirs.length; i++) {
      let candidate = finalNeighbors[finalDirs[i]];
      if (candidate !== startNode && candidate !== nextNode) {
        finalNode = candidate;
        break;
      }
    }
    if (finalNode === "null") finalNode = nextNode; // fallback

    let path = [startNode, nextNode, finalNode];
    path = removeConsecutiveDuplicates(path);

    pathObj = {
      startNode: path[0],
      next: path.length > 1 ? path[1] : "null",
      final: path.length > 0 ? path[path.length - 1] : "null",
      path: path
    };

  } while (
    pathObj.path.length < 2 ||
    (pathObj.next !== "null" && pathObj.startNode === pathObj.next) ||
    (pathObj.final !== "null" && pathObj.next === pathObj.final)
  );

  return pathObj;
}

// Helper: Assign realistic task based on roles
function getRealisticTask(roles) {
  const possibleTasks = {
    transport: ['transport', 'moving', 'idle'],
    loading: ['loading', 'unloading', 'idle'],
    lifting: ['lifting', 'idle'],
    inspection: ['surveying', 'inspecting', 'idle'],
    repair: ['repairing', 'awaiting task', 'idle']
  };
  const role = roles[0] || 'idle';
  const tasks = possibleTasks[role] || ['idle'];
  return 'idle';
}

// Generate initial edges
const initialEdges = devices.map(device => {
  const type = device.id.match(/^[a-z]+/) ? device.id.match(/^[a-z]+/)[0] : 'unknown';
  const pathObj = getRandomPath(type);
  const task = getRealisticTask(device.roles);
  const priority = Math.floor(Math.random() * 10) + 1;
  const eta = Math.floor(Math.random() * 16) + 5;
  const speed = device.speed;
  return {
    id: device.id,
    desc: device.desc || 'Generic device',
    roles: device.roles,
    type,
    currentLocation: pathObj.startNode,
    task: 'idle',
    priority,
    nextNode: 'Null',        // string "null" instead of null
    finalNode: 'None',      // string "null" instead of null
    startNode: pathObj.startNode,
    path: [],
    taskPhase: 'idle',
    eta:'Waiting',
    taskCompletionTime: 0,
    speed,
    journeyTime: 0
  };
});

async function storeInitialEdges() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('port');
    const edgesColl = db.collection('edgeDevices');
    await edgesColl.deleteMany({});
    const result = await edgesColl.insertMany(initialEdges);
    console.log(`Inserted ${result.insertedCount} initial edges with string "null" values.`);
  } catch (error) {
    console.error('Error storing initial edges:', error);
  } finally {
    await client.close();
  }
}

storeInitialEdges().catch(console.error);

module.exports = { initialEdges };
