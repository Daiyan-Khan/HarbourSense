const fs = require('fs');
const { MongoClient } = require('mongodb');
const awsIot = require('aws-iot-device-sdk');
const path = require('path');

// Setup logging to log.txt
const logStream = fs.createWriteStream('log.txt', { flags: 'a' });

const originalConsoleLog = console.log;
console.log = function(...args) {
  originalConsoleLog.apply(console, args);
  logStream.write(args.join(' ') + '\n');
};

const originalConsoleError = console.error;
console.error = function(...args) {
  originalConsoleError.apply(console, args);
  logStream.write('[ERROR] ' + args.join(' ') + '\n');
};

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';

const device = awsIot.device({
  keyPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),
  certPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, 'certs/AmazonRootCA1.pem'),
  clientId: 'port_simulator',
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com',
  offlineQueueMaxSize: 0
});

// Global cache for traffic suggestions and paths from MQTT (avoids Mongo queries)
let suggestionsByEdge = {}; // { edgeId: { eta, suggestedPath, ... } }

// Constants for ETA calculation
const NODE_BASE_DISTANCE = 100; // Base units per node

const EDGE_SPEEDS = {
  truck: 10, // Faster
  agv: 8,
  conveyor: 5,
  crane: 2,
  robot: 6,
  unknown: 5 // Fallback
};

// Enum for states
const TaskPhase = Object.freeze({
  IDLE: 'idle',
  ENROUTE_START: 'en_route_start',
  ASSIGNED: 'assigned',
  COMPLETING: 'completing'
});

// Default idle attributes
const IDLE_DEFAULTS = {
  task: 'idle',
  nextNode: 'Null',
  finalNode: 'Null',
  startNode: 'Null',
  eta: 'N/A',
  journeyTime: 'N/A'
};

// Transition functions (simplified: no location checks, rely on empty path trigger)
function transitionToIdle(currentEdge) {
  console.log(`Transitioning ${currentEdge.id} to IDLE; resetting attributes`);
  return { ...IDLE_DEFAULTS, taskPhase: TaskPhase.IDLE };
}

function transitionToEnrouteStart(currentEdge, taskData) {
  if (currentEdge.taskPhase !== TaskPhase.IDLE) {
    throw new Error('Can only assign task from IDLE');
  }

  console.log(`Transitioning ${currentEdge.id} to ENROUTE_START with task ${taskData.task}`);
  return {
    task: taskData.task,
    taskPhase: TaskPhase.ENROUTE_START,
    startNode: taskData.startNode || 'Null',
    finalNode: taskData.finalNode || 'Null',
    path: taskData.path || [], // Receive full path array
    eta: 'N/A',
    journeyTime: 0
  };
}

function transitionToAssigned(currentEdge) {
  if (currentEdge.taskPhase !== TaskPhase.ENROUTE_START) {
    throw new Error('Can only transition to ASSIGNED from ENROUTE_START');
  }

  console.log(`Transitioning ${currentEdge.id} to ASSIGNED (path empty, start reached)`);
  return { taskPhase: TaskPhase.ASSIGNED };
}

function transitionToCompleting(currentEdge) {
  if (currentEdge.taskPhase !== TaskPhase.ASSIGNED) {
    throw new Error('Can only transition to COMPLETING from ASSIGNED');
  }

  console.log(`Transitioning ${currentEdge.id} to COMPLETING (path empty, destination reached); presetting nulls`);
  return {
    taskPhase: TaskPhase.COMPLETING,
    nextNode: 'Null',
    finalNode: 'Null',
    eta: 'N/A',
    journeyTime: 'N/A',
    path: [] // Clear path on completion
  };
}

// Helper to update state and log changes (skip if new task is "awaiting task")
async function updateEdgeState(db, edgeId, updates) {
  const edgeBefore = await db.collection('edgeDevices').findOne({ id: edgeId });
  await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: updates });
  const edgeAfter = await db.collection('edgeDevices').findOne({ id: edgeId });

  // Skip logging if new task is "awaiting task"
  if (edgeAfter.task === 'awaiting task') return edgeAfter;

  // Log changes
  if (edgeBefore.task !== edgeAfter.task) {
    console.log(`Edge ${edgeId} task changed: ${edgeBefore.task} -> ${edgeAfter.task}`);
  }

  if (edgeBefore.taskPhase !== edgeAfter.taskPhase) {
    console.log(`Edge ${edgeId} phase changed: ${edgeBefore.taskPhase} -> ${edgeAfter.taskPhase}`);
  }

  if (edgeBefore.currentLocation !== edgeAfter.currentLocation) {
    console.log(`Edge ${edgeId} location changed: ${edgeBefore.currentLocation} -> ${edgeAfter.currentLocation}`);
  }

  return edgeAfter;
}

// Calculate distance (Manhattan on grid)
function nodeDistance(a, b) {
  if (!a || typeof a !== 'string' || !b || typeof b !== 'string') {
    console.warn('Invalid or null nodes in nodeDistance:', a, b);
    return 0;
  }

  const matchA = a.match(/([A-Z]+)([0-9]+)/);
  const matchB = b.match(/([A-Z]+)([0-9]+)/);

  if (!matchA || !matchB) {
    console.warn('Invalid node format in nodeDistance:', a, b);
    return 0;
  }

  const colA = matchA[1];
  const rowA = parseInt(matchA[2], 10);
  const colB = matchB[1];
  const rowB = parseInt(matchB[2], 10);

  return Math.abs(rowA - rowB) + Math.abs(colA.charCodeAt(0) - colB.charCodeAt(0));
}

// Function to handle task execution with timer
async function executeTask(edgeId, db, device, durationSeconds = 10) {
  console.log(`Starting task execution for ${edgeId} with duration ${durationSeconds}s`);

  // Countdown in seconds
  for (let remaining = durationSeconds; remaining > 0; remaining--) {
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Publish progress update
    device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({ id: edgeId, remaining }));
  }

  // Task complete: Transition to idle and notify
  const edge = await db.collection('edgeDevices').findOne({ id: edgeId });
  const updates = transitionToIdle(edge);
  await updateEdgeState(db, edgeId, updates);
  device.publish(`harboursense/edge/${edgeId}/task_completed`, JSON.stringify({ id: edgeId, location: edge.currentLocation, task: updates.task, status: 'completed' }));
  console.log(`Task completed for ${edgeId} at ${edge.currentLocation}`);
}

// Simulate movement: Traverse full path autonomously, discard on arrival (enhanced for MQTT path/suggestion pulls)
async function simulateMovement(edgeId, db, device) {
  // At beginning of each tick: Fetch fresh edge data from DB
  let edge = await db.collection('edgeDevices').findOne({ id: edgeId });

  if (!edge) {
    console.log(`No edge found for ${edgeId}`);
    return;
  }

  // Pull path and suggestions from MQTT cache (no Mongo query for trafficData)
  const suggestion = suggestionsByEdge[edgeId] || {};

  if (suggestion.suggestedPath && Array.isArray(suggestion.suggestedPath) && suggestion.suggestedPath.length > 0) {
    // Override path with latest MQTT suggestion if available
    await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: { path: suggestion.suggestedPath } });
    console.log(`Overrode path for ${edgeId} with MQTT suggestion: ${suggestion.suggestedPath.join(' -> ')}`);
    edge.path = suggestion.suggestedPath; // Update local for this movement
  }

  if (!edge.path || edge.path.length === 0) {
    console.log(`No path available for ${edgeId}; checking phases...`);
    return; // Will handle in loop
  }

  let currentLocation = edge.currentLocation;
  let nextNode = edge.path[0]; // First element as next node (from MQTT/DB)

  // If already at nextNode, discard and proceed (no movement needed)
  if (currentLocation === nextNode) {
    // Arrival: before discarding, update DB with arrival and nextNode
    await updateEdgeState(db, edgeId, {
      currentLocation: nextNode, // Force arrival location
      nextNode: nextNode, // Expose the node just arrived at
      taskPhase: TaskPhase.ASSIGNED // Move into ASSIGNED so transitionToCompleting will pass
    });

    // Now safe to discard the node from path
    await db.collection('edgeDevices').updateOne(
      { id: edgeId },
      { $pull: { path: nextNode } }
    );

    console.log(`Already at ${nextNode}; discarding first path element and proceeding`);

    // Refresh edge after pull
    edge = await db.collection('edgeDevices').findOne({ id: edgeId });

    if (edge.path.length === 0) {
      // Path empty: Attempt phase shift
      if (edge.currentLocation === edge.finalNode) {
        const updates = transitionToCompleting(edge);
        await updateEdgeState(db, edgeId, updates);
      } else if (edge.currentLocation === edge.startNode && edge.taskPhase === TaskPhase.ENROUTE_START) {
        const updates = transitionToAssigned(edge);
        await updateEdgeState(db, edgeId, updates);
      }

      // Publish empty remaining path to trigger analyzer (end of journey)
      device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify({
        remainingPath: [],
        currentLocation: nextNode,
        taskPhase: edge.taskPhase
      }));
      return;
    }

    nextNode = edge.path[0]; // Next after discard
  }

  // Calculate distance (Manhattan on grid)
  const distance = nodeDistance(currentLocation, nextNode);
  const speed = EDGE_SPEEDS[edge.type] || EDGE_SPEEDS.unknown;
  const etaSeconds = Math.max(1, (distance * NODE_BASE_DISTANCE) / speed);

  // Use MQTT suggestion for ETA (fallback to calculated)
  const suggestedEta = suggestion.eta ? parseFloat(suggestion.eta) : etaSeconds;

  let journeyTime = (edge.journeyTime || 0) + suggestedEta;

  console.log(`Edge ${edgeId} starting movement (path from MQTT): ${currentLocation} -> ${nextNode} (Distance: ${distance} nodes, Speed: ${speed}, ETA: ${suggestedEta}s)`);

  // Integrate progress updates during movement (similar to executeTask)
  // Do not update currentLocation to transit string; keep it as starting location until arrival
  const startTime = Date.now();
  const updateInterval = 1000; // Update every second

  let intervalId = setInterval(() => {
    const elapsed = (Date.now() - startTime) / 1000;
    const progress = Math.min(100, Math.floor((elapsed / suggestedEta) * 100));
    const remaining = Math.max(0, Math.floor(suggestedEta - elapsed));

    // Publish progress to specific topic
    device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({
      id: edgeId,
      progress: progress,
      remaining: remaining,
      toNode: nextNode,
      currentLocation: currentLocation // Still at start until arrival
    }));

    console.log(`Edge ${edgeId} progress to ${nextNode}: ${progress}%, remaining: ${remaining}s`);

    if (progress >= 100) {
      clearInterval(intervalId);
    }
  }, updateInterval);

  // Wait for the full ETA
  await new Promise(resolve => setTimeout(resolve, suggestedEta * 1000));

  clearInterval(intervalId);

  // On arrival: Update location and discard
  const arrivedNode = nextNode;
  await updateEdgeState(db, edgeId, {
    currentLocation: arrivedNode,
    nextNode: arrivedNode,
    taskPhase: TaskPhase.ASSIGNED
  });

  // Discard the arrived node from path
  await db.collection('edgeDevices').updateOne(
    { id: edgeId },
    { $pull: { path: arrivedNode } }
  );

  // ENHANCED: If path now empty after discard, trigger phase shift immediately (before publishing)
  edge = await db.collection('edgeDevices').findOne({ id: edgeId }); // Refresh
  const remainingPath = edge.path;

  if (remainingPath.length === 0) {
    // Trigger based on current phase (reuse loop logic)
    if (edge.taskPhase === TaskPhase.ENROUTE_START) {
      const updates = transitionToAssigned(edge);
      await updateEdgeState(db, edgeId, updates);
    } else if (edge.taskPhase === TaskPhase.ASSIGNED) {
      const updates = transitionToCompleting(edge);
      await updateEdgeState(db, edgeId, updates);
    }

    console.log(`Path emptied after arrival at ${arrivedNode} for ${edgeId}; phase shifted.`);
  }

  // ENHANCED: Publish remaining path back to traffic/update topic (feedback to analyzer/manager)
  const arrivalPayload = {
    remainingPath: remainingPath, // Key field for analyzer
    currentLocation: arrivedNode,
    taskPhase: edge.taskPhase,
    finalNode: edge.finalNode,
    status: 'arrived',
    traveled: arrivedNode // Report discarded node
  };

  device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify(arrivalPayload)); // NEW: Publish to analyzer's topic
  await db.collection('edgeHistory').insertOne({ ...arrivalPayload, timestamp: new Date() });

  console.log(`Edge ${edgeId} arrived at ${arrivedNode}, discarded first path element, published remaining path (${remainingPath.length} nodes left)`);
}

async function edgeAutonomousLoop(edgeId, db, device) {
  let moveCount = 0; // Safety counter to prevent infinite loops
  let pathEmptyCount = 0; // Throttle empty path logs
  const MAX_EMPTY_LOGS = 3; // Limit spam per edge

  while (true) {
    const edge = await db.collection('edgeDevices').findOne({ id: edgeId });

    if (!edge) {
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    const state = edge.taskPhase;

    if (state === TaskPhase.IDLE) {
      console.log(`DEBUG: Edge ${edgeId} idle - staying at ${edge.currentLocation}, awaiting task.`);
      await new Promise(resolve => setTimeout(resolve, 5000)); // Poll less frequently
    } else if (state === TaskPhase.ENROUTE_START || state === TaskPhase.ASSIGNED) {
      // ENHANCED: In enroute/assigned, ensure path from MQTT is current, then move if available
      if (edge.path && edge.path.length > 0) {
        await simulateMovement(edgeId, db, device);
        moveCount++;
        pathEmptyCount = 0; // Reset throttle on movement

        if (moveCount > 100) { // Safety: Break if too many moves (path should empty)
          console.warn(`Max moves reached for ${edgeId}; resetting to idle`);
          const updates = transitionToIdle(edge);
          await updateEdgeState(db, edgeId, updates);
          break;
        }
      } else {
        // Path empty: Trigger phase shift based on current state (no location check needed)
        pathEmptyCount++;
        if (pathEmptyCount <= MAX_EMPTY_LOGS) {
          console.log(`Path empty for ${edgeId} in ${state}; triggering phase shift...`);
        } else if (pathEmptyCount === MAX_EMPTY_LOGS + 1) {
          console.log(`Further path empty logs for ${edgeId} suppressed.`);
        }

        if (state === TaskPhase.ENROUTE_START) {
          // Empty path in ENROUTE_START → assume start reached → to ASSIGNED
          const updates = transitionToAssigned(edge);
          await updateEdgeState(db, edgeId, updates);
        } else if (state === TaskPhase.ASSIGNED) {
          // Empty path in ASSIGNED → assume destination reached → to COMPLETING
          const updates = transitionToCompleting(edge);
          await updateEdgeState(db, edgeId, updates);

          // Publish empty path feedback to manager/analyzer
          device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify({
            remainingPath: [],
            currentLocation: edge.currentLocation,
            taskPhase: state
          }));
        }
      }
    } else if (state === TaskPhase.COMPLETING) {
      await executeTask(edgeId, db, device); // This already transitions to IDLE on finish
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    await new Promise(resolve => setTimeout(resolve, 1000)); // Small delay to prevent tight loop
  }
}

async function runPortSimulation() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  device.on('connect', async () => {
    console.log('Connected to AWS IoT Core');
    device.subscribe('harboursense/edge/+/task');
    console.log(`Subscribed to ${topic}`);
    device.subscribe('harboursense/traffic/+'); // Subscribe for manager/analyzer-pushed path updates and suggestions
    console.log(`Subscribed to ${topic}`);
    device.subscribe('harboursense/shipments/+'); // FIXED: Wildcard for individual shipments (e.g., /shipment_1)
    console.log(`Subscribed to ${topic}`);
    console.log('Subscribed to edge tasks, traffic updates, and shipments');

    // Start edge loops after connect (moved inside for safety)
    const edges = await db.collection('edgeDevices').find().toArray();
    if (edges.length) {
      console.log(`Starting simulation for ${edges.length} edges...`);
      edges.forEach(edge => edgeAutonomousLoop(edge.id, db, device));
    } else {
      console.log('No edges found.');
    }
  });

  device.on('error', (err) => console.error('AWS IoT error:', err));

  device.on('message', async (topic, payload) => {
    try {
      console.log(`Message on topic: ${topic}`);

      const taskMatch = topic.match(/^harboursense\/edge\/([^/]+)\/task$/);
      const trafficMatch = topic.match(/^harboursense\/traffic\/([^/]+)$/);
      const shipmentMatch = topic.match(/^harboursense\/shipments\/(.+)$/);

      if (taskMatch) {
        const edgeId = taskMatch[1];
        const taskData = JSON.parse(payload.toString());
        console.log(`Task received for edge ${edgeId}:`, taskData);

        const edge = await db.collection('edgeDevices').findOne({ id: edgeId });
        const updates = transitionToEnrouteStart(edge, taskData);
        await updateEdgeState(db, edgeId, updates);
        console.log(`Edge ${edgeId} updated with new task and full path from manager`);
      } else if (trafficMatch) {
        const edgeId = trafficMatch[1];
        const trafficData = JSON.parse(payload.toString());

        // Cache suggestions and paths from MQTT (e.g., from analyzer: { path, eta, suggestedPath, ... })
        if (trafficData.path || trafficData.suggestedPath || trafficData.eta) {
          suggestionsByEdge[edgeId] = {
            ...suggestionsByEdge[edgeId],
            path: trafficData.path || trafficData.suggestedPath,
            eta: trafficData.eta,
            suggestedPath: trafficData.suggestedPath || trafficData.path
          };
          console.log(`Cached MQTT traffic data for ${edgeId}: path=${(trafficData.path || []).join(' -> ')}, eta=${trafficData.eta}`);

          // Optionally still update DB path for persistence, but movement now uses cache
          if (trafficData.path && Array.isArray(trafficData.path)) {
            await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: { path: trafficData.path } });
            console.log(`Updated path for ${edgeId} from MQTT traffic topic: ${trafficData.path.join(' -> ')}`);
          }
        }
      } else if (shipmentMatch) { // FIXED: Handle shipment updates here only
        const shipmentId = shipmentMatch[1];
        const shipmentData = JSON.parse(payload.toString());
        console.log(`Shipment update ${shipmentId}:`, shipmentData);

        // FIXED: Update DB with node/status (e.g., from manager: offload complete moves to warehouse)
        await db.collection('shipments').updateOne(
          { id: shipmentId },
          {
            $set: {
              status: shipmentData.status || 'waiting',
              currentNode: shipmentData.currentNode || 'A1', // Track node (dock/warehouse)
              assignedEdges: shipmentData.assignedEdges || [] // Track assigned cranes/forklifts
            }
          },
          { upsert: true }
        );

        console.log(`Updated shipment ${shipmentId} to node ${shipmentData.currentNode}, status ${shipmentData.status}`);
        return; // Exit after handling shipment
      }
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  // ENHANCED: generateShipmentsPeriodically with initial node
  async function generateShipmentsPeriodically(db) {
    const shipmentsColl = db.collection('shipments');
    let shipmentCounter = 0;

    while (true) {
      shipmentCounter++;
      const newShipment = {
        id: `shipment_${shipmentCounter}`,
        arrivalNode: 'A1', // Dock
        currentNode: 'A1', // NEW: Track current position
        status: 'waiting', // waiting → offloading → offloaded → transporting → completed
        needsOffloading: true, // NEW: Flag for crane priority
        destination: 'C5', // Warehouse (from graph)
        assignedEdges: [],
        createdAt: new Date()
      };

      await shipmentsColl.insertOne(newShipment);

      // Publish to AWS for manager to pick up
      device.publish(`harboursense/shipments/${newShipment.id}`, JSON.stringify(newShipment));

      console.log(`New shipment generated: ${newShipment.id} at node ${newShipment.currentNode} (needs offloading: ${newShipment.needsOffloading})`);

      await new Promise(resolve => setTimeout(resolve, 30000)); // 10s interval
    }
  }

  generateShipmentsPeriodically(db);
}

runPortSimulation().catch(console.error);
