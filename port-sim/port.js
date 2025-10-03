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

// Constants for ETA calculation
const NODE_BASE_DISTANCE = 100; // Base units per node
const EDGE_SPEEDS = {
  truck: 10,   // Faster
  agv: 8,
  conveyor: 5,
  crane: 2,
  robot: 6,
  unknown: 5   // Fallback
};

// New: Sanitize location to single node
function sanitizeLocation(location) {
  if (typeof location !== 'string' || !location || location.toLowerCase() === 'null') return 'Unknown';
  const nodes = location.split('-').filter(node => node.trim() !== '');
  if (nodes.length === 0) return 'Unknown';
  const uniqueNodes = [...new Set(nodes)];  // Remove duplicates
  if (uniqueNodes.length === 2) return uniqueNodes.join('-');  // Keep transit 'A-B'
  return uniqueNodes[uniqueNodes.length - 1];  // Single last node
}

// New: Helper to update state and log changes (skip if new task is "awaiting task")
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

// New: Function to handle task execution with timer
async function executeTask(edgeId, db, device, durationSeconds) {
  console.log(`Starting task execution for ${edgeId} with duration ${durationSeconds}s`);
  await updateEdgeState(db, edgeId, { taskCompletionTime: durationSeconds, taskRemaining: durationSeconds });

  // Countdown in seconds
  for (let remaining = durationSeconds; remaining > 0; remaining--) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    await updateEdgeState(db, edgeId, { taskRemaining: remaining });
    // Publish progress update
    device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({ id: edgeId, remaining }));
  }

  // Task complete: Reset to idle and notify
  const updatedEdge = await updateEdgeState(db, edgeId, { 
    taskPhase: 'idle', 
    task: 'idle', 
    nextNode: 'Null', 
    finalNode: 'None', 
    startNode: 'None', 
    path: [], 
    eta: 'N/A', 
    taskCompletionTime: 'N/A', 
    taskRemaining: 0, 
    journeyTime: 'N/A' 
  });
  device.publish(`harboursense/edge/${edgeId}/task_completed`, JSON.stringify({ id: edgeId, location: updatedEdge.currentLocation, task: updatedEdge.task }));
  console.log(`Task completed for ${edgeId} at ${updatedEdge.currentLocation}`);
}

async function runPortSimulation() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  device.on('connect', async () => {
    console.log('✅ Connected to AWS IoT Core');
    device.subscribe('harboursense/edge/+/task');
    console.log('📡 Subscribed to edge tasks');
    console.log('DEBUG: Subscribed to topic: harboursense/edge/+/task');  // Added debug for subscription

    const edges = await db.collection('edgeDevices').find().toArray();  // Use 'edgeDevices' consistently

    if (edges.length) {
      console.log(`Starting simulation for ${edges.length} edges...`);
      edges.forEach(edge => edgeAutonomousLoop(edge.id, db, device));
    } else {
      console.log('⚠️ No edges found.');
    }
  });

  device.on('error', (err) => console.error('❌ AWS IoT error:', err));

  device.on('message', async (topic, payload) => {
    try {
      console.log(`DEBUG: Message received on topic: ${topic}`);  // Added debug for incoming topic
      const match = topic.match(/^harboursense\/edge\/([^/]+)\/task$/);
      if (!match) return;

      const edgeId = match[1];
      const taskData = JSON.parse(payload.toString());
      console.log(`📥 Task received for edge ${edgeId}:`, taskData);
      console.log(`DEBUG: Parsed task data for ${edgeId}: ${JSON.stringify(taskData)}`);  // Added debug for parsed data

      // Update with logging via helper (no full path stored here; manager handles)
      await updateEdgeState(db, edgeId, {
        task: taskData.task,
        nextNode: taskData.nextNode || "Null",
        finalNode: taskData.finalNode || taskData.nextNode || "None",
        startNode: taskData.startNode || "None",  // New: From manager
        taskPhase: taskData.taskPhase || 'enroute_to_start',  // New: From manager, default to start phase
        eta: taskData.eta || "N/A",
        taskCompletionTime: "N/A",
        journeyTime: taskData.journeyTime || "N/A"
      });
      console.log(`DEBUG: Updated edge ${edgeId} in 'edgeDevices' with new task data including phase`);

      console.log(`🔄 Edge ${edgeId} updated with new task from manager, starting movement simulation`);
      // No immediate simulateMovement here; loop will handle based on state
    } catch (err) {
      console.error('⚠️ Error processing message:', err);
    }
  });

  // Start periodic shipment generation
  generateShipmentsPeriodically(db);
}

async function edgeAutonomousLoop(edgeId, db, device) {
  while (true) {
    const edge = await db.collection('edgeDevices').findOne({ id: edgeId });  // Use 'edgeDevices' consistently
    console.log(`DEBUG: Checking edge ${edgeId} - Task: ${edge?.task || 'none'}, Phase: ${edge?.taskPhase || 'none'}, Next Node: ${edge?.nextNode || 'none'}, Location: ${edge?.currentLocation || 'none'}`);  // Updated debug for phase

    if (!edge) {
      console.log(`DEBUG: Edge ${edgeId} not found, waiting before retry`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    // FIX: Auto-kickstart phase if task exists but phase is idle (prevents sticking without manager update)
    if (edge.taskPhase === 'idle' && (edge.task !== 'idle' || (edge.nextNode && edge.nextNode !== "Null"))) {
      console.log(`DEBUG: Auto-starting phase for ${edgeId} from 'idle' to 'enroute_to_start' (task/nextNode present)`);
      await updateEdgeState(db, edgeId, { 
        taskPhase: 'enroute_to_start',
        task: edge.task || 'move'  // Ensure non-idle task
      });
      continue;  // Re-check in next loop iteration
    }

    if (edge.taskPhase === 'idle' && (!edge.nextNode) || edge.nextNode === "Null"  ) {
      console.log(`DEBUG: Edge ${edgeId} in idle phase, awaiting task or nextNode from manager`);  // Wait for instruction
      await new Promise(resolve => setTimeout(resolve, 5000));  // Wait 5s
      continue;
    }

    // Phase-specific logic
    if (edge.taskPhase === 'enroute_to_start' && edge.currentLocation === edge.startNode) {
      // Arrived at startNode: Switch to 'executing' and simulate task (e.g., loading)
      const executionTime = Math.floor(Math.random() * 30) + 10;  // Mock loading time
      console.log(`✅ Edge ${edgeId} arrived at startNode ${edge.startNode} - Starting execution (Time: ${executionTime}s)`);
      await executeTask(edgeId, db, device, executionTime);

      // Execution complete: Switch to 'enroute_to_complete' and wait for manager's next hop
      await updateEdgeState(db, edgeId, { taskPhase: 'enroute_to_complete', nextNode: "Null" });
      console.log(`🔄 Edge ${edgeId} execution complete at startNode - Awaiting next hop from manager for completion`);
      continue;  // Loop will wait
    }

    if (edge.taskPhase === 'enroute_to_complete' && edge.currentLocation === edge.finalNode) {
      // Arrived at finalNode: Complete task and set to 'idle'
      console.log(`✅ Edge ${edgeId} arrived at finalNode ${edge.finalNode} - Completing task and resetting to idle`);
      await updateEdgeState(db, edgeId, { 
        task: 'idle', 
        taskPhase: 'idle', 
        nextNode: "Null", 
        finalNode: "None", 
        startNode: "None", 
        eta: "N/A", 
        taskCompletionTime: "N/A", 
        journeyTime: "N/A" 
      });

      // Publish completion with phase for manager
      const completionPayload = { id: edgeId, currentLocation: edge.finalNode, taskPhase: 'idle', status: 'completed' };
      device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(completionPayload));
      console.log(`DEBUG: Published task completion for ${edgeId} on topic: harboursense/edge/${edgeId}/update with phase`);
      console.log(`✅ Edge ${edgeId} completed task and is now idle, ready for new assignment from manager`);
      continue;
    }

    console.log(`DEBUG: Starting simulation for active edge ${edgeId} in phase ${edge.taskPhase}`);  // Added debug for starting simulation
    await simulateMovement(edgeId, db, device);
  }
}

// Improved simulateMovement for hop-by-hop: One segment only, reset nextNode on arrival
async function simulateMovement(edgeId, db, device) {
  const edgesCol = db.collection('edgeDevices');
  const historyCol = db.collection('edgeHistory');
  const edge = await edgesCol.findOne({ id: edgeId });
  if (!edge) return;

  let currentLocation = sanitizeLocation(edge.currentLocation);
  let nextNode = sanitizeLocation(edge.nextNode);
  let finalNode = sanitizeLocation(edge.finalNode);
  let taskPhase = edge.taskPhase;
  let status = edge.status || 'unknown';

  // Stationary check: Refactored to only trigger if nextNode null, status 'arrived', and NOT at final node
  if (!nextNode || nextNode.toLowerCase() === 'null') {
    if (status === 'arrived' && currentLocation !== finalNode) {
      // Stationary: Waiting for next hop from manager (intermediate arrival)
      await updateEdgeState(db, edgeId, { 
  currentLocation: sanitizeLocation(nextNode), 
  nextNode: "Null", 
  journeyTime,
  status: 'arrived'  // NEW: Persist in DB for stationary check
});

      console.log(`DEBUG: Edge ${edgeId} is stationary at intermediate node ${currentLocation} (arrived, waiting for next hop); staying put.`);

      // Publish stationary update (only if not idle to avoid spam)
      if (edge.task !== 'idle') {
        const stationaryPayload = { id: edgeId, currentLocation, status: 'stationary', eta: "Waiting" };
        device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(stationaryPayload));
        await historyCol.insertOne({ ...stationaryPayload, timestamp: new Date() });
        console.log(`DEBUG: Published stationary update for ${edgeId}`);
      }
      return;  // Exit to wait in loop
    } else if (status === 'arrived' && currentLocation === finalNode && taskPhase === 'enroute_to_complete') {
      // At final node: Force reset to idle (your existing logic, triggered only here)
      console.log(`DEBUG: Forcing reset for ${edgeId} at final node ${currentLocation} in enroute_to_complete`);
      await updateEdgeState(db, edgeId, { 
        task: 'idle', 
        taskPhase: 'idle', 
        nextNode: "Null", 
        finalNode: "None", 
        startNode: "None", 
        eta: "N/A", 
        taskCompletionTime: "N/A", 
        journeyTime: "N/A" 
      });

      // Publish completion
      const completionPayload = { id: edgeId, currentLocation, taskPhase: 'idle', status: 'completed' };
      device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(completionPayload));
      console.log(`DEBUG: Published forced completion for ${edgeId}`);
      return;
    } else {
      // Not arrived or other conditions: Log and skip stationary (allow potential movement if nextNode gets set)
      console.log(`DEBUG: Skipping stationary for ${edgeId} (status: ${status}, location: ${currentLocation} vs final: ${finalNode})`);
    }
  } else {
    // Valid nextNode: Proceed with movement
    console.log(`DEBUG: Valid nextNode detected for ${edgeId}; continuing movement simulation`);
  }

  // Calculate distance (Manhattan on grid)
  function nodeDistance(a, b) {
    if (!a || typeof a !== 'string' || !b || typeof b !== 'string') {
      console.warn('Invalid or null nodes in nodeDistance:', a, b);
      return 0;  // Treat as "Waiting" with zero distance
    }

    const matchA = a.match(/([A-Z]+)([0-9]+)/);
    const matchB = b.match(/([A-Z]+)([0-9]+)/);
    if (!matchA || !matchB) {
      console.warn('Invalid node format in nodeDistance:', a, b);
      return 0;  // Fallback to "Waiting"
    }

    const colA = matchA[1];
    const rowA = parseInt(matchA[2], 10);
    const colB = matchB[1];
    const rowB = parseInt(matchB[2], 10);

    return Math.abs(rowA - rowB) + Math.abs(colA.charCodeAt(0) - colB.charCodeAt(0));
  }

  const distance = nodeDistance(currentLocation, nextNode);
  const speed = EDGE_SPEEDS[edge.type] || EDGE_SPEEDS.unknown;
  const etaSeconds = Math.max(1, (distance * NODE_BASE_DISTANCE) / speed);  // Min 1s to avoid zero/instant
  const etaMs = etaSeconds * 1000;  // For setTimeout

  console.log(`🚚 Edge ${edgeId} starting movement: ${currentLocation} -> ${nextNode} (Distance: ${distance} nodes, Speed: ${speed}, ETA: ${etaSeconds}s)`);
  console.log(`DEBUG: Edge ${edgeId} movement initiated from ${currentLocation}`);

  // Update to transit location
  const transitLocation = `${currentLocation}-${nextNode}`;  // Standardized to "-"
  await updateEdgeState(db, edgeId, { currentLocation: transitLocation });
  console.log(`DEBUG: Updated location for ${edgeId} to transit: ${transitLocation}`);

  // Incorporate traffic suggestion if available (override ETA if suggested)
  const trafficData = await db.collection('trafficData').findOne({}, { sort: { timestamp: -1 } });
  const suggestion = trafficData?.suggestions.find(s => s.edgeId === edgeId) || {};
  const suggestedEta = suggestion.eta || etaSeconds;
  let journeyTime = (edge.journeyTime || 0) + suggestedEta;  // FIXED: Handle undefined journeyTime

  console.log(`⏱ Edge ${edgeId} transit ETA: ${suggestedEta}s (traffic suggestion: ${JSON.stringify(suggestion)})`);
  console.log(`DEBUG: Calculated ETA for ${edgeId}: ${suggestedEta}s, New Journey Time: ${journeyTime}`);

  const payload = {
    id: edgeId,
    currentLocation: transitLocation,
    nextNode: nextNode,
    finalNode: finalNode,
    taskPhase: edge.taskPhase,  // Include phase in payload
    eta: suggestedEta,
    taskCompletionTime: edge.taskCompletionTime,
    journeyTime
  };

  device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(payload));
  console.log(`DEBUG: Published update for ${edgeId} on topic: harboursense/edge/${edgeId}/update`);
  await historyCol.insertOne({ ...payload, timestamp: new Date(), status: 'enroute' });
  console.log(`DEBUG: Inserted enroute history for ${edgeId}`);

  // Wait for the ETA (use suggestedEta for timing)
  await new Promise(resolve => setTimeout(resolve, suggestedEta * 1000));

  // Arrival: Update location, reset nextNode to Null, update journeyTime
  await updateEdgeState(db, edgeId, { currentLocation: sanitizeLocation(nextNode), nextNode: "Null", journeyTime });
  console.log(`DEBUG: Updated arrival location for ${edgeId} to ${sanitizeLocation(nextNode)}, nextNode reset to Null`);

  // Existing arrival publish (moved after checks for better flow)
  const arrivalPayload = { id: edgeId, currentLocation: nextNode, taskPhase, status: 'arrived' };  
  device.publish(`harboursense/edge/${edgeId}/update`, JSON.stringify(arrivalPayload));
  console.log(`DEBUG: Published arrival update for ${edgeId} on topic: harboursense/edge/${edgeId}/update with phase`);
  await historyCol.insertOne({ ...arrivalPayload, timestamp: new Date() });
  console.log(`DEBUG: Inserted arrival history for ${edgeId}`);
  console.log(`🎯 Edge ${edgeId} arrived at ${nextNode}`);

  // Check if at final (phase-specific)
  if (nextNode === finalNode && edge.taskPhase === 'enroute_to_complete') {
    // Trigger task completion logic in loop
    console.log(`DEBUG: At final node; loop will handle completion`);
  }
}

async function generateShipmentsPeriodically(db) {
  const shipmentsColl = db.collection('shipments');
  let shipmentCounter = 0;
  while (true) {
    shipmentCounter++;
    const newShipment = {
      id: `shipment_${shipmentCounter}`,
      arrivalNode: 'A1',  // Simplified to just "A1" (dock inferred from graph.json or MongoDB 'graph' collection)
      status: 'waiting',
      createdAt: new Date()
    };
    await shipmentsColl.insertOne(newShipment);
    console.log(`New shipment generated: ${newShipment.id} at ${newShipment.arrivalNode}`);
    await new Promise(resolve => setTimeout(resolve, 10000));  // Every 10 seconds (adjusted from 1 min for testing)
  }
}

runPortSimulation().catch(console.error);
