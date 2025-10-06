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
  keyPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),  // Your file name
  certPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, 'certs/AmazonRootCA1.pem'),
  clientId: 'port_simulator',
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com',
  offlineQueueMaxSize: 0
});

// Global cache for traffic suggestions and paths from MQTT
let suggestionsByEdge = {};

// Constants for ETA calculation
const NODE_BASE_DISTANCE = 100;
const EDGE_SPEEDS = {
  truck: 10,
  agv: 8,
  conveyor: 5,
  crane: 2,
  robot: 6,
  unknown: 5
};

// Enum for states
const TaskPhase = Object.freeze({
  IDLE: 'idle',
  ENROUTE_START: 'en_route_start',
  ASSIGNED: 'assigned',
  COMPLETING: 'completing'
});

// FIXED: Default idle attributes (null instead of 'Null' for better Mongo handling)
const IDLE_DEFAULTS = {
  task: 'idle',
  nextNode: null,
  finalNode: null,
  startNode: null,
  eta: 'N/A',
  journeyTime: 'N/A',
  shipmentId: null
};

// Transition functions
function transitionToIdle(currentEdge) {
  console.log(`Transitioning ${currentEdge.id} to IDLE; resetting attributes`);
  return { ...IDLE_DEFAULTS, taskPhase: TaskPhase.IDLE };
}

// FIXED: Relaxed check—allow if idle OR already enroute_start for this task (race tolerance)
function transitionToEnrouteStart(currentEdge, taskData) {
  // FIXED: Relax check—allow if idle OR already enroute_start for this task (race tolerance)
  if (!currentEdge || 
      (currentEdge.taskPhase !== TaskPhase.IDLE && 
       !(currentEdge.taskPhase === TaskPhase.ENROUTE_START && 
         currentEdge.task?.phase === taskData.phase && 
         currentEdge.shipmentId === taskData.shipmentId))) {
    throw new Error(`Can only assign task from IDLE (or edge is null). Current: ${currentEdge?.taskPhase}`);
  }

  console.log(`Transitioning ${currentEdge.id} to ENROUTE_START with task ${taskData.task}`);
  return {
    task: taskData.task,
    taskPhase: TaskPhase.ENROUTE_START,
    startNode: taskData.startNode || null,
    finalNode: taskData.finalNode || null,
    path: taskData.path || [],
    eta: 'N/A',
    journeyTime: 0,
    shipmentId: taskData.shipmentId || null
  };
}

function transitionToAssigned(currentEdge) {
  if (!currentEdge || currentEdge.taskPhase !== TaskPhase.ENROUTE_START) {  // FIXED: Null check
    throw new Error('Can only transition to ASSIGNED from ENROUTE_START');
  }

  console.log(`Transitioning ${currentEdge.id} to ASSIGNED (path empty, start reached)`);
  return { taskPhase: TaskPhase.ASSIGNED };
}

function transitionToCompleting(currentEdge) {
  if (!currentEdge || currentEdge.taskPhase !== TaskPhase.ASSIGNED) {  // FIXED: Null check
    throw new Error('Can only transition to COMPLETING from ASSIGNED');
  }

  console.log(`Transitioning ${currentEdge.id} to COMPLETING (path empty, destination reached); presetting nulls`);
  return {
    taskPhase: TaskPhase.COMPLETING,
    nextNode: null,
    finalNode: null,
    eta: 'N/A',
    journeyTime: 'N/A',
    path: [],
    shipmentId: currentEdge.shipmentId
  };
}

// Helper to update state and log changes
async function updateEdgeState(db, edgeId, updates) {
  const edgeBefore = await db.collection('edgeDevices').findOne({ id: edgeId });
  await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: updates });
  const edgeAfter = await db.collection('edgeDevices').findOne({ id: edgeId });

  if (edgeAfter && edgeAfter.task !== 'awaiting task') {  // Skip logging if awaiting
    if (edgeBefore && edgeBefore.task !== edgeAfter.task) {
      console.log(`Edge ${edgeId} task changed: ${edgeBefore.task} -> ${edgeAfter.task}`);
    }
    if (edgeBefore && edgeBefore.taskPhase !== edgeAfter.taskPhase) {
      console.log(`Edge ${edgeId} phase changed: ${edgeBefore.taskPhase} -> ${edgeAfter.taskPhase}`);
    }
    if (edgeBefore && edgeBefore.currentLocation !== edgeAfter.currentLocation) {
      console.log(`Edge ${edgeId} location changed: ${edgeBefore.currentLocation} -> ${edgeAfter.currentLocation}`);
    }
    if (edgeBefore && edgeBefore.shipmentId !== edgeAfter.shipmentId) {
      console.log(`Edge ${edgeId} shipment changed: ${edgeBefore.shipmentId} -> ${edgeAfter.shipmentId}`);
    }
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

// FIXED: Task execution with shipment updates + dynamic duration + more debug
async function executeTask(edgeId, db, device, edge, taskData, steps = 5) {  // Add edge, taskData, steps params
  if (!edge || !taskData) {
    console.error(`Missing edge/taskData for executeTask ${edgeId} - FORCING IDLE RESET`);
    await updateEdgeState(db, edgeId, transitionToIdle(edge || { id: edgeId }));
    return;
  }

  console.log(`[DEBUG EXECUTE] Starting task execution for ${edgeId} (phase: ${taskData.phase || 'unknown'}, steps: ${steps}, duration: ${steps * 2}s, location: ${edge.currentLocation}, shipment: ${edge.shipmentId})`);

  // FIXED: Set completing phase first (loop may watch this)
  await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: { taskPhase: TaskPhase.COMPLETING } });
  edge.taskPhase = TaskPhase.COMPLETING;

  // Dynamic loop: progress based on steps (e.g., 2s/step for offload/transport)
  for (let step = 1; step <= steps; step++) {
    await new Promise(resolve => setTimeout(resolve, 2000));  // 2s per step
    const remaining = steps - step;
    const progress = Math.floor((step / steps) * 100);

    // FIXED: Publish progress with phase/shipment (for analyzer/manager)
    device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({ 
      id: edgeId, 
      remaining, 
      progress, 
      phase: taskData.phase, 
      shipmentId: taskData.shipmentId || edge.shipmentId,
      currentLocation: edge.currentLocation 
    }));

    // Update DB location (simulate "processing" move, e.g., stay A1 for offload)
    const mockNode = taskData.phase === 'offload' ? edge.currentLocation : (step % 2 === 0 ? 'processing' : edge.currentLocation);
    await db.collection('edgeDevices').updateOne(
      { id: edgeId },
      { $set: { currentLocation: mockNode, updatedAt: new Date() } }
    );
    console.log(`[DEBUG EXECUTE] Edge ${edgeId} progress ${progress}% (step ${step}/${steps}): at ${mockNode}, remaining ${remaining}, shipment ${edge.shipmentId}`);
  }

  console.log(`[DEBUG EXECUTE] Task execution complete for ${edgeId} - updating shipment and resetting`);

  // FIXED: Handle Nulls before shipment update
  if (edge.shipmentId && edge.shipmentId !== null) {
    let newStatus = 'completed';
    let newCurrentNode = edge.currentLocation || 'A1';
    if (taskData.phase === 'offload') {
      newStatus = 'offloaded';
      newCurrentNode = edge.startNode || taskData.startNode || 'A1';  // Stay at dock
    } else if (taskData.phase === 'transport') {
      newStatus = 'transported';
      newCurrentNode = edge.finalNode || taskData.finalNode || 'B4';
    } else if (taskData.phase === 'store') {  // Or 'process'
      newStatus = 'stored';
      newCurrentNode = edge.finalNode || taskData.destNode || 'C5';
    }

    // FIXED: Update shipment (use phase, not edge.task which may be full obj)
    await db.collection('shipments').updateOne(
      { id: edge.shipmentId },
      { 
        $set: { 
          status: newStatus, 
          currentNode: newCurrentNode, 
          completedAt: new Date() 
        }
      }
    );

    const shipmentUpdate = {
      id: edge.shipmentId,
      status: newStatus,
      currentNode: newCurrentNode,
      assignedEdges: edge.assignedEdges || [],
      completedAt: new Date().toISOString()
    };

    // FIXED: Publish shipment update (triggers manager monitor for next phase)
    device.publish(`harboursense/shipments/${edge.shipmentId}`, JSON.stringify(shipmentUpdate));
    console.log(`[DEBUG EXECUTE] Updated linked shipment ${edge.shipmentId}: status ${newStatus} at ${newCurrentNode}`);
  } else {
    console.warn(`[DEBUG EXECUTE] No valid shipmentId for ${edgeId} completion (was '${edge.shipmentId}'); skipping update`);
  }

  // FIXED: Reset to idle (no Nulls) - always do this
  const updates = transitionToIdle(edge);
  updates.shipmentId = null;  // Clean, not 'Null'
  await updateEdgeState(db, edgeId, updates);
  console.log(`[DEBUG EXECUTE] Reset ${edgeId} to IDLE after completion`);

  // FIXED: Publish to /completion/ (matches manager sub: /edge/completion/+)
  const completionPayload = {
    id: edgeId,
    location: edge.currentLocation,
    phase: taskData.phase,  // Use phase, not task (precise)
    status: 'completed',
    shipmentId: edge.shipmentId,
    completedAt: new Date().toISOString()
  };
  device.publish(`harboursense/edge/completion/${edgeId}`, JSON.stringify(completionPayload));  // /completion/ not /completed
  console.log(`[DEBUG EXECUTE] Published completion for ${edgeId}: ${JSON.stringify(completionPayload)}`);

  // FIXED: Insert history
  if (edge) {
    await db.collection('edgeHistory').insertOne({ ...completionPayload, timestamp: new Date() });
  }

  console.log(`[DEBUG EXECUTE] Full execution done for ${edgeId} - now IDLE`);
}

// Simulate movement + more debug
async function simulateMovement(edgeId, db, device) {
  console.log(`[DEBUG SIM] Starting simulateMovement for ${edgeId}`);
  let edge = await db.collection('edgeDevices').findOne({ id: edgeId });

  if (!edge) {  // FIXED: Null check
    console.log(`[DEBUG SIM] No edge found for ${edgeId} - skip`);
    return;
  }

  console.log(`[DEBUG SIM] Edge ${edgeId} state: phase=${edge.taskPhase}, path=${JSON.stringify(edge.path)}, location=${edge.currentLocation}`);

  const suggestion = suggestionsByEdge[edgeId] || {};

  if (suggestion.suggestedPath && Array.isArray(suggestion.suggestedPath) && suggestion.suggestedPath.length > 0) {
    await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: { path: suggestion.suggestedPath } });
    console.log(`[DEBUG SIM] Overrode path for ${edgeId} with MQTT suggestion: ${suggestion.suggestedPath.join(' -> ')}`);
    edge.path = suggestion.suggestedPath;
  }

  if (!edge.path || edge.path.length === 0) {
    console.log(`[DEBUG SIM] No path available for ${edgeId}; checking phases for shift...`);
    return;
  }

  let currentLocation = edge.currentLocation;
  let nextNode = edge.path[0];

  if (currentLocation === nextNode) {
    await updateEdgeState(db, edgeId, {
      currentLocation: nextNode,
      nextNode: nextNode,
      taskPhase: TaskPhase.ASSIGNED
    });

    await db.collection('edgeDevices').updateOne(
      { id: edgeId },
      { $pull: { path: nextNode } }
    );

    console.log(`[DEBUG SIM] Already at ${nextNode}; discarding first path element and proceeding`);

    edge = await db.collection('edgeDevices').findOne({ id: edgeId });

    if (edge && edge.path.length === 0) {
      if (edge.currentLocation === edge.finalNode) {
        const updates = transitionToCompleting(edge);
        await updateEdgeState(db, edgeId, updates);
      } else if (edge.currentLocation === edge.startNode && edge.taskPhase === TaskPhase.ENROUTE_START) {
        const updates = transitionToAssigned(edge);
        await updateEdgeState(db, edgeId, updates);
      }

      device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify({
        remainingPath: [],
        currentLocation: nextNode,
        taskPhase: edge.taskPhase
      }));
      console.log(`[DEBUG SIM] Path was already empty/short - shifted phase for ${edgeId}`);
      return;
    }

    nextNode = edge.path[0];
  }

  const distance = nodeDistance(currentLocation, nextNode);
  const speed = EDGE_SPEEDS[edge.type] || EDGE_SPEEDS.unknown;
  const etaSeconds = Math.max(1, (distance * NODE_BASE_DISTANCE) / speed);

  const suggestedEta = suggestion.eta ? parseFloat(suggestion.eta) : etaSeconds;
  let journeyTime = (edge.journeyTime || 0) + suggestedEta;

  console.log(`[DEBUG SIM] Edge ${edgeId} starting movement: ${currentLocation} -> ${nextNode} (distance: ${distance}, speed: ${speed}, ETA: ${suggestedEta}s)`);

  const startTime = Date.now();
  const updateInterval = 1000;

  let intervalId = setInterval(() => {
    const elapsed = (Date.now() - startTime) / 1000;
    const progress = Math.min(100, Math.floor((elapsed / suggestedEta) * 100));
    const remaining = Math.max(0, Math.floor(suggestedEta - elapsed));

    device.publish(`harboursense/edge/${edgeId}/progress`, JSON.stringify({
      id: edgeId,
      progress: progress,
      remaining: remaining,
      toNode: nextNode,
      currentLocation: currentLocation
    }));

    console.log(`[DEBUG SIM] Edge ${edgeId} progress to ${nextNode}: ${progress}%, remaining: ${remaining}s`);

    if (progress >= 100) {
      clearInterval(intervalId);
    }
  }, updateInterval);

  await new Promise(resolve => setTimeout(resolve, suggestedEta * 1000));
  clearInterval(intervalId);

  const arrivedNode = nextNode;
  await updateEdgeState(db, edgeId, {
    currentLocation: arrivedNode,
    nextNode: arrivedNode,
    taskPhase: TaskPhase.ASSIGNED
  });

  await db.collection('edgeDevices').updateOne(
    { id: edgeId },
    { $pull: { path: arrivedNode } }
  );

  console.log(`[DEBUG SIM] Pulled ${arrivedNode} from path for ${edgeId}`);

  edge = await db.collection('edgeDevices').findOne({ id: edgeId });
  const remainingPath = edge ? edge.path : [];

  console.log(`[DEBUG SIM] After pull - remaining path for ${edgeId}: ${JSON.stringify(remainingPath)}, phase: ${edge.taskPhase}`);

  // FIXED: If path empty after arrival in ASSIGNED, trigger completing + executeTask
  if (remainingPath.length === 0 && edge) {
    if (edge.taskPhase === TaskPhase.ENROUTE_START) {
      const updates = transitionToAssigned(edge);
      await updateEdgeState(db, edgeId, updates);
    } else if (edge.taskPhase === TaskPhase.ASSIGNED) {
      const updates = transitionToCompleting(edge);
      await updateEdgeState(db, edgeId, updates);

      // FIXED: Trigger executeTask immediately if no further movement (e.g., offload done)
      const freshEdge = await db.collection('edgeDevices').findOne({ id: edgeId });
      const taskData = typeof freshEdge.task === 'string' ? { phase: freshEdge.task } : (freshEdge.task || { phase: 'unknown' });
      const steps = 1;  // Short for completion phase
      await executeTask(edgeId, db, device, freshEdge, taskData, steps);  // Direct call for quick phases

      console.log(`[DEBUG SIM] Path emptied after arrival at ${arrivedNode} for ${edgeId}; phase shifted and execution triggered.`);
      return;  // Skip further since executed
    }
  } else {
    console.log(`[DEBUG SIM] Path not empty after arrival at ${arrivedNode} for ${edgeId}; continuing loop.`);
  }

  const arrivalPayload = {
    remainingPath: remainingPath,
    currentLocation: arrivedNode,
    taskPhase: edge ? edge.taskPhase : 'unknown',
    finalNode: edge ? edge.finalNode : null,
    status: 'arrived',
    traveled: arrivedNode
  };

  device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify(arrivalPayload));
  if (edge) {
    await db.collection('edgeHistory').insertOne({ ...arrivalPayload, timestamp: new Date() });
  }

  console.log(`[DEBUG SIM] Edge ${edgeId} arrived at ${arrivedNode}, published remaining path (${remainingPath.length} nodes left)`);
}

async function edgeAutonomousLoop(edgeId, db, device) {
  let moveCount = 0;
  let pathEmptyCount = 0;
  const MAX_EMPTY_LOGS = 3;

  console.log(`[DEBUG LOOP] Starting autonomous loop for ${edgeId}`);

  while (true) {
    const edge = await db.collection('edgeDevices').findOne({ id: edgeId });

    if (!edge) {  // FIXED: Null check
      console.log(`[DEBUG LOOP] Edge ${edgeId} not found; skipping loop`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    const state = edge.taskPhase;
    const pathLen = edge.path ? edge.path.length : 0;
    const shipment = edge.shipmentId || 'null';

    // FIXED: Log EVERY iteration for debug - shows if stuck
    console.log(`[DEBUG LOOP] Edge ${edgeId} (type: ${edge.type}) - state: ${state}, path len: ${pathLen}, location: ${edge.currentLocation}, shipment: ${shipment}`);

    if (state === TaskPhase.IDLE) {
      console.log(`[DEBUG LOOP] Edge ${edgeId} idle - staying at ${edge.currentLocation}, awaiting task.`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    } else if (state === TaskPhase.ENROUTE_START || state === TaskPhase.ASSIGNED) {
      if (edge.path && edge.path.length > 0) {
        await simulateMovement(edgeId, db, device);
        moveCount++;
        pathEmptyCount = 0;

        if (moveCount > 100) {
         console.warn(`[DEBUG LOOP] Max moves reached for ${edgeId}; resetting to idle`);
         const updates = transitionToIdle(edge);
         await updateEdgeState(db, edgeId, updates);
         break;
        }
      } else {
        pathEmptyCount++;
        if (pathEmptyCount <= MAX_EMPTY_LOGS) {
         console.log(`[DEBUG LOOP] Path empty for ${edgeId} in ${state}; triggering phase shift...`);
        } else if (pathEmptyCount === MAX_EMPTY_LOGS + 1) {
         console.log(`[DEBUG LOOP] Further path empty logs for ${edgeId} suppressed.`);
        }

        // FIXED: Force shift and execute if empty in ASSIGNED (prevents stuck)
        if (state === TaskPhase.ENROUTE_START) {
          const updates = transitionToAssigned(edge);
          await updateEdgeState(db, edgeId, updates);
        } else if (state === TaskPhase.ASSIGNED) {
          console.log(`[DEBUG LOOP] Forcing COMPLETING + execute for ${edgeId} (empty path in ASSIGNED)`);
          const updates = transitionToCompleting(edge);
          await updateEdgeState(db, edgeId, updates);

          // FIXED: Direct executeTask here if not already called
          const freshEdge = await db.collection('edgeDevices').findOne({ id: edgeId });
          const taskData = typeof freshEdge.task === 'string' ? { phase: freshEdge.task } : (freshEdge.task || { phase: 'unknown' });
          const steps = freshEdge.path ? freshEdge.path.length : 3;  // Default 3 if no path
          await executeTask(edgeId, db, device, freshEdge, taskData, steps);

          device.publish(`harboursense/traffic/update/${edgeId}`, JSON.stringify({
            remainingPath: [],
            currentLocation: edge.currentLocation,
            taskPhase: state
          }));
        }
      }
    } else if (state === TaskPhase.COMPLETING) {
      console.log(`[DEBUG LOOP] Edge ${edgeId} entering COMPLETING - calling executeTask`);
      // FIXED: Fetch fresh edge/taskData for params
      const freshEdge = await db.collection('edgeDevices').findOne({ id: edgeId });
      if (!freshEdge || !freshEdge.task) {
        console.warn(`[DEBUG LOOP] No task for completing ${edgeId}; resetting to IDLE`);
        await updateEdgeState(db, edgeId, transitionToIdle(freshEdge || { id: edgeId }));
        continue;
      }
      const taskData = typeof freshEdge.task === 'string' ? { phase: freshEdge.task } : (freshEdge.task || { phase: 'unknown' });
      const steps = freshEdge.path ? freshEdge.path.length : 3;  // Dynamic: path len or default 3 for processing

      await executeTask(edgeId, db, device, freshEdge, taskData, steps);
      await new Promise(resolve => setTimeout(resolve, 2000));
    } else {
      console.warn(`[DEBUG LOOP] Unknown state ${state} for ${edgeId} - forcing IDLE`);
      await updateEdgeState(db, edgeId, transitionToIdle(edge));
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

async function runPortSimulation() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  // FIXED: Ensure edgeDevices exist with idle defaults
  const edgesCount = await db.collection('edgeDevices').countDocuments();
  if (edgesCount === 0) {
    console.log('No edgeDevices found; inserting defaults...');
    const defaultEdges = [
      { id: 'crane_1', type: 'crane', currentLocation: 'A1', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'crane_2', type: 'crane', currentLocation: 'A1', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_1', type: 'truck', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_2', type: 'truck', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'truck_3', type: 'truck', currentLocation: 'B4', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot_1', type: 'robot', currentLocation: 'C5', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot_2', type: 'robot', currentLocation: 'C5', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot_3', type: 'robot', currentLocation: 'C5', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS },
      { id: 'robot_4', type: 'robot', currentLocation: 'C5', taskPhase: TaskPhase.IDLE, ...IDLE_DEFAULTS }
    ];
    await db.collection('edgeDevices').insertMany(defaultEdges);
    console.log('Inserted 9 default edges (cranes/trucks/robots).');
  }

  device.on('connect', async () => {
    console.log('Connected to AWS IoT Core');
    const topics = [
      'harboursense/edge/+/task',
      'harboursense/traffic/+',
      'harboursense/shipments/+'
    ];

    topics.forEach(topic => {
      device.subscribe(topic);
      console.log(`Subscribed to ${topic}`);
    });
    console.log('Subscribed to edge tasks, traffic updates, and shipments');

    // Start edge loops
    const edges = await db.collection('edgeDevices').find().toArray();
    if (edges.length) {
      console.log(`Starting simulation for ${edges.length} edges...`);
      edges.forEach(edge => edgeAutonomousLoop(edge.id, db, device));
    } else {
      console.log('No edges found after insert check.');
    }
  });

  device.on('error', (err) => console.error('AWS IoT error:', err));

  device.on('message', async (topic, payload) => {
    try {
      console.log(`Message on topic: ${topic}`);

      const taskMatch = topic.match(/^harboursense\/edge\/([^/]+)\/task$/);
      const trafficMatch = topic.match(/^harboursense\/traffic\/([^/]+)$/);
      const shipmentMatch = topic.match(/^harboursense\/shipments\/(.+)$/);

      // FIXED: Task handler wrapped in try/catch for error handling
      if (taskMatch) {
        const edgeId = taskMatch[1];
        const taskData = JSON.parse(payload.toString());
        console.log(`Task received for edge ${edgeId}:`, taskData);

        // FIXED: Fetch FRESH edge from DB to sync with manager's update (avoids race/local stale)
        let edge = await db.collection('edgeDevices').findOne({ id: edgeId });
        if (!edge) {
          console.warn(`No edge found for ${edgeId} in DB; skipping task assignment.`);
          return;
        }

        console.log(`Fresh DB state for ${edgeId} on task: taskPhase='${edge.taskPhase}', shipmentId='${edge.shipmentId}', currentLocation='${edge.currentLocation}'`);

        // FIXED: If not idle, but task matches (e.g., phase in edge.task), accept as race; else reset
        if (edge.taskPhase !== TaskPhase.IDLE) {
          if (edge.task && edge.task.phase === taskData.phase && edge.shipmentId === taskData.shipmentId) {
            console.warn(`Edge ${edgeId} already in '${edge.taskPhase}' for this task (race); proceeding to execute`);
          } else {
            console.warn(`Edge ${edgeId} not idle (was '${edge.taskPhase}'); forcing reset for new task`);
            await db.collection('edgeDevices').updateOne(
              { id: edgeId },
              {
                $set: { 
                  taskPhase: TaskPhase.IDLE, 
                  task: 'idle', 
                  shipmentId: null, 
                  path: [], 
                  finalNode: null, 
                  startNode: null, 
                  updatedAt: new Date() 
                }
              }
            );
            edge = await db.collection('edgeDevices').findOne({ id: edgeId });  // Refetch
            console.log(`Reset ${edgeId} to idle: ${JSON.stringify({taskPhase: edge.taskPhase, shipmentId: edge.shipmentId})}`);
          }
        }

        // FIXED: Merge taskData into edge (override Nulls)
        const mergedTask = { ... (edge.task || {}), ...taskData };  // e.g., fix startNode='A1' if Null
        if (edge.startNode === 'Null' || edge.startNode === null) edge.startNode = taskData.startNode || 'A1';
        if (edge.shipmentId === 'Null' || edge.shipmentId === null) edge.shipmentId = taskData.shipmentId;

        // FIXED: Update DB with merged state (enroute_start)
        const updates = {
          taskPhase: TaskPhase.ENROUTE_START,
          task: mergedTask,
          shipmentId: edge.shipmentId,
          path: taskData.path || [],
          finalNode: taskData.finalNode,
          startNode: edge.startNode,
          nextNode: taskData.path?.[1] || taskData.destNode,
          currentLocation: taskData.startNode || edge.currentLocation,
          assignedShipment: taskData.shipmentId,  // Sync legacy field if used
          updatedAt: new Date()
        };
        await updateEdgeState(db, edgeId, updates);  // Uses your updateEdgeState (logs changes)

        console.log(`Edge ${edgeId} updated with new task and full path from manager: phase='${updates.taskPhase}', path=${updates.path?.join(' -> ')}, shipmentId='${updates.shipmentId}'`);

        // FIXED: No direct transition call—autonomous loop will detect 'enroute_start' + path and start simulateMovement
        // If path empty/short (e.g., offload at A1), loop shifts to completing → executeTask

      } else if (trafficMatch) {
        const edgeId = trafficMatch[1];
        const trafficData = JSON.parse(payload.toString());

        if (trafficData.path || trafficData.suggestedPath || trafficData.eta) {
          suggestionsByEdge[edgeId] = {
            ...suggestionsByEdge[edgeId],
            path: trafficData.path || trafficData.suggestedPath,
            eta: trafficData.eta,
            suggestedPath: trafficData.suggestedPath || trafficData.path
          };
          console.log(`Cached MQTT traffic data for ${edgeId}: path=${(trafficData.path || []).join(' -> ')}, eta=${trafficData.eta}`);

          if (trafficData.path && Array.isArray(trafficData.path)) {
            await db.collection('edgeDevices').updateOne({ id: edgeId }, { $set: { path: trafficData.path } });
            console.log(`Updated path for ${edgeId} from MQTT: ${trafficData.path.join(' -> ')}`);
          }
        }
      } else if (shipmentMatch) {
        const shipmentId = shipmentMatch[1];
        const shipmentData = JSON.parse(payload.toString());
        console.log(`Shipment update ${shipmentId}:`, shipmentData);

        await db.collection('shipments').updateOne(
          { id: shipmentId },
          {
            $set: {
              status: shipmentData.status || 'waiting',
              currentNode: shipmentData.currentNode || 'A1',
              assignedEdges: shipmentData.assignedEdges || []
            }
          },
          { upsert: true }
        );

        console.log(`Updated shipment ${shipmentId} to node ${shipmentData.currentNode}, status ${shipmentData.status}`);
        return;
      }
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  // FIXED: Publish to manager's expected topic
  // FIXED: Generate as 'arrived' (ready for crane offload at A1)
  async function generateShipmentsPeriodically(db) {
    const shipmentsColl = db.collection('shipments');
    let shipmentCounter = 0;

    while (true) {
      shipmentCounter++;
      const newShipment = {
        id: `shipment_${shipmentCounter}`,
        arrivalNode: 'A1',
        currentNode: 'A1',  // Start at dock
        status: 'arrived',  // NEW: Ready for immediate crane offload
        needsOffloading: false,  // DISABLED: Ignore this field
        destination: 'C5',
        assignedEdges: [],
        createdAt: new Date()
      };

      await shipmentsColl.insertOne(newShipment);

      // FIXED: Publish to per-ID topic (manager subscribes to shipments/+)
      device.publish(`harboursense/shipments/${newShipment.id}`, JSON.stringify(newShipment));

      console.log(`New arrived shipment: ${newShipment.id} at A1 (ready for offload)`);

      await new Promise(resolve => setTimeout(resolve, 30000));  // 30s interval
    }
  }

  generateShipmentsPeriodically(db);
}

runPortSimulation().catch(console.error);
