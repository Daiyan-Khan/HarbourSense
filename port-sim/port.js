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
  crane: 4,
  robot: 6,
  unknown: 5
};


// Enum for states
const TaskPhase = Object.freeze({
  IDLE: 'idle',
  EN_ROUTE_START: 'en_route_start',
  ASSIGNED: 'assigned',
  COMPLETING: 'completing'
});


// FIXED: Default idle attributes (null instead of 'Null' for better Mongo handling)
const IDLE_DEFAULTS = {
  task: 'idle',          // String, not object
  taskPhase: 'idle',     // Explicit phase (standardize casing)
  path: [],              // NEW: Clear stale paths (len=0)
  remainingPath: [],     // NEW: If tracked separately in sim
  nextNode: 'Null',
  finalNode: 'Null',
  startNode: 'Null',
  eta: 'N/A',
  journeyTime: 'N/A',
  shipmentId: "Null",
  assignedShipment: "Null", // NEW: Break shipment links
  taskCompletionTime: 0, // NEW: Reset sim timers
  priority: null,        // NEW: If set per-task
  updatedAt: new Date().toISOString()  // NEW: Fresh timestamp for DB
  // Note: currentLocation stays (edge at final spot); speed/roles persistent
};


// Transition functions
function transitionToIdle(currentEdge) {
  console.log(`Transitioning ${currentEdge.id} to IDLE; resetting attributes`);
  return { ...IDLE_DEFAULTS, taskPhase: TaskPhase.IDLE };
}


// FIXED: Relaxed check—allow if idle OR already en_route_start for this task (race tolerance)
function transitionToEn_routeStart(currentEdge, taskData) {
  // FIXED: Relax check—allow if idle OR already en_route_start for this task (race tolerance)
  if (!currentEdge || 
      (currentEdge.taskPhase !== TaskPhase.IDLE && 
       !(currentEdge.taskPhase === TaskPhase.EN_ROUTE_START && 
         currentEdge.task?.phase === taskData.phase && 
         currentEdge.shipmentId === taskData.shipmentId))) {
    throw new Error(`Can only assign task from IDLE (or edge is null). Current: ${currentEdge?.taskPhase}`);
  }


  console.log(`Transitioning ${currentEdge.id} to EN_ROUTE_START with task ${taskData.task}`);
  return {
    task: taskData.task,
    taskPhase: TaskPhase.EN_ROUTE_START,
    startNode: taskData.startNode || null,
    finalNode: taskData.finalNode || null,
    path: taskData.path || [],
    eta: 'N/A',
    journeyTime: 0,
    shipmentId: taskData.shipmentId || null
  };
}


function transitionToAssigned(currentEdge) {
  if (!currentEdge || currentEdge.taskPhase !== TaskPhase.EN_ROUTE_START) {  // FIXED: Null check
    throw new Error('Can only transition to ASSIGNED from EN_ROUTE_START');
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

  // FIXED: Stringify objects for readable logs; skip if task is string ('idle')
  
  if (edgeAfter && edgeAfter.task !== 'awaiting task') {  // Skip logging if awaiting
    if (edgeBefore && edgeAfter && edgeBefore.task !== edgeAfter.task) {
    const oldTaskStr = typeof edgeBefore.task === 'string' ? edgeBefore.task : JSON.stringify({phase: edgeBefore.task?.phase, shipmentId: edgeBefore.task?.shipmentId});
    const newTaskStr = typeof edgeAfter.task === 'string' ? edgeAfter.task : JSON.stringify({phase: edgeAfter.task?.phase, shipmentId: edgeAfter.task?.shipmentId});
    console.log(`Edge ${edgeId} task changed: ${oldTaskStr} -> ${newTaskStr}`);
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

// Helper: Select nearest warehouse from dock (simple; extend with DB if needed)
async function selectNearestWarehouse(dockNode, db) {
  // Hardcoded warehouses; query DB for dynamic (e.g., B4, D2, E5)
  const warehouses = ['B4', 'D2', 'E5'];
  let nearest = 'B4';
  let minDist = Infinity;
  for (const wh of warehouses) {
    const dist = nodeDistance(dockNode, wh);
    if (dist < minDist) {
      minDist = dist;
      nearest = wh;
    }
  }
  return nearest;
}

// Helper: Check and assign transport if idle truck available (simulates backend trigger)
async function checkAndAssignTransport(shipmentId, dockNode, db, device) {
  console.log(`[CHAIN] Checking transport for pending ${shipmentId} from ${dockNode}`);
  const idleTrucks = await db.collection('edgeDevices').find({ type: 'truck', taskPhase: TaskPhase.IDLE }).toArray();
  if (idleTrucks.length > 0) {
    const truck = idleTrucks[0];  // Nearest (add sort by distance if needed)
    const warehouse = await selectNearestWarehouse(dockNode, db);
    const transportTask = {
      shipmentId: shipmentId,
      phase: 'transport',
      startNode: dockNode,
      finalNode: warehouse,
      requiredPlace: dockNode,  // Pickup at dock
      path: [dockNode, 'B1', warehouse]  // Example path; use analyzer if integrated
    };
    // Publish task to truck (mirrors backend)
    device.publish(`harboursense/edge/${truck.id}/task`, JSON.stringify(transportTask));
    console.log(`[CHAIN] Assigned transport to ${truck.id} for ${shipmentId} to ${warehouse}`);
  } else {
    console.log(`[CHAIN] No idle truck for ${shipmentId}; will retry on next monitor cycle`);
  }
}

// Helper: Assign store forklift/robot to warehouse if idle (pre-position during transport)
async function assignStoreForklift(shipmentId, warehouse, db, device) {
  console.log(`[CHAIN] Assigning store for ${shipmentId} to ${warehouse}`);
  const idleRobots = await db.collection('edgeDevices').find({ type: 'robot', taskPhase: TaskPhase.IDLE }).toArray();
  if (idleRobots.length > 0) {
    const robot = idleRobots[0];  // Nearest to warehouse
    const storeTask = {
      shipmentId: shipmentId,
      phase: 'store',
      startNode: warehouse,
      finalNode: warehouse,  // Stationary at warehouse
      requiredPlace: warehouse,
      pickupNode: warehouse,
      path: [warehouse]  // Already/direct
    };
    // Publish task to robot
    device.publish(`harboursense/edge/${robot.id}/task`, JSON.stringify(storeTask));
    console.log(`[CHAIN] Assigned store to ${robot.id} for ${shipmentId} at ${warehouse}`);
  } else {
    console.log(`[CHAIN] No idle robot for ${shipmentId}; will retry`);
  }
}

// FIXED: Task execution with shipment updates + dynamic duration + more debug + chaining
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


  console.log(`[DEBUG EXECUTE] Task execution complete for ${edgeId} - chaining status update`);


  // FIXED: Call chaining function at end (updates shipment, publishes, chains next)
  await completeTaskAndChain(edgeId, taskData, device, db, edge);


  console.log(`[DEBUG EXECUTE] Full execution and chain done for ${edgeId}`);
}

// FIXED: New function for completion chaining and status updates
async function completeTaskAndChain(edgeId, taskData, device, db, edge) {
  const { shipmentId, phase, finalNode } = taskData;
  if (!shipmentId) {
    console.warn(`[CHAIN] No shipmentId for ${edgeId} completion; skipping chain`);
    return;
  }

  // Update edge to idle/completed
  edge.taskPhase = TaskPhase.IDLE;
  edge.shipmentId = null;
  edge.assignedShipment = null;
  edge.currentLocation = finalNode || edge.currentLocation;
  await updateEdgeInDB(edge, db);  // Use existing update or db.collection('edgeDevices').updateOne

  // Publish completion
  const completionPayload = {
    id: edgeId,
    location: finalNode || edge.currentLocation,
    phase: phase,
    status: 'completed',
    shipmentId: shipmentId,
    completedAt: new Date().toISOString()
  };
  device.publish(`harboursense/edge/${edgeId}/completion`, JSON.stringify(completionPayload));
  console.log(`[CHAIN] Published completion for ${edgeId}: ${JSON.stringify(completionPayload)}`);

  // Update shipment status and chain next phase
  let newStatus;
  let newCurrentNode = finalNode || edge.currentLocation;
  switch (phase) {
    case 'offload':
      newStatus = 'offloaded';
      await db.collection('shipments').updateOne(
        { id: shipmentId },
        { 
          $set: { 
            status: newStatus, 
            currentNode: newCurrentNode, 
            updatedAt: new Date() 
          },
          $addToSet: { assignedEdges: { device: edge.type, phase: phase } }
        }
      );
      console.log(`[CHAIN] Offload complete for ${shipmentId}; status → ${newStatus} at ${newCurrentNode}`);
      // Concurrent: Check/assign transport if truck idle
      setTimeout(() => checkAndAssignTransport(shipmentId, newCurrentNode, db, device), 1000);
      break;
    case 'transport':
      newStatus = 'transported';
      newCurrentNode = taskData.finalNode || 'B4';
      await db.collection('shipments').updateOne(
        { id: shipmentId },
        { 
          $set: { 
            status: newStatus, 
            currentNode: newCurrentNode, 
            updatedAt: new Date() 
          },
          $addToSet: { assignedEdges: { device: edge.type, phase: phase } }
        }
      );
      console.log(`[CHAIN] Transport complete for ${shipmentId} to ${newCurrentNode}; status → ${newStatus}`);
      // Concurrent: Assign store (forklift) to warehouse
      setTimeout(() => assignStoreForklift(shipmentId, newCurrentNode, db, device), 500);
      break;
    case 'store':
      newStatus = 'stored';
      await db.collection('shipments').updateOne(
        { id: shipmentId },
        { 
          $set: { 
            status: newStatus, 
            updatedAt: new Date() 
          },
          $addToSet: { assignedEdges: { device: edge.type, phase: phase } }
        }
      );
      console.log(`[CHAIN] Store complete for ${shipmentId}; status → ${newStatus}`);
      break;
    default:
      newStatus = 'completed';
  }

  // Publish shipment update
  const shipmentUpdate = { 
    id: shipmentId, 
    status: newStatus, 
    currentNode: newCurrentNode, 
    completedAt: new Date().toISOString() 
  };
  device.publish(`harboursense/shipments/${shipmentId}`, JSON.stringify(shipmentUpdate));
  console.log(`[CHAIN] Published shipment update for ${shipmentId}: ${newStatus}`);
}

// Helper: Update edge in DB (inline for simplicity; expand if needed)
async function updateEdgeInDB(edge, db) {
  await db.collection('edgeDevices').updateOne(
    { id: edge.id },
    { 
      $set: { 
        taskPhase: edge.taskPhase,
        shipmentId: edge.shipmentId,
        assignedShipment: edge.assignedShipment,
        currentLocation: edge.currentLocation,
        updatedAt: new Date()
      }
    }
  );
  console.log(`[CHAIN] Updated edge ${edge.id} in DB: phase=${edge.taskPhase}, location=${edge.currentLocation}`);
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
      } else if (edge.currentLocation === edge.startNode && edge.taskPhase === TaskPhase.EN_ROUTE_START) {
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
    if (edge.taskPhase === TaskPhase.EN_ROUTE_START) {
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
    } else if (state === TaskPhase.EN_ROUTE_START || state === TaskPhase.ASSIGNED) {
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
        if (state === TaskPhase.EN_ROUTE_START) {
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

  // FIXED: Ensure edgeDevices exist with idle defaults (as provided)
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

  // NEW: Load port.graph for dynamic docks/warehouses
  const graphNodes = await db.collection('graph').find({}).toArray();
  const docks = graphNodes.filter(node => node.type === 'dock').map(node => node.id);  // e.g., ['A1', 'A2']
  const warehouses = ['B4', 'D2', 'E5'];  // From logs; or filter graph type: 'warehouse'
  if (docks.length < 1) {
    console.warn('No dock nodes in graph; falling back to A1');
    docks.push('A1');
  }
  if (warehouses.length < 1) {
    console.warn('No warehouses; falling back to C5');
    warehouses.push('C5');
  }
  console.log(`Loaded graph: ${docks.length} docks (${docks.join(', ')}), ${warehouses.length} warehouses (${warehouses.join(', ')})`);

  // Pass to generator
  generateShipmentsPeriodically(db, docks, warehouses);

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

      // FIXED: Task handler wrapped in try/catch for error handling (as provided)
      if (taskMatch) {
        const edgeId = taskMatch[1];
        const taskData = JSON.parse(payload.toString());
        console.log(`Task received for edge ${edgeId}:`, taskData);

        // FIXED: Fetch FRESH edge from DB to sync with manager's update (avoids race/local stale) (as provided)
        let edge = await db.collection('edgeDevices').findOne({ id: edgeId });
        if (!edge) {
          console.warn(`No edge found for ${edgeId} in DB; skipping task assignment.`);
          return;
        }

        console.log(`Fresh DB state for ${edgeId} on task: taskPhase='${edge.taskPhase}', shipmentId='${edge.shipmentId}', currentLocation='${edge.currentLocation}'`);

        // FIXED: If not idle, but task matches (e.g., phase in edge.task), accept as race; else reset (as provided)
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

        // FIXED: Merge taskData into edge (override Nulls) (as provided)
        const mergedTask = { ... (edge.task || {}), ...taskData };  // e.g., fix startNode='A1' if Null
        if (edge.startNode === 'Null' || edge.startNode === null) edge.startNode = taskData.startNode || 'A1';
        if (edge.shipmentId === 'Null' || edge.shipmentId === null) edge.shipmentId = taskData.shipmentId;

        // FIXED: Update DB with merged state (en-route_start) (as provided)
        const updates = {
          taskPhase: TaskPhase.EN_ROUTE_START,
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

        // FIXED: No direct transition call—autonomous loop will detect 'en_route_start' + path and start simulateMovement
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

  // FIXED: Publish to manager's expected topic (as provided, but moved generate call up)
}

// UPDATED: Dynamic shipment generator using graph + random interval
async function generateShipmentsPeriodically(db, docks, warehouses) {
  const shipmentsColl = db.collection('shipments');
  let shipmentCounter = await shipmentsColl.countDocuments();  // Resume counter from DB

  console.log(`Starting dynamic shipment gen: docks=${docks.join(', ')}, warehouses=${warehouses.join(', ')}`);

  while (true) {
    shipmentCounter++;
    const randomDock = docks[Math.floor(Math.random() * docks.length)];  // e.g., 'A1' or 'A2'
    const randomWarehouse = warehouses[Math.floor(Math.random() * warehouses.length)];  // e.g., 'B4'

    const newShipment = {
      id: `shipment_${shipmentCounter}`,
      arrivalNode: randomDock,
      currentNode: randomDock,  // Start at random dock
      status: 'arrived',  // Ready for immediate crane offload at dock
      destination: randomWarehouse,  // Random storage target
      assignedEdges: [],
      createdAt: new Date()
    };

    await shipmentsColl.insertOne(newShipment);

    // FIXED: Publish to per-ID topic (manager subscribes to shipments/+)
    device.publish(`harboursense/shipments/${newShipment.id}`, JSON.stringify(newShipment));

    console.log(`New arrived shipment: ${newShipment.id} at ${randomDock} (dest: ${randomWarehouse}, ready for offload)`);

    // NEW: Random interval from [30s, 60s, 90s]
    const intervals = [30000, 60000, 90000];  // ms
    const randomInterval = intervals[Math.floor(Math.random() * intervals.length)];
    console.log(`Next shipment in ${randomInterval / 1000}s...`);

    await new Promise(resolve => setTimeout(resolve, randomInterval));
  }
}


runPortSimulation().catch(console.error);
