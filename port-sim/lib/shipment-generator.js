const { pickShipmentIntervalMs } = require('../runtime-config');
const { shipmentTopic } = require('./mqtt-contract');
const { debugLog } = require('./sim-debug');

const DOCK_QUEUE_STATUSES = ['arrived', 'waiting'];

async function countDockQueue(shipmentsColl, arrivalNode) {
  return shipmentsColl.countDocuments({
    arrivalNode,
    status: { $in: DOCK_QUEUE_STATUSES },
  });
}

async function generateShipmentsPeriodically(db, docks, warehouses, device, simulatorSettings, options = {}) {
  const { maxIterations } = options;
  if (!simulatorSettings.shipmentGenerationEnabled) {
    console.log('Shipment generation disabled (SHIPMENT_GENERATION_ENABLED=false)');
    return;
  }

  const shipmentsColl = db.collection('shipments');
  let shipmentCounter = await shipmentsColl.countDocuments();
  const maxPerDock = simulatorSettings.maxArrivalsPerDock;

  console.log(
    `Starting shipment generator: docks=${docks.join(', ')} warehouses=${warehouses.join(', ')} maxPerDock=${maxPerDock}`,
  );

  let scheduledNextAt = null;
  let iterations = 0;

  while (true) {
    iterations += 1;
    let intervalMs = pickShipmentIntervalMs(simulatorSettings.shipmentIntervalMsList);
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      intervalMs = 60_000;
    }
    scheduledNextAt = new Date(Date.now() + intervalMs).toISOString();

    const randomDock = docks[Math.floor(Math.random() * docks.length)];
    const randomWarehouse = warehouses[Math.floor(Math.random() * warehouses.length)];
    const dockCount = await countDockQueue(shipmentsColl, randomDock);

    if (dockCount >= maxPerDock) {
      debugLog(
        `Dock ${randomDock} at cap (${dockCount}/${maxPerDock}); deferring generation until next interval`,
      );
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
      if (maxIterations && iterations >= maxIterations) {
        break;
      }
      continue;
    }

    shipmentCounter += 1;
    const queuePosition = dockCount > 0 ? dockCount + 1 : undefined;

    const newShipment = {
      id: `shipment_${shipmentCounter}`,
      arrivalNode: randomDock,
      currentNode: randomDock,
      status: 'arrived',
      destination: randomWarehouse,
      assignedEdges: [],
      createdAt: new Date().toISOString(),
      scheduledNextAt,
    };
    if (queuePosition !== undefined) {
      newShipment.queuePosition = queuePosition;
    }

    await shipmentsColl.insertOne(newShipment);
    device.publish(shipmentTopic(newShipment.id), JSON.stringify(newShipment));

    console.log(
      `New shipment ${newShipment.id} at ${randomDock} (dest ${randomWarehouse}, next ${scheduledNextAt})`,
    );

    await new Promise((resolve) => setTimeout(resolve, intervalMs));

    if (maxIterations && iterations >= maxIterations) {
      break;
    }
  }
}

module.exports = {
  DOCK_QUEUE_STATUSES,
  countDockQueue,
  generateShipmentsPeriodically,
};
