const { MongoClient } = require('mongodb');

const graphSeed = require('./test-graph.json');
const edgeDeviceSeed = require('./test-edge.json');
const sensorSeed = require('./sensor.json');
const { getMongoSettings } = require('../runtime-config');

const mongoSettings = getMongoSettings();

function requireSeedArray(name, data) {
  if (!Array.isArray(data) || data.length === 0) {
    throw new Error(`${name} seed must be a non-empty JSON array`);
  }
}

function deduplicateById(name, documents) {
  const byId = new Map();
  for (const doc of documents) {
    if (!doc.id) {
      throw new Error(`${name} contains a document without an id`);
    }
    byId.set(doc.id, doc);
  }

  if (byId.size !== documents.length) {
    console.warn(`${name}: ${documents.length - byId.size} duplicate id rows collapsed; last value wins`);
  }

  return [...byId.values()];
}

function graphDocument(node) {
  const next = { ...node };
  if (next.type === 'warehouse' && next.currentOccupancy === undefined) {
    next.currentOccupancy = 0;
  }
  return next;
}

function edgeDocument(edge) {
  return {
    task: 'idle',
    nextNode: null,
    finalNode: null,
    startNode: null,
    path: [],
    remainingPath: [],
    taskPhase: 'idle',
    eta: null,
    taskCompletionTime: 0,
    journeyTime: null,
    shipmentId: null,
    assignedShipment: null,
    updatedAt: new Date().toISOString(),
    ...edge
  };
}

function parseRetentionDays(envKey, defaultDays) {
  const raw = (process.env[envKey] || '').trim();
  if (!raw) {
    return defaultDays;
  }
  const days = Number.parseInt(raw, 10);
  if (!Number.isFinite(days) || days <= 0) {
    throw new Error(`${envKey} must be a positive integer (days), or omit to use default ${defaultDays}.`);
  }
  return days;
}

function daysToExpireAfterSeconds(days) {
  return days * 24 * 60 * 60;
}

async function ensureTtlIndex(collection, { field = 'timestamp', envKey, defaultDays, label }) {
  const days = parseRetentionDays(envKey, defaultDays);
  const expireAfterSeconds = daysToExpireAfterSeconds(days);
  const indexName = `${label}_${field}_ttl`;

  await collection.createIndex(
    { [field]: 1 },
    { expireAfterSeconds, name: indexName }
  );
  console.log(`${label}: TTL index on ${field} (${days} days, tunable via ${envKey})`);
}

async function upsertById(collection, documents) {
  const result = await collection.bulkWrite(
    documents.map((doc) => ({
      replaceOne: {
        filter: { id: doc.id },
        replacement: doc,
        upsert: true
      }
    })),
    { ordered: true }
  );

  return {
    matched: result.matchedCount,
    modified: result.modifiedCount,
    upserted: result.upsertedCount
  };
}

async function seedMongo() {
  requireSeedArray('test-graph.json', graphSeed);
  requireSeedArray('test-edge.json', edgeDeviceSeed);
  requireSeedArray('sensor.json', sensorSeed);

  const client = new MongoClient(mongoSettings.uri, {
    serverSelectionTimeoutMS: mongoSettings.serverSelectionTimeoutMS,
  });
  await client.connect();

  try {
    const db = client.db(mongoSettings.databaseName);
    console.log(`Seeding MongoDB database '${mongoSettings.databaseName}' from committed local fixtures`);

    const graphDocuments = deduplicateById('test-graph.json', graphSeed.map(graphDocument));
    const edgeDocuments = deduplicateById('test-edge.json', edgeDeviceSeed.map(edgeDocument));
    const sensorDocuments = deduplicateById('sensor.json', sensorSeed);

    const graphCollection = db.collection('graph');
    const graphIds = graphDocuments.map((doc) => doc.id);
    const graphPrune = await graphCollection.deleteMany({ id: { $nin: graphIds } });
    const graphResult = await upsertById(graphCollection, graphDocuments);
    const edgeResult = await upsertById(db.collection('edgeDevices'), edgeDocuments);
    const sensorResult = await upsertById(db.collection('sensorList'), sensorDocuments);

    const resetWorkflow = (process.env.SEED_RESET_WORKFLOW || '').trim().toLowerCase() === 'true';
    if (resetWorkflow) {
      const shipmentReset = await db.collection('shipments').deleteMany({});
      const historyReset = await db.collection('edgeHistory').deleteMany({});
      const occupancyReset = await db.collection('graph').updateMany(
        { type: 'warehouse' },
        { $set: { currentOccupancy: 0 } }
      );
      console.log(
        `workflow reset: cleared ${shipmentReset.deletedCount} shipments, ${historyReset.deletedCount} edgeHistory rows, zeroed ${occupancyReset.modifiedCount} warehouse occupancy fields`
      );
    }

    console.log(`graph: ${graphDocuments.length} docs from ${graphSeed.length} rows (pruned ${graphPrune.deletedCount}, ${JSON.stringify(graphResult)})`);
    console.log(`edgeDevices: ${edgeDocuments.length} docs from ${edgeDeviceSeed.length} rows (${JSON.stringify(edgeResult)})`);
    console.log(`sensorList: ${sensorDocuments.length} docs from ${sensorSeed.length} rows (${JSON.stringify(sensorResult)})`);

    await db.collection('shipments').createIndex({ status: 1 });
    await db.collection('shipments').createIndex({ updatedAt: -1 });
    await db.collection('sensorAlerts').createIndex({ resolved: 1, timestamp: -1 });
    await db.collection('maintenanceAlerts').createIndex({ resolved: 1, timestamp: -1 });
    console.log('indexes ensured for shipments.status, shipments.updatedAt, sensorAlerts.resolved, maintenanceAlerts.resolved');

    await ensureTtlIndex(db.collection('sensorData'), {
      envKey: 'MONGO_TTL_SENSOR_DATA_DAYS',
      defaultDays: 7,
      label: 'sensorData',
    });
    await ensureTtlIndex(db.collection('trafficData'), {
      envKey: 'MONGO_TTL_TRAFFIC_DATA_DAYS',
      defaultDays: 14,
      label: 'trafficData',
    });
    await ensureTtlIndex(db.collection('edgeHistory'), {
      envKey: 'MONGO_TTL_EDGE_HISTORY_DAYS',
      defaultDays: 30,
      label: 'edgeHistory',
    });

    const collections = await db.listCollections({}, { nameOnly: true }).toArray();
    const collectionNames = collections.map((collection) => collection.name);
    console.log(`Collections now present: ${collectionNames.sort().join(', ')}`);
  } finally {
    await client.close();
  }
}

seedMongo().catch((error) => {
  console.error('Mongo seed failed:', error.message);
  process.exit(1);
});
