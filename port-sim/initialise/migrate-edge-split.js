const { MongoClient } = require('mongodb');
const { getMongoSettings } = require('../runtime-config');
const { splitEdgeDocument } = require('../lib/edge-collections');

const mongoSettings = getMongoSettings();

async function migrateEdgeSplit(db) {
  const legacy = await db.collection('edgeDevices').find({}).toArray();
  if (legacy.length === 0) {
    console.log('edge split migration: no edgeDevices documents to migrate');
    return { migrated: 0 };
  }

  const runtimeCol = db.collection('edgeRuntime');
  const assignmentCol = db.collection('edgeAssignments');
  let migrated = 0;

  for (const doc of legacy) {
    const { assignment, runtime } = splitEdgeDocument(doc);
    await runtimeCol.replaceOne({ id: runtime.id }, runtime, { upsert: true });
    await assignmentCol.replaceOne({ id: assignment.id }, assignment, { upsert: true });
    migrated += 1;
  }

  console.log(`edge split migration: migrated ${migrated} edgeDevices -> edgeRuntime + edgeAssignments`);
  return { migrated };
}

async function run() {
  const client = new MongoClient(mongoSettings.uri, {
    serverSelectionTimeoutMS: mongoSettings.serverSelectionTimeoutMS,
  });
  await client.connect();
  try {
    const db = client.db(mongoSettings.databaseName);
    await migrateEdgeSplit(db);
  } finally {
    await client.close();
  }
}

if (require.main === module) {
  run().catch((error) => {
    console.error('migrate-edge-split failed:', error.message);
    process.exit(1);
  });
}

module.exports = { migrateEdgeSplit };
