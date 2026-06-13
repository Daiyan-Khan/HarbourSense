// migrate-graph.js - Migrates graph.json to MongoDB 'port' database, 'graph' collection

const { MongoClient } = require('mongodb');
const graphData = require('./test-graph.json');  // Assumes graph.json is in the same directory
const { getMongoSettings } = require('../runtime-config');

async function migrateGraph() {
  const mongoSettings = getMongoSettings();
  const client = new MongoClient(mongoSettings.uri, {
    serverSelectionTimeoutMS: mongoSettings.serverSelectionTimeoutMS,
  });
  try {
    await client.connect();
    const db = client.db(mongoSettings.databaseName);
    const collection = db.collection('graph');
    
    // Delete all existing documents in the 'graph' collection
    const deleteResult = await collection.deleteMany({});
    console.log(`Deleted ${deleteResult.deletedCount} existing documents.`);
    
    // Insert the data from graph.json
    const insertResult = await collection.insertMany(graphData);
    console.log(`Inserted ${insertResult.insertedCount} new documents into 'graph' collection.`);
  } catch (error) {
    console.error('Error during migration:', error);
  } finally {
    await client.close();
  }
}

// Run the migration
migrateGraph().catch(console.error);
