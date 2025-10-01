// migrate-graph.js - Migrates graph.json to MongoDB 'port' database, 'graph' collection

const { MongoClient } = require('mongodb');
const graphData = require('./graph.json');  // Assumes graph.json is in the same directory

// MongoDB connection URI (replace with your actual URI if needed)
const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port?retryWrites=true&w=majority';

async function migrateGraph() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('port');
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
