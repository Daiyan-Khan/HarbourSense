const { MongoClient } = require('mongodb');
const sensorData = require('./sensor.json');
const { getMongoSettings } = require('../runtime-config');

async function migrateSensors() {
  const mongoSettings = getMongoSettings();
  const client = new MongoClient(mongoSettings.uri, {
    serverSelectionTimeoutMS: mongoSettings.serverSelectionTimeoutMS,
  });

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(mongoSettings.databaseName);
    const sensorListCol = db.collection('sensorList');

    // Delete existing documents (optional)
    const deleteResult = await sensorListCol.deleteMany({});
    console.log(`Deleted ${deleteResult.deletedCount} existing documents.`);

    // Insert sensor data
    const insertResult = await sensorListCol.insertMany(sensorData);
    console.log(`Inserted ${insertResult.insertedCount} sensors. IDs:`, insertResult.insertedIds);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

migrateSensors();
