const { MongoClient } = require('mongodb');
const sensorData = require('./sensor.json');

async function migrateSensors() {
  const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('port');
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
