const { MongoClient } = require('mongodb');

async function migrateEdges() {
  const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('port');
    const edgeDevicesCol = db.collection('edgeDevices');
    const edges = require('./edge.json');  // Load devices from edge.json
    // Delete existing documents (optional)
    const deleteResult = await edgeDevicesCol.deleteMany({});
    console.log(`Deleted ${deleteResult.deletedCount} existing documents.`);

    // Your JSON array (paste the full list here)


    // Clear existing data if you want a fresh start (optional)
    // await edgeDevicesCol.deleteMany({});

    // Insert the array
    const insertResult = await edgeDevicesCol.insertMany(edges);
    console.log(`Inserted ${insertResult.insertedCount} edge devices. IDs:`, insertResult.insertedIds);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

migrateEdges();
