const { MongoClient } = require('mongodb');

async function migrateEdges() {
  const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('port');
    const edgeDevicesCol = db.collection('edgeDevices');
    const edges = require('./test-edge.json');  // Load devices from edge.json

    // Delete existing documents (optional - comment out if you don't want to wipe data)
    const deleteResult = await edgeDevicesCol.deleteMany({});
    console.log(`Deleted ${deleteResult.deletedCount} existing documents.`);

    // Initialize edges with additional required fields
    const updatedEdges = edges.map(edge => ({
      ...edge,
      finalNode: edge.finalNode || edge.nextNode || edge.currentLocation || '',
      currentLocation: edge.currentLocation || edge.nextNode || '',
      task: edge.task || 'idle',
      eta: edge.eta || 10,
      taskCompletionTime: edge.taskCompletionTime || 0,
      journeyTime: edge.journeyTime || 0
    }));

    // Insert the updated array
    const insertResult = await edgeDevicesCol.insertMany(updatedEdges);
    console.log(`Inserted ${insertResult.insertedCount} edge devices. IDs:`, insertResult.insertedIds);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

migrateEdges();
