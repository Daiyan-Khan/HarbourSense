const { MongoClient } = require('mongodb');
const { edgeAutonomousLoop } = require('./edge.js');  // Import the module above

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';  // Your URI

async function runPortSimulation() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  // Fetch all edges
  const edges = await db.collection('edges').find().toArray();
  if (edges.length === 0) {
    console.log('No edges found.');
    await client.close();
    return;
  }

  console.log(`Starting simulation for ${edges.length} edges...`);

  // Run autonomous loops concurrently
  const promises = edges.map(edge => edgeAutonomousLoop(edge.id));
  await Promise.all(promises);  // Waits forever unless loops break

  await client.close();
}

runPortSimulation().catch(console.error);
