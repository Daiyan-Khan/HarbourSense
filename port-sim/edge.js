const { MongoClient } = require('mongodb');

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net';  // Your URI

async function edgeAutonomousLoop(edgeId) {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('port');

  while (true) {
    // Fetch current state (from manager/traffic updates)
    const edge = await db.collection('edges').findOne({ id: edgeId });
    if (!edge) {
      console.log(`Edge ${edgeId} not found, stopping.`);
      break;
    }

    // If no valid route (e.g., idle or no nextNode), wait and recheck
    if (!edge.nextNode || !edge.eta || edge.task === 'idle') {
      console.log(`Edge ${edgeId} idle or no route, checking again in 5s...`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    // Simulate travel: Wait based on ETA (sped up for demo; adjust divisor)
    const etaMs = edge.eta * 3600 * 1000;  // Hours to ms
    console.log(`Edge ${edgeId} traveling to ${edge.nextNode} for ${edge.eta} hours...`);
    await new Promise(resolve => setTimeout(resolve, etaMs / 3600));  // Speed up: 1 real second = 1 sim hour

    // Move: Update currentLocation to nextNode
    const newLocation = edge.nextNode;

    // Plan new nextNode/ETA (simple min-ETA; can integrate congestion later)
    const graphDoc = await db.collection('graph').findOne();
    const neighbors = graphDoc?.nodes?.[newLocation]?.neighbors || {};

    let bestNode = null;
    let bestEta = Infinity;
    for (const [neighbor, dist] of Object.entries(neighbors)) {
      const speed = edge.speed || 10;  // From DB
      const eta = dist / speed;
      if (eta < bestEta) {
        bestEta = eta;
        bestNode = neighbor;
      }
    }

    // Fallback if no better option
    if (!bestNode && Object.keys(neighbors).length > 0) {
      bestNode = Object.keys(neighbors)[0];
      bestEta = neighbors[bestNode] / (edge.speed || 10);
    }

    // Update DB with new state (traffic analyzer will see this)
    await db.collection('edges').updateOne(
      { id: edgeId },
      { $set: { currentLocation: newLocation, nextNode: bestNode, eta: bestEta } }
    );

    console.log(`Edge ${edgeId} arrived at ${newLocation}. New next: ${bestNode}, ETA: ${bestEta.toFixed(2)} hours.`);
  }

  await client.close();
}

module.exports = { edgeAutonomousLoop };
