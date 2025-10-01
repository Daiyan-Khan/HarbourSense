const { MongoClient } = require('mongodb');

async function migrateEdges() {
  const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('port');
    const edgeDevicesCol = db.collection('edgeDevices');

    // Your JSON array (paste the full list here)
    const edges = [
      { "id": "truck001", "desc": "Heavy-duty container transport truck", "roles": ["transport", "offloading", "loading"] },
      { "id": "truck002", "desc": "Medium-duty yard shuttle truck", "roles": ["transport", "maintenance"] },
      { "id": "crane001", "desc": "Ship-to-shore gantry crane at berth", "roles": ["offloading", "loading"] },
      { "id": "crane002", "desc": "Mobile harbor crane for bulk cargo", "roles": ["offloading", "loading", "maintenance"] },
      { "id": "agv001", "desc": "Automated guided vehicle for container movement", "roles": ["transport"] },
      { "id": "agv002", "desc": "High-capacity AGV for yard operations", "roles": ["transport", "monitoring"] },
      { "id": "drone001", "desc": "Aerial surveillance drone", "roles": ["monitoring", "maintenance"] },
      { "id": "drone002", "desc": "Cargo inspection drone", "roles": ["monitoring"] },
      { "id": "forklift001", "desc": "Warehouse forklift robot", "roles": ["loading", "offloading", "transport"] },
      { "id": "forklift002", "desc": "Heavy-lift forklift for containers", "roles": ["loading", "offloading"] },
      { "id": "conveyor001", "desc": "Automated belt conveyor system", "roles": ["transport", "loading"] },
      { "id": "conveyor002", "desc": "Bulk material conveyor", "roles": ["transport", "offloading"] },
      { "id": "robot001", "desc": "Autonomous warehouse robot", "roles": ["transport", "monitoring", "maintenance"] },
      { "id": "robot002", "desc": "Inspection robot for infrastructure", "roles": ["monitoring", "maintenance"] },
      { "id": "truck003", "desc": "Electric transport truck", "roles": ["transport", "offloading"] },
      { "id": "crane003", "desc": "Rail-mounted gantry crane", "roles": ["loading", "offloading"] },
      { "id": "agv003", "desc": "Compact AGV for tight spaces", "roles": ["transport"] },
      { "id": "drone003", "desc": "Environmental monitoring drone", "roles": ["monitoring"] },
      { "id": "forklift003", "desc": "Outdoor forklift for yards", "roles": ["transport", "loading"] },
      { "id": "conveyor003", "desc": "Smart conveyor with sensors", "roles": ["transport", "monitoring"] }
      // Add more if needed
    ];

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
