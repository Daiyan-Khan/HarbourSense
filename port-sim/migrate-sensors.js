const { MongoClient } = require('mongodb');

async function migrateSensors() {
  const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port/sensors';
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db('port');
    const sensorListCol = db.collection('sensorList');

    // Your full sensor JSON array (paste the complete list here)
    const sensors = [
      { "id": "S001", "type": "temperature", "node": "A1" },
      { "id": "S002", "type": "occupancy", "node": "A1" },
      { "id": "S003", "type": "vibration", "node": "A2" },
      { "id": "S004", "type": "humidity", "node": "A3" },
      { "id": "S005", "type": "motion", "node": "A4" },
      { "id": "S006", "type": "temperature", "node": "A5" },
      { "id": "S007", "type": "occupancy", "node": "B1" },
      { "id": "S008", "type": "vibration", "node": "B2" },
      { "id": "S009", "type": "humidity", "node": "B3" },
      { "id": "S010", "type": "motion", "node": "B4" },
      { "id": "S011", "type": "temperature", "node": "B5" },
      { "id": "S012", "type": "occupancy", "node": "C1" },
      { "id": "S013", "type": "vibration", "node": "C2" },
      { "id": "S014", "type": "humidity", "node": "C3" },
      { "id": "S015", "type": "motion", "node": "C4" },
      { "id": "S016", "type": "temperature", "node": "D1" },
      { "id": "S017", "type": "occupancy", "node": "D2" },
      { "id": "S018", "type": "vibration", "node": "D3" },
      { "id": "S019", "type": "humidity", "node": "E1" },
      { "id": "S020", "type": "motion", "node": "E2" },
      { "id": "S021", "type": "temperature", "node": "E3" },
      { "id": "S022", "type": "occupancy", "node": "F1" },
      { "id": "S023", "type": "vibration", "node": "F2" },
      { "id": "S024", "type": "humidity", "node": "G1" },
      { "id": "S025", "type": "motion", "node": "H1" }
      // Add more if needed
    ];

    // Clear existing data if you want a fresh start (optional)
    // await sensorListCol.deleteMany({});

    // Insert the array
    const insertResult = await sensorListCol.insertMany(sensors);
    console.log(`Inserted ${insertResult.insertedCount} sensors. IDs:`, insertResult.insertedIds);
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

migrateSensors();

