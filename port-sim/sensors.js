const { MongoClient } = require('mongodb');
const {
  createMongoClient,
  createMqttDevice,
  getMongoSettings,
  getMqttBrokerLabel,
} = require('./runtime-config');

const mongoSettings = getMongoSettings();
const device = createMqttDevice('sensor_simulator');
const mqttBrokerLabel = getMqttBrokerLabel();

async function runSimulator() {
  const client = createMongoClient(MongoClient, mongoSettings);
  try {
    await client.connect();
    console.log(`Connected to MongoDB database '${mongoSettings.databaseName}' from MONGO_URI`);
    const db = client.db(mongoSettings.databaseName);
    const sensorsCol = db.collection('sensorList');
    const dataCol = db.collection('sensorData');
    const sensors = await sensorsCol.find().toArray();
    if (sensors.length === 0) {
      console.log('No sensors found. Add some to the DB first.');
      return;
    }
    console.log(`Loaded ${sensors.length} sensors.`);

    device.on('connect', () => {
      console.log(`Connected to ${mqttBrokerLabel}`);
    });
    device.on('error', (err) => {
      console.error('MQTT error:', err);
    });
    device.on('reconnect', () => {
      console.log(`Reconnecting to ${mqttBrokerLabel}...`);
    });

    const simulateSensor = (sensor) => {
      const postData = async () => {
        let reading = generateReading(sensor.type);
        if (Math.random() < 0.1) {
          reading = generateSpike(sensor.type, reading);
          console.log(`Spike detected for ${sensor.id}: ${reading}`);
        }
        const payload = {
          id: sensor.id,
          type: sensor.type,
          node: sensor.node,
          reading: reading,
          timestamp: new Date()
        };
        device.publish('harboursense/sensor/data', JSON.stringify(payload), (err) => {
          if (err) {
            console.error(`Error publishing for ${sensor.id}:`, err);
          } else {
            console.log(`Published to MQTT for ${sensor.id} at ${sensor.node}: ${JSON.stringify(payload)}`);
          }
        });
        try {
          await dataCol.insertOne(payload);
          console.log(`Inserted to MongoDB for ${sensor.id}`);
        } catch (error) {
          console.error(`Error inserting to MongoDB for ${sensor.id}:`, error);
        }
      };

      let intervalMs;
      switch (sensor.type) {
        case 'motion':
        case 'occupancy':
          intervalMs = Math.random() * (10000 - 5000) + 5000;
          break;
        case 'temperature':
        case 'humidity':
          intervalMs = Math.random() * (60000 - 30000) + 30000;
          break;
        default:
          intervalMs = Math.random() * (30000 - 10000) + 10000;
      }
      postData();
      setInterval(postData, intervalMs);
    };

    sensors.forEach(simulateSensor);
  } catch (error) {
    console.error('Simulator error:', error);
  }
}

function generateReading(type) {
  switch (type) {
    case 'temperature': return (Math.random() * 40 - 10).toFixed(2);
    case 'humidity': return (Math.random() * 100).toFixed(2);
    case 'vibration': return (Math.random() * 10).toFixed(2);
    case 'occupancy': return Math.floor(Math.random() * 101);
    case 'motion': return Math.random() < 0.5 ? 'detected' : 'none';
    default: return (Math.random() * 100).toFixed(2);
  }
}

function generateSpike(type, baseReading) {
  switch (type) {
    case 'temperature': return (parseFloat(baseReading) + Math.random() * 20 + 10).toFixed(2);
    case 'humidity': return (parseFloat(baseReading) + Math.random() * 50).toFixed(2);
    case 'vibration': return (parseFloat(baseReading) + Math.random() * 15 + 5).toFixed(2);
    case 'occupancy': return 100;
    case 'motion': return 'detected';
    default: return (parseFloat(baseReading) * 2).toFixed(2);
  }
}

runSimulator();
