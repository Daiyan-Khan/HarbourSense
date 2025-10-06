const { MongoClient } = require('mongodb');
const awsIot = require('aws-iot-device-sdk');
const path = require('path');

// MongoDB URI
const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';

// AWS IoT Core setup
const device = awsIot.device({
  keyPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),
  certPath: path.join(__dirname, 'certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, 'certs/AmazonRootCA1.pem'),
  clientId: 'sensor_simulator',
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com'
});

async function runSimulator() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db('port');
    const sensorsCol = db.collection('sensorList');
    const dataCol = db.collection('sensorData');
    const sensors = await sensorsCol.find().toArray();
    if (sensors.length === 0) {
      console.log('No sensors found. Add some to the DB first.');
      return;
    }
    console.log(`Loaded ${sensors.length} sensors.`);

    // Connect to AWS IoT MQTT
    device.on('connect', () => {
      console.log('Connected to AWS IoT Core MQTT');
    });
    device.on('error', (err) => {
      console.error('AWS IoT error:', err);
    });
    device.on('reconnect', () => {
      console.log('Reconnecting to AWS IoT...');
    });

    // Function to generate and post data for a single sensor
    const simulateSensor = (sensor) => {
      const postData = async () => {
        let reading = generateReading(sensor.type);
        // 10% chance of spike/anomaly
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
        // Publish to MQTT topic
        device.publish('harboursense/sensor/data', JSON.stringify(payload), (err) => {
          if (err) {
            console.error(`Error publishing for ${sensor.id}:`, err);
          } else {
            console.log(`Published to MQTT for ${sensor.id} at ${sensor.node}: ${JSON.stringify(payload)}`);
          }
        });
        // Insert to MongoDB
        try {
          await dataCol.insertOne(payload);
          console.log(`Inserted to MongoDB for ${sensor.id}`);
        } catch (error) {
          console.error(`Error inserting to MongoDB for ${sensor.id}:`, error);
        }
      };

      // Set variable interval based on type
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
  // No client.close() to keep running
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
    case 'temperature': return (parseFloat(baseReading) + Math.random() * 20 + 10).toFixed(2); // Spike to >50°C
    case 'humidity': return (parseFloat(baseReading) + Math.random() * 50).toFixed(2); // >100%
    case 'vibration': return (parseFloat(baseReading) + Math.random() * 15 + 5).toFixed(2); // >15
    case 'occupancy': return 100; // Full occupancy
    case 'motion': return 'detected'; // Always detect
    default: return (parseFloat(baseReading) * 2).toFixed(2);
  }
}

runSimulator();
