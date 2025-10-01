const { MongoClient } = require('mongodb');
const awsIot = require('aws-iot-device-sdk');
const path = require('path'); 

// MongoDB URI
const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';

// AWS IoT Core setup (use your endpoint from AWS console)
const device = awsIot.device({
  keyPath: path.join(__dirname, '../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'),
  certPath: path.join(__dirname, '../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt'),
  caPath: path.join(__dirname, '../certs/AmazonRootCA1.pem'),
  clientId: 'sensor_simulator',          // Unique client ID
  host: 'a1dghi6and062t-ats.iot.us-east-1.amazonaws.com' // Replace with your AWS IoT endpoint
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

    // Function to generate and post data for a single sensor
    const simulateSensor = (sensor) => {
      const postData = async () => {
        const reading = generateReading(sensor.type);
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

        // Optional: Insert to MongoDB (hybrid)
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
  } // No finally close to keep running
}

// Your generateReading function remains the same
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

runSimulator();
