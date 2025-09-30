const awsIot = require('aws-iot-device-sdk');
const faker = require('faker');
const { generatePortGraph } = require('./graph.js');  // Import graph for node IDs

// Sensor types
const sensorTypes = ['wind', 'temperature', 'humidity', 'pressure', 'water_level', 'current', 'vibration', 'air_quality'];

// Generate graph and get all node IDs
const portGraph = generatePortGraph();
const allNodes = Array.from(portGraph.nodes.keys());  // e.g., ['A1', 'A2', ..., 'J10']

// Sensor Class
class Sensor {
  constructor(sensorId, sensorType, client) {
    this.sensorId = sensorId;
    this.sensorType = sensorType;
    this.client = client;
  }

  generateReading() {
    switch (this.sensorType) {
      case 'wind': return { speed: faker.random.number({min: 0, max: 50}), direction: faker.random.number({min: 0, max: 359}) };
      case 'temperature': return { temp: faker.random.number({min: -10, max: 40}) };
      case 'humidity': return { humidity: faker.random.number({min: 10, max: 100}) };
      case 'pressure': return { pressure: faker.random.number({min: 950, max: 1050}) };
      case 'water_level': return { level: faker.random.number({min: 0, max: 10}) };
      case 'current': return { speed: faker.random.number({min: 0, max: 5}), direction: faker.random.number({min: 0, max: 359}) };
      case 'vibration': return { vibration: faker.random.number({min: 0, max: 10}) };
      case 'air_quality': return { pm25: faker.random.number({min: 0, max: 100}) };
      default: return {};
    }
  }

  postData(topic) {
    const data = {
      sensorId: this.sensorId,
      type: this.sensorType,
      readings: this.generateReading(),
      nodeId: allNodes[Math.floor(Math.random() * allNodes.length)],  // Random node from graph
      timestamp: Date.now()
    };

    this.client.publish(topic, JSON.stringify(data));
    console.log(`Sensor ${this.sensorId} (${this.sensorType}) posted:`, data);
  }
}

// Create MQTT client factory
function createClient(sensorId) {
  return awsIot.device({
    keyPath: './certs/private.key',
    certPath: './certs/certificate.pem',
    caPath: './certs/AmazonRootCA1.pem',
    clientId: `sensor-${sensorId}`,
    host: 'your-iot-endpoint.amazonaws.com'  // Replace with your AWS IoT endpoint
  });
}

// Initialize sensors
const NUM_SENSORS = 10;  // Adjust for more
const sensors = [];

for (let i = 0; i < NUM_SENSORS; i++) {
  const sensorType = sensorTypes[i % sensorTypes.length];
  const client = createClient(i);
  const sensor = new Sensor(`sensor-${i}`, sensorType, client);

  client.on('connect', () => {
    console.log(`Sensor ${sensor.sensorId} connected.`);
    setInterval(() => sensor.postData('port/sensors/data'), 5000);  // Post every 5s
  });

  client.on('error', (err) => console.error(`Error on ${sensor.sensorId}:`, err));
  sensors.push(sensor);
}
