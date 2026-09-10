if (process.env.DEMO_MODE === 'true') {
  require('./lib/demo-simulator').main('sensors').catch((error) => { console.error(error.message); process.exitCode = 1; });
} else {
const { MongoClient } = require('mongodb');
const {
  createMongoClient,
  createMqttDevice,
  getMongoSettings,
  getMqttBrokerLabel,
  getSensorSettings,
} = require('./runtime-config');
const { startSensorPublisher } = require('./lib/sensor-publisher');

const mongoSettings = getMongoSettings();
const sensorSettings = getSensorSettings();
const device = createMqttDevice('sensor_simulator');
const mqttBrokerLabel = getMqttBrokerLabel();

async function runSimulator() {
  const client = createMongoClient(MongoClient, mongoSettings);
  try {
    await client.connect();
    console.log(`Connected to MongoDB database '${mongoSettings.databaseName}'`);
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
      sensors.forEach((sensor) => startSensorPublisher(sensor, device, dataCol, sensorSettings));
    });
    device.on('error', (err) => console.error('MQTT error:', err));
    device.on('reconnect', () => console.log(`Reconnecting to ${mqttBrokerLabel}...`));
  } catch (error) {
    console.error('Simulator error:', error);
  }
}

runSimulator();

}
