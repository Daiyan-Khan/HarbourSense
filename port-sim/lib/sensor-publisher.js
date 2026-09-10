const { generateReading, generateSpike } = require('./sensor-readings');

function pickIntervalMs(sensorType, settings) {
  const minMs = settings.intervalMinMs;
  const maxMs = settings.intervalMaxMs;
  const span = Math.max(0, maxMs - minMs);

  if (sensorType === 'motion' || sensorType === 'occupancy') {
    return minMs + Math.random() * Math.min(span, 5000);
  }
  if (sensorType === 'temperature' || sensorType === 'humidity') {
    return minMs + Math.random() * span;
  }
  return minMs + Math.random() * (span * 0.5);
}

function startSensorPublisher(sensor, device, dataCol, settings) {
  const postData = async () => {
    let reading = generateReading(sensor.type);
    if (Math.random() < settings.spikeProbability) {
      reading = generateSpike(sensor.type, reading);
      console.log(`Spike for ${sensor.id}: ${reading}`);
    }

    const payload = {
      id: sensor.id,
      type: sensor.type,
      node: sensor.node,
      reading,
      timestamp: new Date(),
    };

    device.publish('harboursense/sensor/data', JSON.stringify(payload), (err) => {
      if (err) console.error(`MQTT publish error for ${sensor.id}:`, err);
    });

    try {
      await dataCol.insertOne(payload);
    } catch (error) {
      console.error(`Mongo insert error for ${sensor.id}:`, error);
    }
  };

  postData();
  const intervalMs = pickIntervalMs(sensor.type, settings);
  setInterval(postData, intervalMs);
}

module.exports = {
  pickIntervalMs,
  startSensorPublisher,
};
