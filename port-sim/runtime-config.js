const awsIot = require('aws-iot-device-sdk');
const mqtt = require('mqtt');

const DEFAULT_MONGO_HOST = 'localhost';
const DEFAULT_MONGO_PORT = '27017';
const DEFAULT_MONGO_DB_NAME = 'port';
const DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 5000;

const PLACEHOLDER_MARKERS = ['<', '>', 'your-', 'username:password', 'user:password', 'example.com'];

function databaseNameFromUri(uri) {
  try {
    const parsed = new URL(uri);
    const pathname = parsed.pathname.replace(/^\//, '');
    return pathname || null;
  } catch {
    return null;
  }
}

function looksLikePlaceholder(value) {
  const lowered = String(value).toLowerCase();
  return PLACEHOLDER_MARKERS.some((marker) => lowered.includes(marker));
}

function parseServerSelectionTimeoutMs(value) {
  if (!value) {
    return DEFAULT_SERVER_SELECTION_TIMEOUT_MS;
  }
  const timeoutMs = Number.parseInt(value, 10);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error('MONGO_SERVER_SELECTION_TIMEOUT_MS must be a positive integer.');
  }
  return timeoutMs;
}

function buildDefaultMongoUri(env = process.env) {
  const host = (env.MONGO_HOST || DEFAULT_MONGO_HOST).trim();
  const port = (env.MONGO_PORT || DEFAULT_MONGO_PORT).trim();
  const databaseName = (env.MONGO_DB_NAME || DEFAULT_MONGO_DB_NAME).trim();
  return `mongodb://${host}:${port}/${databaseName}`;
}

function getMongoSettings(env = process.env) {
  const uri = (env.MONGO_URI || buildDefaultMongoUri(env)).trim();
  const databaseName = (env.MONGO_DB_NAME || databaseNameFromUri(uri) || DEFAULT_MONGO_DB_NAME).trim();

  if (!uri) {
    throw new Error('MONGO_URI is empty. Set it in HarbourSense/.env or use the local default.');
  }
  if (looksLikePlaceholder(uri)) {
    throw new Error('MONGO_URI still looks like a placeholder. Replace it in HarbourSense/.env.');
  }
  if (!databaseName) {
    throw new Error('MONGO_DB_NAME is empty. Set it to the target database name, for example "port".');
  }
  if (looksLikePlaceholder(databaseName)) {
    throw new Error('MONGO_DB_NAME still looks like a placeholder. Replace it in HarbourSense/.env.');
  }

  let parsed;
  try {
    parsed = new URL(uri);
  } catch {
    throw new Error('MONGO_URI is not a valid MongoDB connection string.');
  }

  if (!['mongodb:', 'mongodb+srv:'].includes(parsed.protocol)) {
    throw new Error("MONGO_URI must start with 'mongodb://' or 'mongodb+srv://'.");
  }
  if (!parsed.hostname) {
    throw new Error('MONGO_URI is missing a MongoDB host.');
  }

  return {
    uri,
    databaseName,
    serverSelectionTimeoutMS: parseServerSelectionTimeoutMs(env.MONGO_SERVER_SELECTION_TIMEOUT_MS),
  };
}

function createMongoClient(MongoClient, settings = getMongoSettings()) {
  return new MongoClient(settings.uri, {
    serverSelectionTimeoutMS: settings.serverSelectionTimeoutMS,
  });
}

function createEventEmitterWrapper(client, brokerLabel) {
  const handlers = {
    connect: [],
    error: [],
    message: [],
    reconnect: [],
  };
  let connected = false;

  const wrapper = {
    on(event, handler) {
      if (!handlers[event]) {
        throw new Error(`Unsupported MQTT event: ${event}`);
      }
      handlers[event].push(handler);
      if (event === 'connect' && connected) {
        handler();
      }
      return wrapper;
    },
    subscribe(topic) {
      client.subscribe(topic);
    },
    publish(topic, payload, callback) {
      return client.publish(topic, payload, callback);
    },
    end(force, options, callback) {
      return client.end(force, options, callback);
    },
    brokerLabel,
  };

  client.on('connect', () => {
    connected = true;
    handlers.connect.forEach((handler) => handler());
  });
  client.on('error', (err) => handlers.error.forEach((handler) => handler(err)));
  client.on('message', (topic, payload) => handlers.message.forEach((handler) => handler(topic, payload)));
  client.on('reconnect', () => handlers.reconnect.forEach((handler) => handler()));

  return wrapper;
}

function createLocalMqttDevice(clientId, env = process.env) {
  const host = (env.MQTT_BROKER_HOST || 'localhost').trim();
  const port = (env.MQTT_BROKER_PORT || '1883').trim();
  const brokerUrl = `mqtt://${host}:${port}`;
  const client = mqtt.connect(brokerUrl, { clientId });
  return createEventEmitterWrapper(client, `local Mosquitto at ${host}:${port}`);
}

function createAwsIotDevice(clientId, env = process.env) {
  const endpoint = (env.AWS_IOT_ENDPOINT || '').trim();
  if (!endpoint || looksLikePlaceholder(endpoint)) {
    throw new Error(
      'MQTT_MODE=aws requires AWS_IOT_ENDPOINT in HarbourSense/.env and mounted certificate files.'
    );
  }

  const caPath = (env.AWS_IOT_CA_PATH || '').trim();
  const certPath = (env.AWS_IOT_CERT_PATH || '').trim();
  const keyPath = (env.AWS_IOT_KEY_PATH || '').trim();
  const missing = [
    ['AWS_IOT_CA_PATH', caPath],
    ['AWS_IOT_CERT_PATH', certPath],
    ['AWS_IOT_KEY_PATH', keyPath],
  ]
    .filter(([, value]) => !value || looksLikePlaceholder(value))
    .map(([name]) => name);
  if (missing.length) {
    throw new Error(`MQTT_MODE=aws requires real certificate paths in HarbourSense/.env: ${missing.join(', ')}`);
  }

  return awsIot.device({
    keyPath,
    certPath,
    caPath,
    clientId,
    host: endpoint,
    offlineQueueMaxSize: 0,
  });
}

function createMqttDevice(clientId, env = process.env) {
  const mode = (env.MQTT_MODE || 'local').trim().toLowerCase();
  if (mode === 'aws') {
    return createAwsIotDevice(clientId, env);
  }
  if (mode !== 'local') {
    throw new Error(`Unsupported MQTT_MODE '${mode}'. Use 'local' or 'aws'.`);
  }
  return createLocalMqttDevice(clientId, env);
}

function getMqttBrokerLabel(env = process.env) {
  const mode = (env.MQTT_MODE || 'local').trim().toLowerCase();
  if (mode === 'aws') {
    return 'AWS IoT Core';
  }
  const host = (env.MQTT_BROKER_HOST || 'localhost').trim();
  const port = (env.MQTT_BROKER_PORT || '1883').trim();
  return `local Mosquitto at ${host}:${port}`;
}

const DEFAULT_SHIPMENT_INTERVALS_MS = [30000, 60000, 90000];

function parsePositiveInt(value, fallback, name) {
  if (value === undefined || value === null || String(value).trim() === '') {
    return fallback;
  }
  const parsed = Number.parseInt(String(value).trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return parsed;
}

function parseShipmentIntervalsMs(value, fallback = DEFAULT_SHIPMENT_INTERVALS_MS) {
  if (value === undefined || value === null || String(value).trim() === '') {
    return [...fallback];
  }

  const intervals = String(value)
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part, index) => {
      const parsed = Number.parseInt(part, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`SHIPMENT_INTERVALS_MS entry at position ${index + 1} must be a positive integer.`);
      }
      return parsed;
    });

  if (!intervals.length) {
    throw new Error('SHIPMENT_INTERVALS_MS must contain at least one positive integer.');
  }

  return intervals;
}

function pickShipmentIntervalMs(intervals) {
  if (!intervals.length) {
    throw new Error('Shipment interval list must not be empty.');
  }
  return intervals[Math.floor(Math.random() * intervals.length)];
}

function getSimulatorSettings(env = process.env) {
  return {
    shipmentIntervalMsList: parseShipmentIntervalsMs(env.SHIPMENT_INTERVALS_MS),
    craneTelemetryIntervalMs: parsePositiveInt(env.CRANE_TELEMETRY_INTERVAL_MS, 5000, 'CRANE_TELEMETRY_INTERVAL_MS'),
    simProgressIntervalMs: parsePositiveInt(env.SIM_PROGRESS_INTERVAL_MS, 1000, 'SIM_PROGRESS_INTERVAL_MS'),
    simLoopIdleDelayMs: parsePositiveInt(env.SIM_LOOP_IDLE_DELAY_MS, 5000, 'SIM_LOOP_IDLE_DELAY_MS'),
    simLoopTickMs: parsePositiveInt(env.SIM_LOOP_TICK_MS, 1000, 'SIM_LOOP_TICK_MS'),
    simCompletingDelayMs: parsePositiveInt(env.SIM_COMPLETING_DELAY_MS, 2000, 'SIM_COMPLETING_DELAY_MS'),
    simEdgeMissingDelayMs: parsePositiveInt(env.SIM_EDGE_MISSING_DELAY_MS, 5000, 'SIM_EDGE_MISSING_DELAY_MS'),
  };
}

module.exports = {
  DEFAULT_SHIPMENT_INTERVALS_MS,
  createMongoClient,
  createMqttDevice,
  getMongoSettings,
  getMqttBrokerLabel,
  getSimulatorSettings,
  parseShipmentIntervalsMs,
  pickShipmentIntervalMs,
};
