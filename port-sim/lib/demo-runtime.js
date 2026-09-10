/* Shared demo clock and collection fencing; normal live execution is untouched. */
const { AsyncLocalStorage } = require('async_hooks');
const crypto = require('crypto');
const storage = new AsyncLocalStorage();
const WRITES = new Set(['insertOne', 'insertMany', 'updateOne', 'updateMany', 'replaceOne',
  'deleteOne', 'deleteMany', 'findOneAndUpdate', 'findOneAndReplace', 'findOneAndDelete', 'bulkWrite']);
const wallSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class DemoRunEnded extends Error {}

function validateDemoSettings(settings, env = process.env) {
  const uri = new URL(settings.uri);
  if (env.DEMO_MODE !== 'true' || uri.protocol !== 'mongodb:'
      || !['localhost', '127.0.0.1', '[::1]', 'mongo', 'mongo-demo'].includes(uri.hostname)
      || uri.username || uri.password || uri.host.includes(',')
      || !/^harboursense_demo(?:_test_[a-z0-9_]+)?$/.test(settings.databaseName)
      || (uri.pathname !== '/' && uri.pathname !== '' && uri.pathname !== `/${settings.databaseName}`)
      || (env.MQTT_MODE || 'local') !== 'local'
      || !['localhost', '127.0.0.1', 'mqtt', 'mqtt-demo'].includes(env.MQTT_BROKER_HOST || 'localhost')) {
    throw new Error('Demo requires the owned local MongoDB database and local MQTT broker');
  }
}

function logicalMs(state, wall = Date.now()) {
  return (state.clock?.baseMs || 0) + (state.status === 'running'
    ? Math.max(0, wall - state.clock.anchorWallMs) * state.speed : 0);
}

function runPrefix(runId) {
  if (!/^[a-f0-9]{32}$/.test(runId)) throw new Error('Invalid demo run identity');
  return `run_${runId}_`;
}

class DemoContext {
  constructor(base, state, role) {
    this.base = base;
    this.state = state;
    this.runId = state.runId;
    this.role = role;
    this.stopped = false;
    this.mqtt = null;
    this.db = { collection: (name) => this.collection(name) };
  }

  async refresh() {
    if (this.stopped) throw new DemoRunEnded('Demo worker stopped');
    const state = await this.base.collection('_demoControl').findOne({ _id: 'active' });
    if (!state || state.runId !== this.runId) throw new DemoRunEnded('Demo run was replaced');
    this.state = state;
    return state;
  }

  async checkpoint() {
    while (true) {
      const state = await this.refresh();
      if (state.status === 'running') return state;
      if (state.status === 'resetting') throw new DemoRunEnded('Demo run ended');
      await wallSleep(25);
    }
  }

  async operation(callback) {
    while (true) {
      await this.checkpoint();
      const result = await this.base.collection('_demoControl').updateOne(
        { _id: 'active', runId: this.runId, status: 'running' }, { $inc: { activeOperations: 1 } });
      if (result.modifiedCount) break;
    }
    try { return await callback(); }
    finally {
      await this.base.collection('_demoControl').updateOne(
        { _id: 'active', runId: this.runId }, { $inc: { activeOperations: -1 } });
    }
  }

  collection(name) {
    const raw = this.base.collection(runPrefix(this.runId) + name);
    return new Proxy(raw, { get: (target, key) => {
      const member = target[key];
      if (typeof member !== 'function') return member;
      if (!WRITES.has(key)) return member.bind(target);
      return (...args) => this.operation(() => {
        if (name === 'edgeRuntime' && ['updateOne', 'updateMany', 'findOneAndUpdate'].includes(key) && args[1]?.$set) {
          args[1] = { ...args[1], $set: { ...args[1].$set, demoUpdatedSimMs: logicalMs(this.state) } };
        }
        return member.apply(target, args);
      });
    } });
  }

  async sleep(ms) {
    const target = logicalMs(await this.checkpoint()) + ms;
    while (logicalMs(await this.checkpoint()) < target) await wallSleep(20);
  }

  async heartbeat(status = 'ready', error = null) {
    await this.base.collection('_demoWorkers').updateOne({ _id: `${this.runId}:${this.role}` }, { $set: {
      role: this.role, runId: this.runId, status, updatedAt: new Date(), mqttConnected: Boolean(this.mqtt?.connected), error,
    } }, { upsert: true });
  }

  async publish(topic, payload, callback) {
    try {
      const data = typeof payload === 'string' || Buffer.isBuffer(payload) ? JSON.parse(payload) : { ...payload };
      if (data.runId && data.runId !== this.runId) throw new DemoRunEnded('Previous-run publication rejected');
      const state = await this.checkpoint();
      const sequenceResult = await this.base.collection('_demoControl').findOneAndUpdate(
        { _id: 'active', runId: this.runId }, { $inc: { eventSequence: 1 } }, { returnDocument: 'after' });
      const sequenceState = sequenceResult?.value || sequenceResult;
      Object.assign(data, { sequence: sequenceState?.eventSequence, runId: this.runId, scenarioId: state.scenarioId,
        eventId: data.eventId || crypto.randomUUID(), simulatedTimeMs: logicalMs(state), wallTime: new Date().toISOString() });
      await this.db.collection('mqttOutbox').updateOne({ _id: data.eventId }, {
        $setOnInsert: { topic, payload: data, producer: this.role, delivered: false, createdAt: new Date() },
      }, { upsert: true });
      if (callback) callback(null);
    } catch (error) {
      if (callback) callback(error);
      else throw error;
    }
  }

  async flushOutbox() {
    while (true) {
      await this.checkpoint();
      if (this.mqtt?.connected) {
        const pending = await this.db.collection('mqttOutbox').find({ delivered: false, producer: this.role }).sort('createdAt', 1).limit(100).toArray();
        for (const item of pending) {
          await this.checkpoint();
          await new Promise((resolve, reject) => this.mqtt.publish(item.topic, JSON.stringify(item.payload), { qos: 1 }, (error) => error ? reject(error) : resolve()));
          await this.db.collection('mqttOutbox').updateOne({ _id: item._id }, { $set: { delivered: true } });
        }
      }
      await wallSleep(30);
    }
  }

  async accept(payload) {
    if (payload?.runId !== this.runId || !payload.eventId) return false;
    await this.checkpoint();
    return !await this.db.collection('mqttInbox').findOne({ _id: payload.eventId });
  }

  async acknowledge(payload) {
    await this.db.collection('mqttInbox').updateOne({ _id: payload.eventId }, { $set: { processedAt: new Date() } }, { upsert: true });
  }
}

function simulationNow() {
  const context = storage.getStore();
  return context ? context.state.startedWallMs + logicalMs(context.state) : Date.now();
}
function simulationSleep(ms) {
  const context = storage.getStore();
  return context ? context.sleep(ms) : wallSleep(ms);
}
function currentContext() { return storage.getStore(); }

module.exports = { DemoContext, DemoRunEnded, validateDemoSettings, logicalMs, runPrefix,
  simulationNow, simulationSleep, currentContext, storage, wallSleep };
