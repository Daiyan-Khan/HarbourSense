const test = require('node:test');
const assert = require('node:assert/strict');

const {
  DOCK_QUEUE_STATUSES,
  countDockQueue,
  generateShipmentsPeriodically,
} = require('../lib/shipment-generator');

test('DOCK_QUEUE_STATUSES includes arrived and waiting', () => {
  assert.deepEqual(DOCK_QUEUE_STATUSES, ['arrived', 'waiting']);
});

test('countDockQueue queries dock occupancy', async () => {
  const queries = [];
  const coll = {
    countDocuments(query) {
      queries.push(query);
      return Promise.resolve(2);
    },
  };
  const count = await countDockQueue(coll, 'A1');
  assert.equal(count, 2);
  assert.equal(queries[0].arrivalNode, 'A1');
  assert.deepEqual(queries[0].status.$in, DOCK_QUEUE_STATUSES);
});

test('generateShipmentsPeriodically skips when dock at cap', async () => {
  const inserts = [];
  const publishes = [];
  const coll = {
    countDocuments: async () => 2,
    async insertOne(doc) {
      inserts.push(doc);
    },
  };
  const db = { collection: () => coll };
  const device = { publish: (topic, payload) => publishes.push({ topic, payload }) };

  await generateShipmentsPeriodically(db, ['A1'], ['B4'], device, {
    shipmentGenerationEnabled: true,
    maxArrivalsPerDock: 2,
    shipmentIntervalMsList: [1],
  }, { maxIterations: 1 });

  assert.equal(inserts.length, 0);
  assert.equal(publishes.length, 0);
});

test('generateShipmentsPeriodically publishes scheduledNextAt', async () => {
  const inserts = [];
  const publishes = [];
  let dockCount = 0;
  const coll = {
    countDocuments: async () => 0,
    async insertOne(doc) {
      inserts.push(doc);
    },
  };
  const db = {
    collection(name) {
      if (name === 'shipments') return coll;
      throw new Error(name);
    },
  };
  const device = { publish: (topic, payload) => publishes.push({ topic, payload: JSON.parse(payload) }) };

  await generateShipmentsPeriodically(db, ['A1'], ['B4'], device, {
    shipmentGenerationEnabled: true,
    maxArrivalsPerDock: 2,
    shipmentIntervalMsList: [1],
  }, { maxIterations: 1 });

  assert.equal(inserts.length, 1);
  assert.ok(inserts[0].scheduledNextAt);
  assert.equal(publishes[0].payload.scheduledNextAt, inserts[0].scheduledNextAt);
});
