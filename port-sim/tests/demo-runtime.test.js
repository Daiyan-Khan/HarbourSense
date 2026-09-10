const test = require('node:test');
const assert = require('node:assert/strict');
const { DemoContext, DemoRunEnded, logicalMs, runPrefix, validateDemoSettings } = require('../lib/demo-runtime');

test('demo database and broker guard rejects cloud and ordinary project data', () => {
  const env = { DEMO_MODE: 'true' };
  validateDemoSettings({ uri: 'mongodb://localhost:27018/harboursense_demo', databaseName: 'harboursense_demo' }, env);
  for (const settings of [
    { uri: 'mongodb://localhost/port', databaseName: 'port' },
    { uri: 'mongodb+srv://cluster.example/harboursense_demo', databaseName: 'harboursense_demo' },
    { uri: 'mongodb://username:password@localhost/harboursense_demo', databaseName: 'harboursense_demo' },
  ]) assert.throws(() => validateDemoSettings(settings, env));
});

test('logical time freezes while paused and changes speed without a discontinuity', () => {
  const state = { status: 'running', speed: 1, clock: { baseMs: 100, anchorWallMs: 1000 } };
  assert.equal(logicalMs(state, 1500), 600);
  state.status = 'paused'; state.clock.baseMs = 600;
  assert.equal(logicalMs(state, 100000), 600);
  state.status = 'running'; state.speed = 4; state.clock.anchorWallMs = 100000;
  assert.equal(logicalMs(state, 100000), 600);
  assert.equal(logicalMs(state, 100250), 1600);
});

test('run prefixes cannot alias live data or inject collection names', () => {
  assert.notEqual(runPrefix('a'.repeat(32)), runPrefix('b'.repeat(32)));
  assert.throws(() => runPrefix('../graph'));
});

test('previous-run operation is rejected before its callback executes', async () => {
  const base = { collection: () => ({ findOne: async () => ({ runId: 'b'.repeat(32), status: 'running' }) }) };
  const context = new DemoContext(base, { runId: 'a'.repeat(32) }, 'portsim');
  let wrote = false;
  await assert.rejects(context.operation(() => { wrote = true; }), DemoRunEnded);
  assert.equal(wrote, false);
});
