import test from 'node:test';
import assert from 'node:assert/strict';
import { parseOptions, composeArguments, dockerEnvironment, parseComposeStatus, assessReadiness } from '../demo.mjs';

test('demo cannot inherit Atlas or another Compose project configuration', () => {
  const env = dockerEnvironment(parseOptions(['start']), { PATH: 'bin', MONGO_URI: 'cloud', MQTT_MODE: 'aws', COMPOSE_FILE: 'other.yml', DEMO_API_PORT: '9999' });
  assert.equal(env.PATH, 'bin');
  assert.equal(env.MONGO_URI, undefined);
  assert.equal(env.MQTT_MODE, undefined);
  assert.equal(env.COMPOSE_FILE, undefined);
  assert.equal(env.DEMO_API_PORT, '8000');
  const args = composeArguments(['stop']);
  assert.equal(args[args.indexOf('--project-name') + 1], 'harboursense-demo');
  assert.match(args[args.indexOf('--env-file') + 1], /config[\\/]demo\.env$/);
});

test('ports and destructive intent are explicit', () => {
  assert.throws(() => parseOptions(['start', '--api-port', '0']));
  assert.throws(() => parseOptions(['start', '--api-port', '3000']));
  assert.throws(() => parseOptions(['start', '--database', 'production']));
  assert.equal(parseOptions(['reset']).yes, false);
  assert.deepEqual(parseOptions(['start', '--api-port', '8100', '--dashboard-port', '3100']),
    { command: 'start', apiPort: 8100, dashboardPort: 3100, yes: false });
});

test('status handles array and line-delimited Compose formats', () => {
  assert.deepEqual(parseComposeStatus('[{"Service":"api"}]'), [{ Service: 'api' }]);
  assert.deepEqual(parseComposeStatus('{"Service":"api"}\n{"Service":"mongo"}'), [{ Service: 'api' }, { Service: 'mongo' }]);
  assert.deepEqual(parseComposeStatus(''), []);
});

test('HTTP availability alone cannot make the full pipeline ready', () => {
  const services = Object.fromEntries(['manager', 'portsim', 'sensors', 'analyzer'].map(name =>
    [name, { status: 'ready', updatedAt: '2026-09-10T00:00:00Z', mqttConnected: true }]));
  const state = { enabled: true, mode: 'demo', runId: 'test', services };
  assert.deepEqual(assessReadiness(state, Date.parse('2026-09-10T00:00:01Z')), []);
  assert.equal(assessReadiness(state, Date.parse('2026-09-10T00:00:20Z')).length, 4);
  services.analyzer.mqttConnected = false;
  assert.match(assessReadiness(state, Date.parse('2026-09-10T00:00:01Z'))[0], /analyzer.*broker/);
  assert.equal(assessReadiness({ enabled: false }).length, 1);
});
