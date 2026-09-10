import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { normalizeBasePath, validateRecording } from '../lib/demo-artifacts.mjs';
import { resolveAsset } from '../serve-demo.mjs';
import { detectSecrets } from '../check-secrets.mjs';

test('Pages asset resolution preserves the subpath and confines encoded paths', () => {
  const root = path.resolve('build');
  assert.equal(normalizeBasePath('/HarbourSense/'), '/HarbourSense');
  assert.throws(() => normalizeBasePath('https://example.test'), /deployment base/);
  assert.deepEqual(resolveAsset(root, '/HarbourSense', '/HarbourSense'), { redirect: '/HarbourSense/' });
  assert.equal(resolveAsset(root, '/HarbourSense', '/HarbourSense/static/app.js').file, path.join(root, 'static', 'app.js'));
  assert.equal(resolveAsset(root, '/HarbourSense', '/HarbourSense/%2e%2e/private'), null);
  assert.equal(resolveAsset(root, '/HarbourSense', '/HarbourSense/%5c..%5cprivate'), null);
  assert.equal(resolveAsset(root, '/HarbourSense', '/api/graph'), null);
});

test('credential checks accept validators and flag actual credential-shaped values without printing them', () => {
  assert.deepEqual(detectSecrets('mongodb+srv:// and mongodb://localhost:27017/demo'), []);
  assert.deepEqual(detectSecrets('mongodb+srv://<username>:<password>@example.test'), []);
  assert.equal(detectSecrets('mongodb' + '+srv://specific-user:secret-value@cluster.test')[0].kind, 'database-credentials');
  assert.equal(detectSecrets('-----BEGIN ' + 'PRIVATE KEY-----')[0].kind, 'private-key');
  assert.equal(detectSecrets('ghp_' + 'a'.repeat(36))[0].kind, 'github-token');
});

test('publication rejects fabricated provenance, incomplete outcomes, and reordered recordings', () => {
  const valid = { schemaVersion: 1, scenario: { id: 'normal' }, recordedAt: '2026-09-10T00:00:00Z',
    durationMs: 1000, initialSnapshot: { graph: { nodes: { A1: {} } } },
    provenance: { synthetic: true, recorded: true, runId: 'unit-fixture', terminalState: 'complete' },
    frames: [{ atMs: 0, snapshot: { shipments: [] }, events: [] },
      { atMs: 1000, snapshot: { shipments: [{ id: 'test', status: 'delivered' }], edges: [{ id: 'test', taskPhase: 'idle', task: 'idle' }] }, events: [] }] };
  assert.equal(validateRecording(valid, 'normal'), valid);
  assert.throws(() => validateRecording({ ...valid, provenance: {} }, 'normal'), /real pipeline/);
  assert.throws(() => validateRecording({ ...valid, frames: [...valid.frames].reverse() }, 'normal'), /ordering/);
  assert.throws(() => validateRecording({ ...valid, frames: valid.frames.slice(0, 1) }, 'normal'), /unfinished/);
  assert.throws(() => validateRecording({ ...valid, scenario: { id: 'crane-fault' } }, 'crane-fault'), /completed repair/);
  assert.throws(() => validateRecording({ ...valid, scenario: { id: 'congestion' } }, 'congestion'), /alternative transport route/);
});
