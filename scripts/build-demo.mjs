#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { ROOT } from './demo.mjs';
import { normalizeBasePath, validateReplayDirectory } from './lib/demo-artifacts.mjs';
import { verifyBuild } from './verify-demo-build.mjs';

const frontend = path.join(ROOT, 'dashboard', 'visualizer');
try {
  const basePath = normalizeBasePath(process.env.DEMO_BASE_PATH || '/HarbourSense');
  await validateReplayDirectory(path.join(frontend, 'public', 'replays'));
  const result = spawnSync(process.execPath, ['node_modules/react-scripts/scripts/build.js'], {
    cwd: frontend, stdio: 'inherit', windowsHide: true,
    env: { ...process.env, REACT_APP_DATA_SOURCE: 'replay', REACT_APP_API_BASE_URL: '/',
      PUBLIC_URL: basePath || '/', GENERATE_SOURCEMAP: 'false', CI: 'true' },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Dashboard build failed (${result.status}).`);
  const revision = spawnSync('git', ['-c', `safe.directory=${ROOT.replaceAll('\\', '/')}`, 'rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8', windowsHide: true });
  const status = spawnSync('git', ['-c', `safe.directory=${ROOT.replaceAll('\\', '/')}`, 'status', '--porcelain'], { cwd: ROOT, encoding: 'utf8', windowsHide: true });
  const dirty = status.status !== 0 || Boolean(status.stdout.trim());
  await fs.writeFile(path.join(frontend, 'build', 'demo-build.json'), JSON.stringify({
    schemaVersion: 1, mode: 'replay', basePath, sourceRevision: !dirty && revision.status === 0 ? revision.stdout.trim() : null,
    sourceTreeDirty: dirty,
    builtAt: new Date().toISOString(),
  }, null, 2) + '\n');
  await fs.writeFile(path.join(frontend, 'build', '.nojekyll'), '');
  await verifyBuild();
  console.log(`Portfolio build ready at dashboard/visualizer/build (base ${basePath || '/'}).`);
} catch (error) {
  console.error(`DEMO BUILD FAILED: ${error.message}`);
  process.exitCode = 1;
}
