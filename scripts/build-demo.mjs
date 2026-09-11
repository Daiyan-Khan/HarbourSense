#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { ROOT } from './demo.mjs';
import { normalizeBasePath, validateReplayDirectory } from './lib/demo-artifacts.mjs';
import { verifyBuild } from './verify-demo-build.mjs';
import { verifyProjectReport } from './lib/project-report.mjs';

const frontend = path.join(ROOT, 'dashboard', 'visualizer');
try {
  const basePath = normalizeBasePath(process.env.DEMO_BASE_PATH || '/HarbourSense');
  const resources = JSON.parse(await fs.readFile(path.join(frontend, 'src', 'projectResources.json'), 'utf8'));
  const report = await verifyProjectReport(path.join(frontend, 'public', 'reports'), resources.report);
  await validateReplayDirectory(path.join(frontend, 'public', 'replays'));
  const result = spawnSync(process.execPath, ['node_modules/react-scripts/scripts/build.js'], {
    cwd: frontend, stdio: 'inherit', windowsHide: true,
    env: { ...process.env, REACT_APP_DATA_SOURCE: 'replay', REACT_APP_API_BASE_URL: '/',
      PUBLIC_URL: basePath || '/', GENERATE_SOURCEMAP: 'false', CI: 'true' },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Dashboard build failed (${result.status}).`);
  const mediaSource = path.join(ROOT, 'docs', 'media');
  let capture;
  try { capture = JSON.parse(await fs.readFile(path.join(mediaSource, 'capture.json'), 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  const replayIndex = JSON.parse(await fs.readFile(path.join(frontend, 'public', 'replays', 'index.json'), 'utf8'));
  const mediaMatches = capture && replayIndex.scenarios.every(scenario => capture.recordingHashes?.[scenario.id] === scenario.sha256);
  if (mediaMatches) {
    const mediaOutput = path.join(frontend, 'build', 'media');
    await fs.mkdir(mediaOutput, { recursive: true });
    for (const file of ['harboursense-overview.png', 'harboursense-crane-detail.png', 'harboursense-mobile.png', 'harboursense-walkthrough.webm']) {
      const bytes = await fs.readFile(path.join(mediaSource, file));
      if (createHash('sha256').update(bytes).digest('hex') !== capture.mediaSha256[file]) throw new Error(`Portfolio media hash differs: ${file}`);
      await fs.writeFile(path.join(mediaOutput, file), bytes);
    }
  } else {
    if (process.env.DEMO_REQUIRE_MEDIA === 'true') throw new Error('Capture portfolio media for these recordings before publication.');
    console.log('Portfolio media omitted: preview this build, run node scripts/capture-portfolio.mjs, then rebuild to include matching media.');
  }
  const revision = spawnSync('git', ['-c', `safe.directory=${ROOT.replaceAll('\\', '/')}`, 'rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8', windowsHide: true });
  const status = spawnSync('git', ['-c', `safe.directory=${ROOT.replaceAll('\\', '/')}`, 'status', '--porcelain'], { cwd: ROOT, encoding: 'utf8', windowsHide: true });
  const dirty = status.status !== 0 || Boolean(status.stdout.trim());
  await fs.writeFile(path.join(frontend, 'build', 'demo-build.json'), JSON.stringify({
    schemaVersion: 1, mode: 'replay', basePath, sourceRevision: !dirty && revision.status === 0 ? revision.stdout.trim() : null,
    sourceTreeDirty: dirty,
    report,
    builtAt: new Date().toISOString(),
  }, null, 2) + '\n');
  await fs.writeFile(path.join(frontend, 'build', '.nojekyll'), '');
  await verifyBuild();
  console.log(`Portfolio build ready at dashboard/visualizer/build (base ${basePath || '/'}).`);
} catch (error) {
  console.error(`DEMO BUILD FAILED: ${error.message}`);
  process.exitCode = 1;
}
