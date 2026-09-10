#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { ROOT } from './demo.mjs';
import { normalizeBasePath, validateReplayDirectory, walkFiles } from './lib/demo-artifacts.mjs';
import { detectSecrets } from './check-secrets.mjs';

export async function verifyBuild(directory = path.join(ROOT, 'dashboard', 'visualizer', 'build')) {
  const metadata = JSON.parse(await fs.readFile(path.join(directory, 'demo-build.json'), 'utf8'));
  if (metadata.mode !== 'replay' || metadata.schemaVersion !== 1) throw new Error('The build must explicitly select replay mode.');
  const base = normalizeBasePath(metadata.basePath || '/');
  await validateReplayDirectory(path.join(directory, 'replays'));
  const html = await fs.readFile(path.join(directory, 'index.html'), 'utf8');
  for (const [, url] of html.matchAll(/(?:src|href)="([^"]+)"/g)) {
    if (url.startsWith('data:') || url.startsWith('#')) continue;
    if (!url.startsWith(`${base}/`)) throw new Error(`Asset does not use the deployment subpath: ${url}`);
    await fs.access(path.join(directory, url.slice(base.length)));
  }
  const files = await walkFiles(directory);
  let size = 0;
  for (const file of files) {
    const relative = path.relative(directory, file);
    if (/(^|[/\\])(?:\.env[^/\\]*|certs|venv|node_modules)(?:[/\\]|$)|\.(?:pem|key|p12|pfx|map)$/i.test(relative)) {
      throw new Error(`Unexpected private/development file in public build: ${relative}`);
    }
    const bytes = await fs.readFile(file);
    size += bytes.length;
    if (/\.(?:js|json|css|html|txt|md)$/i.test(file)) {
      const content = bytes.toString('utf8');
      if (detectSecrets(content).length) throw new Error(`Credential-shaped content in public build: ${relative}`);
      if (/https?:\/\/(?:localhost|127\.0\.0\.1|\[::1\]):8000\b/.test(content)) {
        throw new Error(`Local backend URL remains in public build: ${relative}`);
      }
    }
  }
  if (size > 100 * 1024 * 1024) throw new Error('Demo exceeds the 100 MiB project publication budget. Compact recordings first.');
  console.log(`DEMO BUILD PASS: ${files.length} files, ${(size / 1024 / 1024).toFixed(1)} MiB, three verified recordings, subpath ${base || '/'}.`);
  return { files: files.length, bytes: size, basePath: base };
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  verifyBuild().catch(error => { console.error(`DEMO BUILD FAILED: ${error.message}`); process.exitCode = 1; });
}
