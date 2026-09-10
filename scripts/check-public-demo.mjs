#!/usr/bin/env node
import { setTimeout as delay } from 'node:timers/promises';

const address = new URL(process.env.DEMO_PUBLIC_URL || 'https://daiyan-khan.github.io/HarbourSense/');
if (address.protocol !== 'https:') throw new Error('The published demo must use HTTPS.');
const expectedRevision = process.env.GITHUB_SHA || process.env.DEMO_EXPECTED_REVISION;
let lastError;
let verified = false;
for (let attempt = 0; attempt < 12; attempt += 1) {
  try {
    const response = await fetch(new URL(`demo-build.json?verify=${Date.now()}`, address), { signal: AbortSignal.timeout(10000) });
    if (!response.ok) throw new Error(`Build metadata returned HTTP ${response.status}.`);
    const build = await response.json();
    if (build.mode !== 'replay' || build.sourceTreeDirty || !build.sourceRevision) throw new Error('Published build is not an identifiable clean replay release.');
    if (expectedRevision && build.sourceRevision !== expectedRevision) throw new Error('Pages is still serving a different source revision.');
    if (`${build.basePath}/` !== address.pathname) throw new Error('Published subpath does not match build metadata.');
    console.log(`PUBLIC BUILD PASS: ${address.href} revision ${build.sourceRevision}`);
    verified = true;
    break;
  } catch (error) { lastError = error; }
  if (attempt < 11) await delay(5000);
}
if (!verified) throw lastError;
