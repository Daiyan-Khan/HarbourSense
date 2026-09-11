#!/usr/bin/env node
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';

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
    if (build.report) {
      if (!/^[a-z0-9][a-z0-9_-]*\.pdf$/i.test(build.report.file)) throw new Error('Published report filename is invalid.');
      const reportResponse = await fetch(new URL(`reports/${build.report.file}`, address), { signal: AbortSignal.timeout(20000) });
      if (!reportResponse.ok || !reportResponse.headers.get('content-type')?.includes('application/pdf')) throw new Error('Published report is not available as a PDF.');
      const bytes = Buffer.from(await reportResponse.arrayBuffer());
      if (createHash('sha256').update(bytes).digest('hex') !== build.report.sha256) throw new Error('Published PDF does not match the release report.');
      console.log(`PUBLIC REPORT PASS: ${build.report.file}, ${bytes.length} bytes, matching release hash.`);
    }
    console.log(`PUBLIC BUILD PASS: ${address.href} revision ${build.sourceRevision}`);
    verified = true;
    break;
  } catch (error) { lastError = error; }
  if (attempt < 11) await delay(5000);
}
if (!verified) throw lastError;
