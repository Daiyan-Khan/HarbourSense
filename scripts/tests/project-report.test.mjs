import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { verifyProjectReport } from '../lib/project-report.mjs';

test('publication rejects missing, unconfigured, extra and changed report files', async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'harboursense-report-'));
  const report = { title: 'Original project report', file: 'harboursense-report.pdf', pages: 1 };
  try {
    assert.equal(await verifyProjectReport(directory, null), null);
    await assert.rejects(verifyProjectReport(directory, report), /exactly the configured PDF/);
    await assert.rejects(verifyProjectReport(directory, { ...report, file: '../private.pdf' }), /simple PDF filename/);
    await fs.writeFile(path.join(directory, report.file), 'Not a PDF');
    await assert.rejects(verifyProjectReport(directory, report), /complete PDF/);
    await fs.writeFile(path.join(directory, report.file), '%PDF-1.7\nTest signature fixture only\n%%EOF\n');
    await assert.rejects(verifyProjectReport(directory, null), /without an enabled/);
    const checked = await verifyProjectReport(directory, report);
    assert.match(checked.sha256, /^[a-f0-9]{64}$/);
    await assert.rejects(verifyProjectReport(directory, { ...checked, sha256: '0'.repeat(64) }), /release hash/);
    await fs.writeFile(path.join(directory, 'unreviewed.pdf'), '%PDF-1.7\n%%EOF');
    await assert.rejects(verifyProjectReport(directory, report), /exactly the configured PDF/);
  } finally {
    // Remove only this newly created, known temporary test directory.
    await fs.rm(directory, { recursive: true, force: true });
  }
});
