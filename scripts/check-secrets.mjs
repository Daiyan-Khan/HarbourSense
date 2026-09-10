#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { ROOT } from './demo.mjs';

export function detectSecrets(text) {
  const findings = [];
  const patterns = [
    ['private-key', /-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----/g],
    ['github-token', /\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})\b/g],
    ['aws-access-key', /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g],
    ['database-credentials', /mongodb(?:\+srv)?:\/\/([^\s"'`<>]+)@/g],
  ];
  for (const [kind, pattern] of patterns) {
    for (const match of text.matchAll(pattern)) {
      if (kind === 'database-credentials' && (/\$\{|process\.env|<|>/.test(match[1])
          || /^(?:your[-_]username|username|example[-_]user|placeholder):(?:your[-_]password|password|placeholder)$/i.test(match[1]))) continue;
      findings.push({ kind, line: text.slice(0, match.index).split('\n').length });
    }
  }
  return findings;
}

export async function scanSource() {
  const result = spawnSync('git', ['-c', `safe.directory=${ROOT.replaceAll('\\', '/')}`, 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], { cwd: ROOT, encoding: 'utf8', windowsHide: true, maxBuffer: 20 * 1024 * 1024 });
  if (result.status !== 0) throw new Error('Cannot inventory source files for the credential check.');
  const files = [...new Set(result.stdout.split('\0').filter(Boolean))];
  const findings = [];
  for (const file of files) {
    let bytes;
    try { bytes = await fs.readFile(path.join(ROOT, file)); } catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    if (bytes.includes(0)) continue;
    for (const finding of detectSecrets(bytes.toString('utf8'))) findings.push({ file, ...finding });
  }
  if (findings.length) {
    for (const item of findings) console.error(`${item.file}:${item.line}: possible ${item.kind} (value redacted)`);
    throw new Error(`${findings.length} credential-shaped values found. Review before publication.`);
  }
  console.log(`SECRET CHECK PASS: ${files.length} source files; credential patterns checked without treating URI validators as secrets.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  scanSource().catch(error => { console.error(error.message); process.exitCode = 1; });
}
