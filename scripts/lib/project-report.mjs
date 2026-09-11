import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

export async function verifyProjectReport(directory, report) {
  let entries;
  try { entries = await fs.readdir(directory); }
  catch (error) { if (error.code !== 'ENOENT') throw error; entries = []; }
  if (report == null) {
    if (entries.length) throw new Error('Report files exist without an enabled project report. Review the report before publication.');
    return null;
  }
  if (typeof report.title !== 'string' || !report.title.trim()
      || typeof report.file !== 'string' || !/^[a-z0-9][a-z0-9_-]*\.pdf$/i.test(report.file)
      || (report.pages != null && (!Number.isSafeInteger(report.pages) || report.pages < 1))) {
    throw new Error('Project report needs a title, a simple PDF filename and an optional positive page count.');
  }
  if (entries.length !== 1 || entries[0] !== report.file) throw new Error('The reports folder must contain exactly the configured PDF.');
  const file = path.join(directory, report.file);
  const stat = await fs.lstat(file);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('Project report must be a regular PDF file.');
  const bytes = await fs.readFile(file);
  if (!bytes.subarray(0, 5).equals(Buffer.from('%PDF-')) || !bytes.subarray(-4096).includes(Buffer.from('%%EOF'))) {
    throw new Error('Project report is not a complete PDF file.');
  }
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  if (report.sha256 && report.sha256 !== sha256) throw new Error('Project report content does not match its release hash.');
  return { title: report.title, file: report.file,
    ...(report.pages ? { pages: report.pages } : {}),
    ...(typeof report.edition === 'string' && report.edition.trim() ? { edition: report.edition.trim() } : {}),
    bytes: bytes.length, sha256 };
}
