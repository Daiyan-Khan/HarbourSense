#!/usr/bin/env node
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { ROOT } from './demo.mjs';
import { normalizeBasePath } from './lib/demo-artifacts.mjs';

export function resolveAsset(directory, base, requestUrl) {
  let pathname;
  try { pathname = decodeURIComponent(new URL(requestUrl, 'http://preview.local').pathname); } catch { return null; }
  if (pathname === base) return { redirect: `${base}/` };
  if (!pathname.startsWith(`${base}/`) || pathname.includes('\\') || pathname.includes('\0')) return null;
  const suffix = pathname.slice(base.length + 1) || 'index.html';
  const resolved = path.resolve(directory, suffix);
  if (!resolved.startsWith(path.resolve(directory) + path.sep)) return null;
  return { file: resolved };
}

export async function serveDemo({ directory = path.join(ROOT, 'dashboard', 'visualizer', 'build'), port = 4173 } = {}) {
  const metadata = JSON.parse(await fs.readFile(path.join(directory, 'demo-build.json'), 'utf8'));
  const base = normalizeBasePath(metadata.basePath || '/');
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.svg': 'image/svg+xml', '.png': 'image/png', '.ico': 'image/x-icon', '.webm': 'video/webm', '.pdf': 'application/pdf' };
  const server = http.createServer(async (request, response) => {
    if (!['GET', 'HEAD'].includes(request.method)) { response.writeHead(405).end(); return; }
    const target = resolveAsset(directory, base, request.url);
    if (!target) { response.writeHead(404).end('Not found'); return; }
    if (target.redirect) { response.writeHead(302, { Location: target.redirect }).end(); return; }
    try {
      const content = await fs.readFile(target.file);
      response.writeHead(200, { 'Content-Type': types[path.extname(target.file)] || 'application/octet-stream',
        'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
      response.end(request.method === 'HEAD' ? undefined : content);
    } catch { response.writeHead(404).end('Not found'); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  console.log(`Portfolio demo: http://127.0.0.1:${server.address().port}${base}/`);
  return server;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  serveDemo({ port: Number(process.env.DEMO_PREVIEW_PORT || 4173) }).catch(error => {
    console.error(`DEMO PREVIEW FAILED: ${error.message}`); process.exitCode = 1;
  });
}
