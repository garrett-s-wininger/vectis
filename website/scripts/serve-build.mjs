#!/usr/bin/env node

/**
 * Minimal static file server for the Docusaurus `build/` output.
 *
 * Used by Playwright axe tests (avoids tools that enumerate all network interfaces).
 */
import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..', 'build');
const port = Number(process.env.A11Y_STATIC_PORT || 4287);
const host = '127.0.0.1';

const mimeTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'application/javascript; charset=utf-8'],
  ['.mjs', 'application/javascript; charset=utf-8'],
  ['.json', 'application/json'],
  ['.css', 'text/css; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.png', 'image/png'],
  ['.jpg', 'image/jpeg'],
  ['.jpeg', 'image/jpeg'],
  ['.ico', 'image/x-icon'],
  ['.woff2', 'font/woff2'],
  ['.woff', 'font/woff'],
  ['.ttf', 'font/ttf'],
  ['.map', 'application/json'],
  ['.txt', 'text/plain; charset=utf-8'],
  ['.xml', 'application/xml'],
  ['.webmanifest', 'application/manifest+json'],
]);

function contentType(filePath) {
  return mimeTypes.get(path.extname(filePath).toLowerCase()) ?? 'application/octet-stream';
}

async function tryFile(abs) {
  try {
    const st = await fsp.stat(abs);

    if (!st.isFile()) {
      return null;
    }

    return abs;
  } catch {
    return null;
  }
}

function safeAbsolute(...segments) {
  const abs = path.resolve(path.join(root, ...segments));

  if (abs === root || abs.startsWith(root + path.sep)) {
    return abs;
  }

  return null;
}

async function resolveFile(urlPath) {
  const pathname = new URL(urlPath, `http://${host}`).pathname;
  const segments = decodeURIComponent(pathname).split('/').filter(Boolean);

  if (segments.length === 0) {
    return tryFile(path.join(root, 'index.html'));
  }

  const base = safeAbsolute(...segments);
  if (!base) {
    return null;
  }

  let found = await tryFile(base);
  if (found) {
    return found;
  }

  found = await tryFile(path.join(base, 'index.html'));
  if (found) {
    return found;
  }

  return tryFile(`${base}.html`);
}

const server = http.createServer((req, res) => {
  void (async () => {
    if (!req.url || req.method !== 'GET') {
      res.writeHead(405);
      res.end();
      return;
    }

    const filePath = await resolveFile(req.url);
    if (!filePath) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }

    res.setHeader('Content-Type', contentType(filePath));
    fs.createReadStream(filePath).pipe(res);
  })().catch(() => {
    res.writeHead(500);
    res.end();
  });
});

server.listen(port, host, () => {
  process.stdout.write(`a11y static: http://${host}:${port}/\n`);
});
