#!/bin/sh
set -e

cd /app

# The source bind mount + named node_modules volume can hide image-built deps or
# keep a stale volume after package.json/lockfile changes. Sync on every start.
npm ci

exec "$@"
