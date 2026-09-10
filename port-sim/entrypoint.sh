#!/bin/sh
set -e

cd /app

# The legacy development Compose mounts source and a dependency volume.
# The supported demo uses image dependencies without reinstalling on startup.
if [ "${SIM_INSTALL_DEPENDENCIES:-false}" = "true" ]; then
  npm ci --no-audit --no-fund
fi

exec "$@"
