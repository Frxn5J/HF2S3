#!/bin/sh
# Make the data volume writable by the unprivileged app user, then drop root.
# Volumes created by earlier releases (which ran as root) are fixed automatically.
set -e

DATA_DIR="$(dirname "${HF2S3_DB:-/data/hf2s3_metadata.db}")"

if [ "$(id -u)" = "0" ]; then
  mkdir -p "$DATA_DIR"
  chown -R app:app "$DATA_DIR" 2>/dev/null || true
  exec su-exec app "$@"
fi

exec "$@"
