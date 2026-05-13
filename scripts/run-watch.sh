#!/usr/bin/env bash
# PM2 entrypoint for `tg-extract watch`.
#
# Why a wrapper instead of running the binary directly:
# - PM2 has no built-in .env loader. We source the file here so secrets
#   (TG_API_ID, TG_API_HASH) stay in .env and aren't baked into
#   ecosystem.config.cjs.
# - `exec` replaces the shell so PM2 sees the binary's PID directly —
#   signals (SIGTERM on `pm2 stop`) reach grammers without an extra hop.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "fatal: $ROOT/.env missing (TG_API_ID, TG_API_HASH required)" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
. ./.env
set +a

: "${TG_API_ID:?TG_API_ID must be set in .env}"
: "${TG_API_HASH:?TG_API_HASH must be set in .env}"

BIN="$ROOT/target/release/tg-extract"
if [[ ! -x "$BIN" ]]; then
  echo "fatal: $BIN not found — run \`cargo build --release\` first" >&2
  exit 1
fi

exec "$BIN" watch
