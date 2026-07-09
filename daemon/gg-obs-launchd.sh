#!/usr/bin/env bash
#
# gg-obs-launchd.sh — shell wrapper that launches the ggcoder observability
# daemon (gg-obs-replay.ts) under launchd. Resolves the auth token from
# ~/.gg/observability/token (or env) and execs tsx on the daemon.
#
# Usage (manual):
#   ./daemon/gg-obs-launchd.sh
#
# Note for launchd: macOS launchd often rejects bash script executables
# with "Operation not permitted" under the gui/ domain (ProcessSpawningDisallowed
# / TCC). The shipped plist invokes node + tsx loader directly via
# ProgramArguments and uses this script only for interactive use.

set -euo pipefail

# ─── Resolve repo root (parent of daemon/) ──────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_MODULES="${REPO_ROOT}/node_modules"

# ─── Load token from disk if not already set ───────────────────────────────
TOKEN_FILE="${HOME}/.gg/observability/token"
if [[ -z "${OBS_AUTH_TOKEN:-}" && -f "${TOKEN_FILE}" ]]; then
  OBS_AUTH_TOKEN="$(tr -d '[:space:]' < "${TOKEN_FILE}")"
  export OBS_AUTH_TOKEN
fi

if [[ -z "${OBS_AUTH_TOKEN}" ]]; then
  echo "[gg-obs-launchd] OBS_AUTH_TOKEN not set and ${TOKEN_FILE} not found — exiting." >&2
  exit 1
fi

# ─── Defaults (overridable via env) ────────────────────────────────────────
export OBS_SERVER_URL="${OBS_SERVER_URL:-http://127.0.0.1:43190}"
export OBS_WATCH_DIR="${OBS_WATCH_DIR:-${HOME}/.gg/sessions}"
export OBS_QUIET="${OBS_QUIET:-1}"

# ─── Hand off to node + tsx loader directly (skip yarn PnP) ───────────────
# Calling yarn from launchd breaks because yarn can't resolve the workspace
# (cache/ has symlinks, not a real workspace). tsx is a devDep in node_modules
# so we invoke its loader via node's --import hook.
cd "${REPO_ROOT}"
export NODE_PATH="${NODE_MODULES}:${NODE_PATH:-}"
# Allow override for users with node elsewhere (e.g. /opt/homebrew/bin/node)
exec "${NODE_BIN:-/usr/local/bin/node}" \
  --require "${NODE_MODULES}/tsx/dist/preflight.cjs" \
  --import "file://${NODE_MODULES}/tsx/dist/loader.mjs" \
  "${REPO_ROOT}/daemon/gg-obs-replay.ts" \
  --watch-dir="${OBS_WATCH_DIR}" \
  --server-url="${OBS_SERVER_URL}" \
  --token="${OBS_AUTH_TOKEN}" \
  --quiet