#!/usr/bin/env bash
# start-workers.sh — bring up the per-agent worker daemons under launchd.
# Idempotent: unloads + reloads each plist.

set -uo pipefail

LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
WORKER_DIR="$(cd "$(dirname "$0")" && pwd)"

AGENTS=(noledge gg-obs demoshots my-app)

for a in "${AGENTS[@]}"; do
  plist="$LAUNCH_AGENTS/com.gogetta.worker.$a.plist"
  echo "[start-workers] $a"
  if [ ! -f "$plist" ]; then
    echo "  plist not found at $plist — run worker/install-plists.sh first"
    continue
  fi
  # Unload if already loaded (idempotent)
  launchctl unload "$plist" 2>/dev/null || true
  # Load (RunAtLoad:true so launchd starts it immediately)
  launchctl load "$plist"
done

echo ""
echo "[start-workers] status:"
launchctl list 2>/dev/null | grep "com.gogetta.worker" || echo "  (none running)"
