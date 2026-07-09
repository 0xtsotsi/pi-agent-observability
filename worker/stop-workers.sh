#!/usr/bin/env bash
# stop-workers.sh — tear down the per-agent worker daemons.

set -uo pipefail

LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
AGENTS=(noledge gg-obs demoshots my-app)

for a in "${AGENTS[@]}"; do
  plist="$LAUNCH_AGENTS/com.gogetta.worker.$a.plist"
  if [ ! -f "$plist" ]; then continue; fi
  echo "[stop-workers] $a"
  launchctl unload "$plist" 2>/dev/null || true
done

echo "[stop-workers] status:"
launchctl list 2>/dev/null | grep "com.gogetta.worker" || echo "  (none running)"
