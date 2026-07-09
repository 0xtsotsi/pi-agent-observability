#!/usr/bin/env bash
# install-plists.sh — symlink the 4 worker plists from this directory into
# ~/Library/LaunchAgents so launchd can find them.

set -uo pipefail

WORKER_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
mkdir -p "$LAUNCH_AGENTS"

AGENTS=(noledge gg-obs demoshots my-app)

for a in "${AGENTS[@]}"; do
  src="$WORKER_DIR/launchd/com.gogetta.worker.$a.plist"
  dst="$LAUNCH_AGENTS/com.gogetta.worker.$a.plist"
  if [ ! -f "$src" ]; then
    echo "[install-plists] missing $src"
    continue
  fi
  ln -sf "$src" "$dst"
  echo "[install-plists] linked $a"
done
