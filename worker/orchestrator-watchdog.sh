#!/usr/bin/env bash
# orchestrator-watchdog.sh — keeps the overlord orchestrator loop alive.
#
# Sister copy of overlord/bin/orchestrate-watchdog.sh, installed here so the
# gg-observability/worker/ directory owns the full daemon fleet lifecycle.
# Both paths exist intentionally: overlord/bin/ is the source of truth;
# gg-observability/worker/ is where the rest of the worker daemons live
# (worker-poll.sh, worker-deep-work.sh, worker.ts). Operators may symlink or
# pick whichever fits their launchctl plist setup.
#
# Usage:
#   /Users/gogetta/Documents/projects/gg-observability/worker/orchestrator-watchdog.sh &
#
# Behavior: polls for the orchestrator process every 30s and restarts it on
# death. Writes PID to ~/.gg/bg/orchestrator.pid, log to
# ~/.gg/bg/orchestrator-watchdog.log.

set -uo pipefail

LOG="$HOME/.gg/bg/orchestrator-watchdog.log"
SCRIPT="/Users/gogetta/Documents/projects/overlord/bin/orchestrate.ts"
TSX="/Users/gogetta/Documents/projects/gg-observability/node_modules/.bin/tsx"
PID_FILE="$HOME/.gg/bg/orchestrator.pid"
OUT="$HOME/.gg/bg/orchestrator.out"
INTERVAL="${ORCHESTRATE_INTERVAL_SEC:-15}"

mkdir -p "$(dirname "$LOG")" 2>/dev/null || true

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG"; }

is_alive() {
  pgrep -f "tsx.*orchestrate.ts" >/dev/null 2>&1
}

start() {
  log "starting orchestrator"
  ORCHESTRATE_INTERVAL_SEC="$INTERVAL" nohup "$TSX" "$SCRIPT" \
    >"$OUT" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 2
  if is_alive; then
    log "orchestrator up (pid=$(cat "$PID_FILE"))"
  else
    log "orchestrator FAILED to start; see $OUT"
  fi
}

log "orchestrator-watchdog starting (interval=${INTERVAL}s)"
if ! is_alive; then
  start
fi

while true; do
  sleep 30
  if ! is_alive; then
    log "orchestrator not running; restarting"
    start
  fi
done