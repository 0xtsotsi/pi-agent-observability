set dotenv-load := true
set shell := ["bash", "--login", "-e", "-o", "pipefail", "-c"]

obs_port := env_var_or_default("OBS_PORT", "43190")
obs_token := env_var_or_default("OBS_AUTH_TOKEN", "$(cat ~/.gg/observability/token 2>/dev/null || echo devtoken)")
obs_url := env_var_or_default("OBS_SERVER_URL", "http://127.0.0.1:" + obs_port)
bridge_ops_port := env_var_or_default("BRIDGE_OPS_PORT", "45210")
bridge_ops_web_port := env_var_or_default("BRIDGE_OPS_WEB_PORT", "45211")

# List available project commands
default:
    @just --list

# Clear a listener from a pinned project port (private helper)
_clear-port port name:
    @pids="$(lsof -tiTCP:{{port}} -sTCP:LISTEN 2>/dev/null || true)"; \
    if [ -n "$pids" ]; then \
        echo "Clearing {{name}} port {{port}}: $pids"; \
        kill -TERM $pids 2>/dev/null || true; \
        for _ in $(seq 1 30); do \
            sleep 0.1; \
            pids="$(lsof -tiTCP:{{port}} -sTCP:LISTEN 2>/dev/null || true)"; \
            [ -z "$pids" ] && exit 0; \
        done; \
        echo "Force-clearing {{name}} port {{port}}: $pids"; \
        kill -KILL $pids 2>/dev/null || true; \
    fi

# ═══════════════════════════════════════════════════════════════════════════
#  OBSERVABILITY  —  Node + Hono + better-sqlite3
# ═══════════════════════════════════════════════════════════════════════════

# Boot the obs server only
obs:
    @just _clear-port "{{obs_port}}" observability
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_PORT="{{obs_port}}" ./node_modules/.bin/tsx server/server.ts

# Replay a single ggcoder session file into the obs DB
replay file:
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_SERVER_URL="{{obs_url}}" ./node_modules/.bin/tsx daemon/replay-cli.ts {{file}}

# Backfill all real ggcoder sessions in ~/.gg/sessions/Users_gogetta/ with size > 1KB
backfill:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    count=0
    for f in $HOME/.gg/sessions/Users_gogetta/*.jsonl; do
        size=$(wc -c < "$f")
        if [ "$size" -gt 1024 ]; then
            echo "→ $f ($size bytes)"
            OBS_AUTH_TOKEN="{{obs_token}}" OBS_SERVER_URL="{{obs_url}}" ./node_modules/.bin/tsx daemon/replay-cli.ts "$f" 2>&1 | tail -1
            count=$((count + 1))
        fi
    done
    echo "backfilled $count session(s)"

# ═══════════════════════════════════════════════════════════════════════════
#  BRIDGE-OPS  —  Twenty+my-app+bridge operator scenario
# ═══════════════════════════════════════════════════════════════════════════

# Boot the bridge-ops backend only
bridge-ops-server:
    @just _clear-port "{{bridge_ops_port}}" bridge-ops-api
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_SERVER_URL="{{obs_url}}" BRIDGE_OPS_PORT="{{bridge_ops_port}}" ./node_modules/.bin/tsx apps/bridge-ops/server/server.ts

# Boot the bridge-ops web (static file server on a separate port)
bridge-ops-web:
    @just _clear-port "{{bridge_ops_web_port}}" bridge-ops-web
    @echo "bridge-ops web at http://127.0.0.1:{{bridge_ops_web_port}}/"
    @cd apps/bridge-ops/web && python3 -m http.server {{bridge_ops_web_port}} --bind 127.0.0.1

# Boot bridge-ops backend + web together
bridge-ops:
    @just _clear-port "{{bridge_ops_port}}" bridge-ops-api
    @just _clear-port "{{bridge_ops_web_port}}" bridge-ops-web
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_SERVER_URL="{{obs_url}}" BRIDGE_OPS_PORT="{{bridge_ops_port}}" ./node_modules/.bin/tsx apps/bridge-ops/server/server.ts &
    @sleep 2
    @cd apps/bridge-ops/web && python3 -m http.server {{bridge_ops_web_port}} --bind 127.0.0.1 &
    @sleep 1
    @echo "bridge-ops web: http://127.0.0.1:{{bridge_ops_web_port}}/"
    @echo "bridge-ops api: http://127.0.0.1:{{bridge_ops_port}}/"
    @wait

# ═══════════════════════════════════════════════════════════════════════════
#  ALL  —  boot obs + bridge-ops together (matches pi's just all surface)
# ═══════════════════════════════════════════════════════════════════════════

# Boot obs + bridge-ops + bridge-ops web. launchd already runs the chokidar daemon.
all:
    @just _clear-port "{{obs_port}}" observability
    @just _clear-port "{{bridge_ops_port}}" bridge-ops-api
    @just _clear-port "{{bridge_ops_web_port}}" bridge-ops-web
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_PORT="{{obs_port}}" ./node_modules/.bin/tsx server/server.ts &
    @OBS_AUTH_TOKEN="{{obs_token}}" OBS_SERVER_URL="{{obs_url}}" BRIDGE_OPS_PORT="{{bridge_ops_port}}" ./node_modules/.bin/tsx apps/bridge-ops/server/server.ts &
    @sleep 2
    @cd apps/bridge-ops/web && python3 -m http.server {{bridge_ops_web_port}} --bind 127.0.0.1 &
    @sleep 1
    @echo
    @echo "All services up:"
    @echo "  obs server:    http://127.0.0.1:{{obs_port}}/?token={{obs_token}}"
    @echo "  bridge-ops web: http://127.0.0.1:{{bridge_ops_web_port}}/"
    @echo "  bridge-ops api: http://127.0.0.1:{{bridge_ops_port}}/"
    @wait

# ═══════════════════════════════════════════════════════════════════════════
#  EXTRA  —  validate, backup
# ═══════════════════════════════════════════════════════════════════════════

# Smoke test: POST a fake event, GET /sessions, confirm round-trip
validate:
    #!/usr/bin/env bash
    set -euo pipefail
    eid="validate-$(date +%s)"
    echo "→ POST /events with event_id=$eid"
    resp=$(curl -sS -X POST "http://127.0.0.1:{{obs_port}}/events" \
        -H "Authorization: Bearer {{obs_token}}" \
        -H "Content-Type: application/json" \
        -d "{\"event_id\":\"$eid\",\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"type\":\"session_start\",\"session_id\":\"$eid\",\"cwd\":\"/tmp\",\"pool\":\"validate\",\"tags\":[],\"payload\":{},\"seq\":0}")
    echo "  $resp"
    echo "→ GET /sessions"
    curl -sS -H "Authorization: Bearer {{obs_token}}" "http://127.0.0.1:{{obs_port}}/sessions" \
        | python3 -c 'import json,sys;d=json.load(sys.stdin);print(f"  {len(d[\"sessions\"])} sessions, {sum(s[\"event_count\"] for s in d[\"sessions\"])} events total")'
    echo "✓ round-trip ok"

# Create a timestamped backup of the obs database
backup:
    #!/usr/bin/env bash
    set -euo pipefail
    db="$HOME/.gg/observability/obs.db"
    mkdir -p "$HOME/.gg/observability/backups"
    if [ ! -f "$db" ]; then
        echo "✗ No active database at $db found to back up." >&2
        exit 1
    fi
    ts=$(date +"%Y%m%d_%H%M%S")
    if command -v sqlite3 >/dev/null 2>&1; then
        sqlite3 "$db" ".backup '$HOME/.gg/observability/backups/obs_backup_${ts}.db'"
        echo "✓ safe backup: $HOME/.gg/observability/backups/obs_backup_${ts}.db"
    else
        cp "$db" "$HOME/.gg/observability/backups/obs_backup_${ts}.db"
        echo "✓ file-copy backup: $HOME/.gg/observability/backups/obs_backup_${ts}.db"
    fi