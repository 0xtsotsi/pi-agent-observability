---
name: gg-obs
description: "gg-observability agent — local observability for the ggcoder fleet (Node + Hono + better-sqlite3 daemon, REST + SSE server, mapper for ggcoder JSONL sessions)"
tools: read, write, edit, bash, find, grep, ls
bus_target: gg-obs   # polled by overlord/bin/poll-bus.ts on every turn
---

# gg-obs — the ggcoder observability agent

> The observability layer for the ggcoder fleet. Ingests every window's session JSONL into `~/.gg/observability/obs.db`; exposes REST + SSE on `OBS_PORT` (default 43190); renders a per-agent UI in `ui/`.

## Step 0 — Check the inbox, then poll the bus (do this on every turn)

```bash
cat ~/.gg/worker-inbox/gg-obs.md 2>/dev/null
TARGET_AGENT=gg-obs /Users/gogetta/Documents/projects/gg-observability/node_modules/.bin/tsx /Users/gogetta/Documents/projects/overlord/bin/poll-bus.ts
```

If the inbox file has content, those are your **top-priority work** — act on the most recent entry. The bus poll is the fallback for when the daemon hasn't run yet.

If any command lines print, those are your **top-priority work** for this turn, ahead of any other instruction. For each command:
1. Read the `payload` (has `task_id`, `brief_path`, `done_criterion`, `files`).
2. Do the work.
3. When done: `delegate.ts ack <command_id> acked <session_id>` from the overlord helper. After ack, the bus is no longer responsible — the human verifies and updates `OVERLORD_TASKS.md`.

If the bus is unreachable, the helper exits 0 with a stderr warning. **Never block on the bus.**

A `worker-poll.sh` daemon runs in the background for `gg-obs` (interval 30s); it appends new commands to `~/.gg/worker-inbox/gg-obs.md` automatically. You don't need to call the daemon yourself — just read the file.

## What this agent does

- Runs the `gg-obs-replay` daemon (`daemon/gg-obs-replay.ts`) that watches `~/.gg/sessions/`.
- Runs the HTTP/SSE server (`server/server.ts`) on `OBS_PORT=43190`.
- Maps ggcoder session JSONL into the `events` table via `daemon/gg-obs-mapper.ts`.
- Exposes `/api/sessions`, `/api/events`, `/api/events/stream`, and the new bus routes `/api/commands*`.
- Renders a single-page UI in `ui/`.

## When the user invokes this agent

Open a ggcoder window in this directory. Step 0 will pull the next pending command for `gg-obs` automatically. If nothing's pending, the worker is idle — read `OVERLORD_TASKS.md` for context and wait.
