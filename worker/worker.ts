#!/usr/bin/env -S node --experimental-strip-types --import "file:///Users/gogetta/Documents/projects/gg-observability/node_modules/tsx/dist/loader.mjs"
/**
 * worker.ts — per-target-agent headless worker daemon.
 *
 * Usage:
 *   AGENT=noledge node --experimental-strip-types worker/worker.ts
 *
 * Loop:
 *   1. Watch bus for new commands targeted at $AGENT (chokidar on obs.db + 30s poll)
 *   2. For each: load agent's CLAUDE.md as system prompt, run agent loop via
 *      runPrintMode(), which calls the ggcoder SDK's AgentSession
 *   3. The agent's own recipe in its CLAUDE.md tells it to read the inbox,
 *      act on tasks, and ack via delegate.ts
 *   4. Log everything to ~/.gg/worker-logs/<agent>.log
 *
 * Idempotent: restarts re-resume from the last-seen command seq (cursor file).
 * Supervised by launchd (separate plist per agent).
 *
 * SDK surface verified against
 *   /Users/gogetta/Library/Mobile Documents/com~apple~CloudDocs/Documents/ggcoder-pwa/node_modules/@kenkaiiii/ggcoder/dist/
 *     modes/print-mode.d.ts:7-15   (PrintModeOptions)
 *     modes/print-mode.js:13       (runPrintMode implementation)
 *     core/agent-session.d.ts:8-19 (AgentSessionOptions — all 10 fields verified)
 *     core/agent-session.d.ts:65    (AgentSession.prompt one-shot)
 *     core/agent-session.d.ts:28    (private tools; no public setter)
 */

import { runPrintMode, AuthStorage } from "@kenkaiiii/ggcoder";
// NotLoggedInError is declared in core/auth-storage.d.ts but NOT re-exported
// from the package-root index.js. We duck-type it via a message check instead.
import {
  readFileSync,
  writeFileSync,
  appendFileSync,
  existsSync,
  mkdirSync,
} from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import chokidar from "chokidar";
import { randomUUID } from "node:crypto";

const AGENT = process.env.AGENT;
if (!AGENT) {
  console.error("AGENT env var is required (e.g. AGENT=noledge)");
  process.exit(2);
}

const HOME = homedir();
const SAFE = AGENT.toLowerCase().replace(/[^a-z0-9-]/g, "-");
// Per-agent project path map. Most agents live under ~/Documents/projects/<agent>,
// but my-app lives at ~/my-app/ and gg-obs IS this repo (~/Documents/projects/gg-observability/).
const PROJECT_CWD_BY_AGENT: Record<string, string> = {
  "my-app": join(HOME, "my-app"),
  "gg-obs": join(HOME, "Documents", "projects", "gg-observability"),
};
const PROJECT_CWD = PROJECT_CWD_BY_AGENT[AGENT] ?? join(HOME, "Documents", "projects", AGENT);
const CLAUDE_MD = join(PROJECT_CWD, "CLAUDE.md");
const OBS_BASE = process.env.OBS_BASE_URL ?? "http://127.0.0.1:43190";
const TOKEN_FILE = join(HOME, ".gg/observability/token");
const CURSOR_FILE = join(HOME, `.gg/worker-${SAFE}-cursor.json`);
const OBS_DB = join(HOME, ".gg/observability/obs.db");
const LOG_DIR = join(HOME, ".gg/worker-logs");
const LOG_FILE = join(LOG_DIR, `${SAFE}.log`);
const INBOX_FILE = join(HOME, ".gg/worker-inbox", `${SAFE}.md`);
const ACK_TIMEOUT_MS = 5_000;
const POLL_INTERVAL_MS = 30_000;
const EMIT_TIMEOUT_MS = 5_000;
// Per-agent session_id: stable per UTC day so (session_id, seq) uniqueness holds across restarts.
const SESSION_ID = `worker-${SAFE}-${new Date().toISOString().slice(0, 10)}`;

// ggcoder provider auth (minimax, openai) lives in ~/.gg/auth.json.
// This file is written by the ggcoder GUI app when the user logs in.
// launchd workers run as the same user as the GUI, so they can read the file
// if mode is 0600. We probe at startup and warn loudly if auth is missing
// or expired — a worker that runs without auth will fail every runPrintMode
// call with NotLoggedInError, and the bus ack will be `failed`. Better to
// fail fast at startup than spam the bus with failed tasks.
async function probeProviderAuth(): Promise<void> {
  const auth = new AuthStorage();
  try {
    await auth.load();
    const provider = process.env.OBS_PROVIDER ?? "minimax";
    try {
      const creds = await auth.resolveCredentials(provider);
      if (!creds) {
        log(`FATAL: no credentials for provider "${provider}". Log in via the ggcoder GUI app, then \`launchctl kickstart -k gui/$(id -u)/com.gogetta.worker.${SAFE}\``);
        // Fail fast: launchd's KeepAlive will respawn us after the user logs in.
        // This prevents the worker from running on every tick and spamming the
        // bus with failed-task acks. The respawn loop is the recovery path.
        process.exit(1);
      } else {
        log(`provider auth OK for ${provider} (expires ${new Date(creds.expiresAt).toISOString()})`);
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      // ggcoder's NotLoggedInError message is "Not logged in to <provider>".
      if (/Not logged in to /.test(msg)) {
        log(`FATAL: not logged in. Log in via the ggcoder GUI app, then \`launchctl kickstart -k gui/$(id -u)/com.gogetta.worker.${SAFE}\``);
        // Fail fast: same respawn-loop recovery path.
        process.exit(1);
      } else {
        log(`WARN: auth probe error: ${msg}`);
      }
    }
  } catch (e: unknown) {
    log(`WARN: auth file probe failed: ${e instanceof Error ? e.message : String(e)}`);
  }
}

mkdirSync(LOG_DIR, { recursive: true });

if (!existsSync(CLAUDE_MD)) {
  // Some agents (my-app, gg-obs in some future state) may not have a project CLAUDE.md.
  // Fall back to a minimal headless preamble so the worker still runs.
  log(`CLAUDE.md not found at ${CLAUDE_MD}; using fallback preamble`);
}

function readToken(): string {
  if (process.env.OBS_AUTH_TOKEN) return process.env.OBS_AUTH_TOKEN;
  if (existsSync(TOKEN_FILE)) return readFileSync(TOKEN_FILE, "utf8").trim();
  return "devtoken";
}

function log(line: string): void {
  const out = `[${new Date().toISOString()}] [${AGENT}] ${line}\n`;
  process.stdout.write(out);
  try {
    appendFileSync(LOG_FILE, out);
  } catch {
    // best effort
  }
}

type Cursor = { lastSeenSeq: number; updated_at: string };

function readCursor(): number {
  if (!existsSync(CURSOR_FILE)) return 0;
  try {
    const c = JSON.parse(readFileSync(CURSOR_FILE, "utf8")) as Cursor;
    return typeof c.lastSeenSeq === "number" ? c.lastSeenSeq : 0;
  } catch {
    return 0;
  }
}

function writeCursor(seq: number): void {
  const payload: Cursor = { lastSeenSeq: seq, updated_at: new Date().toISOString() };
  try {
    writeFileSync(CURSOR_FILE, JSON.stringify(payload, null, 2));
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    log(`writeCursor failed: ${msg}`);
  }
}


/**
 * Per-process monotonic seq counter for this worker's events.
 * The DB has UNIQUE(session_id, seq); SESSION_ID is stable per day so a
 * restart on the same day continues the counter (the cursor in worker-*.json
 * covers commands, but emit() uses an independent counter since it does not
 * write to the commands table).
 */
let workerSeq = 0;

function nextWorkerSeq(): number {
  workerSeq += 1;
  return workerSeq;
}

/**
 * Fire-and-forget POST to obs. Never throws — failures log a warning. We
 * intentionally do NOT block the tick / processCommand loop on obs availability;
 * observability traffic is best-effort by design.
 */
async function emit(type: string, payload: Record<string, unknown>): Promise<void> {
  const token = readToken();
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), EMIT_TIMEOUT_MS);
  const body = JSON.stringify({
    event_id: `worker-${randomUUID()}`,
    session_id: SESSION_ID,
    seq: nextWorkerSeq(),
    ts: new Date().toISOString(),
    type,
    pool: "default",
    tags: ["worker", AGENT],
    payload,
    provider: "worker",
    model: null,
    agent_name: AGENT,        // ui/worker-dashboard.js filters on this
    cwd: PROJECT_CWD,         // shown in the UI per-agent card
    session_file: process.env.OBS_SESSION_FILE ?? "",  // link to the raw JSONL
  });
  try {
    const resp = await fetch(`${OBS_BASE}/events`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body,
      signal: ctrl.signal,
    });
    if (!resp.ok) {
      log(`emit ${type} http=${resp.status}`);
    } else {
      log(`emit OK ${type}`);
    }
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    log(`emit ${type} dropped: ${msg}`);
  } finally {
    clearTimeout(timer);
  }
}

type BusCommand = {
  command_id: string;
  seq: number;
  payload_json?: string;
};

async function ackCommand(commandId: string, state: "acked" | "failed", sessionId: string, errorPayload?: unknown): Promise<void> {
  const token = readToken();
  const body = {
    ack_state: state,
    ack_session_id: sessionId,
    ...(errorPayload !== undefined ? { ack_payload: errorPayload } : {}),
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ACK_TIMEOUT_MS);
  try {
    const resp = await fetch(`${OBS_BASE}/commands/${commandId}/ack`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!resp.ok) {
      log(`ack failed: ${resp.status} ${await resp.text().catch(() => "")}`);
    } else {
      log(`ack ${state} command_id=${commandId}`);
      void emit("worker.command_acked", { command_id: commandId, state });
    }
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    log(`ack error: ${msg}`);
  } finally {
    clearTimeout(timer);
  }
}

async function processCommand(cmd: BusCommand): Promise<void> {
  const { command_id, seq } = cmd;
  const startMs = Date.now();
  log(`processing command_id=${command_id} seq=${seq}`);
  void emit("worker.command_started", {
    command_id,
    seq,
  });

  let payload: Record<string, unknown> = {};
  try {
    payload = JSON.parse(cmd.payload_json ?? "{}") as Record<string, unknown>;
  } catch {
    log(`command_id=${command_id} payload_json not parseable; using empty`);
  }

  const claudeContent = existsSync(CLAUDE_MD) ? readFileSync(CLAUDE_MD, "utf8") : "";
  const ackCmd = `TARGET_AGENT=${AGENT} ${join(HOME, "Documents", "projects", "gg-observability", "node_modules", ".bin", "tsx")} ${join(HOME, "Documents", "projects", "overlord", "bin", "delegate.ts")} ack ${command_id} acked worker-${SAFE}`;

  const workerPreamble = `[Worker preamble] You are a HEADLESS worker for agent "${AGENT}".\n- Project cwd: ${PROJECT_CWD}\n- Inbox file: ${INBOX_FILE}\n- To ack a command: \`${ackCmd}\`\n- Do NOT ask for user confirmation; act and ack.\n- The bash tool is auto-discovered; use it to run the ack command above.`;
  const systemPrompt = claudeContent
    ? `${claudeContent}\n\n---\n\n${workerPreamble}`
    : workerPreamble;

  const userMessage = `## Bus command
- task_id: \`${String(payload.task_id ?? "?")}\`
- command_id: \`${command_id}\`
- title: ${String(payload.title ?? "?")}
- files: ${JSON.stringify(payload.files ?? [])}
- done_criterion: ${String(payload.done_criterion ?? "")}
- brief_path: ${String(payload.brief_path ?? "")}

Read ${INBOX_FILE} and execute the task. Ack via delegate.ts when done.`;

  const provider = (process.env.OBS_PROVIDER ?? "minimax") as Parameters<typeof runPrintMode>[0]["provider"];
  const model = process.env.OBS_MODEL ?? "MiniMax-M3";

  try {
    await runPrintMode({
      message: userMessage,
      provider,
      model,
      cwd: PROJECT_CWD,
      systemPrompt,
    });
    log(`runPrintMode completed for command_id=${command_id}`);
    void emit("worker.command_completed", {
      command_id,
      task_id: String(payload.task_id ?? "?"),
      duration_ms: Date.now() - startMs,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    log(`runPrintMode FAILED for command_id=${command_id}: ${msg}`);
    void emit("worker.command_failed", {
      command_id,
      task_id: String(payload.task_id ?? "?"),
      error: msg,
      duration_ms: Date.now() - startMs,
    });
    await ackCommand(command_id, "failed", `worker-${SAFE}`, { error: msg });
  }
}

// pollInFlight is a per-tick latch that guards against overlapping polls.
// Every await in the poll path is wrapped in a Promise.race against a
// setTimeout-based timeout, so a stalled fetch or json() can never leave
// pollInFlight=true forever. ACK_TIMEOUT_MS (5s) is the deadline; the
// tick returns early on timeout so the next setInterval tick can retry.
let pollInFlight = false;
async function tick(): Promise<void> {
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    const token = readToken();
    const since = readCursor();
    const url = `${OBS_BASE}/commands?target=${encodeURIComponent(SAFE)}&status=pending&since=${since}&limit=5`;

    // Hard-cap every step of the bus poll. Whoever loses the race is dropped.
    const fetchOrTimeout = new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`tick fetch hard-timeout after ${ACK_TIMEOUT_MS}ms`)), ACK_TIMEOUT_MS),
    );
    let resp: Response;
    try {
      resp = (await Promise.race([
        fetch(url, { headers: { authorization: `Bearer ${token}` } }),
        fetchOrTimeout,
      ])) as Response;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`tick fetch aborted: ${msg} (next tick in ${POLL_INTERVAL_MS}ms will retry)`);
      return;
    }
    if (!resp.ok) {
      log(`bus poll failed: ${resp.status}`);
      return;
    }
    // json() can also hang; cap it the same way.
    let data: { commands?: BusCommand[] };
    try {
      data = (await Promise.race([
        resp.json(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`tick json hard-timeout after ${ACK_TIMEOUT_MS}ms`)), ACK_TIMEOUT_MS),
        ),
      ])) as { commands?: BusCommand[] };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`tick json aborted: ${msg}`);
      return;
    }
    const cmds = data.commands ?? [];
    if (process.env.WORKER_VERBOSE === "1") {
      void emit("worker.tick", { pending_count: cmds.length, since });
    }
    if (cmds.length > 0) log(`tick: ${cmds.length} pending command(s)`);
    for (const cmd of cmds) {
      let payload: Record<string, unknown> = {};
      try { payload = JSON.parse(cmd.payload_json ?? "{}") as Record<string, unknown>; } catch { /* empty */ }
      void emit("worker.command_received", {
        command_id: cmd.command_id,
        task_id: String(payload.task_id ?? "?"),
        seq: cmd.seq,
      });
      await processCommand(cmd);
      writeCursor(cmd.seq);
    }
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    log(`tick error: ${msg}`);
  } finally {
    pollInFlight = false;
  }
}

// Polling loop: chokidar on obs.db mtime as primary trigger; 30s setInterval
// as safety net. With the hard-timeout race inside tick() (above), no single
// fetch can wedge the loop, so a straightforward setInterval is correct here.
chokidar
  .watch(OBS_DB, { ignoreInitial: true })
  .on("change", () => {
    void tick();
  })
  .on("add", () => {
    void tick();
  });

const pollHandle = setInterval(() => {
  void tick();
}, POLL_INTERVAL_MS);

let shuttingDown = false;

function shutdown(signal: string): void {
  log(`received ${signal}; shutting down`);
  shuttingDown = true;
  clearInterval(pollHandle);
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

log(`worker started; cwd=${PROJECT_CWD} agent=${AGENT} safe=${SAFE}`);
void emit("worker.started", { agent: AGENT, cwd: PROJECT_CWD });

// Probe provider auth at startup so we fail loud instead of failing per-task.
void probeProviderAuth();

// Kick the loop immediately so the bus drains on startup, not 30s later.
setTimeout(() => { void tick(); }, 100);