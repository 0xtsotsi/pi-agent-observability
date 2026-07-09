/**
 * gg-obs-replay.ts — long-running daemon that tails ggcoder session files
 * and POSTs canonical ObsEvents to the observability server.
 *
 * Architecture (mirrors the original pi-observability extension):
 *   - chokidar watches ~/.gg/sessions/ recursively for new + modified .jsonl files.
 *   - On every "add"/"change" we re-read the file from byte 0 (cheap on small
 *     session files; larger files are < 1 MB in practice) and stream events
 *     through mapSession(). Each file's events are deduped by (session_id, seq)
 *     on the server side via the (session_id, seq) UNIQUE index — replay is safe.
 *   - EventQueue batches up to 50 events, backs off 250ms→5s on failure,
 *     drops oldest on overflow > 10k, and emits a one-time error event on drop.
 *
 * CLI usage:
 *   tsx daemon/gg-obs-replay.ts [--watch-dir=PATH] [--server-url=URL] [--token=TOKEN] [--quiet]
 *
 * Or programmatic:
 *   import { start, stop } from "./gg-obs-replay.js"
 *   const handle = await start({ ... }); await handle.stop();
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import chokidar from "chokidar";
import { mapSession } from "./gg-obs-mapper.js";
import type { ObsEventEnvelope } from "../shared/types.js";

// ─── Config ─────────────────────────────────────────────────────────────────

export interface ReplayConfig {
  serverUrl: string;
  token: string;
  watchDir: string;
  quiet: boolean;
  /** When true (default), replay every existing session file at startup. */
  backfillOnStart: boolean;
}

export interface ReplayHandle {
  stop: () => Promise<void>;
  /** Returns a promise that resolves when the in-flight queue has drained. */
  drained: () => Promise<void>;
  /** Exposed for tests. */
  queue: EventQueue;
}

function argString(v: string | boolean | undefined, fallback = ""): string {
  return typeof v === "string" ? v : fallback;
}

export function configFromEnvAndArgs(argv: string[]): ReplayConfig {
  const args = parseArgs(argv);
  return {
    serverUrl: argString(args["server-url"], process.env["OBS_SERVER_URL"] ?? "http://127.0.0.1:43190"),
    token: argString(args["token"], process.env["OBS_AUTH_TOKEN"] ?? ""),
    watchDir: argString(args["watch-dir"], process.env["OBS_WATCH_DIR"] ?? path.join(os.homedir(), ".gg", "sessions")),
    quiet: args["quiet"] === true || process.env["OBS_QUIET"] === "1",
    backfillOnStart: process.env["OBS_BACKFILL"] !== "0",
  };
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (const a of argv) {
    if (!a.startsWith("--")) continue;
    const eq = a.indexOf("=");
    if (eq === -1) out[a.slice(2)] = true;
    else out[a.slice(2, eq)] = a.slice(eq + 1);
  }
  return out;
}

// ─── Event queue (lifted from pi-observability.ts) ───────────────────────────

const MAX_QUEUE_SIZE = 10000;
const BATCH_SIZE = 50;
const INITIAL_BACKOFF_MS = 250;
const MAX_BACKOFF_MS = 5000;

export class EventQueue {
  private queue: ObsEventEnvelope[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private isFlushing = false;
  private backoffMs = INITIAL_BACKOFF_MS;
  private consecutiveFailures = 0;
  private droppedEventsCount = 0;
  private overflowErrorEmitted = false;

  constructor(
    private serverUrl: string,
    private token: string,
    private quiet: boolean,
  ) {}

  push(event: ObsEventEnvelope): void {
    if (this.queue.length >= MAX_QUEUE_SIZE) {
      this.queue.shift();
      this.droppedEventsCount++;
      if (!this.overflowErrorEmitted) {
        this.overflowErrorEmitted = true;
        const overflowError: ObsEventEnvelope<{ message: string; where: string }> = {
          event_id: cryptoRandomUUID(),
          ts: new Date().toISOString(),
          type: "error",
          session_id: event.session_id,
          cwd: event.cwd,
          pool: event.pool,
          tags: event.tags,
          payload: {
            message: "gg-obs-replay event queue overflowed. Oldest events dropped.",
            where: "daemon-queue",
          },
          seq: -1,
        };
        this.queue.push(overflowError as unknown as ObsEventEnvelope);
        if (!this.quiet) {
          process.stderr.write(`[gg-obs-replay] queue overflow — dropping oldest events\n`);
        }
      }
    }
    this.queue.push(event);
    if (this.queue.length >= BATCH_SIZE) {
      void this.flush();
    } else {
      this.scheduleFlush();
    }
  }

  private scheduleFlush(): void {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flush();
    }, this.backoffMs);
  }

  async flush(): Promise<void> {
    if (this.isFlushing || this.queue.length === 0) return;
    this.isFlushing = true;
    const batch = this.queue.slice(0, BATCH_SIZE);
    try {
      const res = await fetch(`${this.serverUrl.replace(/\/+$/, "")}/events`, {
        method: "POST",
        headers: {
          "authorization": `Bearer ${this.token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(batch),
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      // Successful flush — remove the batch we sent.
      this.queue.splice(0, batch.length);
      this.consecutiveFailures = 0;
      this.backoffMs = INITIAL_BACKOFF_MS;
      // If more events queued up while we were flushing, schedule another pass.
      if (this.queue.length > 0) this.scheduleFlush();
    } catch (err: any) {
      this.consecutiveFailures++;
      this.backoffMs = Math.min(this.backoffMs * 2, MAX_BACKOFF_MS);
      if (!this.quiet) {
        process.stderr.write(`[gg-obs-replay] POST failed (attempt ${this.consecutiveFailures}, backoff ${this.backoffMs}ms): ${err?.message ?? err}\n`);
      }
      // Reschedule with backoff; do NOT remove the batch.
      setTimeout(() => void this.flush(), this.backoffMs);
    } finally {
      this.isFlushing = false;
    }
  }

  /** Wait for any in-flight flush + remaining queue to drain. */
  async drain(): Promise<void> {
    while (this.isFlushing || this.queue.length > 0) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }
}

function cryptoRandomUUID(): string {
  // crypto.randomUUID() is available in Node 16+, but a fallback keeps us safe
  // on older runtimes.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { randomUUID } = require("node:crypto") as typeof import("node:crypto");
  return randomUUID();
}

// ─── File tracking ───────────────────────────────────────────────────────────

interface FileState {
  /** Path to the .jsonl file. */
  file: string;
  /** Highest seq we've already emitted for this session (keyed by session_id). */
  lastSeqBySession: Map<string, number>;
}

/**
 * Track per-session high-water-mark seqs. The server's (session_id, seq) UNIQUE
 * index makes replay idempotent, but skipping already-sent events saves bandwidth
 * and avoids cluttering /sessions with duplicate noise.
 */
function readStateFile(stateFile: string): Map<string, FileState> {
  if (!fs.existsSync(stateFile)) return new Map();
  try {
    const raw = JSON.parse(fs.readFileSync(stateFile, "utf8"));
    const out = new Map<string, FileState>();
    for (const [file, entry] of Object.entries(raw as Record<string, { sessionId: string; lastSeq: number }[]>)) {
      const lastSeqBySession = new Map<string, number>();
      for (const e of entry) lastSeqBySession.set(e.sessionId, e.lastSeq);
      out.set(file, { file, lastSeqBySession });
    }
    return out;
  } catch {
    return new Map();
  }
}

function writeStateFile(stateFile: string, states: Map<string, FileState>): void {
  const out: Record<string, { sessionId: string; lastSeq: number }[]> = {};
  for (const [file, s] of states) {
    out[file] = [...s.lastSeqBySession.entries()].map(([sessionId, lastSeq]) => ({ sessionId, lastSeq }));
  }
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });
  fs.writeFileSync(stateFile, JSON.stringify(out, null, 2), "utf8");
}

// ─── File → events ───────────────────────────────────────────────────────────
// (The mapper is invoked directly inside processFile; the high-water filter
// is applied per-session there so we can safely replay mixed-session files.)

function processFile(
  filePath: string,
  state: FileState,
  queue: EventQueue,
  quiet: boolean,
): void {
  if (!fs.existsSync(filePath)) return;
  // Per-file session id — events from one file should share a single session_id
  // (the ggcoder session header), so we use the file itself as the state key
  // and per-session seq lookup below.
  let anyEmitted = false;
  for (const evt of mapSession(filePath)) {
    const prev = state.lastSeqBySession.get(evt.session_id) ?? -1;
    if (evt.seq <= prev) continue;
    queue.push(evt);
    state.lastSeqBySession.set(evt.session_id, evt.seq);
    anyEmitted = true;
  }
  if (anyEmitted && !quiet) {
    process.stderr.write(`[gg-obs-replay] ${path.basename(filePath)} → emitted up to seq ${Math.max(...state.lastSeqBySession.values())}\n`);
  }
}

// ─── Daemon ──────────────────────────────────────────────────────────────────

export async function start(config: Partial<ReplayConfig> = {}): Promise<ReplayHandle> {
  const cfg: ReplayConfig = {
    serverUrl: config.serverUrl ?? "http://127.0.0.1:43190",
    token: config.token ?? process.env["OBS_AUTH_TOKEN"] ?? "",
    watchDir: config.watchDir ?? path.join(os.homedir(), ".gg", "sessions"),
    quiet: config.quiet ?? false,
    backfillOnStart: config.backfillOnStart ?? true,
  };

  if (!cfg.token) {
    throw new Error("OBS_AUTH_TOKEN is required (set env or pass --token=...)");
  }
  if (!fs.existsSync(cfg.watchDir)) {
    fs.mkdirSync(cfg.watchDir, { recursive: true });
  }

  const stateFile = path.join(os.homedir(), ".gg", "observability", "replay-state.json");
  const states = readStateFile(stateFile);
  const queue = new EventQueue(cfg.serverUrl, cfg.token, cfg.quiet);

  // ── backfill existing session files ──────────────────────────────────
  if (cfg.backfillOnStart) {
    const existing = fs.readdirSync(cfg.watchDir, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .flatMap((d) => {
        try {
          return fs.readdirSync(path.join(cfg.watchDir, d.name))
            .filter((f) => f.endsWith(".jsonl"))
            .map((f) => path.join(cfg.watchDir, d.name, f));
        } catch { return []; }
      });
    for (const f of existing) {
      const state = states.get(f) ?? { file: f, lastSeqBySession: new Map() };
      try {
        processFile(f, state, queue, cfg.quiet);
        states.set(f, state);
      } catch (err: any) {
        process.stderr.write(`[gg-obs-replay] backfill error ${f}: ${err?.message ?? err}\n`);
      }
    }
    writeStateFile(stateFile, states);
  }

  // ── chokidar watcher ─────────────────────────────────────────────────
  const watcher = chokidar.watch(cfg.watchDir, {
    ignored: (p: string, stats?: fs.Stats) => {
      if (!stats) return false;
      if (stats.isDirectory()) return false;
      return !p.endsWith(".jsonl");
    },
    persistent: true,
    ignoreInitial: true,
    awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
  });

  const onChange = (filePath: string) => {
    const state = states.get(filePath) ?? { file: filePath, lastSeqBySession: new Map() };
    try {
      processFile(filePath, state, queue, cfg.quiet);
      states.set(filePath, state);
      writeStateFile(stateFile, states);
    } catch (err: any) {
      process.stderr.write(`[gg-obs-replay] processFile error ${filePath}: ${err?.message ?? err}\n`);
    }
  };

  watcher.on("add", onChange);
  watcher.on("change", onChange);
  watcher.on("error", (err: unknown) => {
    process.stderr.write(`[gg-obs-replay] watcher error: ${(err as Error)?.message ?? err}\n`);
  });

  if (!cfg.quiet) {
    process.stderr.write(`[gg-obs-replay] watching ${cfg.watchDir} → ${cfg.serverUrl}\n`);
  }

  let stopped = false;
  const stop = async (): Promise<void> => {
    if (stopped) return;
    stopped = true;
    await watcher.close();
    await queue.drain();
    writeStateFile(stateFile, states);
  };
  const drained = async (): Promise<void> => {
    await queue.drain();
  };

  return { stop, drained, queue };
}

// ─── CLI entry ───────────────────────────────────────────────────────────────

// Only run when invoked directly (not when imported as a module).
const isDirectInvocation = (() => {
  try {
    if (!process.argv[1]) return false;
    const url = new URL(import.meta.url);
    if (url.protocol !== "file:") return false;
    return path.resolve(process.argv[1]) === path.resolve(url.pathname);
  } catch {
    return false;
  }
})();

if (isDirectInvocation) {
  const cfg = configFromEnvAndArgs(process.argv.slice(2));
  start(cfg).then((handle) => {
    const shutdown = async (signal: string): Promise<void> => {
      process.stderr.write(`[gg-obs-replay] received ${signal}, draining and stopping…\n`);
      await handle.stop();
      process.exit(0);
    };
    process.on("SIGINT", () => { void shutdown("SIGINT"); });
    process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
  }).catch((err: unknown) => {
    process.stderr.write(`[gg-obs-replay] fatal: ${(err as Error)?.message ?? err}\n`);
    process.exit(1);
  });
}