/**
 * server.ts — Node + Hono + better-sqlite3 observability server.
 *
 * Ported from apps/observability/server.ts (Bun + bun:sqlite). Same routes,
 * same auth model (Bearer header OR ?token= query), same SSE broadcast
 * semantics. Binds 127.0.0.1 only.
 */

import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { serve } from "@hono/node-server";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";
import * as crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  createDb,
  prepare,
  toRow,
  toSessionRow,
  rowToSession,
  rowToEvent,
} from "./db.js";
import { MAX_REQUEST_BYTES } from "../shared/types.js";
import type { ObsEvent } from "../shared/types.js";

// ─── Config ─────────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.OBS_PORT ?? "43190", 10);
const HOST = process.env.OBS_HOST ?? "127.0.0.1";

// Default DB location: ~/.gg/observability/obs.db (per-user, persistent).
// Override with OBS_DB_PATH for tests or alternative storage.
function defaultDbPath(): string {
  const home = os.homedir();
  return path.join(home, ".gg", "observability", "obs.db");
}

const DB_PATH = process.env.OBS_DB_PATH ?? defaultDbPath();

// Ensure parent folder exists before initializing SQLite.
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const AUTH_TOKEN = process.env.OBS_AUTH_TOKEN ?? crypto.randomUUID();
const VERSION = "0.1.0";

const OPEN_URL = `http://${HOST}:${PORT}/?token=${encodeURIComponent(AUTH_TOKEN)}`;

// ─── Init ───────────────────────────────────────────────────────────────────

const db = createDb(DB_PATH);
const q = prepare(db);
const startTime = Date.now();

// Resolve the UI directory (works for both `tsx server/server.ts` and bundled).
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const UI_DIR = path.resolve(__dirname, "..", "ui");

console.log(`\n  gg-observability server v${VERSION}`);
console.log(`  UI:    ${OPEN_URL}`);
console.log(`  Token: ${AUTH_TOKEN}`);
console.log(`  DB:    ${DB_PATH}`);
console.log(`  UI dir: ${UI_DIR}\n`);

// ─── SSE subscriber registry ────────────────────────────────────────────────

interface SSESubscriber {
  id: number;
  controller: { enqueue: (chunk: Uint8Array) => void };
  pool?: string;
  tag?: string;
  session_id?: string;
}

let nextSubId = 1;
const subscribers = new Map<number, SSESubscriber>();

function addSubscriber(
  controller: { enqueue: (chunk: Uint8Array) => void },
  pool?: string,
  tag?: string,
  session_id?: string,
): number {
  const id = nextSubId++;
  subscribers.set(id, { id, controller, pool, tag, session_id });
  return id;
}

function removeSubscriber(id: number) {
  subscribers.delete(id);
}

/** Push an SSE-formatted event to one subscriber. Returns false if closed. */
function pushSSE(sub: SSESubscriber, data: string): boolean {
  try {
    sub.controller.enqueue(new TextEncoder().encode(data));
    return true;
  } catch {
    removeSubscriber(sub.id);
    return false;
  }
}

/** Broadcast an event to all SSE subscribers matching the event's pool/tags/session. */
function broadcastEvent(event: ObsEvent) {
  const payload = JSON.stringify(event);
  const frame = `event: event\ndata: ${payload}\n\n`;
  for (const sub of subscribers.values()) {
    if (sub.pool && sub.pool !== event.pool) continue;
    if (sub.tag && (!event.tags || !event.tags.includes(sub.tag))) continue;
    if (sub.session_id && sub.session_id !== event.session_id) continue;
    pushSSE(sub, frame);
  }
}

// Heartbeat every 15s.
setInterval(() => {
  const ping = ": ping\n\n";
  for (const sub of subscribers.values()) {
    pushSSE(sub, ping);
  }
}, 15_000);

// ─── Helpers ────────────────────────────────────────────────────────────────

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

function contentTypeFor(filename: string): string {
  const idx = filename.lastIndexOf(".");
  const key = idx >= 0 ? filename.slice(idx) : "";
  return MIME[key] ?? "application/octet-stream";
}

function safeStaticPath(rel: string): string | null {
  // Strip leading slash, reject traversal.
  const cleaned = rel.replace(/^\/+/, "");
  if (cleaned.includes("..")) return null;
  const full = path.join(UI_DIR, cleaned);
  // Ensure the resolved path is within UI_DIR.
  if (!full.startsWith(UI_DIR + path.sep) && full !== UI_DIR) return null;
  return full;
}

function serveStatic(rel: string): Response | null {
  const filePath = safeStaticPath(rel);
  if (!filePath) return null;
  if (!fs.existsSync(filePath)) return null;
  const stat = fs.statSync(filePath);
  if (!stat.isFile()) return null;
  const body = fs.readFileSync(filePath);
  const filename = path.basename(filePath);
  return new Response(body, {
    headers: {
      "content-type": contentTypeFor(filename),
      "access-control-allow-origin": "*",
    },
  });
}

function checkAuth(req: Request, url: URL): boolean {
  const auth = req.headers.get("authorization");
  if (auth) {
    const parts = auth.split(" ");
    if (
      parts.length === 2 &&
      parts[0].toLowerCase() === "bearer" &&
      parts[1] === AUTH_TOKEN
    ) {
      return true;
    }
    return false;
  }
  const qToken = url.searchParams.get("token");
  if (qToken && qToken === AUTH_TOKEN) return true;
  return false;
}

/**
 * Ingest a single event: insert into DB, upsert session, broadcast to SSE.
 * Returns the event_id if ingested, null if duplicate.
 *
 * FK enforcement is disabled in db.ts so that the events row can land before
 * its parent sessions row, matching the original pi behavior. The
 * (session_id, seq) UNIQUE index still guarantees wire-contract idempotency.
 */
function ingestEvent(event: ObsEvent): string | null {
  const result = q.insertEvent.run(toRow(event));
  const isNew = result.changes > 0;

  if (isNew) {
    q.upsertSession.run(toSessionRow(event));
  } else {
    q.upsertSessionNoBump.run(toSessionRow(event));
  }

  if (isNew) {
    broadcastEvent(event);
  }

  return isNew ? event.event_id : null;
}

async function readBody(req: Request): Promise<string> {
  const len = parseInt(req.headers.get("content-length") ?? "0", 10);
  if (len > MAX_REQUEST_BYTES) {
    throw new Error("Payload too large");
  }
  return await req.text();
}

// ─── Hono app ───────────────────────────────────────────────────────────────

const app = new Hono();

// CORS preflight — covers all routes.
app.options("*", (c) => {
  return new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "Authorization, Content-Type",
    },
  });
});

// ── /health (unauthenticated) ────────────────────────────────────────────
app.get("/health", (c) => {
  try {
    const totals = q.countTotals.get() as any;
    return c.json({
      ok: true,
      version: VERSION,
      uptime_s: Math.round((Date.now() - startTime) / 1000),
      events_total: totals?.events_total ?? 0,
      sessions_total: totals?.sessions_total ?? 0,
    });
  } catch (err: any) {
    return c.json({ ok: false, error: err?.message ?? String(err) }, 500);
  }
});

// ── /favicon.ico ─────────────────────────────────────────────────────────
app.get("/favicon.ico", () => new Response(null, { status: 204 }));

// ── /  (serves ui/index.html) ────────────────────────────────────────────
app.get("/", (c) => {
  const res = serveStatic("index.html");
  if (res) return res;
  return c.text("not found", 404);
});

// ── Static UI assets (unauthenticated — the UI fetches with ?token= via JS) ─
// The UI files are agent-agnostic; they call /events and /sessions with the
// token from the URL. We allow them through here and the auth check happens
// on the API routes below.
app.get("/index.html", (c) => {
  const res = serveStatic("index.html");
  if (res) return res;
  return c.text("not found", 404);
});

app.get("/app.js", (c) => serveStatic("app.js") ?? c.text("not found", 404));
app.get("/race.js", (c) => serveStatic("race.js") ?? c.text("not found", 404));
app.get("/swimlane.js", (c) => serveStatic("swimlane.js") ?? c.text("not found", 404));
app.get("/logo.svg", (c) => serveStatic("logo.svg") ?? c.text("not found", 404));

// ── Auth wall middleware for everything below ────────────────────────────
app.use("*", async (c, next) => {
  // Only enforce auth on API/SSE routes. UI assets and /health are handled
  // above; this middleware runs after those.
  const path = c.req.path;
  if (
    path === "/health" ||
    path === "/" ||
    path === "/favicon.ico" ||
    path === "/index.html" ||
    /\.(js|css|svg|png|ico)$/.test(path)
  ) {
    return next();
  }
  const ok = checkAuth(c.req.raw, new URL(c.req.url));
  if (!ok) return c.json({ error: "unauthorized" }, 401);
  return next();
});

// ── POST /events ─────────────────────────────────────────────────────────
app.post("/events", async (c) => {
  let bodyText: string;
  try {
    bodyText = await readBody(c.req.raw);
  } catch (err: any) {
    return c.json({ error: err?.message ?? String(err) }, 413);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(bodyText);
  } catch {
    return c.json({ error: "invalid JSON" }, 400);
  }

  const events: ObsEvent[] = Array.isArray(parsed) ? parsed : [parsed];
  const ingested: string[] = [];
  const rejected: string[] = [];

  for (const evt of events) {
    if (!evt || typeof evt !== "object" || !(evt as any).event_id || !(evt as any).type) {
      rejected.push((evt as any)?.event_id ?? "unknown");
      continue;
    }
    // Normalize defaults.
    (evt as any).pool = (evt as any).pool ?? "default";
    (evt as any).tags = (evt as any).tags ?? [];
    (evt as any).seq = typeof (evt as any).seq === "number" ? (evt as any).seq : 0;
    (evt as any).cwd = (evt as any).cwd ?? "";

    const ingestedId = ingestEvent(evt as ObsEvent);
    if (ingestedId) {
      ingested.push(ingestedId);
    } else {
      rejected.push((evt as any).event_id);
    }
  }

  return c.json({ ingested: ingested.length, rejected });
});

// ── GET /sessions ────────────────────────────────────────────────────────
app.get("/sessions", (c) => {
  const url = new URL(c.req.url);
  const pool = url.searchParams.get("pool") ?? "";
  const tag = url.searchParams.get("tag") ?? "";
  const since = url.searchParams.get("since") ?? "";
  const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "50", 10), 200);

  try {
    const rows = q.listSessions.all({
      pool,
      tag,
      limit,
    }) as any[];

    // Optional since filter applied in code (low-frequency path).
    const sessions = rows
      .filter((r) => !since || r.last_ts >= since)
      .map(rowToSession);

    return c.json({ sessions });
  } catch (err: any) {
    return c.json({ error: err?.message ?? String(err) }, 500);
  }
});

// ── GET /sessions/:id/events ─────────────────────────────────────────────
app.get("/sessions/:id/events", (c) => {
  const sid = c.req.param("id");
  const url = new URL(c.req.url);
  const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "200", 10), 1000);
  const beforeSeq = url.searchParams.get("before_seq");
  const sinceSeq = url.searchParams.get("since_seq");
  const type = url.searchParams.get("type") ?? "";

  try {
    if (sinceSeq !== null) {
      const rows = q.getSessionEventsSince.all({
        session_id: sid,
        limit,
        since_seq: parseInt(sinceSeq, 10),
        type,
      }) as any[];
      return c.json({ events: rows.map(rowToEvent) });
    }

    const rows = q.getSessionEvents.all({
      session_id: sid,
      limit,
      before_seq: beforeSeq ? parseInt(beforeSeq, 10) : null,
      type,
    }) as any[];

    const events = rows.map(rowToEvent);
    events.reverse(); // ascending for display
    return c.json({ events });
  } catch (err: any) {
    return c.json({ error: err?.message ?? String(err) }, 500);
  }
});

// ── GET /sessions/:id/stats ──────────────────────────────────────────────
app.get("/sessions/:id/stats", (c) => {
  const sid = c.req.param("id");
  try {
    const row = q.getSessionStats.get({ session_id: sid }) as any;
    const ctx = q.getSessionContext.get({ session_id: sid }) as any;
    return c.json({
      total_tokens: row?.total_tokens ?? 0,
      input_tokens: row?.input_tokens ?? 0,
      output_tokens: row?.output_tokens ?? 0,
      total_cost: row?.total_cost ?? 0,
      error_count: row?.error_count ?? 0,
      latest_input: ctx?.latest_input ?? null,
      latest_ts: ctx?.latest_ts ?? null,
    });
  } catch (err: any) {
    return c.json({ error: err?.message ?? String(err) }, 500);
  }
});

// ── GET /events/stream (SSE) ─────────────────────────────────────────────
app.get("/events/stream", (c) => {
  const url = new URL(c.req.url);
  const streamPool = url.searchParams.get("pool") ?? undefined;
  const streamTag = url.searchParams.get("tag") ?? undefined;
  const streamSession = url.searchParams.get("session_id") ?? undefined;

  return streamSSE(c, async (stream) => {
    const controller = {
      enqueue: (chunk: Uint8Array) => {
        try {
          stream.write(chunk);
        } catch {
          // stream closed
        }
      },
    };
    const subId = addSubscriber(controller, streamPool, streamTag, streamSession);

    // Initial hello.
    const hello = JSON.stringify({ server: "gg-observability", version: VERSION });
    await stream.writeSSE({
      event: "hello",
      data: hello,
      retry: 5000,
    });

    // Hold the connection open until the client disconnects. The global 15s
    // heartbeat above handles keepalive; we just need to wait here.
    await new Promise<void>((resolve) => {
      const abort = () => {
        c.req.raw.signal.removeEventListener("abort", abort);
        resolve();
      };
      c.req.raw.signal.addEventListener("abort", abort);
      // stream.onAbort is the Hono SSE-native close hook if available.
      const onAbort = (stream as unknown as { onAbort?: (fn: () => void) => void }).onAbort;
      if (typeof onAbort === "function") onAbort(abort);
    });

    removeSubscriber(subId);
  });
});

// ── 404 fallback ─────────────────────────────────────────────────────────
app.notFound((c) => c.json({ error: "not found" }, 404));

// ─── Boot ───────────────────────────────────────────────────────────────────

serve(
  {
    fetch: app.fetch,
    port: PORT,
    hostname: HOST,
  },
  (info) => {
    console.log(`  Listening on http://${HOST}:${info.port}`);
    console.log(`  Open the UI →  ${OPEN_URL}\n`);
  },
);