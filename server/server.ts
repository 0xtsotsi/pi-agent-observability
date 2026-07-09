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

const PORT = parseInt(process.env["OBS_PORT"] ?? "43190", 10);
const HOST = process.env["OBS_HOST"] ?? "127.0.0.1";

// Default DB location: ~/.gg/observability/obs.db (per-user, persistent).
// Override with OBS_DB_PATH for tests or alternative storage.
function defaultDbPath(): string {
  const home = os.homedir();
  return path.join(home, ".gg", "observability", "obs.db");
}

const DB_PATH = process.env["OBS_DB_PATH"] ?? defaultDbPath();

// Ensure parent folder exists before initializing SQLite.
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const AUTH_TOKEN = process.env["OBS_AUTH_TOKEN"] ?? crypto.randomUUID();
const VERSION = "0.1.0";

// Persist the token to a file the helper reads, so server restarts and the
// CLI helper stay in sync without env-var coordination. Only writes if
// the file doesn't already exist (don't clobber a stable deployed token).
try {
  const tokenPath = path.join(os.homedir(), ".gg", "observability", "token");
  fs.mkdirSync(path.dirname(tokenPath), { recursive: true });
  if (!fs.existsSync(tokenPath)) {
    fs.writeFileSync(tokenPath, AUTH_TOKEN, { mode: 0o600 });
  }
} catch { /* non-fatal */ }

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
  pool?: string | undefined;
  tag?: string | undefined;
  session_id?: string | undefined;
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
  const stat = fs.statSync(filePath); // follows symlinks
  if (!stat.isFile()) return null;
  // S8: reject if a symlink resolved outside UI_DIR. Compare realpath to the
  // realpath of UI_DIR (not the joined prefix) so macOS's /var → /private
  // prefix doesn't false-positive.
  const real = fs.realpathSync.native ? fs.realpathSync.native(filePath) : fs.realpathSync(filePath);
  const uiReal = fs.realpathSync.native ? fs.realpathSync.native(UI_DIR) : fs.realpathSync(UI_DIR);
  if (!real.startsWith(uiReal + path.sep) && real !== uiReal) return null;
  const body = fs.readFileSync(real);
  const filename = path.basename(real);
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
      parts[0]!.toLowerCase() === "bearer" &&
      parts[1]! === AUTH_TOKEN
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
 * Session is seeded BEFORE the event insert so the FK constraint
 * (foreign_keys=ON) is satisfied. Both writes run inside a single transaction
 * so a crash mid-ingest cannot leave an orphan event row. The
 * (session_id, seq) UNIQUE index still guarantees wire-contract idempotency.
 *
 * event_count is bumped exactly once per *new* event via upsertSession; the
 * pre-seed uses INSERT OR IGNORE so it doesn't double-count.
 */
function ingestEvent(event: ObsEvent): string | null {
  const seedSession = db.prepare(`
    INSERT OR IGNORE INTO sessions
      (session_id, pool, agent_name, cwd, session_file, provider, model, first_ts, last_ts, event_count, tags_json)
    VALUES
      (@session_id, @pool, @agent_name, @cwd, @session_file, @provider, @model, @ts, @ts, 0, @tags_json)
  `);

  const ingestTxn = db.transaction((evt: ObsEvent): string | null => {
    const sessionRow = toSessionRow(evt);
    seedSession.run(sessionRow); // FK target only — no event_count bump
    const result = q.insertEvent.run(toRow(evt));
    if (result.changes > 0) {
      q.upsertSession.run(sessionRow); // bumps event_count by 1
    } else {
      q.upsertSessionNoBump.run(sessionRow);
    }
    return result.changes > 0 ? evt.event_id : null;
  });

  const ingestedId = ingestTxn(event);

  if (ingestedId) {
    broadcastEvent(event);
  }

  return ingestedId;
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
app.get("/worker-dashboard.js", (c) => serveStatic("worker-dashboard.js") ?? c.text("not found", 404));
app.get("/logo.svg", (c) => serveStatic("logo.svg") ?? c.text("not found", 404));

// ── Auth wall middleware for everything below ────────────────────────────
app.use("*", async (c, next) => {
  // Only enforce auth on API/SSE routes. UI assets and /health are handled
  // above; this middleware runs after those.
  const reqPath = c.req.path;
  if (
    reqPath === "/health" ||
    reqPath === "/" ||
    reqPath === "/favicon.ico" ||
    reqPath === "/index.html" ||
    /\.(js|css|svg|png|ico)$/.test(reqPath)
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

// ── GET /events?type=&provider=&tag=&limit=&since= ────────────────────────
// Generic events query, used by the worker-daemon observability path.
// Filters: type (LIKE-prefix), provider (exact), tag (LIKE substring on tags_json),
// since (event.seq > since). Auth-required, returns the most recent matching events
// newest-first.
app.get("/events", (c) => {
  const url = new URL(c.req.url);
  if (!checkAuth(c.req.raw, url)) {
    return c.json({ error: "unauthorized" }, 401);
  }
  const typeLike = url.searchParams.get("type") ?? "";
  const provider = url.searchParams.get("provider") ?? "";
  const tagLike = url.searchParams.get("tag") ?? "";
  const since = parseInt(url.searchParams.get("since") ?? "0", 10) || 0;
  const rawLimit = parseInt(url.searchParams.get("limit") ?? "200", 10);
  const limit = Number.isFinite(rawLimit) ? Math.min(Math.max(rawLimit, 1), 1000) : 200;

  try {
    // Aggregate query: walk events newer than `since` across all sessions, post-filter.
    // We use the existing q.getSessionEventsSince one event at a time
    // by walking backwards from the most recent seq. This is O(N) per
    // request and not great at scale, but sufficient for the worker observability
    // path where event volume is low. A future optimization is to add a
    // session-agnostic prepared statement (deferred).
    //
    // (Removed in PR #1 follow-up: the empty-session_id dummy query above
    //  matched nothing and was being computed + discarded — wasteful. Now
    //  we go straight to the maxSeq probe.)
    // We use the existing q.getSessionEventsSince one event at a time
    // by walking backwards from the most recent seq. This is O(N) per
    // request and not great at scale, but sufficient for the worker observability
    // path where event volume is low. A future optimization is to add a
    // session-agnostic prepared statement (deferred).
    const maxSeq = (q.getSessionEventsSince.all({
      session_id: "_nonempty_",  // arbitrary non-empty to get the latest events
      since_seq: 0,
      type: "",
      limit: 1,
    }) as any[])[0]?.seq ?? 0;

    // Walk back from maxSeq collecting events matching our filter.
    const collected: any[] = [];
    let cursor = maxSeq + 1;
    while (collected.length < limit && cursor > since) {
      // session_id filter: we can't use "" (matches nothing) and we can't
      // use "%" (LIKE wildcards aren't in the SQL). So we use a sentinel
      // session we know doesn't exist, and post-filter by sequence range.
      // Practically: we pull one batch via session_id="_unbound_" which
      // is a sentinel, the query returns 0 rows, we then pull the next
      // batch by session_id="_unbound_2" etc. -- but that's silly.
      //
      // The simplest correct approach: use a raw SQL via db.prepare.
      // We do that in db.ts as `listAllEvents`. If it's missing (e.g. older
      // server build), fall back to a per-session walk.
      break; // placeholder — actual implementation uses listAllEvents if present
    }
    void cursor;

    // The right answer: use the prepared statement. If the file is at
    // the right rev, listAllEvents is defined and this works. If the
    // prepared statement isn't registered, fall back to a session walk.
    if (typeof (q as any).listAllEvents?.all === "function") {
      const rows = (q as any).listAllEvents.all({
        type: typeLike ? `${typeLike}%` : "",
        provider,
        tag: tagLike ? `%${tagLike}%` : "",
        since_seq: since,
        limit,
      }) as any[];
      return c.json({ events: rows, count: rows.length });
    }

    // Fallback: raw SQL via the db handle if listAllEvents isn't registered.
    const rawRows = (db as any).prepare(`
      SELECT event_id, session_id, seq, ts, type, pool, tags_json, payload_json, provider, model, cwd, session_file, agent_name
      FROM events
      WHERE (@type = '' OR type LIKE @type)
        AND (@provider = '' OR provider = @provider)
        AND (@tag = '' OR tags_json LIKE @tag)
        AND (@since_seq = 0 OR seq > @since_seq)
      ORDER BY seq DESC
      LIMIT @limit
    `).all({
      type: typeLike ? `${typeLike}%` : "",
      provider,
      tag: tagLike ? `%${tagLike}%` : "",
      since_seq: since,
      limit,
    }) as any[];
    return c.json({ events: rawRows, count: rawRows.length });
  } catch (e: unknown) {
    return c.json({ error: "db_error", detail: e instanceof Error ? e.message : String(e) }, 500);
  }
});

// ── POST /commands — overlord dispatches a task to a worker ──────────────
app.post("/commands", async (c) => {
  const url = new URL(c.req.url);
  if (!checkAuth(c.req.raw, url)) {
    return c.json({ error: "unauthorized" }, 401);
  }
  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid_json" }, 400);
  }
  const target_agent = String(body?.target_agent ?? "").trim();
  const action = String(body?.action ?? "").trim();
  if (!target_agent || !action) {
    return c.json({ error: "missing_target_agent_or_action" }, 400);
  }
  const command_id = crypto.randomUUID();
  const created_ts = new Date().toISOString();
  const created_by = String(body?.created_by ?? "overlord").trim();
  const target_pool = String(body?.target_pool ?? "default").trim();
  const payload_json = JSON.stringify(body?.payload ?? {});

  let seq: number;
  try {
    const row = q.nextCommandSeq.get(target_agent) as { next_seq: number };
    seq = row.next_seq;
  } catch (e: any) {
    return c.json({ error: "db_error", detail: String(e?.message ?? e) }, 500);
  }

  try {
    q.insertCommand.run({
      command_id, target_agent, target_pool, action, payload_json, created_ts, created_by, seq,
    });
  } catch (e: any) {
    return c.json({ error: "insert_failed", detail: String(e?.message ?? e) }, 500);
  }

  // Step 2 (sse-sync): broadcast bus.command_created so SSE subscribers see
  // new dispatches live. Routed via ingestEvent so the event also lands in
  // the events table for /events?type=bus. history support.
  ingestEvent({
    event_id: `bus-c-${command_id}`,
    session_id: `bus-${target_agent}`,
    seq: Date.now(),   // unique-per-event so two acks on different commands don't collide on UNIQUE(session_id, seq)
    ts: created_ts,
    type: "bus.command_created",
    pool: target_pool,
    tags: ["bus", target_agent],
    payload: {
      command_id,
      target_agent,
      target_pool,
      action,
      created_by,
      seq,
      payload_json,
    },
    provider: "gg-obs",
    model: null,
  } as unknown as ObsEvent);

  return c.json({ command_id, seq, created_ts, target_agent, action }, 201);
});

// ── GET /commands?target=&status=&since=&limit= — worker polls the queue ──
app.get("/commands", (c) => {
  const url = new URL(c.req.url);
  if (!checkAuth(c.req.raw, url)) {
    return c.json({ error: "unauthorized" }, 401);
  }
  const target = url.searchParams.get("target") ?? "";
  const status = url.searchParams.get("status") ?? "pending";
  const since = parseInt(url.searchParams.get("since") ?? "0", 10) || 0;
  const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") ?? "50", 10) || 50, 1), 200);

  try {
    const rows = q.listCommands.all({ target, status, since_seq: since, limit });
    // latest_seq: targeted MAX(seq) when target is set; otherwise the global max.
    let latest_seq = 0;
    if (target) {
      const r = q.nextCommandSeq.get(target) as { next_seq: number };
      latest_seq = r.next_seq - 1; // next_seq is "next"; current max is one less (or 0 if none)
    } else {
      const r = db.prepare("SELECT COALESCE(MAX(seq), 0) AS m FROM commands").get() as { m: number };
      latest_seq = r.m;
    }
    return c.json({ commands: rows, latest_seq, count: rows.length });
  } catch (e: any) {
    return c.json({ error: "db_error", detail: String(e?.message ?? e) }, 500);
  }
});

// ── POST /commands/:id/ack — worker reports done/failed ─────────────────
app.post("/commands/:id/ack", async (c) => {
  const url = new URL(c.req.url);
  if (!checkAuth(c.req.raw, url)) {
    return c.json({ error: "unauthorized" }, 401);
  }
  const id = c.req.param("id");
  let body: any = {};
  try { body = await c.req.json(); } catch { /* allow empty body */ }
  const ack_state = String(body?.ack_state ?? "acked");
  if (!["acked", "failed"].includes(ack_state)) {
    return c.json({ error: "invalid_ack_state" }, 400);
  }
  const ack_session_id = String(body?.ack_session_id ?? "unknown");
  const ack_ts = new Date().toISOString();
  const ack_payload_json = JSON.stringify(body?.ack_payload ?? {});

  try {
    const result = q.ackCommand.run({ command_id: id, ack_state, ack_session_id, ack_ts, ack_payload_json });
    if (result.changes === 0) {
      return c.json({ error: "not_found_or_already_acked" }, 404);
    }

    // Step 2 (sse-sync): broadcast bus.command_acked. Look up the target_agent
    // from the row so subscribers can route by agent. Tolerate the lookup
    // failing — broadcast is best-effort.
    let target_agent_for_tag = "unknown";
    try {
      const row = db.prepare("SELECT target_agent FROM commands WHERE command_id = ?").get(id) as { target_agent?: string } | undefined;
      if (row?.target_agent) target_agent_for_tag = row.target_agent;
    } catch { /* non-fatal */ }
    ingestEvent({
      event_id: `bus-a-${id}-${Date.now()}`,
      session_id: `bus-${target_agent_for_tag}`,
      seq: Date.now() + 1, // unique-per-event; +1 to avoid same-ms-collision with a create emitted in the same tick
      ts: ack_ts,
      type: "bus.command_acked",
      pool: "default",
      tags: ["bus", target_agent_for_tag],
      payload: {
        command_id: id,
        state: ack_state,
        ack_session_id,
        ack_ts,
      },
      provider: "gg-obs",
      model: null,
    } as unknown as ObsEvent);

    return c.json({ ok: true, command_id: id, ack_state, ack_ts });
  } catch (e: any) {
    return c.json({ error: "db_error", detail: String(e?.message ?? e) }, 500);
  }
});

// ── GET /sessions ────────────────────────────────────────────────────────
app.get("/sessions", (c) => {
  const url = new URL(c.req.url);
  const pool = url.searchParams.get("pool") ?? "";
  const tag = url.searchParams.get("tag") ?? "";
  const since = url.searchParams.get("since") ?? "";
  // G4 fix (audit 2026-07-06): Number.isFinite guards so ?limit=foo returns 200 with default 50, not 500.
  const rawLimit = parseInt(url.searchParams.get("limit") ?? "50", 10);
  const limit = Number.isFinite(rawLimit) ? Math.min(Math.max(rawLimit, 1), 200) : 50;

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
  // G4 / T8: guard NaN and out-of-range on every numeric query param.
  const rawLimit = parseInt(url.searchParams.get("limit") ?? "200", 10);
  const limit = Number.isFinite(rawLimit) ? Math.min(Math.max(rawLimit, 1), 1000) : 200;
  const beforeSeqRaw = parseInt(url.searchParams.get("before_seq") ?? "", 10);
  const beforeSeq = Number.isFinite(beforeSeqRaw) ? beforeSeqRaw : null;
  const sinceSeqRaw = parseInt(url.searchParams.get("since_seq") ?? "", 10);
  const sinceSeq = Number.isFinite(sinceSeqRaw) ? sinceSeqRaw : null;
  const type = url.searchParams.get("type") ?? "";

  try {
    if (sinceSeq !== null) {
      const rows = q.getSessionEventsSince.all({
        session_id: sid,
        limit,
        since_seq: sinceSeq,
        type,
      }) as any[];
      return c.json({ events: rows.map(rowToEvent) });
    }

    const rows = q.getSessionEvents.all({
      session_id: sid,
      limit,
      before_seq: beforeSeq,
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
      // BUGFIX 2026-07-07: stream.onAbort is an inherited prototype method on
      // SSEStreamingApi (extends Hono's StreamingApi). Calling it bare —  —
      // makes  undefined inside the method, throws TypeError, and the
      // surrounding run() catches + closes the stream. That cut every subscriber
      // off right after the hello frame, so live events never streamed to the UI.
      // Bound call keeps  wired to the stream instance, so the callback
      // actually registers and only fires on real abort.
      const streamApi = stream as unknown as { onAbort?: (fn: () => void) => void };
      if (typeof streamApi.onAbort === "function") streamApi.onAbort.call(stream, abort);
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