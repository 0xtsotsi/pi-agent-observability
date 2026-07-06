/**
 * db.ts — SQLite schema + prepared queries for gg-observability.
 *
 * Ported from apps/observability/db.ts (Bun + bun:sqlite) to Node + better-sqlite3.
 * Schema matches shared/types.ts exactly. (session_id, seq) UNIQUE is the
 * idempotency guarantee for the wire contract.
 */

import Database from "better-sqlite3";
import type {
  ObsEvent,
  SessionSummary,
} from "../shared/types.js";

// ─── Schema ─────────────────────────────────────────────────────────────────

const SCHEMA = `
CREATE TABLE IF NOT EXISTS sessions (
  session_id   TEXT PRIMARY KEY,
  pool         TEXT NOT NULL DEFAULT 'default',
  agent_name   TEXT,
  cwd          TEXT,
  session_file TEXT,
  provider     TEXT,
  model        TEXT,
  first_ts     TEXT NOT NULL,
  last_ts      TEXT NOT NULL,
  event_count  INTEGER NOT NULL DEFAULT 0,
  tags_json    TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS events (
  event_id     TEXT PRIMARY KEY,
  session_id   TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  ts           TEXT NOT NULL,
  type         TEXT NOT NULL,
  pool         TEXT NOT NULL DEFAULT 'default',
  tags_json    TEXT NOT NULL DEFAULT '[]',
  payload_json TEXT NOT NULL,
  provider     TEXT,
  model        TEXT,
  cwd          TEXT NOT NULL DEFAULT '',
  session_file TEXT,
  agent_name   TEXT,
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_session_seq ON events(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_pool ON events(pool);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id);

-- ── Bus: command queue (overlord → workers) ──
CREATE TABLE IF NOT EXISTS commands (
  command_id      TEXT PRIMARY KEY,         -- ULID, server-generated
  target_agent    TEXT NOT NULL,            -- e.g. 'noledge', 'gg-obs', 'demoshots', 'overlord'
  target_pool     TEXT NOT NULL DEFAULT 'default',
  action          TEXT NOT NULL,            -- dotted verb: 'task.dispatch', 'task.ack', 'task.report'
  payload_json    TEXT NOT NULL DEFAULT '{}',
  created_ts      TEXT NOT NULL,            -- ISO-8601
  created_by      TEXT NOT NULL,            -- issuer (overlord's session_id, or 'human')
  ack_state       TEXT NOT NULL DEFAULT 'pending',  -- pending | acked | failed | expired
  ack_session_id  TEXT,
  ack_ts          TEXT,
  ack_payload_json TEXT,
  seq             INTEGER NOT NULL,         -- monotonic per (target_agent), polling cursor
  UNIQUE(target_agent, seq)
);
CREATE INDEX IF NOT EXISTS idx_commands_target_state ON commands(target_agent, ack_state, seq);
CREATE INDEX IF NOT EXISTS idx_commands_created_ts ON commands(created_ts);
`;

// ─── Prepared queries ──────────────────────────────────────────────────────

export interface PreparedQueries {
  insertEvent: Database.Statement;
  upsertSession: Database.Statement;
  upsertSessionNoBump: Database.Statement;
  listSessions: Database.Statement;
  getSessionEvents: Database.Statement;
  getSessionEventsSince: Database.Statement;
  getSessionStats: Database.Statement;
  getSessionContext: Database.Statement;
  countTotals: Database.Statement;
  // Bus: command queue
  insertCommand: Database.Statement;
  getCommand: Database.Statement;
  listCommands: Database.Statement;
  ackCommand: Database.Statement;
  nextCommandSeq: Database.Statement;
}

// ─── Init ───────────────────────────────────────────────────────────────────

export function createDb(path: string): Database.Database {
  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.pragma("busy_timeout = 5000");
  // FK enforcement ON: the server upserts the parent session row before
  // inserting the event, so the FK can be trusted. The (session_id, seq)
  // UNIQUE index still guarantees wire-contract idempotency.
  db.pragma("foreign_keys = ON");

  // Migrate older DBs: add the new envelope columns if missing. Safe to run
  // every startup — ADD COLUMN errors are swallowed per column.
  const eventsCols = (db.prepare(`PRAGMA table_info(events)`).all() as Array<{ name: string }>).map((c) => c.name);
  if (!eventsCols.includes("cwd"))          try { db.exec(`ALTER TABLE events ADD COLUMN cwd TEXT NOT NULL DEFAULT ''`); } catch {}
  if (!eventsCols.includes("session_file")) try { db.exec(`ALTER TABLE events ADD COLUMN session_file TEXT`); } catch {}
  if (!eventsCols.includes("agent_name"))   try { db.exec(`ALTER TABLE events ADD COLUMN agent_name TEXT`); } catch {}

  db.exec(SCHEMA);
  return db;
}

export function prepare(db: Database.Database): PreparedQueries {
  // ── Insert event (idempotent) ───────────────────────────────────────────
  const insertEvent = db.prepare(`
    INSERT OR IGNORE INTO events
      (event_id, session_id, seq, ts, type, pool, tags_json, payload_json, provider, model, cwd, session_file, agent_name)
    VALUES
      (@event_id, @session_id, @seq, @ts, @type, @pool, @tags_json, @payload_json, @provider, @model, @cwd, @session_file, @agent_name)
  `);

  // ── Upsert session (bumps event_count) ──────────────────────────────────
  // COALESCE logic: don't overwrite non-null existing fields with null
  // incoming values. Tags are merged via UNION to accumulate unique tags.
  const upsertSession = db.prepare(`
    INSERT INTO sessions
      (session_id, pool, agent_name, cwd, session_file, provider, model, first_ts, last_ts, event_count, tags_json)
    VALUES
      (@session_id, @pool, @agent_name, @cwd, @session_file, @provider, @model, @ts, @ts, 1, @tags_json)
    ON CONFLICT(session_id) DO UPDATE SET
      pool         = COALESCE(excluded.pool,         sessions.pool),
      agent_name   = COALESCE(excluded.agent_name,   sessions.agent_name),
      cwd          = COALESCE(excluded.cwd,          sessions.cwd),
      session_file = COALESCE(excluded.session_file, sessions.session_file),
      provider     = COALESCE(excluded.provider,     sessions.provider),
      model        = COALESCE(excluded.model,        sessions.model),
      first_ts     = COALESCE(sessions.first_ts,     excluded.last_ts),
      last_ts      = MAX(excluded.last_ts,           sessions.last_ts),
      event_count  = sessions.event_count + 1,
      tags_json    = (
        SELECT json_group_array(DISTINCT value)
        FROM (
          SELECT value FROM json_each(sessions.tags_json)
          UNION
          SELECT value FROM json_each(excluded.tags_json)
        )
      )
  `);

  // ── Upsert session without bumping event_count (duplicate events) ──────
  const upsertSessionNoBump = db.prepare(`
    INSERT INTO sessions
      (session_id, pool, agent_name, cwd, session_file, provider, model, first_ts, last_ts, event_count, tags_json)
    VALUES
      (@session_id, @pool, @agent_name, @cwd, @session_file, @provider, @model, @ts, @ts, 1, @tags_json)
    ON CONFLICT(session_id) DO UPDATE SET
      pool         = COALESCE(excluded.pool,         sessions.pool),
      agent_name   = COALESCE(excluded.agent_name,   sessions.agent_name),
      cwd          = COALESCE(excluded.cwd,          sessions.cwd),
      session_file = COALESCE(excluded.session_file, sessions.session_file),
      provider     = COALESCE(excluded.provider,     sessions.provider),
      model        = COALESCE(excluded.model,        sessions.model),
      first_ts     = COALESCE(sessions.first_ts,     excluded.last_ts),
      last_ts      = MAX(excluded.last_ts,           sessions.last_ts),
      tags_json    = (
        SELECT json_group_array(DISTINCT value)
        FROM (
          SELECT value FROM json_each(sessions.tags_json)
          UNION
          SELECT value FROM json_each(excluded.tags_json)
        )
      )
  `);

  // ── List sessions (with optional pool/tag filters) ──────────────────────
  const listSessions = db.prepare(`
    SELECT
      session_id, pool,
      COALESCE(agent_name, '') AS agent_name,
      COALESCE(cwd, '') AS cwd,
      COALESCE(session_file, '') AS session_file,
      COALESCE(provider, '') AS provider,
      COALESCE(model, '') AS model,
      first_ts, last_ts, event_count,
      tags_json
    FROM sessions
    WHERE (@pool = '' OR pool = @pool)
      AND (@tag = '' OR EXISTS (
        SELECT 1 FROM json_each(tags_json) WHERE value = @tag
      ))
    ORDER BY last_ts DESC
    LIMIT @limit
  `);

  // ── Get events for a session (backward pagination) ─────────────────────
  const getSessionEvents = db.prepare(`
    SELECT
      event_id, session_id, seq, ts, type, pool, tags_json, payload_json, provider, model, cwd, session_file, agent_name
    FROM events
    WHERE session_id = @session_id
      AND (@type = '' OR type = @type)
      AND (@before_seq IS NULL OR seq < @before_seq)
    ORDER BY seq DESC
    LIMIT @limit
  `);

  // ── Get events since seq (forward resync) ──────────────────────────────
  const getSessionEventsSince = db.prepare(`
    SELECT
      event_id, session_id, seq, ts, type, pool, tags_json, payload_json, provider, model, cwd, session_file, agent_name
    FROM events
    WHERE session_id = @session_id
      AND seq > @since_seq
      AND (@type = '' OR type = @type)
    ORDER BY seq ASC
    LIMIT @limit
  `);

  // ── Session stats (cost, tokens, errors) ──────────────────────────────
  const getSessionStats = db.prepare(`
    SELECT
      COALESCE(SUM(CASE WHEN type = 'assistant_message' THEN json_extract(payload_json, '$.usage.total_tokens') ELSE 0 END), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 'assistant_message' THEN json_extract(payload_json, '$.usage.input') ELSE 0 END), 0)        AS input_tokens,
      COALESCE(SUM(CASE WHEN type = 'assistant_message' THEN json_extract(payload_json, '$.usage.output') ELSE 0 END), 0)       AS output_tokens,
      COALESCE(SUM(CASE WHEN type = 'assistant_message' THEN json_extract(payload_json, '$.usage.cost_total') ELSE 0 END), 0)   AS total_cost,
      COALESCE(SUM(CASE WHEN type = 'error' THEN 1 ELSE 0 END), 0) AS error_count
    FROM events
    WHERE session_id = @session_id
  `);

  // ── Latest assistant_message context size ─────────────────────────────
  const getSessionContext = db.prepare(`
    SELECT
      (COALESCE(json_extract(payload_json, '$.usage.input'),       0)
     + COALESCE(json_extract(payload_json, '$.usage.cache_read'),  0)
     + COALESCE(json_extract(payload_json, '$.usage.cache_write'), 0)) AS latest_input,
      ts AS latest_ts
    FROM events
    WHERE session_id = @session_id
      AND type = 'assistant_message'
      AND json_extract(payload_json, '$.usage.input') IS NOT NULL
    ORDER BY seq DESC
    LIMIT 1
  `);

  // ── Totals for /health ──────────────────────────────────────────────────
  const countTotals = db.prepare(`
    SELECT
      (SELECT COUNT(*) FROM events) AS events_total,
      (SELECT COUNT(*) FROM sessions) AS sessions_total
  `);

  // ── Bus: next seq for a target_agent (monotonic per agent) ──────────────
  const nextCommandSeq = db.prepare(`
    SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
    FROM commands WHERE target_agent = ?
  `);

  // ── Bus: insert command ─────────────────────────────────────────────────
  const insertCommand = db.prepare(`
    INSERT INTO commands
      (command_id, target_agent, target_pool, action, payload_json, created_ts, created_by, ack_state, seq)
    VALUES
      (@command_id, @target_agent, @target_pool, @action, @payload_json, @created_ts, @created_by, 'pending', @seq)
  `);

  // ── Bus: get one command ────────────────────────────────────────────────
  const getCommand = db.prepare(`
    SELECT command_id, target_agent, target_pool, action, payload_json, created_ts, created_by,
           ack_state, ack_session_id, ack_ts, ack_payload_json, seq
    FROM commands WHERE command_id = ?
  `);

  // ── Bus: list commands (filter by target + status; since-seq pagination) ─
  const listCommands = db.prepare(`
    SELECT command_id, target_agent, target_pool, action, payload_json, created_ts, created_by,
           ack_state, ack_session_id, ack_ts, ack_payload_json, seq
    FROM commands
    WHERE (@target = '' OR target_agent = @target)
      AND (@status = '' OR ack_state = @status)
      AND (@since_seq = 0 OR seq > @since_seq)
    ORDER BY seq ASC
    LIMIT @limit
  `);

  // ── Bus: ack a command (worker reports done/failed) ─────────────────────
  const ackCommand = db.prepare(`
    UPDATE commands
    SET ack_state = @ack_state,
        ack_session_id = @ack_session_id,
        ack_ts = @ack_ts,
        ack_payload_json = @ack_payload_json
    WHERE command_id = @command_id AND ack_state = 'pending'
  `);

  return {
    insertEvent,
    upsertSession,
    upsertSessionNoBump,
    listSessions,
    getSessionEvents,
    getSessionEventsSince,
    getSessionStats,
    getSessionContext,
    countTotals,
    insertCommand,
    getCommand,
    listCommands,
    ackCommand,
    nextCommandSeq,
  };
}

// ─── Helpers ────────────────────────────────────────────────────────────────

export function toRow(e: ObsEvent): Record<string, unknown> {
  return {
    event_id: e.event_id,
    session_id: e.session_id,
    seq: e.seq,
    ts: e.ts,
    type: e.type,
    pool: e.pool ?? "default",
    tags_json: JSON.stringify(e.tags ?? []),
    payload_json: JSON.stringify(e.payload ?? {}),
    provider: e.provider ?? null,
    model: e.model ?? null,
    cwd: e.cwd ?? "",
    session_file: e.session_file ?? null,
    agent_name: e.agent_name ?? null,
  };
}

export function toSessionRow(e: ObsEvent): Record<string, unknown> {
  return {
    session_id: e.session_id,
    pool: e.pool ?? "default",
    agent_name: e.agent_name ?? null,
    cwd: e.cwd ?? null,
    session_file: e.session_file ?? null,
    provider: e.provider ?? null,
    model: e.model ?? null,
    ts: e.ts,
    tags_json: JSON.stringify(e.tags ?? []),
  };
}

export function rowToSession(row: any): SessionSummary {
  let tags: string[] = [];
  try {
    tags = JSON.parse(row.tags_json ?? "[]");
  } catch {
    tags = [];
  }
  return {
    session_id: row.session_id,
    pool: row.pool,
    agent_name: row.agent_name || undefined,
    cwd: row.cwd || undefined,
    session_file: row.session_file || undefined,
    provider: row.provider || undefined,
    model: row.model || undefined,
    first_ts: row.first_ts,
    last_ts: row.last_ts,
    event_count: row.event_count,
    tags,
  };
}

export function rowToEvent(row: any): ObsEvent {
  let tags: string[] = [];
  try {
    tags = JSON.parse(row.tags_json ?? "[]");
  } catch {
    tags = [];
  }
  let payload: unknown = {};
  try {
    payload = JSON.parse(row.payload_json ?? "{}");
  } catch {
    payload = {};
  }
  return {
    event_id: row.event_id,
    ts: row.ts,
    type: row.type,
    session_id: row.session_id,
    cwd: row.cwd ?? "",
    session_file: row.session_file ?? undefined,
    agent_name: row.agent_name ?? undefined,
    pool: row.pool,
    tags,
    provider: row.provider ?? undefined,
    model: row.model ?? undefined,
    payload,
    seq: row.seq,
  } as ObsEvent;
}