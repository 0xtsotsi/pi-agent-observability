/**
 * T2 — DB round-trip with cwd / session_file / agent_name.
 *
 * Asserts the new events-table columns (task 36 / C5+G10) survive insert+read.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createDb, prepare, toRow, rowToEvent, toSessionRow } from "./db.js";

let tmpDir: string;
let dbPath: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-db-"));
  dbPath = path.join(tmpDir, "test.db");
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe("db round-trip", () => {
  it("T2.a — toRow populates cwd, session_file, agent_name", () => {
    const db = createDb(dbPath);
    const q = prepare(db);
    const evt = {
      event_id: "e1",
      ts: "2026-07-06T12:00:00.000Z",
      type: "session_start" as const,
      session_id: "s1",
      cwd: "/tmp/proj",
      session_file: "/tmp/proj/.gg/session.jsonl",
      agent_name: "test-agent",
      pool: "default",
      tags: ["t1"],
      payload: { reason: "startup" },
      seq: 0,
    } as any;
    const row = toRow(evt);
    expect(row["cwd"]).toBe("/tmp/proj");
    expect(row["session_file"]).toBe("/tmp/proj/.gg/session.jsonl");
    expect(row["agent_name"]).toBe("test-agent");
  });

  it("T2.b — insertEvent + rowToEvent round-trips cwd / session_file / agent_name", () => {
    const db = createDb(dbPath);
    const q = prepare(db);
    const evt = {
      event_id: "e2",
      ts: "2026-07-06T12:00:01.000Z",
      type: "assistant_message" as const,
      session_id: "s2",
      cwd: "/home/u/r",
      session_file: "/home/u/r/.gg/s.jsonl",
      agent_name: "gg-coder",
      pool: "default",
      tags: [],
      payload: { text: "hi", tool_call_ids: [], stop_reason: "stop", usage: { input: 1, output: 1, cache_read: 0, cache_write: 0, total_tokens: 2, cost_total: 0 }, turn_index: 0 },
      seq: 0,
    } as any;
    q.upsertSession.run(toSessionRow(evt));
    q.insertEvent.run(toRow(evt));
    const row = db.prepare(`SELECT * FROM events WHERE event_id = ?`).get("e2") as any;
    const back = rowToEvent(row);
    expect(back.cwd).toBe("/home/u/r");
    expect(back.session_file).toBe("/home/u/r/.gg/s.jsonl");
    expect(back.agent_name).toBe("gg-coder");
    expect(back.payload).toEqual(evt.payload);
  });

  it("T2.c — schema migration adds cwd/session_file/agent_name to a pre-existing DB", () => {
    // Simulate an "old" DB by creating one with only the legacy columns.
    const legacyPath = path.join(tmpDir, "legacy.db");
    const Database = require("better-sqlite3");
    const legacy = new Database(legacyPath);
    legacy.exec(`
      CREATE TABLE sessions (
        session_id TEXT PRIMARY KEY, pool TEXT NOT NULL DEFAULT 'default',
        agent_name TEXT, cwd TEXT, session_file TEXT, provider TEXT, model TEXT,
        first_ts TEXT NOT NULL, last_ts TEXT NOT NULL, event_count INTEGER NOT NULL DEFAULT 0,
        tags_json TEXT NOT NULL DEFAULT '[]'
      );
      CREATE TABLE events (
        event_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, seq INTEGER NOT NULL,
        ts TEXT NOT NULL, type TEXT NOT NULL, pool TEXT NOT NULL DEFAULT 'default',
        tags_json TEXT NOT NULL DEFAULT '[]', payload_json TEXT NOT NULL,
        provider TEXT, model TEXT,
        FOREIGN KEY (session_id) REFERENCES sessions(session_id)
      );
    `);
    legacy.close();

    // Open with createDb — migration should ALTER TABLE to add the 3 cols.
    const db = createDb(legacyPath);
    const cols = (db.prepare(`PRAGMA table_info(events)`).all() as Array<{ name: string }>).map((c) => c.name);
    expect(cols).toContain("cwd");
    expect(cols).toContain("session_file");
    expect(cols).toContain("agent_name");
  });

  it("T2.d — duplicate event insert is idempotent (changes === 0)", () => {
    const db = createDb(dbPath);
    const q = prepare(db);
    const evt = {
      event_id: "e3", ts: "2026-07-06T12:00:00.000Z", type: "user_message" as const,
      session_id: "s3", cwd: "/x", pool: "default", tags: [], payload: { text: "x", images_count: 0 }, seq: 0,
    } as any;
    q.upsertSession.run(toSessionRow(evt));
    const r1 = q.insertEvent.run(toRow(evt));
    const r2 = q.insertEvent.run(toRow(evt));
    expect(r1.changes).toBe(1);
    expect(r2.changes).toBe(0);
    const count = (db.prepare(`SELECT COUNT(*) AS c FROM events`).get() as any).c;
    expect(count).toBe(1);
  });
});