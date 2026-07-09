/**
 * Test for the generic events query route added in this turn.
 *
 * Route: GET /events?type=&provider=&tag=&limit=&since=
 *
 * Coverage:
 *  - type filter (LIKE prefix) matches the supplied type
 *  - provider filter (exact) matches the supplied provider
 *  - tag filter (LIKE substring on tags_json) matches one of the row's tags
 *  - since filter (event.seq > since) excludes older events
 *  - limit clamps to [1, 1000] with default 200
 *  - no-auth returns 401
 *  - no filters returns all events newest-first
 *
 * The test spins up a fresh server on an ephemeral port with a temp DB
 * (same pattern as validation.test.ts) and posts a few events of
 * different shapes via the existing POST /events route, then queries
 * GET /events with various filter combinations and asserts the shape.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";

let server: ChildProcess | null = null;
let port = 0;
const token = "testtoken-events";
const TEST_PORT = 54323;

interface Event {
  event_id: string;
  session_id: string;
  seq: number;
  ts: string;
  type: string;
  pool: string;
  tags: string[];          // wire shape: array, server stringifies internally
  payload_json: string;
  provider: string;
  model: string | null;
  cwd: string;
  session_file: string | null;
  agent_name: string | null;
}
interface EventsResponse {
  events: Event[];
  count: number;
}

beforeAll(async () => {
  const tmpDb = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-events-test-"));
  const dbPath = path.join(tmpDb, "test.db");
  server = spawn("node", ["--import", "tsx", "server/server.ts"], {
    env: {
      ...process.env,
      OBS_PORT: String(TEST_PORT),
      OBS_AUTH_TOKEN: token,
      OBS_DB_PATH: dbPath,
      OBS_HOST: "127.0.0.1",
    },
    cwd: process.cwd(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server didn't start in 10s")), 10000);
    server!.stdout?.on("data", (chunk) => {
      if (chunk.toString().includes(`Listening on http://127.0.0.1:${TEST_PORT}`)) {
        clearTimeout(timer); resolve();
      }
    });
  });
  port = TEST_PORT;
  // Populate the bus with a known set of events covering every filter dimension.
  // We POST directly so we don't depend on event_id generation from a specific
  // shape — we hand-pick event_ids and rely on the server's INSERT OR IGNORE
  // for idempotency in the test's repeat runs.
  const seed: Event[] = [
    {
      event_id: "ev-type-A", session_id: "sess-A", seq: 1, ts: "2026-07-01T00:00:00.000Z",
      type: "twenty.skill_fired", pool: "default",
      tags: ["twenty", "my-app"],
      payload_json: JSON.stringify({ skill: "ask", ok: true }),
      provider: "twenty", model: null, cwd: "", session_file: null, agent_name: null,
    },
    {
      event_id: "ev-type-B", session_id: "sess-B", seq: 2, ts: "2026-07-02T00:00:00.000Z",
      type: "twenty.timeline_logged", pool: "default",
      tags: ["twenty", "my-app"],
      payload_json: JSON.stringify({ object_name: "company" }),
      provider: "twenty", model: null, cwd: "", session_file: null, agent_name: null,
    },
    {
      event_id: "ev-type-C", session_id: "sess-C", seq: 3, ts: "2026-07-03T00:00:00.000Z",
      type: "assistant_message", pool: "default",
      tags: ["ggcoder", "noledge"],
      payload_json: JSON.stringify({ text: "ok" }),
      provider: "ggcoder", model: "claude-sonnet-4-6", cwd: "", session_file: null, agent_name: null,
    },
    {
      event_id: "ev-type-D", session_id: "sess-D", seq: 4, ts: "2026-07-04T00:00:00.000Z",
      type: "turn_start", pool: "default",
      tags: ["ggcoder", "my-app"],
      payload_json: JSON.stringify({ turn_index: 1 }),
      provider: "ggcoder", model: "claude-sonnet-4-6", cwd: "", session_file: null, agent_name: null,
    },
  ];
  for (const e of seed) {
    const res = await fetch(`http://127.0.0.1:${port}/events?token=${token}`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify(e),
    });
    expect(res.status).toBe(200);
  }
}, 15000);

afterAll(async () => {
  if (server) {
    server.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 500));
    if (!server.killed) server.kill("SIGKILL");
  }
});

async function getEvents(qs: string): Promise<{ status: number; body: EventsResponse | { error: string } }> {
  const res = await fetch(`http://127.0.0.1:${port}/events?${qs}&token=${token}`);
  let body: any;
  try { body = await res.json(); } catch { body = { error: "non_json" }; }
  return { status: res.status, body };
}

async function getEventsNoAuth(qs: string): Promise<{ status: number }> {
  const res = await fetch(`http://127.0.0.1:${port}/events?${qs}`);
  return { status: res.status };
}

describe("GET /events generic filter route", () => {
  it("E1 — no filters returns all 4 events newest-first", async () => {
    const { status, body } = await getEvents("limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(4);
    expect(ok.events.length).toBe(4);
    // Newest first: seq 4, 3, 2, 1
    expect(ok.events.map((e) => e.seq)).toEqual([4, 3, 2, 1]);
  });

  it("E2 — type prefix 'twenty.' returns the 2 twenty.* events", async () => {
    const { status, body } = await getEvents("type=twenty.&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(2);
    const types = ok.events.map((e) => e.type).sort();
    expect(types).toEqual(["twenty.skill_fired", "twenty.timeline_logged"]);
  });

  it("E3 — type exact match 'twenty.skill_fired' returns only 1", async () => {
    const { status, body } = await getEvents("type=twenty.skill_fired&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(1);
    expect(ok.events[0].type).toBe("twenty.skill_fired");
  });

  it("E4 — provider='twenty' returns the 2 twenty.* events", async () => {
    const { status, body } = await getEvents("provider=twenty&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(2);
    expect(ok.events.every((e) => e.provider === "twenty")).toBe(true);
  });

  it("E5 — provider='ggcoder' returns the 2 ggcoder events", async () => {
    const { status, body } = await getEvents("provider=ggcoder&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(2);
    expect(ok.events.every((e) => e.provider === "ggcoder")).toBe(true);
  });

  it("E6 — tag='my-app' returns the 3 events tagged with my-app", async () => {
    const { status, body } = await getEvents("tag=my-app&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(3);
    for (const e of ok.events) {
      expect(JSON.parse(e.tags_json)).toContain("my-app");
    }
  });

  it("E7 — tag='noledge' returns only the 1 event tagged with noledge", async () => {
    const { status, body } = await getEvents("tag=noledge&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(1);
    expect(JSON.parse(ok.events[0].tags_json)).toContain("noledge");
  });

  it("E8 — combined type+provider: twenty.skill_fired AND provider=twenty → 1 event", async () => {
    const { status, body } = await getEvents("type=twenty.skill_fired&provider=twenty&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(1);
    expect(ok.events[0].event_id).toBe("ev-type-A");
  });

  it("E9 — combined: type=twenty.* & tag=noledge → 0 (no twenty.* has noledge tag)", async () => {
    const { status, body } = await getEvents("type=twenty.&tag=noledge&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(0);
  });

  it("E10 — since=2 returns only events with seq > 2", async () => {
    const { status, body } = await getEvents("since=2&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(2);
    const seqs = ok.events.map((e) => e.seq).sort();
    expect(seqs).toEqual([3, 4]);
  });

  it("E11 — since=10 returns 0 events (all are below)", async () => {
    const { status, body } = await getEvents("since=10&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(0);
  });

  it("E12 — limit=1 returns 1 event (the newest)", async () => {
    const { status, body } = await getEvents("limit=1");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(1);
    expect(ok.events[0].seq).toBe(4);
  });

  it("E13 — limit=99999 clamps to 200 (we only have 4 events, so still 4)", async () => {
    const { status, body } = await getEvents("limit=99999");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(4);
  });

  it("E14 — limit=foo returns defaults to 200 (no crash)", async () => {
    const { status, body } = await getEvents("limit=foo");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(4);
  });

  it("E15 — limit=-1 clamps to 1 (returns 1 event)", async () => {
    const { status, body } = await getEvents("limit=-1");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(1);
  });

  it("E16 — no auth returns 401", async () => {
    const { status } = await getEventsNoAuth("type=twenty.skill_fired");
    expect(status).toBe(401);
  });

  it("E17 — empty response (no matching type) returns count=0 with events=[]", async () => {
    const { status, body } = await getEvents("type=does.not.exist&limit=10");
    expect(status).toBe(200);
    const ok = body as EventsResponse;
    expect(ok.count).toBe(0);
    expect(ok.events).toEqual([]);
  });
});
