/**
 * bridge-ops/server.ts — minimal Hono server for the bridge-ops demo.
 *
 * This is the ggcoder-observability equivalent of pi-agent-observability's
 * apps/steelman/ — a real product app on top of the telemetry stack. For now
 * it's the skeleton: the /api/runs endpoint stores an in-memory run record and
 * emits a small handful of synthetic ObsEvents into the observability server
 * via POST /events, simulating an agent walking through the operator workflow
 * (Twenty+my-app+bridge: write-proposal approval flow). The full agent loop
 * lands week 2; for week 1 we just want to prove the obs layer accepts and
 * renders a non-trivial product workflow.
 *
 * Wire contract: see ../../../shared/types.ts (the same ObsEvent the live
 * daemon emits). The events we synthesize here use the exact same shape so the
 * UI / DB / replay path don't know or care that we generated them locally.
 */

import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { serve } from "@hono/node-server";
import { randomUUID, createHash } from "node:crypto";
import * as os from "node:os";
import type { ObsEvent } from "../../../shared/types.js";

// ─── Config ─────────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.BRIDGE_OPS_PORT ?? "45210", 10);
const HOST = process.env.BRIDGE_OPS_HOST ?? "127.0.0.1";
const OBS_SERVER_URL = process.env.OBS_SERVER_URL ?? "http://127.0.0.1:43190";
const OBS_AUTH_TOKEN = process.env.OBS_AUTH_TOKEN ?? "";

// ─── Run model ──────────────────────────────────────────────────────────────

type RunStatus = "starting" | "running" | "done" | "error";

interface ObsStreamEvent {
  ts: string;
  type:
    | "run"
    | "status"
    | "obs_event"
    | "obs_url"
    | "error";
  status?: RunStatus;
  message?: string;
  /** An ObsEvent envelope that we forwarded to the obs server. */
  event?: ObsEvent;
  /** URL into the obs UI scoped to this run (filled in when we know session_id). */
  obs_url?: string;
  run_id: string;
  scenario: string;
  company_id: string;
  started_at: string;
}

interface Run {
  id: string;
  scenario: string;
  company_id: string;
  status: RunStatus;
  started_at: string;
  updated_at: string;
  obs_session_id: string;
  obs_url?: string;
  events: ObsStreamEvent[];
  error?: string;
}

const runs = new Map<string, Run>();
const startTime = Date.now();

// SSE subscriber registry — keyed by run_id.
const subscribers = new Map<string, Set<{ enqueue: (chunk: Uint8Array) => void }>>();

// ─── Helpers ────────────────────────────────────────────────────────────────

function nowIso(): string {
  return new Date().toISOString();
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "Authorization, Content-Type",
    },
  });
}

function sseFrame(event: string, data: unknown): Uint8Array {
  return new TextEncoder().encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

/** Push an SSE event to every subscriber of this run. */
function broadcastRunEvent(run: Run, evt: ObsStreamEvent): void {
  run.events.push(evt);
  if (run.events.length > 500) run.events.splice(0, run.events.length - 500);
  run.updated_at = nowIso();
  const frame = sseFrame(evt.type, evt);
  for (const sub of subscribers.get(run.id) ?? []) {
    try {
      sub.enqueue(frame);
    } catch {
      /* closed */
    }
  }
}

function setStatus(run: Run, status: RunStatus, message?: string): void {
  run.status = status;
  broadcastRunEvent(run, { ts: nowIso(), type: "status", run_id: run.id, scenario: run.scenario, company_id: run.company_id, started_at: run.started_at, status, message });
}

/**
 * POST a single ObsEvent envelope to the observability server's /events.
 * Returns true on success, false on HTTP/auth failure (logged, never fatal —
 * the run proceeds; telemetry is best-effort during the demo).
 */
async function postObsEvent(event: ObsEvent): Promise<boolean> {
  if (!OBS_AUTH_TOKEN) {
    process.stderr.write("[bridge-ops] OBS_AUTH_TOKEN not set; cannot forward synthetic events\n");
    return false;
  }
  try {
    const res = await fetch(`${OBS_SERVER_URL.replace(/\/+$/, "")}/events`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${OBS_AUTH_TOKEN}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(event),
    });
    if (!res.ok) {
      process.stderr.write(`[bridge-ops] /events -> ${res.status}: ${await res.text().catch(() => "")}\n`);
      return false;
    }
    return true;
  } catch (err: any) {
    process.stderr.write(`[bridge-ops] /events fetch failed: ${err?.message ?? err}\n`);
    return false;
  }
}

/** Build a fresh ObsEvent envelope with the canonical fields filled in. */
function makeEvent<K extends ObsEvent["type"]>(
  run: Run,
  type: K,
  payload: Extract<ObsEvent, { type: K }>["payload"],
  seq: number,
  extra: Partial<ObsEvent> = {},
): ObsEvent {
  const base = {
    event_id: randomUUID(),
    ts: nowIso(),
    type,
    session_id: run.obs_session_id,
    cwd: process.cwd(),
    agent_name: `bridge-ops-${run.id}`,
    pool: "product-bridge-ops",
    tags: ["scenario:write-proposal", "demo"],
    payload,
    seq,
  };
  return { ...base, ...extra } as ObsEvent;
}

function obsUrlFor(run: Run): string {
  const u = new URL(OBS_SERVER_URL);
  if (OBS_AUTH_TOKEN) u.searchParams.set("token", OBS_AUTH_TOKEN);
  u.hash = new URLSearchParams({
    view: "single",
    pool: "product-bridge-ops",
    tag: `scenario:write-proposal`,
    session: run.obs_session_id,
  }).toString();
  return u.toString();
}

/** Stable session_id keyed off the run id (and a short sha of company/scenario). */
function makeObsSessionId(run: Run): string {
  const h = createHash("sha256").update(`${run.scenario}:${run.company_id}:${run.id}`).digest("hex").slice(0, 12);
  return `run-${run.id}-${h}`;
}

// ─── Synthetic workflow ─────────────────────────────────────────────────────

/**
 * Walks the operator-side approval flow as 5 canonical events:
 *   1. session_start  (reason: "startup", ties the whole run to one session_id)
 *   2. agent_start    (a single-shot operator prompt — Twenty/my-app/bridge approval)
 *   3. tool_call      (ask_record → writes a Twenty decision record)
 *   4. tool_result    (record id returned, success)
 *   5. session_shutdown
 *
 * This is intentionally short: the goal of week-1 is to prove that the obs
 * pipeline accepts and persists a non-trivial product workflow, not to ship
 * the full agent. The full demo (real tool loop, retry, approval back-channel,
 * notifications) lands week 2.
 */
async function runScenario(run: Run): Promise<void> {
  setStatus(run, "running", "Starting operator workflow");

  const events: ObsEvent[] = [
    makeEvent(run, "session_start", { reason: "startup" }, 0),
    makeEvent(
      run,
      "agent_start",
      {
        prompt:
          `Operator workflow: write-proposal-approval for company ${run.company_id}.\n` +
          `Open the Twenty record, draft the proposal in my-app, then ask the bridge agent to ` +
          `record approval status.`,
        images_count: 0,
      },
      1,
    ),
    makeEvent(
      run,
      "tool_call",
      {
        tool_call_id: randomUUID(),
        tool_name: "ask_record",
        args: {
          action: "write_proposal_approval",
          company_id: run.company_id,
          proposal_id: `prop-${run.id}`,
        },
        args_truncated: false,
      },
      2,
    ),
    makeEvent(
      run,
      "tool_result",
      {
        tool_call_id: "self", // overwritten below
        tool_name: "ask_record",
        content_text: `Decision recorded for company ${run.company_id} (proposal prop-${run.id})`,
        content_truncated: false,
        is_error: false,
        details_summary: { decision_id: `dec-${run.id}`, status: "pending" },
      },
      3,
    ),
    makeEvent(
      run,
      "session_shutdown",
      { reason: "quit" },
      4,
    ),
  ];

  // Patch the tool_result's tool_call_id to match the tool_call above so the
  // UI can correlate them visually. (We can do this post-hoc because we
  // control the entire workflow.)
  const toolCallId = (events[2].payload as { tool_call_id: string }).tool_call_id;
  (events[3].payload as { tool_call_id: string }).tool_call_id = toolCallId;

  for (const evt of events) {
    const ok = await postObsEvent(evt);
    broadcastRunEvent(run, {
      ts: nowIso(),
      type: "obs_event",
      run_id: run.id,
      scenario: run.scenario,
      company_id: run.company_id,
      started_at: run.started_at,
      event: evt,
      message: ok ? undefined : "obs forward failed",
    });
    // Tiny delay so the SSE stream feels like a real workflow.
    await new Promise((r) => setTimeout(r, 120));
  }

  run.obs_url = obsUrlFor(run);
  broadcastRunEvent(run, {
    ts: nowIso(),
    type: "obs_url",
    run_id: run.id,
    scenario: run.scenario,
    company_id: run.company_id,
    started_at: run.started_at,
    obs_url: run.obs_url,
  });

  setStatus(run, "done", "Operator workflow finished");
}

// ─── Hono app ───────────────────────────────────────────────────────────────

const app = new Hono();

app.options("*", () =>
  new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "Authorization, Content-Type",
    },
  }),
);

// /health — un-authenticated; matches the obs server's surface.
app.get("/health", (c) =>
  c.json({
    ok: true,
    app: "bridge-ops",
    uptime_s: Math.round((Date.now() - startTime) / 1000),
    runs: runs.size,
    obs_server_url: OBS_SERVER_URL,
    obs_auth_set: !!OBS_AUTH_TOKEN,
  }),
);

// POST /api/runs — create a new run and start the synthetic workflow.
app.post("/api/runs", async (c) => {
  let body: { scenario?: string; companyId?: string };
  try {
    body = (await c.req.json()) as typeof body;
  } catch {
    return jsonResponse({ error: "invalid JSON body" }, 400);
  }

  const scenario = String(body?.scenario ?? "").trim();
  const companyId = String(body?.companyId ?? "").trim();
  if (!scenario) return jsonResponse({ error: "scenario is required" }, 400);
  if (!companyId) return jsonResponse({ error: "companyId is required" }, 400);
  if (scenario !== "write-proposal-approval") {
    return jsonResponse({ error: `unknown scenario: ${scenario}` }, 400);
  }

  const id = randomUUID().slice(0, 12);
  const run: Run = {
    id,
    scenario,
    company_id: companyId,
    status: "starting",
    started_at: nowIso(),
    updated_at: nowIso(),
    obs_session_id: "", // filled in below
    events: [],
  };
  run.obs_session_id = makeObsSessionId(run);
  runs.set(id, run);

  // Emit the initial "run" frame so a fresh SSE subscriber sees the snapshot.
  broadcastRunEvent(run, {
    ts: nowIso(),
    type: "run",
    run_id: run.id,
    scenario: run.scenario,
    company_id: run.company_id,
    started_at: run.started_at,
    status: run.status,
  });

  // Fire and forget — the SSE stream will surface progress.
  runScenario(run).catch((err) => {
    run.error = err?.message ?? String(err);
    setStatus(run, "error", run.error);
  });

  return jsonResponse(
    {
      run_id: id,
      scenario: run.scenario,
      company_id: run.company_id,
      obs_session_id: run.obs_session_id,
      status: run.status,
    },
    201,
  );
});

// GET /api/runs/:id — fetch the current run snapshot.
app.get("/api/runs/:id", (c) => {
  const run = runs.get(c.req.param("id"));
  if (!run) return jsonResponse({ error: "run not found" }, 404);
  return jsonResponse({
    run_id: run.id,
    scenario: run.scenario,
    company_id: run.company_id,
    status: run.status,
    started_at: run.started_at,
    updated_at: run.updated_at,
    obs_session_id: run.obs_session_id,
    obs_url: run.obs_url,
    event_count: run.events.length,
    events: run.events,
    error: run.error,
  });
});

// GET /api/runs/:id/stream — SSE stream of run status + forwarded obs events.
app.get("/api/runs/:id/stream", (c) => {
  const run = runs.get(c.req.param("id"));
  if (!run) return jsonResponse({ error: "run not found" }, 404);

  return streamSSE(c, async (stream) => {
    let active = true;
    const subscriber = {
      enqueue: (chunk: Uint8Array) => {
        if (!active) return;
        try {
          stream.write(chunk);
        } catch {
          /* closed */
        }
      },
    };
    if (!subscribers.has(run.id)) subscribers.set(run.id, new Set());
    subscribers.get(run.id)!.add(subscriber);

    // Initial snapshot: current run state + the events we have so far.
    await stream.writeSSE({
      event: "run",
      data: JSON.stringify({
        ts: nowIso(),
        type: "run",
        run_id: run.id,
        scenario: run.scenario,
        company_id: run.company_id,
        started_at: run.started_at,
        status: run.status,
      }),
    });
    for (const evt of run.events) {
      await stream.writeSSE({ event: evt.type, data: JSON.stringify(evt) });
    }

    // Hold open until client disconnects.
    await new Promise<void>((resolve) => {
      const abort = () => {
        c.req.raw.signal.removeEventListener("abort", abort);
        resolve();
      };
      c.req.raw.signal.addEventListener("abort", abort);
      const onAbort = (stream as unknown as { onAbort?: (fn: () => void) => void }).onAbort;
      if (typeof onAbort === "function") onAbort(abort);
    });

    active = false;
    subscribers.get(run.id)?.delete(subscriber);
  });
});

app.notFound((c) => jsonResponse({ error: "not found" }, 404));

// ─── Boot ───────────────────────────────────────────────────────────────────

serve(
  {
    fetch: app.fetch,
    port: PORT,
    hostname: HOST,
  },
  (info) => {
    console.log(`  bridge-ops server v0.1.0`);
    console.log(`  Listening on http://${HOST}:${info.port}`);
    console.log(`  OBS_SERVER_URL: ${OBS_SERVER_URL}  token=${OBS_AUTH_TOKEN ? "set" : "MISSING"}`);
    console.log(`  Hostname: ${os.hostname()}`);
  },
);