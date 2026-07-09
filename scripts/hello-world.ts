/**
 * hello-world.ts — minimal smoke test for the gg-observability stack.
 *
 * Emits 5 canonical ObsEvents (session_start → turn_start → assistant_message
 * → turn_end → session_shutdown) directly to the obs server's POST /events
 * endpoint, with text="Hello, World!". Used to verify the UI is rendering
 * end-to-end without needing to launch a real ggcoder session.
 *
 * Run from the repo root:
 *   OBS_AUTH_TOKEN=$(cat ~/.gg/observability/token) \
 *     ./node_modules/.bin/tsx scripts/hello-world.ts
 *
 * Then open http://127.0.0.1:43190/#view=single in the obs UI to confirm.
 */

const SERVER_URL = process.env.OBS_SERVER_URL ?? "http://127.0.0.1:43190";
const TOKEN = process.env.OBS_AUTH_TOKEN ?? "";

if (!TOKEN) {
  console.error("✗ OBS_AUTH_TOKEN is required (e.g. $(cat ~/.gg/observability/token))");
  process.exit(1);
}

const sessionId = `hello-world-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const now = () => new Date().toISOString();

const envelope = (
  seq: number,
  type: string,
  payload: Record<string, unknown>,
): Record<string, unknown> => ({
  event_id: crypto.randomUUID(),
  ts: now(),
  type,
  session_id: sessionId,
  cwd: process.cwd(),
  pool: "hello-world",
  agent_name: "hello-world-script",  // matches what pi's --o-name would set
  tags: ["smoke-test", "task"],
  provider: "manual",
  model: "hello-world-script",
  payload,
  seq,
});

const events = [
  envelope(0, "session_start", { reason: "startup" }),
  envelope(1, "turn_start", { turn_index: 0 }),
  envelope(2, "assistant_message", {
    text: "Hello, World!",
    thinking: "",
    tool_call_ids: [],
    stop_reason: "stop",
    usage: { input: 0, output: 0, cache_read: 0, cache_write: 0, total_tokens: 0, cost_total: 0 },
    latency_ms: 1,
    turn_index: 0,
  }),
  envelope(3, "turn_end", { turn_index: 0, usage: { input: 0, output: 0, cache_read: 0, cache_write: 0, total_tokens: 0, cost_total: 0 } }),
  envelope(4, "session_shutdown", { reason: "quit" }),
];

const response = await fetch(`${SERVER_URL}/events`, {
  method: "POST",
  headers: {
    "authorization": `Bearer ${TOKEN}`,
    "content-type": "application/json",
  },
  body: JSON.stringify(events),
});

if (!response.ok) {
  console.error(`✗ POST /events → HTTP ${response.status}: ${await response.text()}`);
  process.exit(1);
}

const result = await response.json() as { ingested: number; rejected: string[] };
console.log(`✓ emitted ${events.length} events to ${SERVER_URL}`);
console.log(`  session_id: ${sessionId}`);
console.log(`  server accepted: ingested=${result.ingested}, rejected=${result.rejected.length}`);
console.log(`  open: ${SERVER_URL}/#view=single&session=${sessionId}`);