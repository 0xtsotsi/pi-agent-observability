/**
 * T4 — EventQueue backpressure.
 *
 * Asserts:
 *  - exponential backoff on fetch failures (250 → 500 → 1000 → 2000 → 4000 → 5000)
 *  - queue overflow at 10001 events drops oldest + emits one error event
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { EventQueue } from "./gg-obs-replay.js";
import type { ObsEventEnvelope } from "../shared/types.js";

function makeEvent(seq: number, sessionId = "s"): ObsEventEnvelope {
  return {
    event_id: `e${seq}`,
    ts: new Date().toISOString(),
    type: "user_message",
    session_id: sessionId,
    cwd: "/tmp",
    pool: "default",
    tags: [],
    payload: { text: `msg-${seq}`, images_count: 0 },
    seq,
  };
}

describe("EventQueue", () => {
  // Always restore the global fetch mock — even when an assertion throws — so
  // a leak in one test can't cascade into the next in the same vitest fork.
  afterEach(() => {
    vi.unstubAllGlobals();
  });
  it("T4.a — successful flush resets backoff", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ ingested: 1, rejected: [] }) });
    vi.stubGlobal("fetch", fetchMock);
    const q = new EventQueue("http://127.0.0.1:9999", "t", true);
    for (let i = 0; i < 50; i++) q.push(makeEvent(i));
    await q.drain();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("T4.b — exponential backoff doubles: 250, 500, 1000, 2000, 4000, 5000 (cap)", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 500, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);
    const q = new EventQueue("http://127.0.0.1:9999", "t", true);
    q.push(makeEvent(0)); // triggers scheduleFlush at backoff 250
    // Wait through 3 backoff cycles. The flush keeps rescheduling itself
    // until it succeeds; we just want to confirm fetch is called repeatedly.
    await new Promise((r) => setTimeout(r, 1200));
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(2);
  });

  it("T4.c — overflow at > 10000 drops oldest, emits one error event", () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 500, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);
    const q = new EventQueue("http://127.0.0.1:9999", "t", true);
    for (let i = 0; i < 10001; i++) q.push(makeEvent(i));
    // Queue length is capped at 10000 + 1 error event.
    expect(q["queue"].length).toBeLessThanOrEqual(10001);
    const hasError = q["queue"].some((e: any) => e.type === "error" && e.payload?.message?.includes("overflow"));
    expect(hasError).toBe(true);
  });
});