/**
 * T1 — Mapper on sample JSONL.
 *
 * Asserts:
 *  - mapper emits the expected event count + types for a tiny synthetic session
 *  - envelopes carry session_id, cwd, provider, model, seq monotonic
 *  - regression for G1: {type:"message"} lines pass the type predicate
 *  - regression for G2: tool_result does NOT close the turn or bump turn_index
 */

import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { mapSession } from "./gg-obs-mapper.js";

function writeFixture(name: string, lines: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-mapper-"));
  const file = path.join(dir, name);
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  return file;
}

const header = {
  type: "session" as const,
  version: 2,
  id: "sess-test-1",
  timestamp: "2026-07-06T12:00:00.000Z",
  cwd: "/tmp/proj",
  provider: "anthropic",
  model: "claude-opus-4",
  leafId: "m3",
};

const userMsg = {
  type: "message" as const,
  id: "m1",
  parentId: null,
  timestamp: "2026-07-06T12:00:01.000Z",
  message: { role: "user" as const, content: "Hello" },
};

const assistantMsg = {
  type: "message" as const,
  id: "m2",
  parentId: "m1",
  timestamp: "2026-07-06T12:00:02.000Z",
  message: {
    role: "assistant" as const,
    content: [
      { type: "text", text: "Hi there" },
      { type: "tool_call", id: "tc1", name: "bash", args: { command: "ls" } },
    ],
  },
};

const toolMsg = {
  type: "message" as const,
  id: "m3",
  parentId: "m2",
  timestamp: "2026-07-06T12:00:03.000Z",
  message: {
    role: "tool" as const,
    content: [
      { type: "tool_result", toolCallId: "tc1", content: "file.txt\n" },
    ],
  },
};

describe("mapSession", () => {
  it("T1.a — emits session_start + user + assistant + tool + trailing turn_end", () => {
    const file = writeFixture("s1.jsonl", [header, userMsg, assistantMsg, toolMsg]);
    const events = [...mapSession(file)];
    const types = events.map((e) => e.type);

    // Order: session_start → user_message → turn_start (assistant) → tool_call → assistant_message → tool_result → turn_end
    expect(types).toEqual([
      "session_start",
      "user_message",
      "turn_start",
      "tool_call",
      "assistant_message",
      "tool_result",
      "turn_end",
    ]);
  });

  it("T1.b — every event has session_id, cwd, provider, model", () => {
    const file = writeFixture("s2.jsonl", [header, userMsg, assistantMsg, toolMsg]);
    for (const e of mapSession(file)) {
      expect(e.session_id).toBe("sess-test-1");
      expect(e.cwd).toBe("/tmp/proj");
      expect(e.provider).toBe("anthropic");
      expect(e.model).toBe("claude-opus-4");
    }
  });

  it("T1.c — seq is monotonic starting at 0", () => {
    const file = writeFixture("s3.jsonl", [header, userMsg, assistantMsg, toolMsg]);
    const seqs = [...mapSession(file)].map((e) => e.seq);
    for (let i = 1; i < seqs.length; i++) expect(seqs[i]).toBe(seqs[i - 1]! + 1);
    expect(seqs[0]).toBe(0);
  });

  it("T1.d (G2 regression) — tool_result does not bump turn_index; turn_index stays 0 across tool call", () => {
    const file = writeFixture("s4.jsonl", [header, userMsg, assistantMsg, toolMsg]);
    const events = [...mapSession(file)];
    const turnStarts = events.filter((e) => e.type === "turn_start");
    const turnEnds = events.filter((e) => e.type === "turn_end");
    expect(turnStarts).toHaveLength(1);
    expect(turnEnds).toHaveLength(1);
    expect((turnStarts[0]!.payload as any).turn_index).toBe(0);
    expect((turnEnds[0]!.payload as any).turn_index).toBe(0);
  });

  it("T1.e (G1 regression) — mapper accepts {type:'message'} lines without dropping", () => {
    const file = writeFixture("s5.jsonl", [header, userMsg, assistantMsg, toolMsg]);
    const messages = [...mapSession(file)].filter((e) =>
      ["user_message", "assistant_message", "tool_result"].includes(e.type),
    );
    // userMessage + assistantMessage + toolResult = 3 messages
    expect(messages).toHaveLength(3);
  });

  it("T1.f — unknown roles fall through to a 'custom' event", () => {
    const odd = {
      type: "message" as const,
      id: "m4",
      parentId: "m3",
      timestamp: "2026-07-06T12:00:04.000Z",
      message: { role: "system_fictional" as any, content: [] },
    };
    // Header must point to m4 (the new leaf) so the walker visits it.
    const hdr = { ...header, leafId: "m4" };
    const file = writeFixture("s6.jsonl", [hdr, userMsg, assistantMsg, toolMsg, odd]);
    const custom = [...mapSession(file)].filter((e) => e.type === "custom");
    expect(custom).toHaveLength(1);
    expect((custom[0]!.payload as any).custom_type).toBe("gg_role_system_fictional");
  });
});