/**
 * gg-obs-mapper.ts — ggcoder session JSONL → canonical ObsEvent envelopes.
 *
 * ggcoder writes session files to ~/.gg/sessions/<project>/<iso-ts>_<id>.jsonl.
 * Each line is one of:
 *
 *   {type:"session", version:2, id, timestamp, cwd, provider, model, leafId}
 *     — header; the first line of the file. Identifies the session.
 *
 *   {type:"message", id, parentId, timestamp, message:{role, content}}
 *     — a node in a parentId-linked tree. `role` is "user" | "assistant" | "tool".
 *     `content` is a string (user messages) or an array of blocks.
 *
 *     Block types we care about:
 *       - "text"       {type, text}                        — assistant narration
 *       - "thinking"   {type, text, signature?}             — model thinking
 *       - "tool_call"  {type, id, name, args}               — model issuing a call
 *       - "tool_result" {type, toolCallId, content}         — tool returning output
 *       - "raw"        {type, data:{id, type:"reasoning", content, encrypted_content, summary}}
 *                                                          — OpenAI reasoning w/ encrypted blobs
 *
 *     Anything else falls into a `custom` event so nothing is silently dropped.
 *
 *   Unknown top-level types are emitted as `custom` events with the raw payload.
 *
 * Output: an iterable of canonical ObsEvent envelopes matching shared/types.ts.
 * Default walk is DFS from the leafId chain; `allBranches:true` emits every
 * reachable node in BFS order (turn_start/turn_end still synthesized per
 * assistant message boundary).
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as readline from "node:readline";
import {
  type ObsEventEnvelope,
  type SessionStartPayload,
  type UserMessagePayload,
  type AssistantMessagePayload,
  type ToolCallPayload,
  type ToolResultPayload,
  type ThinkingPayload,
  type TurnStartPayload,
  type TurnEndPayload,
  type CustomPayload,
  type BranchNavPayload,
  type UsageSummary,
} from "../shared/types.js";

// ─── ggcoder wire types (loose — we accept anything that quacks) ─────────────

interface GgSessionHeader {
  type: "session";
  version: number;
  id: string;
  timestamp: string;
  cwd: string;
  provider: string;
  model: string;
  leafId: string | null;
}

interface GgTextBlock       { type: "text";        text: string }
interface GgThinkingBlock   { type: "thinking";    text: string; signature?: string }
interface GgToolCallBlock   { type: "tool_call";   id: string; name: string; args: Record<string, unknown> }
interface GgToolResultBlock { type: "tool_result"; toolCallId: string; content: string; is_error?: boolean }
interface GgRawBlock        { type: "raw"; data: { id?: string; type?: string; content?: unknown[]; encrypted_content?: string; summary?: unknown[] } }
interface GgUnknownBlock    { type: string; [k: string]: unknown }

type GgContentBlock = GgTextBlock | GgThinkingBlock | GgToolCallBlock | GgToolResultBlock | GgRawBlock | GgUnknownBlock;

interface GgMessage {
  type: "message";
  id: string;
  parentId: string | null;
  timestamp: string;
  message: {
    role: "user" | "assistant" | "tool" | string;
    content: string | GgContentBlock[];
  };
}

type GgLine = GgSessionHeader | GgMessage | { type: string; [k: string]: unknown };

// ─── Public API ─────────────────────────────────────────────────────────────

export interface MapperOptions {
  /** When false (default), walk only the leafId chain. When true, BFS through every reachable message. */
  allBranches?: boolean;
  /** Tags applied to every emitted event. Defaults to []. */
  tags?: string[];
  /** Pool/bucket name. Defaults to "default". */
  pool?: string;
  /** Friendly agent name; copied from session header when not provided. */
  agentName?: string;
}

/**
 * Map a ggcoder session.jsonl file to canonical ObsEvents.
 * Sync generator — easier to compose with the EventQueue in replay.ts.
 */
export function* mapSession(
  filePath: string,
  options: MapperOptions = {},
): Generator<ObsEventEnvelope> {
  const lines = readJsonlSync(filePath);
  const header = lines.find((l): l is GgSessionHeader => (l as any).type === "session") as GgSessionHeader | undefined;
  if (!header) {
    process.stderr.write(`[gg-obs-mapper] skipping ${filePath}: no {type:"session"} header\n`);
    return;
  }

  const messages = lines.filter((l): l is GgMessage => l.type === "message");
  const childrenByParent = buildChildIndex(messages);

  const sessionInfo: SessionSessionInfo = {
    sessionId: header.id,
    sessionFile: filePath,
    cwd: header.cwd,
    provider: header.provider,
    model: header.model,
    pool: options.pool ?? "default",
    tags: options.tags ?? [],
    agentName: options.agentName,
  };

  // ── seq counter is local to this generator ───────────────────────────────
  let seq = 0;
  const next = (): number => seq++;

  // ── session_start (first event) ─────────────────────────────────────────
  yield makeEnvelope<SessionStartPayload>("session_start", sessionInfo, next(), header.timestamp, {
    reason: "startup",
    pi_version: undefined,
    previous_session_file: undefined,
  });

  // ── figure out the walk order ───────────────────────────────────────────
  const walked = options.allBranches
    ? bfsFromAll(messages)
    : walkLeafChain(header.leafId, messages, childrenByParent);

  // ── emit branch_nav for skipped siblings of every walked parent ─────────
  const emittedBranchNav = new Set<string>();
  for (const msg of walked) {
    if (msg.parentId == null) continue;
    const siblings = childrenByParent.get(msg.parentId) ?? [];
    if (siblings.length <= 1) continue;
    for (const sib of siblings) {
      if (sib.id === msg.id) continue;
      const key = `${msg.parentId}->${sib.id}`;
      if (emittedBranchNav.has(key)) continue;
      emittedBranchNav.add(key);
      const payload: BranchNavPayload = { from_id: msg.parentId, to_id: sib.id, has_summary: false };
      yield makeEnvelope<BranchNavPayload>("branch_nav", sessionInfo, next(), sib.timestamp, payload);
    }
  }

  // ── turn boundary tracking ──────────────────────────────────────────────
  let turnIndex = 0;
  let openTurn: { started: boolean } = { started: false };

  for (const msg of walked) {
    const role = msg.message.role;
    const blocks = normalizeContent(msg.message.content);

    // Emit a synthetic turn_start at the boundary of every assistant message
    // (and at the start of a tool chain that follows).
    const isAssistantOrToolTurnStart = role === "assistant";
    if (isAssistantOrToolTurnStart && !openTurn.started) {
      const tsPayload: TurnStartPayload = { turn_index: turnIndex };
      yield makeEnvelope<TurnStartPayload>("turn_start", sessionInfo, next(), msg.timestamp, tsPayload);
      openTurn.started = true;
    }

    switch (role) {
      case "user": {
        const payload = userMessagePayload(blocks);
        yield makeEnvelope<UserMessagePayload>("user_message", sessionInfo, next(), msg.timestamp, payload);
        // A user message closes any open turn (the assistant that follows will start a new one).
        if (openTurn.started) {
          yield makeEnvelope<TurnEndPayload>("turn_end", sessionInfo, next(), msg.timestamp, { turn_index: turnIndex });
          openTurn.started = false;
          turnIndex++;
        }
        break;
      }

      case "assistant": {
        const result = assistantMessagePayload(blocks);
        // Thinking (ggcoder-style)
        for (const t of result.thinkings) {
          const tPayload: ThinkingPayload = { text: t };
          yield makeEnvelope<ThinkingPayload>("thinking", sessionInfo, next(), msg.timestamp, tPayload);
        }
        // Tool calls
        for (const tc of result.toolCalls) {
          yield makeEnvelope<ToolCallPayload>("tool_call", sessionInfo, next(), msg.timestamp, tc);
        }
        // OpenAI reasoning (ggcoder raw block) → custom event (per spec — wire format
        // requires payload to match type, so we can't reuse "thinking" with custom_type).
        for (const r of result.openaiReasonings) {
          const cp: CustomPayload = { custom_type: "openai_reasoning", data: r };
          yield makeEnvelope<CustomPayload>("custom", sessionInfo, next(), msg.timestamp, cp);
        }
        // Assistant message envelope (text + tool_call_ids + stop_reason)
        const aPayload: AssistantMessagePayload = {
          text: result.text,
          thinking: "",
          tool_call_ids: result.toolCalls.map((tc) => tc.tool_call_id),
          stop_reason: result.toolCalls.length > 0 ? "toolUse" : "stop",
          usage: emptyUsage(),
          turn_index: turnIndex,
        };
        yield makeEnvelope<AssistantMessagePayload>("assistant_message", sessionInfo, next(), msg.timestamp, aPayload);
        break;
      }

      case "tool": {
        const toolResults = toolResultPayloads(blocks);
        for (const tr of toolResults) {
          yield makeEnvelope<ToolResultPayload>("tool_result", sessionInfo, next(), msg.timestamp, tr);
        }
        // tool_result is part of the *same* assistant turn that issued the call;
        // turn_end is emitted by the user-message boundary or the trailing close
        // below. Do not increment turn_index here.
        break;
      }

      default: {
        const cp: CustomPayload = { custom_type: `gg_role_${role}`, data: msg };
        yield makeEnvelope<CustomPayload>("custom", sessionInfo, next(), msg.timestamp, cp);
        break;
      }
    }
  }

  // Close any unterminated turn so the session is well-formed.
  if (openTurn.started) {
    yield makeEnvelope<TurnEndPayload>("turn_end", sessionInfo, next(), new Date().toISOString(), { turn_index: turnIndex });
  }
}

// ─── Internals ──────────────────────────────────────────────────────────────

interface SessionSessionInfo {
  sessionId: string;
  sessionFile: string;
  cwd: string;
  provider: string;
  model: string;
  pool: string;
  tags: string[];
  agentName?: string;
}

function makeEnvelope<P>(
  type: ObsEventEnvelope["type"],
  info: SessionSessionInfo,
  seq: number,
  ts: string,
  payload: P,
): ObsEventEnvelope<P> {
  return {
    event_id: crypto.randomUUID(),
    ts,
    type,
    session_id: info.sessionId,
    session_file: info.sessionFile,
    cwd: info.cwd,
    agent_name: info.agentName,
    pool: info.pool,
    tags: info.tags,
    provider: info.provider,
    model: info.model,
    payload,
    seq,
  };
}

function emptyUsage(): UsageSummary {
  return { input: 0, output: 0, cache_read: 0, cache_write: 0, total_tokens: 0, cost_total: 0 };
}

function readJsonlSync(filePath: string): GgLine[] {
  const text = fs.readFileSync(filePath, "utf8");
  const out: GgLine[] = [];
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    try {
      out.push(JSON.parse(line) as GgLine);
    } catch (err: any) {
      process.stderr.write(`[gg-obs-mapper] bad JSON line in ${filePath}: ${err?.message ?? err}\n`);
    }
  }
  return out;
}

/**
 * Async variant of readJsonlSync — used when callers want streaming for very
 * large files. Same return shape.
 */
export async function readJsonlStream(filePath: string): Promise<GgLine[]> {
  const stream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out: GgLine[] = [];
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;
    try {
      out.push(JSON.parse(line) as GgLine);
    } catch (err: any) {
      process.stderr.write(`[gg-obs-mapper] bad JSON line in ${filePath}: ${err?.message ?? err}\n`);
    }
  }
  return out;
}

function buildChildIndex(messages: GgMessage[]): Map<string, GgMessage[]> {
  const idx = new Map<string, GgMessage[]>();
  for (const m of messages) {
    if (m.parentId == null) continue;
    const arr = idx.get(m.parentId);
    if (arr) arr.push(m);
    else idx.set(m.parentId, [m]);
  }
  return idx;
}

/**
 * Walk the parentId chain from leafId backwards to root, then reverse to
 * get the linear top-down order. Skips messages outside the chain entirely.
 */
function walkLeafChain(
  leafId: string | null,
  messages: GgMessage[],
  _childrenByParent: Map<string, GgMessage[]>,
): GgMessage[] {
  if (leafId == null) return [];
  const byId = new Map<string, GgMessage>();
  for (const m of messages) byId.set(m.id, m);

  const chain: GgMessage[] = [];
  let cur = byId.get(leafId) ?? null;
  while (cur) {
    chain.push(cur);
    if (cur.parentId == null) break;
    cur = byId.get(cur.parentId) ?? null;
  }
  chain.reverse();
  return chain;
}

/** BFS over every reachable message, oldest-first, dedup by id. */
function bfsFromAll(messages: GgMessage[]): GgMessage[] {
  const childrenByParent = buildChildIndex(messages);
  const roots = messages.filter((m) => m.parentId == null);
  const seen = new Set<string>();
  const out: GgMessage[] = [];
  const queue: GgMessage[] = [...roots];
  while (queue.length > 0) {
    const m = queue.shift()!;
    if (seen.has(m.id)) continue;
    seen.add(m.id);
    out.push(m);
    const kids = childrenByParent.get(m.id) ?? [];
    for (const k of kids) queue.push(k);
  }
  return out;
}

function normalizeContent(content: string | GgContentBlock[]): GgContentBlock[] | { stringContent: string } {
  if (typeof content === "string") return { stringContent: content };
  if (Array.isArray(content)) return content;
  return [];
}

function userMessagePayload(
  blocksOrString: GgContentBlock[] | { stringContent: string },
): UserMessagePayload {
  if (!Array.isArray(blocksOrString)) {
    return { text: blocksOrString.stringContent, images_count: 0 };
  }
  let text = "";
  let images = 0;
  for (const b of blocksOrString) {
    if ((b as any).type === "text") text += ((b as GgTextBlock).text ?? "") + "\n";
    else if ((b as any).type === "image") images++;
  }
  return { text: text.trim(), images_count: images };
}

interface AssistantResult {
  text: string;
  thinkings: string[];
  toolCalls: ToolCallPayload[];
  openaiReasonings: Array<Record<string, unknown>>;
}

function assistantMessagePayload(blocksOrString: GgContentBlock[] | { stringContent: string }): AssistantResult {
  const result: AssistantResult = { text: "", thinkings: [], toolCalls: [], openaiReasonings: [] };
  if (!Array.isArray(blocksOrString)) {
    // assistant content is almost always an array; the string case is unusual but harmless.
    result.text = blocksOrString.stringContent;
    return result;
  }
  const textParts: string[] = [];
  for (const b of blocksOrString) {
    const t = (b as any).type;
    if (t === "text") {
      const text = (b as GgTextBlock).text ?? "";
      if (text) textParts.push(text);
    } else if (t === "thinking") {
      const text = (b as GgThinkingBlock).text ?? "";
      if (text) result.thinkings.push(text);
    } else if (t === "tool_call") {
      const tc = b as GgToolCallBlock;
      result.toolCalls.push({
        tool_call_id: tc.id,
        tool_name: tc.name,
        args: tc.args ?? {},
        args_truncated: false,
      });
    } else if (t === "raw") {
      const raw = b as GgRawBlock;
      // OpenAI reasoning block — keep the raw data verbatim in a custom event.
      if ((raw.data as any)?.type === "reasoning" || (raw.data as any)?.encrypted_content) {
        result.openaiReasonings.push({
          id: raw.data.id,
          type: raw.data.type,
          content: raw.data.content ?? [],
          encrypted_content: raw.data.encrypted_content ? "[present, encrypted]" : undefined,
          summary: raw.data.summary ?? [],
          has_encrypted_content: Boolean((raw.data as any).encrypted_content),
        });
      } else {
        result.openaiReasonings.push({ raw: raw.data });
      }
    } else {
      // Unknown block — keep the block under its own bucket so nothing is lost.
      result.openaiReasonings.push({ unknown_block: b });
    }
  }
  result.text = textParts.join("\n").trim();
  return result;
}

function toolResultPayloads(blocksOrString: GgContentBlock[] | { stringContent: string }): ToolResultPayload[] {
  if (!Array.isArray(blocksOrString)) {
    // Tool content is normally an array; if ggcoder ever sends a string, wrap it.
    return [{
      tool_call_id: "unknown",
      tool_name: "unknown",
      content_text: blocksOrString.stringContent,
      content_truncated: false,
      is_error: false,
    }];
  }
  const out: ToolResultPayload[] = [];
  for (const b of blocksOrString) {
    const t = (b as any).type;
    if (t === "tool_result") {
      const tr = b as GgToolResultBlock;
      out.push({
        tool_call_id: tr.toolCallId ?? "unknown",
        tool_name: "unknown", // ggcoder doesn't echo the tool name in the result block
        content_text: tr.content ?? "",
        content_truncated: false,
        is_error: tr.is_error ?? false,
      });
    } else if (t === "text") {
      // Some tool runs may interleave text blocks — fold into a synthetic result.
      out.push({
        tool_call_id: "unknown",
        tool_name: "unknown",
        content_text: (b as GgTextBlock).text ?? "",
        content_truncated: false,
        is_error: false,
      });
    }
  }
  return out;
}