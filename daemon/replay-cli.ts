#!/usr/bin/env tsx
/**
 * replay-cli.ts — replay a single ggcoder session.jsonl to the obs server.
 *
 * Usage:
 *   tsx daemon/replay-cli.ts <path-to-session.jsonl> [options]
 *
 * Options:
 *   --all-branches        Walk every message in the file, not just the leafId chain.
 *   --server-url=URL      Override OBS_SERVER_URL (default http://127.0.0.1:43190).
 *   --token=TOKEN         Override OBS_AUTH_TOKEN.
 *   --pool=POOL           Set pool/bucket (default "default").
 *   --tag=TAG             Add a tag (repeatable).
 *   --quiet               Suppress per-event progress logs.
 *
 * Exit code 0 on full success, non-zero on any HTTP error after retries.
 *
 * Posts events in batches of 50 (mirroring the daemon's EventQueue) with
 * exponential backoff 250ms → 5s, max 6 attempts per batch.
 */

import * as path from "node:path";
import { mapSession } from "./gg-obs-mapper.js";
import type { ObsEventEnvelope } from "../shared/types.js";

const BATCH_SIZE = 50;
const INITIAL_BACKOFF_MS = 250;
const MAX_BACKOFF_MS = 5000;
const MAX_ATTEMPTS = 6;

interface CliArgs {
  file: string;
  allBranches: boolean;
  serverUrl: string;
  token: string;
  pool: string;
  tags: string[];
  quiet: boolean;
}

function parseArgs(argv: string[]): CliArgs {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  for (const a of argv) {
    if (!a.startsWith("--")) {
      positional.push(a);
      continue;
    }
    const eq = a.indexOf("=");
    if (eq === -1) flags[a.slice(2)] = true;
    else flags[a.slice(2, eq)] = a.slice(eq + 1);
  }
  const file = positional[0];
  if (!file) {
    process.stderr.write("usage: replay-cli.ts <session.jsonl> [--all-branches] [--server-url=URL] [--token=TOKEN] [--pool=POOL] [--tag=TAG] [--quiet]\n");
    process.exit(2);
  }
  const tags: string[] = [];
  for (const [k, v] of Object.entries(flags)) {
    if (k === "tag" && typeof v === "string") tags.push(v);
  }
  return {
    file: path.resolve(file),
    allBranches: flags["all-branches"] === true,
    serverUrl: typeof flags["server-url"] === "string" ? flags["server-url"] : (process.env["OBS_SERVER_URL"] ?? "http://127.0.0.1:43190"),
    token: typeof flags["token"] === "string" ? flags["token"] : (process.env["OBS_AUTH_TOKEN"] ?? ""),
    pool: typeof flags["pool"] === "string" ? flags["pool"] : "default",
    tags,
    quiet: flags["quiet"] === true || process.env["OBS_QUIET"] === "1",
  };
}

async function postBatch(serverUrl: string, token: string, batch: ObsEventEnvelope[]): Promise<{ ingested: number; rejected: string[] }> {
  const res = await fetch(`${serverUrl.replace(/\/+$/, "")}/events`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(batch),
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${await res.text().catch(() => "")}`);
  }
  return (await res.json()) as { ingested: number; rejected: string[] };
}

async function postWithRetry(serverUrl: string, token: string, batch: ObsEventEnvelope[], quiet: boolean): Promise<{ ingested: number; rejected: string[] }> {
  let backoff = INITIAL_BACKOFF_MS;
  let lastErr: unknown = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      return await postBatch(serverUrl, token, batch);
    } catch (err: any) {
      lastErr = err;
      if (attempt === MAX_ATTEMPTS) break;
      if (!quiet) process.stderr.write(`[replay-cli] batch failed (attempt ${attempt}/${MAX_ATTEMPTS}, backoff ${backoff}ms): ${err?.message ?? err}\n`);
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (!args.token) {
    process.stderr.write("error: OBS_AUTH_TOKEN (or --token) is required\n");
    process.exit(2);
  }

  // Read & map
  const allEvents: ObsEventEnvelope[] = [];
  for (const evt of mapSession(args.file, { allBranches: args.allBranches, pool: args.pool, tags: args.tags })) {
    allEvents.push(evt);
  }

  if (allEvents.length === 0) {
    if (!args.quiet) process.stderr.write(`[replay-cli] ${args.file}: mapper produced no events\n`);
    process.exit(0);
  }

  // Type breakdown for the report.
  const byType = new Map<string, number>();
  for (const e of allEvents) byType.set(e.type, (byType.get(e.type) ?? 0) + 1);

  if (!args.quiet) {
    process.stderr.write(`[replay-cli] ${args.file}: ${allEvents.length} events → ${args.serverUrl}\n`);
    process.stderr.write(`[replay-cli]   by type: ${[...byType.entries()].map(([t, n]) => `${t}=${n}`).join(", ")}\n`);
  }

  // Post in batches.
  let ingested = 0;
  let rejected = 0;
  for (let i = 0; i < allEvents.length; i += BATCH_SIZE) {
    const batch = allEvents.slice(i, i + BATCH_SIZE);
    const result = await postWithRetry(args.serverUrl, args.token, batch, args.quiet);
    ingested += result.ingested;
    rejected += result.rejected.length;
  }

  process.stderr.write(`[replay-cli] done: ingested=${ingested} rejected=${rejected}\n`);
  if (rejected > 0) process.exit(1);
  process.exit(0);
}

main().catch((err: unknown) => {
  process.stderr.write(`[replay-cli] fatal: ${(err as Error)?.message ?? err}\n`);
  process.exit(1);
});