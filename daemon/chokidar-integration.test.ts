/**
 * T6 — chokidar integration: spawn a temp dir, write a JSONL, assert the
 * mapper ingests it. We don't boot the full daemon here (that requires a
 * live server); we verify the file → events wiring by importing the daemon's
 * start/stop and pointing it at a temp dir + a local mock server.
 *
 * NOTE: Run this test in isolation (`vitest run chokidar-integration.test.ts`)
 * for reliable results. In the full suite it can occasionally fail under load
 * because chokidar's fsevents stream is shared across vitest worker_threads;
 * the underlying file→events wiring is otherwise covered by T1/T2/T5.
 */

import { describe, it, expect, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import { start } from "./gg-obs-replay.js";

let server: http.Server | null = null;
let port = 0;
let receivedBatches: any[][] = [];

afterEach(async () => {
  if (server) { server.close(); server = null; }
});

describe("chokidar daemon", () => {
  it("T6.a — writing a JSONL into the watch dir triggers POST /events", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-chokidar-"));
    const watchDir = path.join(tmpDir, "sessions");
    fs.mkdirSync(watchDir, { recursive: true });

    receivedBatches = [];
    server = http.createServer((req, res) => {
      if (req.method === "POST" && req.url === "/events") {
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          let arr: any[] = [];
          try {
            arr = JSON.parse(body);
            receivedBatches.push(arr);
          } catch {}
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ingested: arr.length, rejected: [] }));
        });
      } else { res.writeHead(404); res.end(); }
    });
    await new Promise<void>((r) => server!.listen(0, r));
    port = (server!.address() as any).port;

    const handle = await start({
      serverUrl: `http://127.0.0.1:${port}`,
      token: "t",
      watchDir,
      quiet: true,
      backfillOnStart: false,
    });

    // Give chokidar's underlying fsevents watcher time to register. chokidar's
    // own `awaitWriteFinish` debounce + macOS fsevents pipeline means the very
    // first event after watcher start can take 1-2s on a busy worker pool.
    await new Promise((r) => setTimeout(r, 1500));

    // Write a tiny session file (leafId must point to the last message so
    // the mapper walker visits it; null leafId walks nothing past session_start).
    const msgTs = new Date().toISOString();
    const file = path.join(watchDir, "s1.jsonl");
    fs.writeFileSync(file, [
      JSON.stringify({ type: "session", version: 2, id: "s1", timestamp: msgTs, cwd: "/tmp", provider: "x", model: "y", leafId: "m1" }),
      JSON.stringify({ type: "message", id: "m1", parentId: null, timestamp: msgTs, message: { role: "user", content: "hi" } }),
    ].join("\n") + "\n");

    // Poll up to 15s for the first batch to arrive (CI can be slow under load).
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline && receivedBatches.length === 0) {
      await new Promise((r) => setTimeout(r, 100));
    }

    await handle.stop();

    expect(receivedBatches.length).toBeGreaterThanOrEqual(1);
    const flat = receivedBatches.flat();
    expect(flat.some((e: any) => e.type === "session_start")).toBe(true);
    expect(flat.some((e: any) => e.type === "user_message")).toBe(true);
  }, 30000);
});