/**
 * T5 — replay CLI end-to-end.
 *
 * Asserts:
 *  - feed JSONL → CLI POSTs in batches of 50 to /events
 *  - bare `--token` (no value) exits with non-zero status; URL is NOT ".../events?true"
 *
 * NOTE: Uses `spawn` (async) instead of `spawnSync` because spawnSync's
 * internal stdio pipe buffers deadlock on long-running children in this setup.
 */

import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";

function writeFixture(name: string, lines: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-cli-"));
  const file = path.join(dir, name);
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  return file;
}

const header = {
  type: "session" as const, version: 2, id: "sess-cli",
  timestamp: "2026-07-06T12:00:00.000Z", cwd: "/tmp", provider: "x", model: "y", leafId: null,
};

function buildJsonl(n: number): string {
  const lines: unknown[] = [{ ...header, leafId: n === 0 ? null : `m${n - 1}` }];
  for (let i = 0; i < n; i++) {
    lines.push({
      type: "message" as const, id: `m${i}`, parentId: i === 0 ? null : `m${i - 1}`,
      timestamp: new Date(2026, 6, 6, 12, 0, i).toISOString(),
      message: { role: "user" as const, content: `msg-${i}` },
    });
  }
  return writeFixture(`batch-${n}.jsonl`, lines);
}

function runCli(args: string[], env: NodeJS.ProcessEnv): Promise<{ status: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn("./node_modules/.bin/tsx", ["daemon/replay-cli.ts", ...args], {
      env: { ...process.env, PATH: `${process.cwd()}/node_modules/.bin:${process.env.PATH ?? ""}`, ...env },
      cwd: process.cwd(),
    });
    let so = "", se = "";
    child.stdout.on("data", (c) => (so += c.toString()));
    child.stderr.on("data", (c) => (se += c.toString()));
    child.on("error", reject);
    child.on("exit", (status) => resolve({ status, stdout: so, stderr: se }));
    // Hard cap to prevent hangs from destabilizing CI.
    setTimeout(() => child.kill("SIGKILL"), 25000).unref();
  });
}

describe("replay-cli", () => {
  it("T5.a — posts 61 events in 2 batches (50 + 11)", async () => {
    const file = buildJsonl(60);

    const batches: number[] = [];
    const server = http.createServer((req, res) => {
      if (req.method === "POST" && req.url === "/events") {
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          try {
            const arr = JSON.parse(body);
            batches.push(arr.length);
            res.writeHead(200, { "content-type": "application/json" });
            res.end(JSON.stringify({ ingested: arr.length, rejected: [] }));
          } catch {
            res.writeHead(400); res.end();
          }
        });
      } else {
        res.writeHead(404); res.end();
      }
    });
    await new Promise<void>((r) => server.listen(0, r));
    const port = (server.address() as any).port;

    try {
      const result = await runCli([file, "--quiet"], {
        OBS_SERVER_URL: `http://127.0.0.1:${port}`,
        OBS_AUTH_TOKEN: "tok",
      });
      expect(result.status).toBe(0);
      // 1 session_start + 60 user_message = 61 events → batches of 50 + 11
      expect(batches).toEqual([50, 11]);
    } finally {
      server.close();
    }
  });

  it("T5.b (G7 regression) — bare --token exits non-zero with a clear error, not a fetch to ?true", async () => {
    const file = buildJsonl(1);
    const result = await runCli([file, "--token"], { OBS_AUTH_TOKEN: "" });
    expect(result.status).not.toBe(0);
    expect(result.stderr + result.stdout).not.toContain("?true");
    expect(result.stderr + result.stdout).toMatch(/token/i);
  });
});
