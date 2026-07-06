/**
 * T8 — query-param validation. `?limit=foo`, `?limit=-1`, `?before_seq=abc`
 * must return 200 with defaults, not 500.
 *
 * Spin up the real server on an ephemeral port and exercise the contract.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";

let server: ChildProcess | null = null;
let port = 0;
let token = "testtoken123";
const TEST_PORT = 54321;

beforeAll(async () => {
  const tmpDb = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-test-"));
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
  // The server prints its listening URL on stdout. Wait for it.
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server didn't start in 10s")), 10000);
    server!.stdout?.on("data", (chunk) => {
      if (chunk.toString().includes(`Listening on http://127.0.0.1:${TEST_PORT}`)) {
        clearTimeout(timer); resolve();
      }
    });
  });
  port = TEST_PORT;
}, 15000);

afterAll(async () => {
  if (server) {
    server.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 500));
    if (!server.killed) server.kill("SIGKILL");
  }
});

async function get(p: string): Promise<{ status: number }> {
  const res = await fetch(`http://127.0.0.1:${port}${p}`);
  return { status: res.status };
}

describe("query-param validation", () => {
  it("T8.a — ?limit=foo returns 200 (defaults to 50)", async () => {
    const { status } = await get(`/sessions?token=${token}&limit=foo`);
    expect(status).toBe(200);
  });

  it("T8.b — ?limit=-1 returns 200 (clamped to 1)", async () => {
    const { status } = await get(`/sessions?token=${token}&limit=-1`);
    expect(status).toBe(200);
  });

  it("T8.c — ?limit=99999 returns 200 (clamped to 200)", async () => {
    const { status } = await get(`/sessions?token=${token}&limit=99999`);
    expect(status).toBe(200);
  });

  it("T8.d — ?before_seq=abc on /sessions/:id/events returns 200 (defaults)", async () => {
    const { status } = await get(`/sessions/does-not-exist/events?token=${token}&before_seq=abc`);
    expect(status).toBe(200);
  });

  it("T8.e — ?limit=foo on /sessions/:id/events returns 200", async () => {
    const { status } = await get(`/sessions/does-not-exist/events?token=${token}&limit=foo`);
    expect(status).toBe(200);
  });
});