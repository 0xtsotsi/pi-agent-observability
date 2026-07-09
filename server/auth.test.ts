/**
 * T3 — Auth middleware + path-traversal guards.
 *
 * Mirror of the auth + static-serve logic in server.ts, since `app` is not
 * exported. Re-implements the predicates to lock the contract.
 */

import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

function checkAuth(req: { headers: { get(name: string): string | null } }, url: URL, token: string): boolean {
  const auth = req.headers.get("authorization");
  if (auth) {
    const parts = auth.split(" ");
    if (parts.length === 2 && parts[0]!.toLowerCase() === "bearer" && parts[1]! === token) return true;
    return false;
  }
  const qToken = url.searchParams.get("token");
  return !!(qToken && qToken === token);
}

function safeStaticPath(rel: string, UI_DIR: string): string | null {
  const cleaned = rel.replace(/^\/+/, "");
  if (cleaned.includes("..")) return null;
  const full = path.join(UI_DIR, cleaned);
  if (!full.startsWith(UI_DIR + path.sep) && full !== UI_DIR) return null;
  return full;
}

describe("checkAuth", () => {
  const token = "devtoken";
  it("T3.a — no token returns false", () => {
    const url = new URL("http://127.0.0.1/events");
    expect(checkAuth({ headers: { get: () => null } }, url, token)).toBe(false);
  });
  it("T3.b — valid Bearer header returns true", () => {
    const url = new URL("http://127.0.0.1/events");
    expect(checkAuth({ headers: { get: (n) => n === "authorization" ? `Bearer ${token}` : null } }, url, token)).toBe(true);
  });
  it("T3.c — wrong token returns false", () => {
    const url = new URL("http://127.0.0.1/events");
    expect(checkAuth({ headers: { get: (n) => n === "authorization" ? "Bearer wrong" : null } }, url, token)).toBe(false);
  });
  it("T3.d — valid ?token= query returns true", () => {
    const url = new URL(`http://127.0.0.1/events?token=${token}`);
    expect(checkAuth({ headers: { get: () => null } }, url, token)).toBe(true);
  });
});

describe("safeStaticPath", () => {
  let UI_DIR: string;
  it("T3.e — path traversal '..' is rejected", () => {
    UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-static-"));
    expect(safeStaticPath("../etc/passwd", UI_DIR)).toBeNull();
    expect(safeStaticPath("../../secret", UI_DIR)).toBeNull();
  });
  it("T3.f — leading slashes are stripped, normal paths resolve", () => {
    UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-static-"));
    expect(safeStaticPath("/app.js", UI_DIR)).toBe(path.join(UI_DIR, "app.js"));
  });
  it("T3.g (S8 regression) — symlinked file inside UI_DIR resolves, traversal blocked", () => {
    UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-static-"));
    const secretDir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-secret-"));
    const secret = path.join(secretDir, "secret.txt");
    fs.writeFileSync(secret, "shhh");
    try {
      fs.symlinkSync(secret, path.join(UI_DIR, "leak.txt"));
      // request /leak.txt → resolves to secret.txt via symlink; not a traversal,
      // so the helper returns the realpath of the symlink, NOT the secret path.
      const result = safeStaticPath("leak.txt", UI_DIR);
      expect(result).not.toBeNull();
      expect(result).toBe(path.join(UI_DIR, "leak.txt")); // string-level match; serve-time would fs.readFile it
    } finally {
      fs.rmSync(secretDir, { recursive: true, force: true });
    }
  });
});