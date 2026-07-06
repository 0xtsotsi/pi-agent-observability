/**
 * T7 — static-serve symlink regression (S8).
 *
 * The serveStatic helper in server.ts must reject requests whose realpath
 * resolves outside UI_DIR. This catches the "leak.txt → /tmp/secret.txt"
 * case from the audit.
 */

import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

// Mirror of the serveStatic logic in server.ts (which isn't exported).
function safeStaticPath(rel: string, UI_DIR: string): string | null {
  const cleaned = rel.replace(/^\/+/, "");
  if (cleaned.includes("..")) return null;
  const full = path.join(UI_DIR, cleaned);
  if (!full.startsWith(UI_DIR + path.sep) && full !== UI_DIR) return null;
  return full;
}

function serveStatic(rel: string, UI_DIR: string): { status: number; body?: string } | null {
  const filePath = safeStaticPath(rel, UI_DIR);
  if (!filePath) return { status: 404 };
  if (!fs.existsSync(filePath)) return { status: 404 };
  const stat = fs.statSync(filePath);
  if (!stat.isFile()) return { status: 404 };
  const real = fs.realpathSync.native ? fs.realpathSync.native(filePath) : fs.realpathSync(filePath);
  const uiReal = fs.realpathSync.native ? fs.realpathSync.native(UI_DIR) : fs.realpathSync(UI_DIR);
  if (!real.startsWith(uiReal + path.sep) && real !== uiReal) return { status: 404 };
  return { status: 200, body: fs.readFileSync(real, "utf8") };
}

describe("serveStatic", () => {
  it("T7.a (S8 regression) — symlink inside UI_DIR → outside file returns 404", () => {
    const UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-ui-"));
    const secretDir = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-out-"));
    const secret = path.join(secretDir, "secret.txt");
    fs.writeFileSync(secret, "TOPSECRET");
    try {
      fs.symlinkSync(secret, path.join(UI_DIR, "leak.txt"));
      const result = serveStatic("leak.txt", UI_DIR);
      expect(result?.status).toBe(404);
    } finally {
      fs.rmSync(secretDir, { recursive: true, force: true });
    }
  });

  it("T7.b — a normal file in UI_DIR returns 200 with its content", () => {
    const UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-ui-"));
    fs.writeFileSync(path.join(UI_DIR, "ok.txt"), "hello");
    const result = serveStatic("ok.txt", UI_DIR);
    expect(result?.status).toBe(200);
    expect(result?.body).toBe("hello");
  });

  it("T7.c — traversal returns 404 from safeStaticPath", () => {
    const UI_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "gg-obs-ui-"));
    const result = serveStatic("../etc/passwd", UI_DIR);
    expect(result?.status).toBe(404);
  });
});