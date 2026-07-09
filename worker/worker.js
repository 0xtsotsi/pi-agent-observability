#!/usr/bin/env -S node --experimental-strip-types --import "file:///Users/gogetta/Documents/projects/gg-observability/node_modules/tsx/dist/loader.mjs"
"use strict";
/**
 * worker.ts — per-target-agent headless worker daemon.
 *
 * Usage:
 *   AGENT=noledge node --experimental-strip-types worker/worker.ts
 *
 * Loop:
 *   1. Watch bus for new commands targeted at $AGENT (chokidar on obs.db + 30s poll)
 *   2. For each: load agent's CLAUDE.md as system prompt, run agent loop via
 *      runPrintMode(), which calls the ggcoder SDK's AgentSession
 *   3. The agent's own recipe in its CLAUDE.md tells it to read the inbox,
 *      act on tasks, and ack via delegate.ts
 *   4. Log everything to ~/.gg/worker-logs/<agent>.log
 *
 * Idempotent: restarts re-resume from the last-seen command seq (cursor file).
 * Supervised by launchd (separate plist per agent).
 *
 * SDK surface verified against
 *   /Users/gogetta/Library/Mobile Documents/com~apple~CloudDocs/Documents/ggcoder-pwa/node_modules/@kenkaiiii/ggcoder/dist/
 *     modes/print-mode.d.ts:7-15   (PrintModeOptions)
 *     modes/print-mode.js:13       (runPrintMode implementation)
 *     core/agent-session.d.ts:8-19 (AgentSessionOptions — all 10 fields verified)
 *     core/agent-session.d.ts:65    (AgentSession.prompt one-shot)
 *     core/agent-session.d.ts:28    (private tools; no public setter)
 */
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var _a, _b;
Object.defineProperty(exports, "__esModule", { value: true });
var ggcoder_1 = require("@kenkaiiii/ggcoder");
var node_fs_1 = require("node:fs");
var node_path_1 = require("node:path");
var node_os_1 = require("node:os");
var chokidar_1 = require("chokidar");
var AGENT = process.env.AGENT;
if (!AGENT) {
    console.error("AGENT env var is required (e.g. AGENT=noledge)");
    process.exit(2);
}
var HOME = (0, node_os_1.homedir)();
var SAFE = AGENT.toLowerCase().replace(/[^a-z0-9-]/g, "-");
// Per-agent project path map. Most agents live under ~/Documents/projects/<agent>,
// but my-app lives at ~/my-app/ and gg-obs IS this repo (~/Documents/projects/gg-observability/).
var PROJECT_CWD_BY_AGENT = {
    "my-app": (0, node_path_1.join)(HOME, "my-app"),
    "gg-obs": (0, node_path_1.join)(HOME, "Documents", "projects", "gg-observability"),
};
var PROJECT_CWD = (_a = PROJECT_CWD_BY_AGENT[AGENT]) !== null && _a !== void 0 ? _a : (0, node_path_1.join)(HOME, "Documents", "projects", AGENT);
var CLAUDE_MD = (0, node_path_1.join)(PROJECT_CWD, "CLAUDE.md");
var OBS_BASE = (_b = process.env.OBS_BASE_URL) !== null && _b !== void 0 ? _b : "http://127.0.0.1:43190";
var TOKEN_FILE = (0, node_path_1.join)(HOME, ".gg/observability/token");
var CURSOR_FILE = (0, node_path_1.join)(HOME, ".gg/worker-".concat(SAFE, "-cursor.json"));
var OBS_DB = (0, node_path_1.join)(HOME, ".gg/observability/obs.db");
var LOG_DIR = (0, node_path_1.join)(HOME, ".gg/worker-logs");
var LOG_FILE = (0, node_path_1.join)(LOG_DIR, "".concat(SAFE, ".log"));
var INBOX_FILE = (0, node_path_1.join)(HOME, ".gg/worker-inbox", "".concat(SAFE, ".md"));
var ACK_TIMEOUT_MS = 5000;
var POLL_INTERVAL_MS = 30000;
(0, node_fs_1.mkdirSync)(LOG_DIR, { recursive: true });
if (!(0, node_fs_1.existsSync)(CLAUDE_MD)) {
    // Some agents (my-app, gg-obs in some future state) may not have a project CLAUDE.md.
    // Fall back to a minimal headless preamble so the worker still runs.
    log("CLAUDE.md not found at ".concat(CLAUDE_MD, "; using fallback preamble"));
}
function readToken() {
    if (process.env.OBS_AUTH_TOKEN)
        return process.env.OBS_AUTH_TOKEN;
    if ((0, node_fs_1.existsSync)(TOKEN_FILE))
        return (0, node_fs_1.readFileSync)(TOKEN_FILE, "utf8").trim();
    return "devtoken";
}
function log(line) {
    var out = "[".concat(new Date().toISOString(), "] [").concat(AGENT, "] ").concat(line, "\n");
    process.stdout.write(out);
    try {
        (0, node_fs_1.appendFileSync)(LOG_FILE, out);
    }
    catch (_a) {
        // best effort
    }
}
function readCursor() {
    if (!(0, node_fs_1.existsSync)(CURSOR_FILE))
        return 0;
    try {
        var c = JSON.parse((0, node_fs_1.readFileSync)(CURSOR_FILE, "utf8"));
        return typeof c.lastSeenSeq === "number" ? c.lastSeenSeq : 0;
    }
    catch (_a) {
        return 0;
    }
}
function writeCursor(seq) {
    var payload = { lastSeenSeq: seq, updated_at: new Date().toISOString() };
    try {
        (0, node_fs_1.writeFileSync)(CURSOR_FILE, JSON.stringify(payload, null, 2));
    }
    catch (e) {
        var msg = e instanceof Error ? e.message : String(e);
        log("writeCursor failed: ".concat(msg));
    }
}
function ackCommand(commandId, state, sessionId, errorPayload) {
    return __awaiter(this, void 0, void 0, function () {
        var token, body, controller, timer, resp, _a, _b, _c, e_1, msg;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    token = readToken();
                    body = __assign({ ack_state: state, ack_session_id: sessionId }, (errorPayload !== undefined ? { ack_payload: errorPayload } : {}));
                    controller = new AbortController();
                    timer = setTimeout(function () { return controller.abort(); }, ACK_TIMEOUT_MS);
                    _d.label = 1;
                case 1:
                    _d.trys.push([1, 6, 7, 8]);
                    return [4 /*yield*/, fetch("".concat(OBS_BASE, "/commands/").concat(commandId, "/ack"), {
                            method: "POST",
                            headers: { "content-type": "application/json", authorization: "Bearer ".concat(token) },
                            body: JSON.stringify(body),
                            signal: controller.signal,
                        })];
                case 2:
                    resp = _d.sent();
                    if (!!resp.ok) return [3 /*break*/, 4];
                    _a = log;
                    _c = (_b = "ack failed: ".concat(resp.status, " ")).concat;
                    return [4 /*yield*/, resp.text().catch(function () { return ""; })];
                case 3:
                    _a.apply(void 0, [_c.apply(_b, [_d.sent()])]);
                    return [3 /*break*/, 5];
                case 4:
                    log("ack ".concat(state, " command_id=").concat(commandId));
                    _d.label = 5;
                case 5: return [3 /*break*/, 8];
                case 6:
                    e_1 = _d.sent();
                    msg = e_1 instanceof Error ? e_1.message : String(e_1);
                    log("ack error: ".concat(msg));
                    return [3 /*break*/, 8];
                case 7:
                    clearTimeout(timer);
                    return [7 /*endfinally*/];
                case 8: return [2 /*return*/];
            }
        });
    });
}
function processCommand(cmd) {
    return __awaiter(this, void 0, void 0, function () {
        var command_id, seq, payload, claudeContent, ackCmd, workerPreamble, systemPrompt, userMessage, provider, model, e_2, msg;
        var _a, _b, _c, _d, _e, _f, _g, _h;
        return __generator(this, function (_j) {
            switch (_j.label) {
                case 0:
                    command_id = cmd.command_id, seq = cmd.seq;
                    log("processing command_id=".concat(command_id, " seq=").concat(seq));
                    payload = {};
                    try {
                        payload = JSON.parse((_a = cmd.payload_json) !== null && _a !== void 0 ? _a : "{}");
                    }
                    catch (_k) {
                        log("command_id=".concat(command_id, " payload_json not parseable; using empty"));
                    }
                    claudeContent = (0, node_fs_1.existsSync)(CLAUDE_MD) ? (0, node_fs_1.readFileSync)(CLAUDE_MD, "utf8") : "";
                    ackCmd = "TARGET_AGENT=".concat(AGENT, " ").concat((0, node_path_1.join)(HOME, "Documents", "projects", "gg-observability", "node_modules", ".bin", "tsx"), " ").concat((0, node_path_1.join)(HOME, "Documents", "projects", "overlord", "bin", "delegate.ts"), " ack ").concat(command_id, " acked worker-").concat(SAFE);
                    workerPreamble = "[Worker preamble] You are a HEADLESS worker for agent \"".concat(AGENT, "\".\n- Project cwd: ").concat(PROJECT_CWD, "\n- Inbox file: ").concat(INBOX_FILE, "\n- To ack a command: `").concat(ackCmd, "`\n- Do NOT ask for user confirmation; act and ack.\n- The bash tool is auto-discovered; use it to run the ack command above.");
                    systemPrompt = claudeContent
                        ? "".concat(claudeContent, "\n\n---\n\n").concat(workerPreamble)
                        : workerPreamble;
                    userMessage = "## Bus command\n- task_id: `".concat(String((_b = payload.task_id) !== null && _b !== void 0 ? _b : "?"), "`\n- command_id: `").concat(command_id, "`\n- title: ").concat(String((_c = payload.title) !== null && _c !== void 0 ? _c : "?"), "\n- files: ").concat(JSON.stringify((_d = payload.files) !== null && _d !== void 0 ? _d : []), "\n- done_criterion: ").concat(String((_e = payload.done_criterion) !== null && _e !== void 0 ? _e : ""), "\n- brief_path: ").concat(String((_f = payload.brief_path) !== null && _f !== void 0 ? _f : ""), "\n\nRead ").concat(INBOX_FILE, " and execute the task. Ack via delegate.ts when done.");
                    provider = ((_g = process.env.OBS_PROVIDER) !== null && _g !== void 0 ? _g : "minimax");
                    model = (_h = process.env.OBS_MODEL) !== null && _h !== void 0 ? _h : "MiniMax-M3";
                    _j.label = 1;
                case 1:
                    _j.trys.push([1, 3, , 5]);
                    return [4 /*yield*/, (0, ggcoder_1.runPrintMode)({
                            message: userMessage,
                            provider: provider,
                            model: model,
                            cwd: PROJECT_CWD,
                            systemPrompt: systemPrompt,
                        })];
                case 2:
                    _j.sent();
                    log("runPrintMode completed for command_id=".concat(command_id));
                    return [3 /*break*/, 5];
                case 3:
                    e_2 = _j.sent();
                    msg = e_2 instanceof Error ? e_2.message : String(e_2);
                    log("runPrintMode FAILED for command_id=".concat(command_id, ": ").concat(msg));
                    return [4 /*yield*/, ackCommand(command_id, "failed", "worker-".concat(SAFE), { error: msg })];
                case 4:
                    _j.sent();
                    return [3 /*break*/, 5];
                case 5: return [2 /*return*/];
            }
        });
    });
}
var pollInFlight = false;
function tick() {
    return __awaiter(this, void 0, void 0, function () {
        var token, since, url, controller_1, timer, resp, data, cmds, _i, cmds_1, cmd, e_3, msg;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    if (pollInFlight)
                        return [2 /*return*/];
                    pollInFlight = true;
                    _b.label = 1;
                case 1:
                    _b.trys.push([1, 11, 12, 13]);
                    token = readToken();
                    since = readCursor();
                    url = "".concat(OBS_BASE, "/commands?target=").concat(encodeURIComponent(SAFE), "&status=pending&since=").concat(since, "&limit=5");
                    controller_1 = new AbortController();
                    timer = setTimeout(function () { return controller_1.abort(); }, ACK_TIMEOUT_MS);
                    resp = void 0;
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, , 4, 5]);
                    return [4 /*yield*/, fetch(url, {
                            headers: { authorization: "Bearer ".concat(token) },
                            signal: controller_1.signal,
                        })];
                case 3:
                    resp = _b.sent();
                    return [3 /*break*/, 5];
                case 4:
                    clearTimeout(timer);
                    return [7 /*endfinally*/];
                case 5:
                    if (!resp.ok) {
                        log("bus poll failed: ".concat(resp.status));
                        return [2 /*return*/];
                    }
                    return [4 /*yield*/, resp.json()];
                case 6:
                    data = (_b.sent());
                    cmds = (_a = data.commands) !== null && _a !== void 0 ? _a : [];
                    if (cmds.length > 0)
                        log("tick: ".concat(cmds.length, " pending command(s)"));
                    _i = 0, cmds_1 = cmds;
                    _b.label = 7;
                case 7:
                    if (!(_i < cmds_1.length)) return [3 /*break*/, 10];
                    cmd = cmds_1[_i];
                    return [4 /*yield*/, processCommand(cmd)];
                case 8:
                    _b.sent();
                    writeCursor(cmd.seq);
                    _b.label = 9;
                case 9:
                    _i++;
                    return [3 /*break*/, 7];
                case 10: return [3 /*break*/, 13];
                case 11:
                    e_3 = _b.sent();
                    msg = e_3 instanceof Error ? e_3.message : String(e_3);
                    log("tick error: ".concat(msg));
                    return [3 /*break*/, 13];
                case 12:
                    pollInFlight = false;
                    return [7 /*endfinally*/];
                case 13: return [2 /*return*/];
            }
        });
    });
}
// chokidar on obs.db mtime as primary trigger; 30s poll as safety net.
chokidar_1.default
    .watch(OBS_DB, { ignoreInitial: true })
    .on("change", function () {
    void tick();
})
    .on("add", function () {
    void tick();
});
var pollHandle = setInterval(function () {
    void tick();
}, POLL_INTERVAL_MS);
function shutdown(signal) {
    log("received ".concat(signal, "; shutting down"));
    clearInterval(pollHandle);
    process.exit(0);
}
process.on("SIGTERM", function () { return shutdown("SIGTERM"); });
process.on("SIGINT", function () { return shutdown("SIGINT"); });
log("worker started; cwd=".concat(PROJECT_CWD, " agent=").concat(AGENT, " safe=").concat(SAFE));
