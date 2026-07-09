// worker-dashboard.js — live per-agent worker surface for gg-observability.
//
// Source of truth: SSE events of type `worker.*` and `bus.*` from /events/stream.
// The host (app.js) forwards each event to `window.__workersOnEvent(evt)`.
//
// Per-agent state in AGENT_STATE:
//   - agent: string
//   - status: "idle" | "processing" | "failed" | "unknown"
//   - lastSeen: ISO ts of last event received
//   - currentTask: { command_id, task_id, seq, started_at } | null
//   - lastCompleted: { task_id, duration_ms } | null
//   - lastFailed: { error } | null
//   - pending: count (bus.command_created for target_agent - bus.command_acked for target_agent)
//
// Renders into #workers-pane on every state change. Idempotent render.
(function() {
  // The five agent names shown in the Workers tab. Order = render order, left to
  // right in the auto-fit grid. overlord is included even though it doesn't
  // emit `worker.*` events of its own — its tile still lights up whenever it
  // dispatches or acks a command (which fires bus.command_* tagged with
  // target_agent=overlord), so the operator gets a live signal.
  const KNOWN_AGENTS = ["overlord", "noledge", "gg-obs", "demoshots", "my-app"];
  const STALE_THRESHOLD_MS = 60_000;   // <60s = green, 60-300s = amber, >300s = red
  const ATTENTION_THRESHOLD_MS = 300_000;

  const AGENT_STATE = {};
  for (const a of KNOWN_AGENTS) {
    AGENT_STATE[a] = {
      agent: a,
      status: "unknown",
      lastSeen: null,
      currentTask: null,
      lastCompleted: null,
      lastFailed: null,
      pending: 0,
    };
  }

  // SSE reconnects fire `replayMissedEvents()` which re-feeds events through
  // this handler. Without dedupe, every reconnect would double-count pending
  // (bus.command_created) or zero it (bus.command_acked). Track seen event_ids
  // for the bus.* pair so a reconnect can't double-bump the badge.
  const __seenEventIds = new Set();
  const __MAX_SEEN = 500; // bound the dedupe set; older ids fall out FIFO

  function classifyAge(lastSeen) {
    if (!lastSeen) return "red";
    const age = Date.now() - new Date(lastSeen).getTime();
    if (age < STALE_THRESHOLD_MS) return "green";
    if (age < ATTENTION_THRESHOLD_MS) return "amber";
    return "red";
  }

  function statusFor(state) {
    if (state.lastFailed && Date.now() - new Date(state.lastFailed.ts).getTime() < 60_000) {
      return "failed";
    }
    if (state.currentTask) return "processing";
    return "idle";
  }

  function applyEvent(evt) {
    if (!evt || !evt.type) return;
    const t = evt.type;
    const payload = evt.payload || {};
    const ts = evt.ts || new Date().toISOString();

    // Dedup by event_id (bounded LRU). SSE replay-on-reconnect would otherwise
    // double-count pending across reconnects.
    if (evt.event_id) {
      if (__seenEventIds.has(evt.event_id)) return;
      __seenEventIds.add(evt.event_id);
      if (__seenEventIds.size > __MAX_SEEN) {
        // Drop the oldest entries (insertion order = Set iteration order).
        const drop = __seenEventIds.size - __MAX_SEEN;
        const it = __seenEventIds.values();
        for (let i = 0; i < drop; i++) { __seenEventIds.delete(it.next().value); }
      }
    }

    const tags = Array.isArray(evt.tags) ? evt.tags : [];
    // Find the target agent from tags (always emitted first) or from payload
    let agent = (tags.find(t => KNOWN_AGENTS.includes(t))) || payload.target_agent || payload.agent;
    if (!agent || !KNOWN_AGENTS.includes(agent)) return;
    const s = AGENT_STATE[agent];
    s.lastSeen = ts;

    if (t === "worker.started") {
      // boot signal — just stamps lastSeen; statusFor decides idle/processing.
    } else if (t === "worker.command_received") {
      // Worker's local poll saw a new cmd in its mailbox. The authoritative
      // pending counter is updated via bus.command_created (server-side
      // broadcast). This handler is intentionally a no-op so we don't double
      // count — comment kept for future readers.
    } else if (t === "worker.command_started") {
      s.currentTask = {
        command_id: payload.command_id || "?",
        task_id: payload.task_id || null,
        seq: payload.seq,
        started_at: ts,
      };
    } else if (t === "worker.command_completed") {
      s.currentTask = null;
      s.lastCompleted = { task_id: payload.task_id, duration_ms: payload.duration_ms, ts };
    } else if (t === "worker.command_failed") {
      s.currentTask = null;
      s.lastFailed = { task_id: payload.task_id, error: payload.error, ts };
    } else if (t === "worker.command_acked") {
      // No-op for tile; the underlying command was just acked (server will
      // also emit bus.command_acked which decrements pending).
    } else if (t === "bus.command_created") {
      if (payload.target_agent === agent) s.pending += 1;
    } else if (t === "bus.command_acked") {
      // We don't know target_agent from this shape; infer from session_id "bus-<agent>"
      if (typeof evt.session_id === "string" && evt.session_id === `bus-${agent}`) {
        s.pending = Math.max(0, s.pending - 1);
      }
    }
    s.status = statusFor(s);
    render();
  }

  let renderQueued = false;
  function render() {
    if (renderQueued) return;
    renderQueued = true;
    requestAnimationFrame(() => {
      renderQueued = false;
      doRender();
    });
  }

  function doRender() {
    const root = document.getElementById("workers-grid");
    if (!root) return;
    // Build the tiles fresh; cheap given the small N and the panels are sparse.
    root.innerHTML = "";
    for (const a of KNOWN_AGENTS) {
      const s = AGENT_STATE[a];
      const age = s.lastSeen ? Math.max(0, Date.now() - new Date(s.lastSeen).getTime()) : null;
      const ageDot = classifyAge(s.lastSeen);
      const statusDot = {
        idle: "green",
        processing: "cyan",
        failed: "red",
        unknown: "red",
      }[s.status] || "red";

      // Line 2: current task > recent failure > last completed > idle.
      let line2;
      if (s.currentTask) {
        const tid = s.currentTask.task_id || s.currentTask.command_id.slice(0, 8);
        const elapsed = formatAge(Date.now() - new Date(s.currentTask.started_at).getTime());
        line2 = `processing <code title="${escapeHtml(s.currentTask.command_id)}">${escapeHtml(tid)}</code> · ${elapsed}`;
      } else if (s.lastFailed && Date.now() - new Date(s.lastFailed.ts).getTime() < 60_000) {
        line2 = `failed <code>${escapeHtml(s.lastFailed.task_id || "?")}</code>`;
      } else if (s.lastCompleted) {
        line2 = `last <code>${escapeHtml(s.lastCompleted.task_id || "?")}</code> in ${s.lastCompleted.duration_ms}ms`;
      } else {
        line2 = "idle";
      }

      const tile = document.createElement("div");
      tile.className = "worker-tile";
      tile.dataset.agent = a;
      tile.innerHTML = `
        <div class="worker-head">
          <span class="worker-status-dot ${statusDot}" title="status: ${s.status}"></span>
          <span class="worker-name">${a}</span>
          <span class="worker-pending">${s.pending > 0 ? `<span class="worker-badge" title="${s.pending} pending command(s) in bus queue">pending ${s.pending}</span>` : ""}</span>
        </div>
        <div class="worker-line">${line2}</div>
        <div class="worker-line dim worker-freshness">
          <span class="worker-age-dot ${ageDot}" title="${ageDot === "green" ? "fresh (<60s)" : ageDot === "amber" ? "stale (60-300s)" : "no signal (>300s)"}"></span>
          ${age !== null ? `last-seen ${formatAge(age)} ago` : "no heartbeat yet"}
        </div>
      `;
      root.appendChild(tile);
    }
    const refreshed = document.getElementById("workers-refreshed");
    if (refreshed) refreshed.textContent = new Date().toISOString().slice(11, 19) + " UTC";
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  }

  function formatAge(ms) {
    if (ms < 1000) return `${ms}ms`;
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m`;
    const h = Math.floor(m / 60);
    return `${h}h`;
  }

  // Re-render every 5s to reflect age-tick (heartbeat dot color flips as lastSeen ages)
  setInterval(render, 5000);

  // Initial paint so the panel isn't empty on first open.
  render();

  // Public hooks for app.js to call.
  window.__workersOnEvent = applyEvent;
  window.__workersRender = render;
  window.__workersAgents = () => KNOWN_AGENTS.slice();
  window.__workersState = () => JSON.parse(JSON.stringify(AGENT_STATE));
})();
