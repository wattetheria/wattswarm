pub const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>WattSwarm Network Diagnostics</title>
  <style>
    :root {
      --bg: #f0e8d8;
      --ink: #1e1a16;
      --card: #fff6e8;
      --card-2: #efe2cf;
      --accent: #d46a1f;
      --blue: #1f7fd4;
      --line: #2b2520;
      --muted: #5f5549;
      --ok: #2b8a3e;
      --warn: #a35b00;
      --bad: #9f1f1f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Mono", "Fira Code", "JetBrains Mono", monospace;
      background:
        linear-gradient(90deg, rgba(30,26,22,0.05) 1px, transparent 1px),
        linear-gradient(rgba(30,26,22,0.05) 1px, transparent 1px),
        var(--bg);
      background-size: 12px 12px, 12px 12px, auto;
      color: var(--ink);
    }
    .wrap {
      max-width: 1560px;
      margin: 0 auto;
      padding: 20px;
    }
    .head, .panel {
      border: 3px solid var(--line);
      background: var(--card);
      box-shadow: 8px 8px 0 var(--line);
      padding: 16px;
    }
    .head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 24px;
      letter-spacing: 1px;
      text-transform: uppercase;
    }
    h2 {
      margin: 0 0 10px;
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: .6px;
      color: var(--accent);
    }
    .hint {
      color: var(--muted);
      font-size: 13px;
      margin-top: 6px;
      line-height: 1.35;
    }
    .timestamp {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(5, minmax(150px, 1fr));
      margin-bottom: 16px;
    }
    .metric {
      border: 2px solid var(--line);
      background: var(--card-2);
      padding: 12px;
      min-width: 0;
    }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .6px;
      font-weight: 800;
    }
    .value {
      margin-top: 6px;
      font-size: 18px;
      font-weight: 900;
      overflow-wrap: anywhere;
    }
    .filters {
      display: grid;
      gap: 10px;
      grid-template-columns: minmax(220px, 1.5fr) repeat(5, minmax(130px, .7fr)) minmax(86px, .35fr) repeat(2, minmax(108px, .35fr));
      align-items: end;
      margin-bottom: 14px;
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .5px;
    }
    input, select, button {
      font: inherit;
      min-height: 38px;
      border-radius: 0;
      border: 2px solid var(--line);
    }
    input, select {
      width: 100%;
      padding: 8px;
      background: #fff;
      color: var(--ink);
    }
    button {
      padding: 8px 10px;
      background: var(--accent);
      color: #fff;
      cursor: pointer;
      text-transform: uppercase;
      letter-spacing: .4px;
      font-weight: 800;
    }
    button.secondary {
      background: var(--card-2);
      color: var(--ink);
    }
    .tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
    }
    .tab {
      min-height: 34px;
      background: var(--card-2);
      color: var(--ink);
    }
    .tab.active {
      background: var(--blue);
      color: #fff;
    }
    .list {
      display: grid;
      gap: 10px;
    }
    .row {
      border: 2px solid var(--line);
      background: #fffdf8;
      padding: 10px;
      display: grid;
      gap: 6px;
    }
    .row-head {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: start;
    }
    .row-title {
      font-weight: 900;
      overflow-wrap: anywhere;
    }
    .pills {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: end;
    }
    .pill {
      border: 1px solid var(--line);
      background: var(--card-2);
      padding: 2px 6px;
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .pill.ok { color: var(--ok); }
    .pill.warn { color: var(--warn); }
    .pill.error, .pill.failed { color: var(--bad); }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 6px 12px;
      color: var(--muted);
      font-size: 12px;
    }
    details {
      border-top: 1px solid rgba(43, 37, 32, .25);
      padding-top: 6px;
    }
    summary {
      cursor: pointer;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }
    pre {
      margin: 8px 0 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      max-height: 280px;
      overflow: auto;
    }
    .empty {
      border: 2px dashed var(--line);
      padding: 14px;
      color: var(--muted);
      background: #fffdf8;
    }
    @media (max-width: 1100px) {
      .head { align-items: start; flex-direction: column; }
      .grid, .filters { grid-template-columns: 1fr; }
      .row-head { flex-direction: column; }
      .pills { justify-content: start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <header class="head">
      <div>
        <h1>WattSwarm Network Diagnostics</h1>
        <div class="hint">Iroh transport, gossip publish and ingest, subscribed scopes, backfill, callback delivery, and node network state.</div>
      </div>
      <div id="generatedAt" class="timestamp">Not refreshed</div>
    </header>

    <section class="grid" aria-label="Network diagnostic summary">
      <div class="metric">
        <div class="label">Service</div>
        <div id="serviceState" class="value">unknown</div>
      </div>
      <div class="metric">
        <div class="label">Iroh Endpoint</div>
        <div id="irohEndpoint" class="value">-</div>
      </div>
      <div class="metric">
        <div class="label">Known Iroh Contacts</div>
        <div id="knownIrohContacts" class="value">0</div>
      </div>
      <div class="metric">
        <div class="label">Subscribed Scopes</div>
        <div id="scopeCount" class="value">0</div>
      </div>
      <div class="metric">
        <div class="label">Diagnostics</div>
        <div id="diagnosticCount" class="value">0</div>
      </div>
    </section>

    <section class="panel">
      <h2>Network Log Stream</h2>
      <div class="filters">
        <label>
          Search
          <input id="search" placeholder="task id, event id, node id, scope">
        </label>
        <label>
          Level
          <select id="level">
            <option value="">All</option>
            <option value="debug">Debug</option>
            <option value="info">Info</option>
            <option value="warn">Warning</option>
            <option value="error">Error</option>
          </select>
        </label>
        <label>
          Component
          <input id="component" placeholder="all">
        </label>
        <label>
          Category
          <input id="category" placeholder="transport/gossip">
        </label>
        <label>
          Object ID
          <input id="objectId" placeholder="task/topic/event">
        </label>
        <label>
          Source Node
          <input id="sourceNodeId" placeholder="node id">
        </label>
        <label>
          Lines
          <input id="limit" type="number" min="10" max="1000" value="200">
        </label>
        <button type="button" onclick="refreshDiagnostics()">Refresh</button>
        <button type="button" class="secondary" onclick="exportDiagnostics()">Export</button>
      </div>
      <div class="tabs" aria-label="Diagnostic filters">
        <button class="tab active" type="button" data-mode="all">All</button>
        <button class="tab" type="button" data-mode="transport">Transport</button>
        <button class="tab" type="button" data-mode="gossip">Gossip</button>
        <button class="tab" type="button" data-mode="backfill">Backfill</button>
        <button class="tab" type="button" data-mode="agent-events">Agent Events</button>
        <button class="tab" type="button" data-mode="callback">Callback</button>
        <button class="tab" type="button" data-mode="errors">Errors</button>
      </div>
      <div id="diagnosticsList" class="list"></div>
    </section>
  </div>

  <script>
    let lastPayload = null;
    let lastRows = [];
    let activeMode = "all";

    function qs(id) {
      return document.getElementById(id);
    }

    function safeArray(value) {
      return Array.isArray(value) ? value : [];
    }

    function escapeHtml(value) {
      return String(value == null ? "" : value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function compact(value, size = 20) {
      const text = String(value == null || value === "" ? "-" : value);
      if (text.length <= size + 8) return text;
      return `${text.slice(0, size)}...${text.slice(-6)}`;
    }

    function formatTime(value) {
      if (value == null || value === "") return "-";
      if (typeof value === "number") {
        const milliseconds = value > 100000000000 ? value : value * 1000;
        return new Date(milliseconds).toLocaleString();
      }
      const parsed = Date.parse(value);
      return Number.isNaN(parsed) ? String(value) : new Date(parsed).toLocaleString();
    }

    function queryParams() {
      const params = new URLSearchParams();
      const fields = [
        ["search", "search"],
        ["level", "level"],
        ["component", "component"],
        ["category", "category"],
        ["objectId", "object_id"],
        ["sourceNodeId", "source_node_id"],
      ];
      params.set("limit", qs("limit").value || "200");
      if (activeMode !== "all") params.set("mode", activeMode);
      for (const [id, key] of fields) {
        const value = qs(id).value.trim();
        if (value) params.set(key, value);
      }
      return params;
    }

    async function refreshDiagnostics() {
      const response = await fetch(`/api/diagnostics?${queryParams().toString()}`);
      const payload = await response.json();
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || `HTTP ${response.status}`);
      }
      lastPayload = payload;
      lastRows = safeArray(payload.diagnostics);
      render(payload, lastRows);
    }

    function isError(row) {
      const text = `${row.level || ""} ${row.status || ""}`.toLowerCase();
      return text.includes("error") || text.includes("fail") || text.includes("warn");
    }

    function diagnosticDetails(row) {
      return row && row.details && typeof row.details === "object" && !Array.isArray(row.details)
        ? row.details
        : {};
    }

    function diagnosticText(row) {
      const details = diagnosticDetails(row);
      return [
        row.category,
        row.phase,
        row.component,
        row.object_kind,
        row.message,
        details.event_type,
        details.feed_key,
        details.payload_kind,
      ].filter(Boolean).join(" ").toLowerCase();
    }

    function isAgentEvent(row) {
      const category = String(row.category || "").toLowerCase();
      const objectKind = String(row.object_kind || "").toLowerCase();
      return category === "agent_event" || objectKind === "agent_event";
    }

    function modeMatches(row) {
      if (activeMode === "all") return true;
      if (activeMode === "errors") return isError(row);
      const category = String(row.category || "").toLowerCase();
      if (activeMode === "transport") return category === "transport";
      if (activeMode === "gossip") return category === "gossip";
      if (activeMode === "agent-events") return isAgentEvent(row);
      const text = diagnosticText(row);
      if (activeMode === "backfill") return text.includes("backfill") && !isError(row);
      if (activeMode === "callback") return text.includes("callback") && !isError(row);
      return text.includes(activeMode);
    }

    function render(payload, rows) {
      const snapshot = payload.snapshot || {};
      const visible = safeArray(rows).filter(modeMatches);
      qs("generatedAt").textContent = `Refreshed ${formatTime(payload.generated_at || new Date().toISOString())}`;
      qs("serviceState").textContent = payload.network_service_started ? "running" : "stopped";
      qs("irohEndpoint").textContent = compact(snapshot.local_iroh_endpoint_id || "-", 24);
      qs("knownIrohContacts").textContent = String(snapshot.known_iroh_contacts || 0);
      qs("scopeCount").textContent = String(safeArray(snapshot.subscribed_scopes).length);
      qs("diagnosticCount").textContent = String(visible.length);

      const list = qs("diagnosticsList");
      if (!visible.length) {
        list.innerHTML = `<div class="empty">No network diagnostics recorded for the current filters.</div>`;
        return;
      }
      list.innerHTML = visible.map((row) => {
        const details = row.details && typeof row.details === "object" ? row.details : {};
        const targetScope = details.target_scope_hint || details.target_scope || "";
        const feedKey = details.feed_key || "";
        const subscriberNode = details.subscriber_node_id || "";
        const meta = [
          row.component,
          row.category,
          row.phase,
          row.scope_hint ? `lane ${row.scope_hint}` : "",
          targetScope ? `target scope ${targetScope}` : "",
          feedKey ? `feed ${compact(feedKey, 26)}` : "",
          subscriberNode ? `subscriber ${compact(subscriberNode, 18)}` : "",
          row.object_id ? `object ${row.object_id}` : "",
          row.event_id ? `event ${compact(row.event_id, 18)}` : "",
          row.source_node_id ? `from ${compact(row.source_node_id, 18)}` : "",
        ].filter(Boolean);
        const status = row.status || row.level || "info";
        return `
          <article class="row">
            <div class="row-head">
              <div class="row-title">${escapeHtml(row.message || row.phase || "diagnostic")}</div>
              <div class="pills">
                <span class="pill ${escapeHtml(String(row.level || "").toLowerCase())}">${escapeHtml(row.level || "info")}</span>
                <span class="pill ${escapeHtml(String(status).toLowerCase())}">${escapeHtml(status)}</span>
              </div>
            </div>
            <div class="meta"><span>${escapeHtml(formatTime(row.timestamp_ms || row.timestamp))}</span>${meta.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}</div>
            <details>
              <summary>Raw JSON</summary>
              <pre>${escapeHtml(JSON.stringify(row, null, 2))}</pre>
            </details>
          </article>
        `;
      }).join("");
    }

    function exportDiagnostics() {
      const rows = lastRows.length ? lastRows : [];
      const body = rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : "");
      const blob = new Blob([body], { type: "application/x-ndjson" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `wattswarm-network-diagnostics-${new Date().toISOString().replace(/[:.]/g, "-")}.jsonl`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    document.querySelectorAll("[data-mode]").forEach((button) => {
      button.addEventListener("click", () => {
        activeMode = button.dataset.mode || "all";
        document.querySelectorAll("[data-mode]").forEach((item) => {
          item.classList.toggle("active", item === button);
        });
        refreshDiagnostics().catch((error) => {
          qs("diagnosticsList").innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
        });
      });
    });

    refreshDiagnostics().catch((error) => {
      qs("diagnosticsList").innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
    });
  </script>
</body>
</html>"#;
