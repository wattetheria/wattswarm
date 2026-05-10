pub const STARTUP_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>WattSwarm Startup</title>
  <style>
    :root {
      --bg: #f0e8d8;
      --ink: #1e1a16;
      --card: #fff6e8;
      --line: #2b2520;
      --accent: #d46a1f;
      --accent-2: #1f7fd4;
      --soft: #f7eee0;
      --ok: #2b8a3e;
      --warn: #a35b00;
      --err: #9f1f1f;
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
      max-width: 1360px;
      margin: 0 auto;
      padding: 20px;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 16px;
      align-items: stretch;
    }
    .panel {
      border: 3px solid var(--line);
      background: var(--card);
      box-shadow: 8px 8px 0 var(--line);
      padding: 16px;
    }
    .section-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      grid-auto-rows: 1fr;
      gap: 14px;
    }
    .section-grid > .panel {
      height: 100%;
    }
    .hero-panel {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 18px;
      align-items: end;
    }
    .hero-actions {
      display: grid;
      gap: 8px;
      min-width: 310px;
    }
    .hero-actions .hint {
      margin: 0;
      max-width: 360px;
    }
    .action-row {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      letter-spacing: 1px;
      text-transform: uppercase;
    }
    h2 {
      margin: 0 0 8px;
      font-size: 14px;
      text-transform: uppercase;
      color: var(--accent);
    }
    h3 {
      margin: 0 0 10px;
      font-size: 12px;
      text-transform: uppercase;
      color: #5e5448;
    }
    .hint {
      margin-top: 6px;
      font-size: 12px;
      color: #5f5549;
      line-height: 1.6;
    }
    .field {
      display: grid;
      gap: 6px;
      margin-top: 10px;
    }
    .field label {
      font-size: 12px;
      text-transform: uppercase;
      color: #4c4339;
    }
    input, select, textarea {
      width: 100%;
      border: 2px solid var(--line);
      background: #fff;
      color: var(--ink);
      font: inherit;
      padding: 9px;
      border-radius: 0;
    }
    textarea {
      min-height: 110px;
      resize: vertical;
    }
    input[readonly] {
      background: #f6efe3;
      color: #5f5549;
    }
    .row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .mode-buttons {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      margin-top: 10px;
    }
    button {
      border: 2px solid var(--line);
      background: var(--accent);
      color: #fff;
      font: inherit;
      padding: 9px 12px;
      cursor: pointer;
      text-transform: uppercase;
      letter-spacing: .4px;
    }
    button.alt { background: var(--accent-2); }
    button.ghost { background: #efe2cf; color: var(--ink); }
    button.mode {
      background: #f1e4d0;
      color: var(--ink);
    }
    button.mode.active {
      background: var(--accent-2);
      color: #fff;
    }
    .meta {
      display: grid;
      gap: 8px;
      margin-top: 10px;
      padding: 10px;
      background: var(--soft);
      border: 2px solid var(--line);
    }
    .meta-line {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 12px;
      text-transform: uppercase;
      min-width: 0;
    }
    .meta-line span {
      flex: 0 0 92px;
    }
    .meta-line code {
      display: block;
      flex: 1 1 auto;
      min-width: 0;
      width: 100%;
      background: #fff;
      border: 1px solid #8c7f71;
      padding: 0 4px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .monitor {
      position: sticky;
      top: 20px;
      display: grid;
      gap: 12px;
    }
    .monitor-block {
      border: 2px solid var(--line);
      background: #fff;
    }
    .monitor-label {
      padding: 8px 10px;
      border-bottom: 2px solid var(--line);
      background: var(--soft);
      font-size: 12px;
      text-transform: uppercase;
      color: #423930;
    }
    pre {
      margin: 0;
      padding: 10px;
      min-height: 160px;
      max-height: 320px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      background: #fffdf8;
    }
    .pill {
      position: fixed;
      top: 10px;
      right: 10px;
      z-index: 9999;
      border: 3px solid var(--line);
      box-shadow: 4px 4px 0 var(--line);
      background: #efe2cf;
      color: var(--ink);
      padding: 6px 10px;
      font-size: 12px;
      text-transform: uppercase;
    }
    .pill.ok { background: #cfe8d4; color: var(--ok); }
    .pill.warn { background: #efe1cc; color: var(--warn); }
    .pill.err { background: #f5d5d5; color: var(--err); }
    .links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
      font-size: 12px;
    }
    .links a {
      color: var(--accent-2);
      text-decoration: none;
      border-bottom: 1px solid currentColor;
    }
    .hidden {
      display: none;
    }
    @media (max-width: 1120px) {
      .layout {
        grid-template-columns: minmax(0, 1fr);
      }
      .section-grid {
        grid-auto-rows: auto;
      }
      .monitor {
        position: static;
      }
      .hero-panel {
        grid-template-columns: minmax(0, 1fr);
      }
      .hero-actions {
        min-width: 0;
      }
      .action-row {
        justify-content: flex-start;
      }
    }
    @media (max-width: 720px) {
      .meta-line {
        align-items: start;
        flex-direction: column;
        gap: 6px;
      }
      .meta-line span {
        flex: none;
      }
    }
  </style>
</head>
<body>
  <div id="statusPill" class="pill">idle</div>
  <div class="wrap">
    <div class="panel hero-panel">
      <div>
        <h1>WattSwarm Startup</h1>
        <div class="hint">
          First-run configuration for the local node. Only required startup fields are shown here.
          Required startup settings stay here. Deeper tuning and gateway deployment still stay in CLI or compose.
        </div>
        <div class="links">
          <a href="/diagnostics">Open Network Diagnostics</a>
        </div>
      </div>
      <div class="hero-actions">
        <div class="hint">Saving writes wattswarm startup only. Agent and model configuration lives in Wattetheria.</div>
        <div class="action-row">
          <button type="button" onclick="saveStartupConfig()">save startup config</button>
          <button type="button" class="alt" onclick="refreshStartupConfig()">reload</button>
          <button type="button" class="ghost" onclick="refreshNodeStatus()">refresh node</button>
        </div>
      </div>
    </div>

    <div class="layout" style="margin-top:16px;">
      <div class="section-grid">
        <div class="panel">
          <h2>Node Identity</h2>
          <div class="hint">The node ID is generated by Wattswarm. Use a display name so this node is readable in network-facing clients.</div>
          <div class="field">
            <label for="nodeId">Node ID</label>
            <input id="nodeId" readonly value="loading..." />
          </div>
          <div class="field">
            <label for="displayName">Display Name</label>
            <input id="displayName" placeholder="Captain Aurora" />
          </div>
          <div class="field">
            <label>Geo Location</label>
            <input id="geoLocation" readonly value="loading..." />
          </div>
          <div class="hint">Read-only. Wattetheria resolves this automatically and syncs it into gateway-backed clients.</div>
        </div>

        <div class="panel">
          <h2>Network Mode</h2>
          <div class="hint">Choose where this node should live. WAN discovers Wattetheria bootstrap and gateway endpoints automatically.</div>
          <div class="mode-buttons">
            <button id="networkLocal" class="mode" type="button" onclick="setNetworkMode('local')">Local</button>
            <button id="networkLan" class="mode" type="button" onclick="setNetworkMode('lan')">LAN</button>
            <button id="networkWan" class="mode" type="button" onclick="setNetworkMode('wan')">WAN</button>
          </div>
          <div class="meta">
            <div class="meta-line"><span>Selected</span><code id="networkModeLabel">local</code></div>
            <div class="meta-line"><span>Meaning</span><code id="networkModeMeaning">single-machine startup</code></div>
          </div>
          <div id="bootstrapContactsField" class="field hidden">
            <label for="bootstrapContacts">Bootstrap Contacts</label>
            <textarea id="bootstrapContacts" rows="4" placeholder="paste output from:
wattswarm --state-dir &lt;genesis-state-dir&gt; node export-contact

format: &lt;node-id&gt;@&lt;host:port&gt;"></textarea>
            <div class="hint">Used only for LAN or private test networks. Paste one Iroh bootstrap contact per line.</div>
          </div>
          <div id="gatewayUrlsField" class="field hidden">
            <label for="gatewayUrls">Gateway URLs</label>
            <textarea id="gatewayUrls" rows="3" placeholder="http://gateway.example.com:8080
https://gw.example.com"></textarea>
            <div class="hint">Used only for LAN or private test networks. WAN receives gateway URLs from the join manifest.</div>
          </div>
          <div class="hint">Gateway deployment itself still stays in DNS, CLI, compose, or direct config edits.</div>
        </div>

      </div>

      <div class="panel monitor">
        <h2>Startup Monitor</h2>
        <div class="monitor-block">
          <div class="monitor-label">Latest Response</div>
          <pre id="responseOut">Ready.</pre>
        </div>
        <div class="monitor-block">
          <div class="monitor-label">Notes</div>
          <pre>Required here: node identity and minimal network mode.

Not configured here:
- Wattetheria brain provider / agent host
- gateway deployment
- servicenet deployment
- P2P tuning
- worker tuning
- direct DB, NATS, token, or signing-key setup

Those stay in CLI, compose, or config files for advanced operators.</pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    const responseOut = document.getElementById('responseOut');
    const statusPill = document.getElementById('statusPill');
    let startupConfig = null;

    function setStatus(kind, text) {
      statusPill.className = `pill ${kind || ''}`.trim();
      statusPill.textContent = text;
    }

    function showResponse(value) {
      if (typeof value === 'string') {
        responseOut.textContent = value;
      } else {
        responseOut.textContent = JSON.stringify(value, null, 2);
      }
    }

    function setNetworkMode(mode) {
      startupConfig.network_mode = mode;
      document.getElementById('networkModeLabel').textContent = mode;
      document.getElementById('networkLocal').classList.toggle('active', mode === 'local');
      document.getElementById('networkLan').classList.toggle('active', mode === 'lan');
      document.getElementById('networkWan').classList.toggle('active', mode === 'wan');
      const meaning = mode === 'wan'
        ? 'public or routed peer network'
        : mode === 'lan'
          ? 'local-area distributed network'
          : 'single-machine startup';
      document.getElementById('networkModeMeaning').textContent = meaning;
      const manualJoin = mode === 'lan';
      document.getElementById('bootstrapContactsField').classList.toggle('hidden', !manualJoin);
      document.getElementById('gatewayUrlsField').classList.toggle('hidden', !manualJoin);
    }

    function syncFormFromConfig(cfg) {
      startupConfig = cfg;
      document.getElementById('displayName').value = cfg.display_name || '';
      const hasGeo = Number.isFinite(cfg.latitude) && Number.isFinite(cfg.longitude);
      document.getElementById('geoLocation').value = hasGeo
        ? `${cfg.latitude}, ${cfg.longitude}`
        : 'automatic location pending';
      const manualJoin = cfg.network_mode === 'lan';
      document.getElementById('bootstrapContacts').value = manualJoin ? (cfg.bootstrap_contacts || []).join('\n') : '';
      document.getElementById('gatewayUrls').value = manualJoin ? (cfg.gateway_urls || []).join('\n') : '';
      setNetworkMode(cfg.network_mode || 'local');
    }

    function buildPayload() {
      const bootstrapContacts = document.getElementById('bootstrapContacts').value
        .split(/\n+/)
        .map((value) => value.trim())
        .filter(Boolean);
      const gatewayUrls = document.getElementById('gatewayUrls').value
        .split(/[\n,]+/)
        .map((value) => value.trim())
        .filter(Boolean);
      const isLan = startupConfig.network_mode === 'lan';
      return {
        display_name: document.getElementById('displayName').value,
        network_mode: startupConfig.network_mode,
        bootstrap_contacts: isLan ? bootstrapContacts : [],
        gateway_urls: isLan ? gatewayUrls : []
      };
    }

    async function refreshNodeStatus() {
      setStatus('warn', 'loading-node');
      const res = await fetch('/api/node/status');
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus('err', 'node-failed');
        showResponse(data);
        return;
      }
      document.getElementById('nodeId').value = data.node_id || 'unavailable';
      setStatus('ok', 'node-ready');
    }

    async function refreshStartupConfig() {
      setStatus('warn', 'loading-config');
      const res = await fetch('/api/startup-config');
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus('err', 'config-failed');
        showResponse(data);
        return;
      }
      syncFormFromConfig(data.config);
      showResponse(data);
      setStatus('ok', 'config-ready');
    }

    async function saveStartupConfig() {
      setStatus('warn', 'saving');
      const payload = buildPayload();
      const res = await fetch('/api/startup-config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      showResponse(data);
      if (!res.ok || !data.ok) {
        setStatus('err', 'save-failed');
        return;
      }
      syncFormFromConfig(data.config);
      setStatus('ok', 'saved');
    }

    Promise.all([refreshNodeStatus(), refreshStartupConfig()]).catch((err) => {
      setStatus('err', 'bootstrap-failed');
      showResponse({ ok: false, error: String(err) });
    });
  </script>
</body>
</html>
"#;
