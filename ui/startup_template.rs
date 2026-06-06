pub const STARTUP_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Wattswarm Startup</title>
  <style>
    :root {
      --bg: #f4f6f8;
      --surface: #ffffff;
      --surface-alt: #f7f8fa;
      --surface-inset: #fbfcfd;
      --ink: #111827;
      --muted: #6b7280;
      --faint: #9aa1ac;
      --line: #e9ebf0;
      --line-soft: #eef0f4;
      --line-strong: #d6dae1;
      --green: #16a34a;
      --green-soft: #e9f7ee;
      --green-ink: #166534;
      --red: #dc2626;
      --red-soft: #fdecec;
      --red-ink: #991b1b;
      --amber: #b45309;
      --amber-soft: #fef3e2;
      --amber-ink: #92400e;
      --radius-sm: 6px;
      --radius: 8px;
      --radius-lg: 12px;
      --shadow-sm: 0 1px 2px rgba(16, 24, 40, 0.04), 0 1px 3px rgba(16, 24, 40, 0.06);
      --shadow-md: 0 4px 12px rgba(16, 24, 40, 0.08);
      --accent: #16a34a;
      --accent-strong: #14532d;
      --accent-soft: #e9f7ee;
      --accent-contrast: #ffffff;
    }
    :root[data-theme="teal"] { --accent: #0d9488; --accent-strong: #115e59; --accent-soft: #e4f5f3; --accent-contrast: #ffffff; }
    :root[data-theme="emerald"] { --accent: #10b981; --accent-strong: #065f46; --accent-soft: #e7f8f1; --accent-contrast: #ffffff; }
    :root[data-theme="forest"] { --accent: #16a34a; --accent-strong: #14532d; --accent-soft: #e9f7ee; --accent-contrast: #ffffff; }
    :root[data-theme="blue-royal"] { --accent: #2563eb; --accent-strong: #1e3a8a; --accent-soft: #eff4ff; --accent-contrast: #ffffff; }
    :root[data-theme="blue-sky"] { --accent: #0284c7; --accent-strong: #075985; --accent-soft: #e8f7fe; --accent-contrast: #ffffff; }
    :root[data-theme="indigo"] { --accent: #4f46e5; --accent-strong: #3730a3; --accent-soft: #eef0fe; --accent-contrast: #ffffff; }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Inter", "Helvetica Neue", Arial, sans-serif;
      background: var(--bg);
      color: var(--ink);
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }
    .wrap {
      max-width: 1360px;
      margin: 0 auto;
      padding: 24px;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 16px;
      align-items: stretch;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      background: var(--surface);
      box-shadow: var(--shadow-sm);
      padding: 20px;
    }
    .section-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      grid-auto-rows: 1fr;
      gap: 16px;
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
      gap: 10px;
      justify-items: end;
      min-width: 310px;
    }
    .hero-actions .hint {
      margin: 0;
      max-width: 360px;
      text-align: right;
    }
    .action-row {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
    }
    h1 {
      margin: 0;
      font-size: 1.5rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      line-height: 1.1;
    }
    h2 {
      margin: 0 0 8px;
      font-size: 0.74rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--accent-strong);
    }
    h3 {
      margin: 0 0 10px;
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--faint);
    }
    .hint {
      margin-top: 6px;
      font-size: 0.84rem;
      color: var(--muted);
      line-height: 1.6;
    }
    .field {
      display: grid;
      gap: 6px;
      margin-top: 12px;
    }
    .field label {
      font-size: 0.72rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }
    input, select, textarea {
      width: 100%;
      min-height: 2.4rem;
      border: 1px solid var(--line-strong);
      background: var(--surface);
      color: var(--ink);
      font: inherit;
      padding: 0.55rem 0.7rem;
      border-radius: var(--radius);
      transition: border-color 0.12s ease, box-shadow 0.12s ease;
    }
    input::placeholder, textarea::placeholder { color: var(--faint); }
    input:hover, textarea:hover, select:hover { border-color: var(--muted); }
    input:focus, select:focus, textarea:focus {
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-soft);
    }
    textarea {
      min-height: 110px;
      resize: vertical;
      line-height: 1.5;
    }
    input[readonly] {
      background: var(--surface-alt);
      color: var(--muted);
      border-style: dashed;
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
      margin-top: 12px;
    }
    button {
      border: 1px solid transparent;
      background: var(--accent);
      color: var(--accent-contrast);
      font: inherit;
      font-weight: 600;
      font-size: 0.86rem;
      padding: 0.55rem 0.95rem;
      min-height: 2.4rem;
      border-radius: var(--radius);
      cursor: pointer;
      transition: filter 0.12s ease, background 0.12s ease;
    }
    button:hover { filter: brightness(0.95); }
    button.alt {
      background: var(--surface);
      color: var(--ink);
      border: 1px solid var(--line-strong);
    }
    button.alt:hover { background: var(--surface-alt); filter: none; }
    button.ghost {
      background: var(--surface-alt);
      color: var(--muted);
      border: 1px solid var(--line);
    }
    button.ghost:hover { color: var(--ink); filter: none; }
    button.mode {
      background: var(--surface);
      color: var(--muted);
      border: 1px solid var(--line-strong);
    }
    button.mode:hover { background: var(--surface-alt); color: var(--ink); filter: none; }
    button.mode.active {
      background: var(--accent);
      color: var(--accent-contrast);
      border-color: var(--accent);
    }
    .meta {
      display: grid;
      gap: 8px;
      margin-top: 12px;
      padding: 12px;
      background: var(--surface-inset);
      border: 1px solid var(--line);
      border-radius: var(--radius);
    }
    .meta-line {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 0.76rem;
      min-width: 0;
    }
    .meta-line span {
      flex: 0 0 92px;
      color: var(--faint);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .meta-line code {
      display: block;
      flex: 1 1 auto;
      min-width: 0;
      width: 100%;
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      padding: 0.2rem 0.5rem;
      font-family: ui-monospace, "SF Mono", "SFMono-Regular", Menlo, Consolas, monospace;
      color: var(--ink);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .monitor {
      position: sticky;
      top: 24px;
      display: grid;
      gap: 12px;
      align-content: start;
    }
    .monitor-block {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      overflow: hidden;
    }
    .monitor-label {
      padding: 0.6rem 0.8rem;
      border-bottom: 1px solid var(--line);
      background: var(--surface-alt);
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
    }
    pre {
      margin: 0;
      padding: 0.85rem;
      min-height: 160px;
      max-height: 320px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, "SF Mono", "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.78rem;
      line-height: 1.5;
      background: var(--surface);
      color: var(--ink);
    }
    .pill {
      position: fixed;
      top: 14px;
      right: 14px;
      z-index: 9999;
      border: 1px solid var(--line);
      border-radius: 999px;
      box-shadow: var(--shadow-md);
      background: var(--surface);
      color: var(--muted);
      padding: 0.35rem 0.8rem;
      font-size: 0.74rem;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .pill.ok { background: var(--green-soft); color: var(--green-ink); border-color: transparent; }
    .pill.warn { background: var(--amber-soft); color: var(--amber-ink); border-color: transparent; }
    .pill.err { background: var(--red-soft); color: var(--red-ink); border-color: transparent; }
    .links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
      font-size: 0.84rem;
    }
    .links a {
      color: var(--accent-strong);
      font-weight: 500;
      text-decoration: none;
      border-bottom: 1px solid var(--accent-soft);
    }
    .links a:hover { border-bottom-color: currentColor; }
    .theme-picker {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.4rem 0.6rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--surface);
    }
    .theme-picker-label {
      color: var(--faint);
      font-size: 0.64rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .theme-swatches { display: flex; gap: 0.35rem; }
    .theme-swatch {
      width: 1.25rem;
      height: 1.25rem;
      min-height: 0;
      padding: 0;
      border: 1px solid rgba(17, 24, 39, 0.12);
      border-radius: var(--radius-sm);
      background: var(--sw, #888);
      cursor: pointer;
      transition: transform 0.12s ease, box-shadow 0.12s ease;
    }
    .theme-swatch:hover { transform: translateY(-1px); box-shadow: var(--shadow-sm); filter: none; }
    .theme-swatch.active { outline: 2px solid var(--accent); outline-offset: 2px; }
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
        justify-items: start;
      }
      .hero-actions .hint { text-align: left; }
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
        <h1>Wattswarm startup</h1>
        <div class="hint">
          First-run configuration for the local node. Only required startup fields are shown here.
          Required startup settings stay here. Deeper tuning and gateway deployment still stay in CLI or compose.
        </div>
        <div class="links">
          <a href="/diagnostics">Open Network Diagnostics</a>
        </div>
      </div>
      <div class="hero-actions">
        <div class="theme-picker" aria-label="Accent color theme">
          <span class="theme-picker-label">Theme</span>
          <div class="theme-swatches" role="group" aria-label="Choose accent color">
            <button class="theme-swatch" type="button" data-theme-swatch="teal" style="--sw:#0d9488" title="Teal" aria-label="Teal theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="emerald" style="--sw:#10b981" title="Emerald" aria-label="Emerald theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="forest" style="--sw:#16a34a" title="Forest" aria-label="Forest theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="blue-royal" style="--sw:#2563eb" title="Royal blue" aria-label="Royal blue theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="blue-sky" style="--sw:#0284c7" title="Sky blue" aria-label="Sky blue theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="indigo" style="--sw:#4f46e5" title="Indigo" aria-label="Indigo theme"></button>
          </div>
        </div>
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
          <div class="hint">The node ID is generated by Wattswarm.</div>
          <div class="field">
            <label for="nodeId">Node ID</label>
            <input id="nodeId" readonly value="loading..." />
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

    const STORAGE_KEY = "wattswarm-console";
    const themeOptions = ["teal", "emerald", "forest", "blue-royal", "blue-sky", "indigo"];
    const defaultTheme = "forest";
    function readStoredSettings() {
      try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}") || {}; }
      catch (_) { return {}; }
    }
    function applyTheme(theme) {
      const next = themeOptions.includes(theme) ? theme : defaultTheme;
      document.documentElement.setAttribute("data-theme", next);
      document.querySelectorAll("[data-theme-swatch]").forEach((swatch) => {
        const active = swatch.dataset.themeSwatch === next;
        swatch.classList.toggle("active", active);
        if (active) swatch.setAttribute("aria-current", "true");
        else swatch.removeAttribute("aria-current");
      });
    }
    function saveTheme(theme) {
      const saved = readStoredSettings();
      saved.theme = theme;
      localStorage.setItem(STORAGE_KEY, JSON.stringify(saved));
    }
    function initThemePicker() {
      applyTheme(readStoredSettings().theme || defaultTheme);
      document.querySelectorAll("[data-theme-swatch]").forEach((swatch) => {
        swatch.addEventListener("click", () => {
          const theme = swatch.dataset.themeSwatch;
          applyTheme(theme);
          saveTheme(theme);
        });
      });
    }

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

    initThemePicker();

    Promise.all([refreshNodeStatus(), refreshStartupConfig()]).catch((err) => {
      setStatus('err', 'bootstrap-failed');
      showResponse({ ok: false, error: String(err) });
    });
  </script>
</body>
</html>
"#;
