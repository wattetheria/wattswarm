pub const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Wattswarm Network Diagnostics</title>
  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png">
  <style>
    /*__SWARM_THEME_CSS__*/

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--font-body);
      background: var(--bg);
      color: var(--ink);
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }
    h1 {
      font-family: var(--font-head);
    }
    .wrap {
      max-width: 1560px;
      margin: 0 auto;
      padding: 24px;
    }
    .head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-end;
      margin-bottom: 22px;
      padding: 8px 2px 0;
    }
    .head-actions {
      display: flex;
      align-items: center;
      gap: 16px;
    }
    h1 {
      margin: 0;
      font-size: 1.6rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      line-height: 1.1;
    }
    h2 {
      margin: 0 0 14px;
      font-size: 0.74rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--accent-strong);
    }
    .hint {
      color: var(--muted);
      font-size: 0.9rem;
      margin-top: 6px;
      line-height: 1.5;
      max-width: 70rem;
    }
    .timestamp {
      color: var(--faint);
      font-size: 0.8rem;
      white-space: nowrap;
    }
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
    .theme-swatches {
      display: flex;
      gap: 0.35rem;
    }
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
    .theme-swatch:hover { transform: translateY(-1px); box-shadow: var(--shadow-sm); }
    .theme-swatch.active { outline: 2px solid var(--accent); outline-offset: 2px; }

    .grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(5, minmax(150px, 1fr));
      margin-bottom: 18px;
    }
    .metric {
      border: 1px solid var(--line);
      border-left: 3px solid var(--accent);
      border-radius: var(--radius-lg);
      background: var(--surface);
      box-shadow: var(--shadow-sm);
      padding: 14px 16px;
      min-width: 0;
    }
    .label {
      color: var(--faint);
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 600;
    }
    .value {
      margin-top: 6px;
      font-size: 1.4rem;
      font-weight: 600;
      letter-spacing: -0.01em;
      overflow-wrap: anywhere;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      background: var(--surface);
      box-shadow: var(--shadow-sm);
      padding: 20px;
    }
    .connectivity-panel {
      margin-bottom: 18px;
    }
    .connectivity-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .connectivity-item {
      display: grid;
      gap: 6px;
      min-width: 0;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface-inset);
    }
    .connectivity-item code {
      display: block;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-family: ui-monospace, "SF Mono", "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.92rem;
      color: var(--ink);
    }
    .connectivity-item code.route-current {
      font-weight: 800;
      text-transform: uppercase;
    }
    .connectivity-item code.route-current.direct { color: var(--green-ink); }
    .connectivity-item code.route-current.relay { color: var(--amber-ink); }
    .connectivity-item code.route-current.custom { color: var(--blue-ink); }
    .connectivity-item code.route-current.unknown { color: var(--muted); }
    .filters {
      display: grid;
      gap: 12px;
      grid-template-columns: minmax(220px, 1.5fr) repeat(5, minmax(130px, .7fr)) minmax(86px, .35fr) repeat(2, minmax(108px, .35fr));
      align-items: end;
      margin-bottom: 16px;
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    input, select, button {
      font: inherit;
    }
    input, select {
      width: 100%;
      min-height: 2.4rem;
      padding: 0.55rem 0.7rem;
      border-radius: var(--radius);
      border: 1px solid var(--line-strong);
      background: var(--surface);
      color: var(--ink);
      transition: border-color 0.12s ease, box-shadow 0.12s ease;
    }
    input::placeholder { color: var(--faint); }
    input:hover, select:hover { border-color: var(--muted); }
    input:focus, select:focus {
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-soft);
    }
    button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 2.4rem;
      padding: 0.55rem 0.95rem;
      border: 1px solid transparent;
      border-radius: var(--radius-btn);
      background: var(--accent);
      color: var(--accent-contrast);
      cursor: pointer;
      font-weight: 600;
      font-size: 0.86rem;
      transition: filter 0.12s ease;
    }
    button:hover { filter: brightness(0.95); }
    button.secondary {
      background: var(--surface);
      color: var(--ink);
      border: 1px solid var(--line-strong);
    }
    button.secondary:hover { background: var(--surface-alt); filter: none; }

    .select-shell { position: relative; width: 100%; min-width: 0; }
    .select-shell > select.select-native {
      position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
      border: 0; clip: rect(0 0 0 0); clip-path: inset(50%); overflow: hidden;
      white-space: nowrap; pointer-events: none;
    }
    .select-trigger {
      display: flex; align-items: center; justify-content: space-between; gap: 0.5rem;
      width: 100%; min-height: 2.4rem; padding: 0.55rem 0.7rem;
      border: 1px solid var(--line-strong); border-radius: var(--radius);
      background: var(--surface); color: var(--ink); font: inherit; font-weight: 400;
      text-transform: none; letter-spacing: 0; text-align: left; cursor: pointer;
      transition: border-color 0.12s ease, box-shadow 0.12s ease;
    }
    .select-trigger:hover { border-color: var(--muted); filter: none; background: var(--surface); }
    .select-shell.open .select-trigger {
      outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-soft);
    }
    .select-value { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .select-value.placeholder { color: var(--faint); }
    .select-arrow { flex: 0 0 auto; color: var(--faint); font-size: 0.7rem; line-height: 1; transition: transform 0.15s ease; }
    .select-shell.open .select-arrow { transform: rotate(180deg); color: var(--accent); }
    .select-popup {
      position: absolute; z-index: 60; top: calc(100% + 4px); left: 0; width: 100%;
      min-width: max-content; max-height: 16rem; overflow-y: auto; padding: 0.25rem;
      border: 1px solid var(--line); border-radius: var(--radius);
      background: var(--surface); box-shadow: var(--shadow-md);
    }
    .select-popup.up { top: auto; bottom: calc(100% + 4px); }
    .select-popup[hidden] { display: none; }
    .select-option {
      display: flex; align-items: center; justify-content: space-between; gap: 0.6rem;
      padding: 0.5rem 0.6rem; border-radius: var(--radius-sm); color: var(--ink);
      font-size: 0.88rem; line-height: 1.3; cursor: pointer; white-space: nowrap;
    }
    .select-option:hover, .select-option.focused { background: var(--surface-alt); }
    .select-option.selected { background: var(--accent-soft); color: var(--accent-strong); font-weight: 600; }
    .select-option.selected::after { content: "\2713"; color: var(--accent); font-size: 0.85rem; }

    .tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 16px;
    }
    .tab {
      min-height: 2.1rem;
      padding: 0.4rem 0.85rem;
      background: var(--surface);
      color: var(--muted);
      border: 1px solid var(--line-strong);
      border-radius: 999px;
      font-weight: 500;
    }
    .tab:hover { background: var(--surface-alt); color: var(--ink); filter: none; }
    .tab.active { background: var(--accent); color: var(--accent-contrast); border-color: var(--accent); }
    .list {
      display: grid;
      gap: 10px;
    }
    .row {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 14px;
      display: grid;
      gap: 8px;
      transition: border-color 0.12s ease, box-shadow 0.12s ease;
    }
    .row:hover { border-color: var(--line-strong); box-shadow: var(--shadow-sm); }
    .row-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      flex-wrap: wrap;
    }
    .row-title {
      flex: 1 1 20rem;
      min-width: 0;
      font-weight: 600;
      overflow-wrap: anywhere;
      word-break: break-word;
      display: -webkit-box;
      -webkit-box-orient: vertical;
      -webkit-line-clamp: 2;
      line-clamp: 2;
      overflow: hidden;
    }
    .pills {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: end;
      flex: 0 0 auto;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      border: 1px solid transparent;
      border-radius: 999px;
      background: var(--surface-alt);
      padding: 0.15rem 0.55rem;
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 600;
      white-space: nowrap;
    }
    .pill.info { color: var(--blue-ink); background: var(--blue-soft); }
    .pill.debug { color: var(--muted); background: var(--surface-alt); }
    .pill.ok, .pill.settled, .pill.ready { color: var(--green-ink); background: var(--green-soft); }
    .pill.warn { color: var(--amber-ink); background: var(--amber-soft); }
    .pill.error, .pill.failed { color: var(--red-ink); background: var(--red-soft); }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 0.4rem;
      color: var(--faint);
      font-size: 0.76rem;
    }
    .meta > span {
      display: inline-flex;
      align-items: center;
      max-width: 100%;
      padding: 0.12rem 0.5rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--surface-alt);
      color: var(--muted);
      font-weight: 500;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    details {
      border-top: 1px solid var(--line-soft);
      padding-top: 8px;
    }
    summary {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      width: fit-content;
      padding: 0.25rem 0.6rem;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius-sm);
      background: var(--surface-alt);
      color: var(--muted);
      font-size: 0.74rem;
      font-weight: 600;
      cursor: pointer;
      list-style: none;
      user-select: none;
    }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: "\25B8"; color: var(--faint); font-size: 0.7rem; }
    details[open] > summary { color: var(--accent-strong); border-color: var(--accent); background: var(--accent-soft); }
    details[open] > summary::before { content: "\25BE"; color: var(--accent); }
    pre {
      margin: 10px 0 0;
      background: var(--surface-alt);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 0.85rem;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, "SF Mono", "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 0.78rem;
      line-height: 1.5;
      max-height: 280px;
      overflow: auto;
    }
    .empty {
      border: 1px dashed var(--line-strong);
      border-radius: var(--radius);
      padding: 1rem;
      color: var(--muted);
      background: var(--surface-alt);
      font-size: 0.88rem;
    }
    @media (max-width: 1100px) {
      .head { align-items: stretch; flex-direction: column; }
      .head-actions { justify-content: space-between; }
      .grid, .filters, .connectivity-grid { grid-template-columns: 1fr; }
      .row-head { flex-direction: column; }
      .pills { justify-content: start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <header class="head">
      <div>
        <h1>Wattswarm Network Diagnostics</h1>
        <div class="hint">Iroh transport, gossip publish and ingest, subscribed scopes, backfill, callback delivery, and node network state.</div>
      </div>
      <div class="head-actions">
        <div class="theme-picker" aria-label="Accent color theme">
          <span class="theme-picker-label">Theme</span>
          <div class="theme-swatches" role="group" aria-label="Choose theme">
            <button class="theme-swatch" type="button" data-theme-swatch="forest" style="--sw:#16a34a" title="Forest" aria-label="Forest theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="matcha" style="--sw:#3E481D" title="Matcha" aria-label="Matcha theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="butter" style="--sw:#225BFF" title="Butter" aria-label="Butter theme"></button>
            <button class="theme-swatch" type="button" data-theme-swatch="chocolate" style="--sw:#8C5927" title="Chocolate" aria-label="Chocolate theme"></button>
          </div>
        </div>
        <div id="generatedAt" class="timestamp">Not refreshed</div>
      </div>
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

    <section class="panel connectivity-panel" aria-label="Iroh connectivity">
      <h2>Iroh Connectivity</h2>
      <div class="connectivity-grid">
        <div class="connectivity-item">
          <div class="label">Endpoint ID</div>
          <code id="irohEndpointId">-</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Current Path</div>
          <code id="irohCurrentPath" class="route-current unknown">unknown</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Dial Target</div>
          <code id="irohDialTarget">-</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Relay URLs</div>
          <code id="irohRelayUrls">none</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Route Policy</div>
          <code id="irohRoutePolicy">Iroh native direct-first</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Fallback</div>
          <code id="irohFallback">relay fallback managed by Iroh</code>
        </div>
        <div class="connectivity-item">
          <div class="label">Transport Detail</div>
          <code id="irohTransportDetail">managed by Iroh</code>
        </div>
      </div>
      <div class="hint">Wattswarm uses Endpoint ID as the identity and dial target. Iroh manages direct path selection and relay fallback.</div>
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
	    let lastIrohRouteRows = [];
	    let activeMode = "all";

    const STORAGE_KEY = "wattswarm-console";
    const themeOptions = ["forest", "matcha", "butter", "chocolate"];
    const defaultTheme = "matcha";

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

    const nativeSelectValue = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value");
    function enhanceAllSelects() { document.querySelectorAll("select").forEach(enhanceSelect); }
    function enhanceSelect(select) {
      if (!select || select.dataset.enhanced === "1") return;
      select.dataset.enhanced = "1";
      select.classList.add("select-native");
      const shell = document.createElement("div");
      shell.className = "select-shell";
      select.parentNode.insertBefore(shell, select);
      shell.appendChild(select);
      const trigger = document.createElement("button");
      trigger.type = "button";
      trigger.className = "select-trigger";
      trigger.setAttribute("aria-haspopup", "listbox");
      trigger.setAttribute("aria-expanded", "false");
      trigger.innerHTML = '<span class="select-value"></span><span class="select-arrow" aria-hidden="true">▾</span>';
      const popup = document.createElement("div");
      popup.className = "select-popup";
      popup.setAttribute("role", "listbox");
      popup.hidden = true;
      shell.appendChild(trigger);
      shell.appendChild(popup);
      const valueEl = trigger.querySelector(".select-value");
      let focusedIndex = -1;
      function rebuildOptions() {
        popup.innerHTML = "";
        Array.from(select.options).forEach((opt, index) => {
          const item = document.createElement("div");
          item.className = "select-option";
          item.setAttribute("role", "option");
          item.dataset.index = String(index);
          item.textContent = opt.textContent;
          item.addEventListener("mousedown", (event) => { event.preventDefault(); choose(index); });
          popup.appendChild(item);
        });
        syncFromSelect();
      }
      function syncFromSelect() {
        const opt = select.options[select.selectedIndex];
        valueEl.textContent = opt ? opt.textContent : "";
        valueEl.classList.toggle("placeholder", !opt || opt.value === "");
        Array.from(popup.children).forEach((item) => {
          const isSel = Number(item.dataset.index) === select.selectedIndex;
          item.classList.toggle("selected", isSel);
          item.setAttribute("aria-selected", isSel ? "true" : "false");
        });
      }
      function choose(index) {
        if (index !== select.selectedIndex) {
          nativeSelectValue.set.call(select, select.options[index] ? select.options[index].value : "");
          syncFromSelect();
          select.dispatchEvent(new Event("change", { bubbles: true }));
        }
        close();
        trigger.focus();
      }
      function setFocused(index) {
        const items = popup.children;
        if (!items.length) return;
        focusedIndex = Math.max(0, Math.min(index, items.length - 1));
        Array.from(items).forEach((item, i) => item.classList.toggle("focused", i === focusedIndex));
        items[focusedIndex].scrollIntoView({ block: "nearest" });
      }
      function open() {
        document.querySelectorAll(".select-shell.open").forEach((other) => {
          if (other !== shell) other.dispatchEvent(new CustomEvent("select-close"));
        });
        shell.classList.add("open");
        popup.hidden = false;
        trigger.setAttribute("aria-expanded", "true");
        popup.classList.toggle("up", trigger.getBoundingClientRect().bottom + 260 > window.innerHeight);
        setFocused(select.selectedIndex < 0 ? 0 : select.selectedIndex);
      }
      function close() {
        shell.classList.remove("open");
        popup.hidden = true;
        popup.classList.remove("up");
        trigger.setAttribute("aria-expanded", "false");
        Array.from(popup.children).forEach((item) => item.classList.remove("focused"));
      }
      function toggle() { if (shell.classList.contains("open")) close(); else open(); }
      shell.addEventListener("select-close", close);
      trigger.addEventListener("click", toggle);
      trigger.addEventListener("keydown", (event) => {
        const isOpen = shell.classList.contains("open");
        if (!isOpen && (event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Enter" || event.key === " ")) {
          event.preventDefault(); open(); return;
        }
        if (!isOpen) return;
        if (event.key === "ArrowDown") { event.preventDefault(); setFocused(focusedIndex + 1); }
        else if (event.key === "ArrowUp") { event.preventDefault(); setFocused(focusedIndex - 1); }
        else if (event.key === "Enter" || event.key === " ") { event.preventDefault(); if (focusedIndex >= 0) choose(focusedIndex); }
        else if (event.key === "Escape") { event.preventDefault(); close(); }
        else if (event.key === "Tab") { close(); }
      });
      Object.defineProperty(select, "value", {
        configurable: true,
        get() { return nativeSelectValue.get.call(this); },
        set(v) { nativeSelectValue.set.call(this, v); syncFromSelect(); },
      });
      select.addEventListener("change", syncFromSelect);
      new MutationObserver(rebuildOptions).observe(select, { childList: true });
      rebuildOptions();
    }
    document.addEventListener("click", (event) => {
      document.querySelectorAll(".select-shell.open").forEach((shell) => {
        if (!shell.contains(event.target)) shell.dispatchEvent(new CustomEvent("select-close"));
      });
    });

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

    const RAW_JSON_TIME_FIELDS = new Set([
      "acknowledged_at",
      "created_at",
      "generated_at",
      "queued_at",
      "received_at",
      "sent_at",
      "timestamp",
      "timestamp_ms",
      "updated_at",
    ]);
    const RAW_JSON_STRING_PARSE_LIMIT = 200000;
    const rawJsonRows = new Map();

    function isRawJsonTimeField(key) {
      const normalized = String(key || "").toLowerCase();
      return RAW_JSON_TIME_FIELDS.has(normalized)
        || normalized.endsWith("_at")
        || normalized.endsWith("_at_ms")
        || normalized.startsWith("timestamp_")
        || normalized.endsWith("_timestamp");
    }

    function rawJsonStringLooksStructured(value) {
      const trimmed = String(value || "").trim();
      return (trimmed.startsWith("{") && trimmed.endsWith("}"))
        || (trimmed.startsWith("[") && trimmed.endsWith("]"));
    }

    function parseRawJsonString(value) {
      if (typeof value !== "string") return null;
      if (value.length > RAW_JSON_STRING_PARSE_LIMIT) return null;
      if (!rawJsonStringLooksStructured(value)) return null;
      try {
        return JSON.parse(value);
      } catch (_error) {
        return null;
      }
    }

    function formatRawJsonValue(key, value) {
      if (Array.isArray(value)) {
        return value.map((item) => formatRawJsonValue("", item));
      }
      if (value && typeof value === "object") {
        return Object.fromEntries(
          Object.entries(value).map(([childKey, childValue]) => [
            childKey,
            formatRawJsonValue(childKey, childValue),
          ]),
        );
      }
      if (typeof value === "string") {
        if (isRawJsonTimeField(key) && /^\d{10,13}$/.test(value.trim())) {
          return formatTime(Number(value));
        }
        if (isRawJsonTimeField(key) && !Number.isNaN(Date.parse(value))) {
          return formatTime(value);
        }
        const parsed = parseRawJsonString(value);
        if (parsed !== null) return formatRawJsonValue(key, parsed);
        return value;
      }
      if (!isRawJsonTimeField(key)) return value;
      if (typeof value === "number") return formatTime(value);
      return value;
    }

    function formatRawJson(row) {
      return JSON.stringify(formatRawJsonValue("", row), null, 2);
    }

    function renderRawJsonDetails(details) {
      if (!details || !details.open || details.dataset.rendered === "true") return;
      const pre = details.querySelector("pre");
      const row = rawJsonRows.get(details.dataset.rawJsonKey);
      if (!pre || !row) return;
      pre.textContent = "Loading...";
      requestAnimationFrame(() => {
        pre.textContent = formatRawJson(row);
        details.dataset.rendered = "true";
      });
    }

    const TITLE_MAX = 160;
    function conciseTitle(row) {
      const raw = String(row.message || row.phase || "diagnostic");
      if (raw.length <= TITLE_MAX && !raw.includes("\n")) return raw;
      const phase = String(row.phase || "").trim();
      if (phase) {
        const label = phase.replace(/[._]+/g, " ").trim();
        const details = diagnosticDetails(row);
        const eventType = details.event_type || row.event_type || "";
        return eventType ? `${label}: ${eventType}` : label;
      }
      const firstLine = raw.split(/\r?\n/)[0];
      return firstLine.length > TITLE_MAX ? `${firstLine.slice(0, TITLE_MAX - 1)}…` : firstLine;
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
	      try {
	        lastIrohRouteRows = await fetchLatestIrohRouteDiagnostics();
	      } catch (_) {
	        lastIrohRouteRows = [];
	      }
	      render(payload, lastRows, lastIrohRouteRows);
	    }

	    async function fetchLatestIrohRouteDiagnostics() {
	      const params = new URLSearchParams();
	      params.set("phase", "iroh.route.selected");
	      params.set("limit", "1");
	      const response = await fetch(`/api/diagnostics?${params.toString()}`);
	      const payload = await response.json();
	      if (!response.ok || payload.ok === false) {
	        throw new Error(payload.error || `HTTP ${response.status}`);
	      }
	      return safeArray(payload.diagnostics);
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

    function setConnectivityText(id, value, size = 36) {
      const el = qs(id);
      const text = Array.isArray(value) ? value.filter(Boolean).join(", ") : String(value || "");
      el.textContent = compact(text || "-", size);
      el.title = text || "-";
    }

    function latestIrohRouteDiagnostic(rows) {
      return safeArray(rows)
        .filter((row) => row.phase === "iroh.route.selected")
        .sort((a, b) => Number(b.timestamp_ms || b.timestamp || 0) - Number(a.timestamp_ms || a.timestamp || 0))[0] || null;
    }

    function setIrohCurrentPath(value) {
      const route = String(value || "unknown").toLowerCase();
      const normalized = ["direct", "relay", "custom"].includes(route) ? route : "unknown";
      const el = qs("irohCurrentPath");
      el.textContent = normalized;
      el.className = `route-current ${normalized}`;
      el.title = normalized === "unknown"
        ? "No successful Iroh stream has recorded a selected path yet."
        : `Latest Iroh selected path: ${normalized}`;
    }

	    function renderIrohConnectivity(snapshot, irohRouteRows) {
	      const endpointId = snapshot.local_iroh_endpoint_id || "-";
	      const relayUrls = safeArray(snapshot.relay_reservations);
	      const dialTarget = relayUrls.length
	        ? `EndpointAddr { id: ${compact(endpointId, 18)}, relay_urls: ${relayUrls.length} }`
	        : `EndpointId ${compact(endpointId, 28)}`;
	      const routeDiagnostic = latestIrohRouteDiagnostic(irohRouteRows);
	      const routeDetails = routeDiagnostic ? diagnosticDetails(routeDiagnostic) : {};

      setConnectivityText("irohEndpointId", endpointId, 42);
      setIrohCurrentPath(routeDetails.selected_route);
      setConnectivityText("irohDialTarget", dialTarget, 46);
      setConnectivityText("irohRelayUrls", relayUrls.length ? relayUrls : "none", 46);
      setConnectivityText("irohRoutePolicy", "Iroh native direct-first", 46);
      setConnectivityText("irohFallback", "relay fallback managed by Iroh", 46);
      setConnectivityText("irohTransportDetail", "managed by Iroh", 46);
    }

	    function render(payload, rows, irohRouteRows = []) {
	      const snapshot = payload.snapshot || {};
	      const visible = safeArray(rows).filter(modeMatches);
      qs("generatedAt").textContent = `Refreshed ${formatTime(payload.generated_at || new Date().toISOString())}`;
      qs("serviceState").textContent = payload.network_service_started ? "running" : "stopped";
      qs("irohEndpoint").textContent = compact(snapshot.local_iroh_endpoint_id || "-", 24);
      qs("knownIrohContacts").textContent = String(snapshot.known_iroh_contacts || 0);
      qs("scopeCount").textContent = String(safeArray(snapshot.subscribed_scopes).length);
      qs("diagnosticCount").textContent = String(visible.length);
	      renderIrohConnectivity(snapshot, irohRouteRows);

      const list = qs("diagnosticsList");
      if (!visible.length) {
        list.innerHTML = `<div class="empty">No network diagnostics recorded for the current filters.</div>`;
        return;
      }
      rawJsonRows.clear();
      list.innerHTML = visible.map((row, index) => {
        const rawJsonKey = String(index);
        rawJsonRows.set(rawJsonKey, row);
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
              <div class="row-title">${escapeHtml(conciseTitle(row))}</div>
              <div class="pills">
                <span class="pill ${escapeHtml(String(row.level || "").toLowerCase())}">${escapeHtml(row.level || "info")}</span>
                <span class="pill ${escapeHtml(String(status).toLowerCase())}">${escapeHtml(status)}</span>
              </div>
            </div>
            <div class="meta"><span>${escapeHtml(formatTime(row.timestamp_ms || row.timestamp))}</span>${meta.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}</div>
            <details data-raw-json-key="${escapeHtml(rawJsonKey)}" ontoggle="renderRawJsonDetails(this)">
              <summary>Raw JSON</summary>
              <pre>Open to render formatted JSON.</pre>
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

    initThemePicker();
    enhanceAllSelects();

    refreshDiagnostics().catch((error) => {
      qs("diagnosticsList").innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
    });
  </script>
</body>
</html>"#;
