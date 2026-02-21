pub const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>WattSwarm Kernel Console</title>
  <style>
    :root {
      --bg: #f0e8d8;
      --ink: #1e1a16;
      --card: #fff6e8;
      --accent: #d46a1f;
      --accent-2: #1f7fd4;
      --line: #2b2520;
      --ok: #2b8a3e;
      --warn: #a35b00;
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
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 420px;
      gap: 14px;
      align-items: start;
    }
    .head {
      border: 3px solid var(--line);
      background: var(--card);
      box-shadow: 8px 8px 0 var(--line);
      padding: 16px;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      letter-spacing: 1px;
      text-transform: uppercase;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(310px, 1fr));
      gap: 14px;
    }
    .card {
      border: 3px solid var(--line);
      background: var(--card);
      box-shadow: 6px 6px 0 var(--line);
      padding: 12px;
    }
    h2 {
      margin: 0 0 8px;
      font-size: 14px;
      text-transform: uppercase;
      color: var(--accent);
    }
    .row {
      display: flex;
      gap: 8px;
      margin: 8px 0;
      flex-wrap: wrap;
    }
    input, textarea {
      width: 100%;
      border: 2px solid var(--line);
      background: #fff;
      color: var(--ink);
      font: inherit;
      padding: 8px;
      border-radius: 0;
    }
    textarea {
      min-height: 160px;
    }
    select {
      width: 100%;
      border: 2px solid var(--line);
      background: #fff;
      color: var(--ink);
      font: inherit;
      padding: 8px;
      border-radius: 0;
    }
    button {
      border: 2px solid var(--line);
      background: var(--accent);
      color: #fff;
      font: inherit;
      padding: 8px 10px;
      cursor: pointer;
      text-transform: uppercase;
      letter-spacing: .4px;
    }
    button.alt { background: var(--accent-2); }
    button.ghost { background: #efe2cf; color: var(--ink); }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      max-height: 360px;
      overflow: auto;
    }
    .hint { color: #5f5549; font-size: 12px; margin-top: 6px; }
    .help-list {
      margin: 0;
      padding-left: 18px;
      line-height: 1.6;
      font-size: 12px;
      color: #4d443a;
    }
    .help-list b { color: #2b2520; }
    details {
      border: 2px solid var(--line);
      background: #f7eee0;
      padding: 8px;
      margin-top: 8px;
    }
    summary {
      cursor: pointer;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .4px;
      color: #3b342c;
    }
    .code-sample {
      margin-top: 8px;
      font-size: 11px;
      max-height: 180px;
      overflow: auto;
      background: #fff;
      border: 1px solid #8c7f71;
      padding: 8px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .status-pill {
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
    .status-pill.ok { background: #cfe8d4; color: var(--ok); }
    .status-pill.warn { background: #efe1cc; color: var(--warn); }
    .status-pill.err { background: #f5d5d5; color: #9f1f1f; }
    .status-pill.run { background: #d4e4f7; color: #1f4e8b; }
    .monitor {
      position: sticky;
      top: 50px;
      max-height: calc(100vh - 70px);
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .monitor-status {
      border: 2px solid var(--line);
      background: #f7eee0;
      padding: 8px;
      display: grid;
      gap: 6px;
    }
    .status-row {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-size: 12px;
      text-transform: uppercase;
    }
    .status-row span {
      color: #655c52;
    }
    .status-row code {
      font: inherit;
      background: #fff;
      border: 1px solid #8c7f71;
      padding: 0 4px;
      max-width: 230px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .monitor-block {
      border: 2px solid var(--line);
      background: #fff;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    .monitor-label {
      padding: 6px 8px;
      background: #efe2cf;
      border-bottom: 2px solid var(--line);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .4px;
      color: #3f3830;
    }
    .monitor-pre {
      flex: 1;
      padding: 8px;
      font-size: 12px;
      min-height: 180px;
      max-height: 320px;
      overflow: auto;
      background: #fffdf8;
    }
    .monitor-pre.compact {
      min-height: 220px;
      max-height: 420px;
      background: #fff;
    }
    @media (max-width: 1200px) {
      .layout {
        grid-template-columns: minmax(0, 1fr);
      }
      .monitor {
        position: static;
        max-height: none;
      }
      .monitor-pre, .monitor-pre.compact {
        max-height: 280px;
      }
    }
  </style>
</head>
<body>
  <div id="statusPill" class="status-pill">idle</div>
  <div class="wrap">
    <div class="head">
      <h1>WattSwarm Kernel Console</h1>
      <div class="hint">2D block control panel for kernel configuration, operations, and monitoring.</div>
    </div>

    <div class="layout">
      <div class="grid">
        <div class="card">
          <h2>Node</h2>
          <div class="row">
            <button onclick="api('POST','/api/node/up')">up</button>
            <button onclick="api('POST','/api/node/down')" class="ghost">down</button>
            <button onclick="api('GET','/api/node/status')" class="alt">status</button>
          </div>
        </div>

        <div class="card">
          <h2>Peers + Log</h2>
          <div class="row">
            <button onclick="api('GET','/api/peers/list')">peers list</button>
            <button onclick="api('GET','/api/log/head')" class="alt">log head</button>
          </div>
          <div class="row">
            <button onclick="api('POST','/api/log/replay')">log replay</button>
            <button onclick="api('POST','/api/log/verify')" class="ghost">log verify</button>
          </div>
        </div>

        <div class="card">
          <h2>Executors</h2>
          <input id="execName" placeholder="name (e.g. rt)" />
          <div class="row">
            <input id="execUrl" placeholder="base_url (e.g. http://127.0.0.1:8787)" />
          </div>
          <div class="row">
            <button onclick="addExecutor()">add</button>
            <button onclick="api('GET','/api/executors/list')" class="alt">list</button>
            <button onclick="checkExecutor()" class="ghost">check</button>
          </div>
        </div>

        <div class="card">
          <h2>Task</h2>
          <div class="row">
            <input id="sampleTaskId" placeholder="sample task_id" value="task-ui-1"/>
            <button onclick="loadSample()" class="alt">sample</button>
          </div>
          <div class="row">
            <select id="taskPreset">
              <option value="swarm">preset: swarm</option>
              <option value="arbitrage">preset: arbitrage</option>
              <option value="security">preset: security</option>
            </select>
            <button onclick="applyPreset()" class="ghost">apply preset</button>
          </div>
          <textarea id="taskJson" placeholder="TaskContract JSON"></textarea>
          <div class="hint">Required: protocol_version, task_id, task_type, inputs, output_schema, budget, assignment, acceptance, expiry_ms, evidence_policy.</div>
          <div class="row">
            <button onclick="submitTask()">submit</button>
            <input id="taskIdWatch" placeholder="task_id for watch/decision/run-real" value="task-ui-1"/>
          </div>
          <div class="row">
            <button onclick="watchTask()" class="alt">watch</button>
            <button onclick="decisionTask()" class="ghost">decision</button>
          </div>
          <div class="row">
            <input id="runExecutor" placeholder="executor name" value="rt"/>
            <input id="runProfile" placeholder="profile" value="default"/>
            <button onclick="runReal()">run-real</button>
          </div>
        </div>

        <div class="card">
          <h2>Knowledge</h2>
          <div class="row">
            <input id="knowledgeTaskId" placeholder="task_id"/>
            <button onclick="knowledgeByTaskId()">export by task_id</button>
          </div>
          <div class="row">
            <input id="knowledgeTaskType" placeholder="task_type"/>
            <button onclick="knowledgeByTaskType()" class="alt">export by task_type</button>
          </div>
        </div>

        <div class="card">
          <h2>Quick Start</h2>
          <ol class="help-list">
            <li><b>Node</b>: click <code>UP</code>.</li>
            <li><b>Executor</b>: fill <code>name/base_url</code>, click <code>ADD</code>, then <code>CHECK</code>.</li>
            <li><b>Task</b>: click <code>SAMPLE</code> (or set preset), then <code>SUBMIT</code>.</li>
            <li><b>Run</b>: click <code>RUN-REAL</code>, then use <code>WATCH</code>/<code>DECISION</code>.</li>
            <li><b>Knowledge</b>: export by <code>task_id</code> or <code>task_type</code> for replay/audit.</li>
          </ol>
          <details>
            <summary>TaskContract Minimal Reference</summary>
            <pre class="code-sample">{
  "protocol_version": "v0.1",
  "task_id": "task-ui-1",
  "task_type": "swarm",
  "inputs": { "prompt": "hello" },
  "output_schema": { "type": "object", "required": ["answer"] },
  "budget": { "time_ms": 30000, "max_steps": 10, "cost_units": 1000 },
  "assignment": { "mode": "CLAIM" },
  "acceptance": { "quorum_threshold": 1, "vote": { "commit_reveal": true } },
  "expiry_ms": 1730000000000,
  "evidence_policy": { "max_inline_evidence_bytes": 65536, "max_inline_media_bytes": 0 }
}</pre>
          </details>
        </div>
      </div>

      <div class="card monitor">
        <h2>Live Monitor</h2>
        <div class="monitor-status">
          <div class="status-row"><span>state</span><code id="monitorState">idle</code></div>
          <div class="status-row"><span>last op</span><code id="lastOp">none</code></div>
          <div class="status-row"><span>http</span><code id="lastHttp">-</code></div>
        </div>
        <div class="monitor-block">
          <div class="monitor-label">Latest Response</div>
          <pre id="responseOut" class="monitor-pre">Ready.</pre>
        </div>
        <div class="monitor-block">
          <div class="monitor-label">Event Stream</div>
          <pre id="eventOut" class="monitor-pre compact"></pre>
        </div>
      </div>
    </div>
  </div>
  <script>
    const responseOut = document.getElementById("responseOut");
    const eventOut = document.getElementById("eventOut");
    const statusPill = document.getElementById("statusPill");
    const monitorState = document.getElementById("monitorState");
    const lastOp = document.getElementById("lastOp");
    const lastHttp = document.getElementById("lastHttp");
    let eventLines = [];

    function setStatus(kind, text) {
      statusPill.className = `status-pill ${kind || ""}`.trim();
      statusPill.textContent = text;
      monitorState.textContent = text;
    }
    function pushEvent(line) {
      const now = new Date().toLocaleTimeString();
      eventLines.unshift(`[${now}] ${line}`);
      eventLines = eventLines.slice(0, 80);
      eventOut.textContent = eventLines.join("\n");
    }
    function showResponse(value) {
      if (typeof value === "string") {
        responseOut.textContent = value;
      } else {
        responseOut.textContent = JSON.stringify(value, null, 2);
      }
    }
    setStatus("", "js-ready");
    pushEvent("ready");

    async function api(method, url, body) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 12000);
      setStatus("run", `${method} ${url}`);
      lastOp.textContent = `${method} ${url}`;
      lastHttp.textContent = "...";
      pushEvent(`request -> ${method} ${url}`);
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: body ? JSON.stringify(body) : undefined,
          signal: controller.signal
        });
        const txt = await res.text();
        try {
          showResponse(JSON.parse(txt));
        } catch (_) {
          showResponse(txt);
        }
        lastHttp.textContent = String(res.status);
        setStatus("ok", `${res.status} ${method}`);
        pushEvent(`response <- ${res.status} ${method} ${url}`);
      } catch (err) {
        showResponse({
          ok: false,
          error: String(err)
        });
        lastHttp.textContent = "ERR";
        setStatus("err", "request-failed");
        pushEvent(`error -> ${String(err)}`);
      } finally {
        clearTimeout(timer);
      }
    }
    async function loadSample() {
      try {
        setStatus("run", "GET /api/task/sample");
        const taskId = document.getElementById("sampleTaskId").value;
        const url = `/api/task/sample?task_id=${encodeURIComponent(taskId)}`;
        lastOp.textContent = `GET ${url}`;
        lastHttp.textContent = "...";
        pushEvent(`request -> GET ${url}`);
        const res = await fetch(url);
        const data = await res.json();
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`);
        document.getElementById("taskJson").value = JSON.stringify(data.contract, null, 2);
        showResponse(data);
        document.getElementById("taskIdWatch").value = taskId;
        lastHttp.textContent = String(res.status);
        setStatus("ok", "sample-loaded");
        pushEvent(`response <- ${res.status} GET /api/task/sample`);
      } catch (err) {
        showResponse({ ok: false, error: String(err) });
        lastHttp.textContent = "ERR";
        setStatus("err", "sample-failed");
        pushEvent(`error -> ${String(err)}`);
      }
    }
    function applyPreset() {
      const preset = document.getElementById("taskPreset").value;
      const txt = document.getElementById("taskJson").value.trim();
      if (!txt) {
        setStatus("warn", "load sample first");
        pushEvent("hint -> click SAMPLE before applying preset");
        return;
      }
      try {
        const task = JSON.parse(txt);
        const taskId = document.getElementById("sampleTaskId").value || task.task_id || "task-ui-1";
        task.task_id = taskId;
        if (!task.inputs || typeof task.inputs !== "object") task.inputs = {};
        if (preset === "arbitrage") {
          task.task_type = "arbitrage";
          task.inputs.prompt = "find low-risk arbitrage opportunity";
          task.inputs.market = "SOL/USDC";
        } else if (preset === "security") {
          task.task_type = "security";
          task.inputs.prompt = "check smart contract risk";
          task.inputs.contract = "0x1234...abcd";
        } else {
          task.task_type = "swarm";
          task.inputs.prompt = "hello";
        }
        document.getElementById("taskJson").value = JSON.stringify(task, null, 2);
        document.getElementById("taskIdWatch").value = taskId;
        setStatus("ok", `preset-${preset}`);
        pushEvent(`preset applied -> ${preset}`);
      } catch (err) {
        showResponse({ ok: false, error: String(err) });
        setStatus("err", "preset-failed");
        pushEvent(`error -> ${String(err)}`);
      }
    }
    function submitTask() {
      const txt = document.getElementById("taskJson").value.trim();
      if (!txt) {
        setStatus("warn", "task-json-empty");
        pushEvent("hint -> task json is empty");
        return;
      }
      try {
        api("POST", "/api/task/submit", { contract: JSON.parse(txt) });
      } catch (err) {
        showResponse({ ok: false, error: String(err) });
        setStatus("err", "task-json-invalid");
        pushEvent(`error -> invalid task json (${String(err)})`);
      }
    }
    function watchTask() {
      const taskId = document.getElementById("taskIdWatch").value;
      api("GET", `/api/task/watch/${encodeURIComponent(taskId)}`);
    }
    function decisionTask() {
      const taskId = document.getElementById("taskIdWatch").value;
      api("GET", `/api/task/decision/${encodeURIComponent(taskId)}`);
    }
    function addExecutor() {
      api("POST", "/api/executors/add", {
        name: document.getElementById("execName").value,
        base_url: document.getElementById("execUrl").value
      });
    }
    function checkExecutor() {
      api("POST", "/api/executors/check", { name: document.getElementById("execName").value });
    }
    function runReal() {
      api("POST", "/api/task/run-real", {
        executor: document.getElementById("runExecutor").value,
        profile: document.getElementById("runProfile").value,
        task_id: document.getElementById("taskIdWatch").value
      });
    }
    function knowledgeByTaskId() {
      api("POST", "/api/knowledge/export", { task_id: document.getElementById("knowledgeTaskId").value });
    }
    function knowledgeByTaskType() {
      api("POST", "/api/knowledge/export", { task_type: document.getElementById("knowledgeTaskType").value });
    }
  </script>
</body>
</html>"#;
