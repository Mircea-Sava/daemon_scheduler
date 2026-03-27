"""Web-based Sequencer Monitor – Databricks Apps edition.

Reads scheduler state from a UC Volume (pushed by the local sequencer daemon).
Sends commands (pause, resume, run, pull, push) by writing command files to the
same volume — the sequencer polls and processes them.

Falls back to a local ``sequencer_state.json`` during development.
"""
from __future__ import annotations

import io
import json
import os
import datetime as dt
from functools import lru_cache
from pathlib import Path

from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

_APP_DIR = Path(__file__).resolve().parent

VOLUME_BASE = os.environ.get(
    "STATE_VOLUME_BASE",
    "/Volumes/team_aftermarket_mro/mro_all/scheduler",
)
VOLUME_STATE = f"{VOLUME_BASE}/sequencer_state.json"
VOLUME_COMMANDS = f"{VOLUME_BASE}/commands"

STATE_FILE = _APP_DIR / "sequencer_state.json"
SETTINGS_FILE = _APP_DIR / "settings.yaml"


# ── data loading ─────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _ws_client():
    """Lazy-init WorkspaceClient (auto-authenticates in Databricks Apps)."""
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except Exception:
        return None


def load_state() -> tuple[dict, str]:
    """Read state from UC Volume (production) or local file (dev).

    Returns (state_dict, source_label).
    """
    ws = _ws_client()
    if ws is not None:
        try:
            resp = ws.files.download(VOLUME_STATE)
            raw = resp.contents.read()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, dict) and data:
                return data, "volume"
        except Exception:
            pass
    # Fallback: local file
    if STATE_FILE.exists():
        try:
            text = STATE_FILE.read_text(encoding="utf-8").strip()
            if text:
                return json.loads(text), "local"
        except (json.JSONDecodeError, OSError):
            pass
    return {}, "local"


def load_local_settings() -> dict:
    """Load settings.yaml from app directory (dev fallback)."""
    try:
        import yaml
    except ImportError:
        return {}
    if not SETTINGS_FILE.exists():
        return {}
    try:
        raw = yaml.safe_load(SETTINGS_FILE.read_text(encoding="utf-8")) or {}
        return raw.get("settings", raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def format_countdown(last_time_str: str | None, interval_minutes: int) -> str:
    if not last_time_str or interval_minutes <= 0:
        return ""
    try:
        last = dt.datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
        remaining = (last + dt.timedelta(minutes=interval_minutes) - dt.datetime.now()).total_seconds()
        if remaining <= 0:
            return "due now"
        m, s = divmod(int(remaining), 60)
        return f"{m}m {s:02d}s"
    except (ValueError, TypeError):
        return ""


def write_command(filename: str) -> bool:
    """Write a command file to the UC Volume commands directory."""
    ws = _ws_client()
    if ws is None:
        return False
    try:
        ws.files.upload(f"{VOLUME_COMMANDS}/{filename}", io.BytesIO(b""), overwrite=True)
        return True
    except Exception:
        return False


# ── dashboard HTML ───────────────────────────────────────────────────────

DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sequencer Monitor</title>
<style>
  :root {
    --bg: #1a1a2e;
    --surface: #16213e;
    --border: #0f3460;
    --text: #e0e0e0;
    --accent: #00d4ff;
    --green: #00e676;
    --red: #ff5252;
    --yellow: #ffab40;
    --magenta: #e040fb;
    --dim: #666;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
    background: var(--bg);
    color: var(--text);
    padding: 20px;
    min-height: 100vh;
  }
  .header {
    background: linear-gradient(135deg, #0f3460, #533483);
    padding: 16px 24px;
    border-radius: 8px;
    margin-bottom: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .header h1 { font-size: 1.3rem; color: #fff; }
  .header .meta { font-size: 0.85rem; color: #aaa; }
  .source-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: bold;
    margin-left: 10px;
    text-transform: uppercase;
  }
  .source-volume { background: #00695c; color: #a7ffeb; }
  .source-local  { background: #4a148c; color: #ea80fc; }
  .panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    margin-bottom: 16px;
    overflow: hidden;
  }
  .panel-title {
    background: var(--border);
    padding: 8px 16px;
    font-size: 0.9rem;
    font-weight: bold;
    color: var(--accent);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th {
    padding: 8px 12px;
    text-align: left;
    font-size: 0.8rem;
    color: var(--accent);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  td {
    padding: 8px 12px;
    font-size: 0.85rem;
    border-bottom: 1px solid rgba(255,255,255,0.05);
  }
  tr:hover { background: rgba(0,212,255,0.05); }
  .status-running { color: var(--yellow); font-weight: bold; }
  .status-paused  { color: var(--magenta); font-weight: bold; }
  .status-idle    { color: var(--dim); }
  .outcome-success { color: var(--green); font-weight: bold; }
  .outcome-failure { color: var(--red); font-weight: bold; }
  .outcome-skipped { color: var(--yellow); }
  .outcome-none    { color: var(--dim); }
  .git-panel-body { padding: 12px 16px; line-height: 1.8; }
  .git-label { color: var(--accent); }
  .pending { color: var(--yellow); font-weight: bold; }
  .no-pending { color: var(--dim); }
  .refresh-note {
    text-align: center;
    color: var(--dim);
    font-size: 0.75rem;
    margin-top: 10px;
  }
  .empty-row { color: var(--dim); font-style: italic; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }

  /* Action buttons */
  .btn {
    padding: 3px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    font-family: inherit;
    font-size: 0.75rem;
    cursor: pointer;
    background: var(--surface);
    color: var(--text);
    transition: background 0.15s, border-color 0.15s;
    margin-right: 4px;
  }
  .btn:hover { border-color: var(--accent); background: rgba(0,212,255,0.1); }
  .btn-pause  { color: var(--magenta); border-color: rgba(224,64,251,0.3); }
  .btn-pause:hover  { background: rgba(224,64,251,0.15); }
  .btn-resume { color: var(--green); border-color: rgba(0,230,118,0.3); }
  .btn-resume:hover { background: rgba(0,230,118,0.15); }
  .btn-run    { color: var(--yellow); border-color: rgba(255,171,64,0.3); }
  .btn-run:hover    { background: rgba(255,171,64,0.15); }
  .btn-git    { color: var(--accent); border-color: rgba(0,212,255,0.3); }
  .btn-git:hover    { background: rgba(0,212,255,0.15); }
  .btn:disabled { opacity: 0.3; cursor: not-allowed; }
  .toast {
    position: fixed; bottom: 20px; right: 20px;
    background: var(--border); color: var(--accent);
    padding: 10px 20px; border-radius: 6px;
    font-size: 0.85rem; opacity: 0;
    transition: opacity 0.3s;
    pointer-events: none;
    z-index: 100;
  }
  .toast.show { opacity: 1; }

  /* Logs panel */
  .logs-body {
    padding: 8px 16px;
    max-height: 300px;
    overflow-y: auto;
    font-size: 0.78rem;
    line-height: 1.5;
  }
  .log-line { color: var(--dim); white-space: pre-wrap; word-break: break-all; }
  .log-line:hover { color: var(--text); }
</style>
</head>
<body>

<div class="header">
  <h1>Sequencer Monitor
    <span class="source-badge source-{{ source }}">{{ source }}</span>
  </h1>
  <div class="meta">{{ now }} &nbsp;|&nbsp; Auto-refresh: 5s</div>
</div>

<!-- Task Status -->
<div class="panel">
  <div class="panel-title">
    <span>Task Status ({{ tasks|length }} tasks)</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Task ID</th><th>Status</th><th>Last Slot</th>
        <th>Outcome</th><th>Last Run</th><th>Actions</th>
      </tr>
    </thead>
    <tbody>
    {% if not tasks %}
      <tr><td class="empty-row" colspan="6">No tasks found &mdash; waiting for scheduler data</td></tr>
    {% endif %}
    {% for t in tasks %}
      <tr>
        <td>{{ t.id }}</td>
        <td class="{{ t.status_class }}">{{ t.status }}</td>
        <td>{{ t.slot }}</td>
        <td class="{{ t.outcome_class }}">{{ t.outcome }}</td>
        <td>{{ t.last_run }}</td>
        <td>
          {% if t.status == 'PAUSED' %}
            <button class="btn btn-resume" onclick="sendCmd('unpause','{{ t.id }}')">Resume</button>
          {% elif t.status != 'RUNNING' %}
            <button class="btn btn-pause" onclick="sendCmd('pause','{{ t.id }}')">Pause</button>
          {% else %}
            <button class="btn btn-pause" disabled>Running</button>
          {% endif %}
          <button class="btn btn-run" onclick="sendCmd('run','{{ t.id }}')"
            {% if t.status == 'RUNNING' %}disabled{% endif %}>Run</button>
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</div>

<div class="grid">
  <!-- Profiling -->
  <div class="panel">
    <div class="panel-title">Profiling ({{ profiling|length }} entries)</div>
    <table>
      <thead>
        <tr><th>Task ID</th><th>Peak RAM %</th><th>Avg CPU %</th><th>Learned Cost</th></tr>
      </thead>
      <tbody>
      {% if not profiling %}
        <tr><td class="empty-row" colspan="4">No profiling data</td></tr>
      {% endif %}
      {% for p in profiling %}
        <tr>
          <td>{{ p.id }}</td>
          <td>{{ p.ram }}</td>
          <td>{{ p.cpu }}</td>
          <td>{{ p.cost }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <!-- Git Sync -->
  <div class="panel">
    <div class="panel-title">
      <span>Git Sync</span>
      <span>
        <button class="btn btn-git" onclick="sendCmd('pull')">Pull</button>
        <button class="btn btn-git" onclick="sendCmd('push')">Push</button>
      </span>
    </div>
    <div class="git-panel-body">
      <div><span class="git-label">Pull:</span> {{ git.pull_info }}</div>
      <div><span class="git-label">Push:</span> {{ git.push_info }}</div>
    </div>
  </div>
</div>

<!-- Recent Logs -->
<div class="panel">
  <div class="panel-title">Recent Logs (last {{ logs|length }} lines)</div>
  <div class="logs-body" id="logs-body">
    {% if not logs %}
      <div class="log-line empty-row">No log data available</div>
    {% endif %}
    {% for line in logs %}
      <div class="log-line">{{ line }}</div>
    {% endfor %}
  </div>
</div>

<div class="refresh-note">Page auto-refreshes every 5 seconds</div>
<div class="toast" id="toast"></div>

<script>
  // Smooth refresh: swap body content without full-page flash.
  async function refresh() {
    try {
      const resp = await fetch(window.location.href);
      if (resp.ok) {
        const html = await resp.text();
        const doc = new DOMParser().parseFromString(html, 'text/html');
        document.body.innerHTML = doc.body.innerHTML;
        // Auto-scroll logs to bottom
        const lb = document.getElementById('logs-body');
        if (lb) lb.scrollTop = lb.scrollHeight;
      }
    } catch (e) { /* retry next cycle */ }
    setTimeout(refresh, 5000);
  }
  setTimeout(refresh, 5000);

  // Auto-scroll logs on first load
  (function() {
    const lb = document.getElementById('logs-body');
    if (lb) lb.scrollTop = lb.scrollHeight;
  })();

  // Send command to sequencer via volume
  async function sendCmd(action, taskId) {
    const url = taskId
      ? '/api/cmd/' + action + '/' + taskId
      : '/api/cmd/' + action;
    try {
      const resp = await fetch(url, {method: 'POST'});
      const data = await resp.json();
      showToast(data.ok ? 'Command sent: ' + action : 'Failed: ' + (data.error || 'unknown'));
    } catch (e) {
      showToast('Error: ' + e.message);
    }
  }

  function showToast(msg) {
    const t = document.getElementById('toast');
    if (!t) return;
    t.textContent = msg;
    t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 2000);
  }
</script>

</body>
</html>
"""


# ── command API ──────────────────────────────────────────────────────────

@app.route("/api/cmd/<action>", methods=["POST"])
@app.route("/api/cmd/<action>/<task_id>", methods=["POST"])
def api_command(action: str, task_id: str | None = None):
    """Write a command file to the UC Volume for the sequencer to consume."""
    valid_actions = {"pause", "unpause", "run", "pull", "push"}
    if action not in valid_actions:
        return jsonify(ok=False, error=f"unknown action: {action}"), 400
    if action in ("pause", "unpause", "run") and not task_id:
        return jsonify(ok=False, error=f"{action} requires a task_id"), 400

    # Build filename: pause--task_id or pull
    filename = f"{action}--{task_id}" if task_id else action
    ok = write_command(filename)
    if ok:
        return jsonify(ok=True, action=action, task_id=task_id)
    return jsonify(ok=False, error="volume write failed"), 500


# ── dashboard route ──────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    state, source = load_state()

    # ---- Task rows ----
    last_slots = state.get("last_triggered_slot", {})
    in_progress = state.get("in_progress", {})
    paused_set = set(state.get("paused_tasks", []))

    all_keys = sorted(
        set(list(last_slots.keys()) + list(in_progress.keys())),
        key=lambda k: (
            0 if k in in_progress else
            1 if (last_slots.get(k, {}) or {}).get("outcome") == "failure" else
            2 if k in paused_set else 3,
            k,
        ),
    )

    tasks = []
    for key in all_keys:
        is_running = key in in_progress
        is_paused = key in paused_set
        entry = last_slots.get(key, {})
        if isinstance(entry, dict):
            slot = entry.get("slot", "")
            outcome_raw = entry.get("outcome", "")
            last_run = entry.get("last_run", "")
        else:
            slot, outcome_raw, last_run = str(entry), "", ""

        if is_running:
            status, status_class = "RUNNING", "status-running"
        elif is_paused:
            status, status_class = "PAUSED", "status-paused"
        else:
            status, status_class = "Idle", "status-idle"

        outcome_class = {
            "success": "outcome-success",
            "failure": "outcome-failure",
            "skipped": "outcome-skipped",
        }.get(outcome_raw, "outcome-none")

        tasks.append({
            "id": key,
            "status": status,
            "status_class": status_class,
            "slot": slot or "-",
            "outcome": outcome_raw or "-",
            "outcome_class": outcome_class,
            "last_run": last_run or "-",
        })

    # ---- Profiling rows ----
    profiling_data = state.get("profiling", {})
    profiling = []
    for key in sorted(k for k, v in profiling_data.items() if isinstance(v, dict)):
        e = profiling_data[key]
        profiling.append({
            "id": key,
            "ram": f"{e.get('peak_ram_pct', 0):.1f}%",
            "cpu": f"{e.get('avg_cpu_pct', 0):.1f}%",
            "cost": str(e.get("learned_cost", "?")),
        })

    # ---- Git sync info ----
    display = state.get("_display_settings")
    if not isinstance(display, dict) or not display:
        display = load_local_settings()

    pull_interval = display.get("git_pull_interval_minutes", 0)
    push_interval = display.get("git_push_interval_minutes", 0)
    daemon = state.get("daemon", {})

    if pull_interval:
        cd = format_countdown(daemon.get("last_pull_time"), pull_interval)
        pull_info = f"every {pull_interval} min" + (f"  \u2014  {cd}" if cd else "")
    else:
        pull_info = "disabled"

    if push_interval:
        cd = format_countdown(daemon.get("last_push_time"), push_interval)
        push_info = f"every {push_interval} min" + (f"  \u2014  {cd}" if cd else "")
    else:
        push_info = "disabled"

    git = {
        "pull_info": pull_info,
        "push_info": push_info,
    }

    # ---- Recent logs ----
    logs = state.get("_recent_log", [])

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return render_template_string(
        DASHBOARD_HTML,
        now=now, tasks=tasks, profiling=profiling, git=git,
        source=source, logs=logs,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
