"""Web-based Sequencer Monitor for Databricks Apps deployment."""

from __future__ import annotations

import json
import datetime as dt
from pathlib import Path

from flask import Flask, render_template_string

app = Flask(__name__)

# On Databricks, the state/settings files are synced alongside this app.
# Adjust these paths if your workspace layout differs.
_APP_DIR = Path(__file__).resolve().parent
STATE_FILE = _APP_DIR / "sequencer_state.json"
SETTINGS_FILE = _APP_DIR / "settings.yaml"


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def load_settings() -> dict:
    try:
        import yaml
    except ImportError:
        return {}
    if not SETTINGS_FILE.exists():
        return {}
    try:
        data = yaml.safe_load(SETTINGS_FILE.read_text(encoding="utf-8")) or {}
        return data.get("settings", data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def format_countdown(last_time_str: str | None, interval_minutes: int) -> str:
    if not last_time_str or interval_minutes <= 0:
        return ""
    try:
        last = dt.datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
        next_time = last + dt.timedelta(minutes=interval_minutes)
        remaining = (next_time - dt.datetime.now()).total_seconds()
        if remaining <= 0:
            return "due now"
        m, s = divmod(int(remaining), 60)
        return f"{m}m {s:02d}s"
    except (ValueError, TypeError):
        return ""


DASHBOARD_HTML = """
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
  .status-idle { color: var(--dim); }
  .outcome-success { color: var(--green); font-weight: bold; }
  .outcome-failure { color: var(--red); font-weight: bold; }
  .outcome-skipped { color: var(--yellow); }
  .outcome-none { color: var(--dim); }
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
</style>
</head>
<body>

<div class="header">
  <h1>Sequencer Monitor</h1>
  <div class="meta">{{ now }} &nbsp;|&nbsp; Auto-refresh: 5s</div>
</div>

<!-- Task Status -->
<div class="panel">
  <div class="panel-title">Task Status ({{ tasks|length }} tasks)</div>
  <table>
    <thead>
      <tr><th>Task ID</th><th>Status</th><th>Last Slot</th><th>Outcome</th><th>Last Run</th></tr>
    </thead>
    <tbody>
    {% if not tasks %}
      <tr><td class="empty-row" colspan="5">No tasks found</td></tr>
    {% endif %}
    {% for t in tasks %}
      <tr>
        <td>{{ t.id }}</td>
        <td class="{{ t.status_class }}">{{ t.status }}</td>
        <td>{{ t.slot }}</td>
        <td class="{{ t.outcome_class }}">{{ t.outcome }}</td>
        <td>{{ t.last_run }}</td>
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
    <div class="panel-title">Git Sync</div>
    <div class="git-panel-body">
      <div><span class="git-label">Pull:</span> {{ git.pull_info }}</div>
      <div><span class="git-label">Push:</span> {{ git.push_info }}</div>
      <br>
      {% if git.pull_pending %}
        <div class="pending">&gt;&gt; Pull trigger PENDING</div>
      {% endif %}
      {% if git.push_pending %}
        <div class="pending">&gt;&gt; Push trigger PENDING</div>
      {% endif %}
      {% if not git.pull_pending and not git.push_pending %}
        <div class="no-pending">No pending triggers</div>
      {% endif %}
    </div>
  </div>
</div>

<div class="refresh-note">Page auto-refreshes every 5 seconds</div>

<script>
  setTimeout(() => location.reload(), 5000);
</script>

</body>
</html>
"""


@app.route("/")
def dashboard():
    state = load_json(STATE_FILE)
    settings = load_settings()

    # Build task rows
    last_slots = state.get("last_triggered_slot", {})
    in_progress = state.get("in_progress", {})
    all_keys = sorted(
        set(list(last_slots.keys()) + list(in_progress.keys())),
        key=lambda k: (
            0 if (last_slots.get(k, {}) or {}).get("outcome") == "failure" else
            1 if k in in_progress else 2,
            k,
        ),
    )

    tasks = []
    for key in all_keys:
        is_running = key in in_progress
        entry = last_slots.get(key, {})
        if isinstance(entry, dict):
            slot = entry.get("slot", "")
            outcome_raw = entry.get("outcome", "")
            last_run = entry.get("last_run", "")
        else:
            slot = str(entry)
            outcome_raw = ""
            last_run = ""

        outcome_class = {
            "success": "outcome-success",
            "failure": "outcome-failure",
            "skipped": "outcome-skipped",
        }.get(outcome_raw, "outcome-none")

        tasks.append({
            "id": key,
            "status": "RUNNING" if is_running else "Idle",
            "status_class": "status-running" if is_running else "status-idle",
            "slot": slot or "-",
            "outcome": outcome_raw or "-",
            "outcome_class": outcome_class,
            "last_run": last_run or "-",
        })

    # Build profiling rows
    profiling_data = state.get("profiling", {})
    profiling = []
    for key in sorted(k for k, v in profiling_data.items() if isinstance(v, dict)):
        entry = profiling_data[key]
        profiling.append({
            "id": key,
            "ram": f"{entry.get('peak_ram_pct', 0):.1f}%",
            "cpu": f"{entry.get('avg_cpu_pct', 0):.1f}%",
            "cost": str(entry.get("learned_cost", "?")),
        })

    # Build git sync info
    pull_interval = settings.get("git_pull_interval_minutes", 0)
    push_interval = settings.get("git_push_interval_minutes", 0)
    daemon = state.get("daemon", {})

    if pull_interval:
        countdown = format_countdown(daemon.get("last_pull_time"), pull_interval)
        pull_info = f"every {pull_interval} min"
        if countdown:
            pull_info += f"  —  {countdown}"
    else:
        pull_info = "disabled"

    if push_interval:
        countdown = format_countdown(daemon.get("last_push_time"), push_interval)
        push_info = f"every {push_interval} min"
        if countdown:
            push_info += f"  —  {countdown}"
    else:
        push_info = "disabled"

    git_pull_trigger = _APP_DIR / ".git_pull_now"
    git_push_trigger = _APP_DIR / ".git_push_now"

    git = {
        "pull_info": pull_info,
        "push_info": push_info,
        "pull_pending": git_pull_trigger.exists(),
        "push_pending": git_push_trigger.exists(),
    }

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return render_template_string(
        DASHBOARD_HTML,
        now=now, tasks=tasks, profiling=profiling, git=git,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
