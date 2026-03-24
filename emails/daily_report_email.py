"""
Daily Report Email Script
Reads sequencer_state.json and schedule.yaml to build a full status report:
  - Successful tasks (with running time)
  - Failed tasks (with running time)
  - Missed tasks (scheduled but never ran)
  - Paused tasks
Sends the report via SMTP.  If nothing ran and nothing was missed, no email is sent.
Scheduled by the sequencer (schedule.yaml).
"""

import json
import smtplib
import sys
import socket
import datetime as dt
from email.message import EmailMessage
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_SETTINGS_FILE = _PROJECT_DIR / "settings.yaml"
_STATE_FILE = _PROJECT_DIR / "sequencer_state.json"
_SCHEDULE_FILE = _PROJECT_DIR / "schedule.yaml"

_FOOTER = "<br><br><small>Sent automatically by the Sequencer.</small>"

# Task IDs that belong to the email scripts themselves — exclude from "missed" checks
_EMAIL_TASK_IDS: set[str] = set()


def _load_email_config() -> dict:
    """Load email settings from settings.yaml."""
    try:
        libs_dir = _PROJECT_DIR / "libs"
        if libs_dir.is_dir():
            for p in sorted(libs_dir.iterdir()):
                if p.is_dir() and str(p) not in sys.path:
                    sys.path.insert(0, str(p))
        import yaml

        data = yaml.safe_load(_SETTINGS_FILE.read_text(encoding="utf-8")) or {}
        return (data.get("settings") or {}).get("email") or {}
    except Exception as exc:
        print(f"[ERROR] Cannot load {_SETTINGS_FILE}: {exc}", file=sys.stderr)
        sys.exit(1)


def _load_state() -> dict:
    if not _STATE_FILE.exists():
        return {}
    with _STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_dt(s: str) -> dt.datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return dt.datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _running_time(slot: str, last_run: str) -> str:
    """Compute HH:MM:SS between slot (start) and last_run (end)."""
    slot_dt = _parse_dt(slot)
    last_dt = _parse_dt(last_run)
    if slot_dt is None or last_dt is None:
        return "N/A"
    total = int((last_dt - slot_dt).total_seconds())
    if total < 0:
        return "N/A"
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _get_tasks_by_outcome(state: dict) -> tuple[list[dict], list[dict]]:
    """Return (successful, failed) task lists from state."""
    successful, failed = [], []
    for task_id, info in state.get("last_triggered_slot", {}).items():
        entry = {
            "task": task_id,
            "slot": info.get("slot", "N/A"),
            "last_run": info.get("last_run", "N/A"),
            "outcome": info.get("outcome", "unknown"),
        }
        if entry["outcome"] == "success":
            successful.append(entry)
        else:
            failed.append(entry)
    return successful, failed


def _get_missed_tasks(
    state: dict, successful: list[dict], failed: list[dict], now: dt.datetime,
) -> list[dict]:
    """Find tasks scheduled to run today (before *now*) that never fired.

    Imports should_run / validate_task from the sequencer so the logic
    stays in sync automatically — no reimplementation needed.
    """
    # Import sequencer functions (add project dir to path if needed)
    if str(_PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(_PROJECT_DIR))
    from sequencer import load_config, validate_task, should_run

    try:
        cfg = load_config(_SCHEDULE_FILE)
    except Exception:
        return []

    raw_tasks = cfg.get("tasks") or []
    validated = []
    for i, rt in enumerate(raw_tasks, 1):
        try:
            validated.append(validate_task(rt, i))
        except ValueError:
            continue

    ran_ids = {t["task"] for t in successful} | {t["task"] for t in failed}

    # Auto-detect email script IDs from schedule (tasks whose path starts with "emails/")
    email_ids = set(_EMAIL_TASK_IDS)
    for t in validated:
        path = t.get("path", "")
        if path.startswith("emails/") or path.startswith("emails\\"):
            email_ids.add(t["id"])

    # Scan every minute from midnight to now — did should_run() fire?
    since = now.replace(hour=0, minute=0, second=0, microsecond=0)
    missed = []
    for task in validated:
        task_id = task["id"]
        if task_id in email_ids or task_id in ran_ids:
            continue

        # Walk minute-by-minute and check if this task was supposed to fire
        expected = False
        first_fire = None
        cursor = since
        while cursor < now:
            if should_run(task, cursor):
                expected = True
                if first_fire is None:
                    first_fire = cursor
            cursor += dt.timedelta(minutes=1)

        if expected:
            missed.append({
                "task": task_id,
                "path": task.get("path", "N/A"),
                "expected_time": first_fire.strftime("%H:%M") if first_fire else "N/A",
            })

    return missed


def _get_paused_tasks(state: dict) -> list[str]:
    return state.get("paused_tasks", [])


# ── HTML table builders ───────────────────────────────────────────

def _table(headers: list[str], rows: list[list[str]], header_color: str) -> str:
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
        for row in rows
    )
    return (
        f"<table border='1' cellpadding='5' cellspacing='0'>"
        f"<tr style='background-color:{header_color};'>{head}</tr>"
        f"{body}</table>"
    )


def _build_html(
    successful: list[dict],
    failed: list[dict],
    missed: list[dict],
    paused: list[str],
    hostname: str,
    now: dt.datetime,
) -> str:
    parts: list[str] = []

    parts.append(
        "<br>Hello,"
        "<br><br>"
        f"Daily status report from: <b>{hostname}</b><br>"
        f"Timestamp: {now:%Y-%m-%d %H:%M:%S}<br><br>"
        f"Successful: {len(successful)} &nbsp;|&nbsp; "
        f"Failed: {len(failed)} &nbsp;|&nbsp; "
        f"Missed: {len(missed)} &nbsp;|&nbsp; "
        f"Paused: {len(paused)}<br>"
    )

    # ── Successful ─────────────────────────────────────────────
    if successful:
        rows = [
            [t["task"], t["slot"], t["last_run"], _running_time(t["slot"], t["last_run"])]
            for t in successful
        ]
        parts.append(
            "<br><h3 style='color:#2e7d32;'>&#10003; Successful Tasks</h3>"
            + _table(["Task", "Slot", "Last Run", "Duration"], rows, "#c8e6c9")
        )
    else:
        parts.append(
            "<br><h3 style='color:#888;'>No successful tasks recorded yet</h3>"
        )

    # ── Failed ─────────────────────────────────────────────────
    if failed:
        rows = [
            [t["task"], t["outcome"], t["slot"], t["last_run"],
             _running_time(t["slot"], t["last_run"])]
            for t in failed
        ]
        parts.append(
            "<br><h3 style='color:#e65100;'>&#10007; Failed Tasks</h3>"
            + _table(["Task", "Outcome", "Slot", "Last Run", "Duration"], rows, "#ffe0b2")
        )
    else:
        parts.append(
            "<br><h3 style='color:#2e7d32;'>&#10003; No failed tasks</h3>"
        )

    # ── Missed ─────────────────────────────────────────────────
    if missed:
        rows = [[m["task"], m["expected_time"], m["path"]] for m in missed]
        parts.append(
            "<br><h3 style='color:#c62828;'>&#9888; Scripts That Did Not Run</h3>"
            "<p style='color:#555;font-size:13px;'>"
            "Scheduled to run today before this report but have no execution recorded.</p>"
            + _table(["Task ID", "First Expected", "Script Path"], rows, "#ffcdd2")
        )
    else:
        parts.append(
            "<br><h3 style='color:#2e7d32;'>&#10003; No missed scripts</h3>"
        )

    # ── Paused ─────────────────────────────────────────────────
    if paused:
        rows = [[p] for p in paused]
        parts.append(
            "<br><h3 style='color:#1565c0;'>&#9208; Paused Tasks</h3>"
            + _table(["Task ID"], rows, "#bbdefb")
        )

    parts.append(_FOOTER)
    return "".join(parts)


def send_report(config: dict) -> None:
    smtp_server = config.get("smtp_server", "")
    from_addr = config.get("from", "")
    to_addr = config.get("to", "")
    cc_addr = config.get("cc", "")

    if not smtp_server or not to_addr:
        print("[ERROR] Missing smtp_server or to in settings.yaml email config.", file=sys.stderr)
        sys.exit(1)

    state = _load_state()
    now = dt.datetime.now()
    hostname = socket.gethostname()

    successful, failed = _get_tasks_by_outcome(state)
    missed = _get_missed_tasks(state, successful, failed, now)
    paused = _get_paused_tasks(state)

    # Nothing to report — skip email
    if not successful and not failed and not missed:
        print(f"[{now:%H:%M:%S}] Nothing to report. No email sent.")
        return

    subject = (
        f"Sequencer Daily Report | {hostname} | {now:%Y-%m-%d %H:%M:%S}"
    )
    html = _build_html(successful, failed, missed, paused, hostname, now)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    if cc_addr:
        msg["Cc"] = cc_addr
    msg.add_alternative(html, subtype="html")

    port = int(config.get("smtp_port", 25))
    username = config.get("username", "")
    password = config.get("password", "")

    try:
        with smtplib.SMTP(smtp_server, port) as smtp:
            if username and password:
                smtp.starttls()
                smtp.login(username, password)
            smtp.send_message(msg)
        print(
            f"[{now:%H:%M:%S}] Daily report sent to {to_addr} "
            f"({len(successful)} ok, {len(failed)} failed, "
            f"{len(missed)} missed, {len(paused)} paused)"
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cfg = _load_email_config()
    send_report(cfg)
