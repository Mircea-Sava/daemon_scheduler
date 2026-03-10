"""
Error Email Script
Reads sequencer_state.json for failed tasks and sends a summary email via SMTP.
If no failures are found, no email is sent.
Scheduled by the sequencer (schedule.yaml).
"""

import json
import smtplib
import sys
import socket
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_SETTINGS_FILE = _PROJECT_DIR / "settings.yaml"
_STATE_FILE = _PROJECT_DIR / "sequencer_state.json"

_FOOTER = "<br><br><small>Sent automatically by the Sequencer.</small>"


def _load_email_config() -> dict:
    """Load email settings from settings.yaml."""
    try:
        # Use the same yaml loader path as sequencer.py
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


def get_failed_tasks() -> list[dict]:
    """Read sequencer_state.json and return tasks whose outcome is not 'success'."""
    if not _STATE_FILE.exists():
        print(f"State file not found: {_STATE_FILE}")
        return []

    with _STATE_FILE.open("r", encoding="utf-8") as f:
        state = json.load(f)

    failed = []
    for task_id, info in state.get("last_triggered_slot", {}).items():
        outcome = info.get("outcome", "unknown")
        if outcome != "success":
            failed.append({
                "task": task_id,
                "slot": info.get("slot", "N/A"),
                "last_run": info.get("last_run", "N/A"),
                "outcome": outcome,
            })

    return failed


def send_error_email(failed_tasks: list[dict], config: dict) -> None:
    """Send a failure summary email via SMTP."""
    smtp_server = config.get("smtp_server", "")
    from_addr = config.get("from", "")
    to_addr = config.get("to", "")
    cc_addr = config.get("cc", "")

    if not smtp_server or not to_addr:
        print("[ERROR] Missing smtp_server or to in settings.yaml email config.", file=sys.stderr)
        sys.exit(1)

    now = datetime.now()
    hostname = socket.gethostname()
    subject = f"Sequencer Errors | {hostname} | {now:%Y-%m-%d %H:%M:%S}"

    rows = ""
    for t in failed_tasks:
        rows += (
            f"<tr><td>{t['task']}</td><td>{t['outcome']}</td>"
            f"<td>{t['slot']}</td><td>{t['last_run']}</td></tr>"
        )

    html = (
        "<br>Hello,"
        "<br><br>"
        f"Error report from: <b>{hostname}</b><br>"
        f"Timestamp: {now:%Y-%m-%d %H:%M:%S}<br>"
        f"Failed tasks: {len(failed_tasks)}<br><br>"
        "<table border='1' cellpadding='5' cellspacing='0'>"
        "<tr><th>Task</th><th>Outcome</th><th>Slot</th><th>Last Run</th></tr>"
        f"{rows}"
        "</table>"
        + _FOOTER
    )

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
        print(f"[{now:%H:%M:%S}] Error email sent to {to_addr} ({len(failed_tasks)} failed task(s))")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cfg = _load_email_config()
    failed = get_failed_tasks()
    if failed:
        send_error_email(failed, cfg)
    else:
        print(f"[{datetime.now():%H:%M:%S}] No failed tasks found. No email sent.")
