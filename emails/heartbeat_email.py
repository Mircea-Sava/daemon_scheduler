"""
Heartbeat Email Script
Sends a single heartbeat email via SMTP.
Scheduled by the sequencer (schedule.yaml).
"""

import smtplib
import sys
import socket
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_SETTINGS_FILE = _PROJECT_DIR / "settings.yaml"

_FOOTER = "<br><br><small>Sent automatically by the Sequencer.</small>"


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


def send_heartbeat(config: dict) -> None:
    """Send a heartbeat email via SMTP."""
    smtp_server = config.get("smtp_server", "")
    from_addr = config.get("from", "")
    to_addr = config.get("to", "")
    cc_addr = config.get("cc", "")

    if not smtp_server or not to_addr:
        print("[ERROR] Missing smtp_server or to in settings.yaml email config.", file=sys.stderr)
        sys.exit(1)

    now = datetime.now()
    hostname = socket.gethostname()
    subject = f"Heartbeat | {hostname} | {now:%Y-%m-%d %H:%M:%S}"

    html = (
        "<br>Hello,"
        "<br><br>"
        f"Heartbeat from: <b>{hostname}</b><br>"
        f"Timestamp: {now:%Y-%m-%d %H:%M:%S}<br>"
        f"Status: <b>OK</b><br>"
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
        print(f"[{now:%H:%M:%S}] Heartbeat sent to {to_addr}")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cfg = _load_email_config()
    send_heartbeat(cfg)
