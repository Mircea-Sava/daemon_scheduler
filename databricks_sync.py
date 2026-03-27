"""Fire-and-forget push of scheduler state to a Databricks UC Volume.

Volume layout::

    /Volumes/team_aftermarket_mro/mro_all/scheduler/
        sequencer_state.json          ← state (pushed every ~3 s)
        logs/
            sequencer-2026-03-27.log  ← daily log file (pushed every ~30 s)
        commands/
            pause--<task_id>          ← written by app, consumed by sequencer
            unpause--<task_id>
            run--<task_id>
            pull
            push

Prerequisites – run once in a Databricks SQL editor::

    CREATE VOLUME IF NOT EXISTS team_aftermarket_mro.mro_all.scheduler;
    GRANT READ VOLUME, WRITE VOLUME
        ON VOLUME team_aftermarket_mro.mro_all.scheduler
        TO `app-4ft980`;

Requires DATABRICKS_HOST + DATABRICKS_TOKEN in the environment or .env file.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger(__name__)

_PROJECT_DIR = Path(__file__).resolve().parent
_ENV_FILE = _PROJECT_DIR / ".env"

# Volume paths (no leading slash for the REST API)
_VOLUME_BASE = "Volumes/team_aftermarket_mro/mro_all/scheduler"
_STATE_PATH = f"{_VOLUME_BASE}/sequencer_state.json"
_COMMANDS_DIR = f"{_VOLUME_BASE}/commands"
_LOGS_DIR = f"{_VOLUME_BASE}/logs"

# Debounce intervals
_STATE_DEBOUNCE_SEC = 3.0
_LOG_DEBOUNCE_SEC = 30.0

# State push
_state_lock = threading.Lock()
_last_state_push: float = 0.0

# Log push
_log_lock = threading.Lock()
_last_log_push: float = 0.0

# Credentials & callbacks
_host: str = ""
_token: str = ""
_initialized = False
_log_fn: Callable[[str], None] | None = None
_log_dir: Path | None = None
_cmd_callback: Callable[[str], None] | None = None


# ── helpers ──────────────────────────────────────────────────────────────

def _load_env_file() -> dict[str, str]:
    """Parse KEY=VALUE lines from .env (simple reader, no external deps)."""
    result: dict[str, str] = {}
    if not _ENV_FILE.exists():
        return result
    for raw in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        result[k.strip()] = v.strip().strip("\"'")
    return result


def _emit(msg: str) -> None:
    if _log_fn:
        _log_fn(msg)
    else:
        log.info(msg)


def _ssl_ctx() -> ssl.SSLContext:
    return ssl.create_default_context()


def _api_put(path: str, body: bytes, timeout: int = 10) -> bool:
    """PUT raw bytes to the Databricks Files API."""
    req = urllib.request.Request(
        f"{_host}/api/2.0/fs/files/{path}",
        data=body,
        method="PUT",
        headers={
            "Authorization": f"Bearer {_token}",
            "Content-Type": "application/octet-stream",
        },
    )
    with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=timeout):
        pass
    return True


def _api_get_json(path: str, timeout: int = 10) -> dict:
    """GET JSON from the Databricks API (for directory listings)."""
    req = urllib.request.Request(
        f"{_host}/api/2.0/fs/directories/{path}",
        headers={"Authorization": f"Bearer {_token}"},
    )
    with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=timeout) as resp:
        return json.loads(resp.read())


def _api_delete(path: str, timeout: int = 10) -> None:
    """DELETE a file from the Databricks Files API."""
    req = urllib.request.Request(
        f"{_host}/api/2.0/fs/files/{path}",
        method="DELETE",
        headers={"Authorization": f"Bearer {_token}"},
    )
    urllib.request.urlopen(req, context=_ssl_ctx(), timeout=timeout)


def _read_recent_log(n_lines: int = 50) -> list[str]:
    """Read the last *n_lines* from today's log file (fast tail read)."""
    if not _log_dir:
        return []
    day = dt.datetime.now().strftime("%Y-%m-%d")
    log_file = _log_dir / f"sequencer-{day}.log"
    try:
        if not log_file.exists() or log_file.stat().st_size == 0:
            return []
        with log_file.open("rb") as f:
            sz = f.seek(0, 2)
            f.seek(max(0, sz - 8192))  # read last ~8 KB
            tail = f.read().decode("utf-8", errors="replace")
        return tail.splitlines()[-n_lines:]
    except Exception:
        return []


# ── public API ───────────────────────────────────────────────────────────

def init(
    log_callback: Callable[[str], None] | None = None,
    log_dir: Path | None = None,
    cmd_callback: Callable[[str], None] | None = None,
) -> None:
    """Resolve Databricks credentials.  Call once at startup.

    *cmd_callback* receives command strings (e.g. ``"pause:task_id"``)
    discovered while pushing state — no dedicated polling loop needed.
    """
    global _host, _token, _initialized, _log_fn, _log_dir, _cmd_callback
    _log_fn = log_callback
    _log_dir = log_dir
    _cmd_callback = cmd_callback

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not (host and token):
        env = _load_env_file()
        host = host or env.get("DATABRICKS_HOST", "")
        token = token or env.get("DATABRICKS_TOKEN", "")

    if host and token:
        _host = host.rstrip("/")
        _token = token
        _initialized = True
        _emit(f"[DatabricksSync] Initialized -> {_host}")
    else:
        _emit("[DatabricksSync] Missing DATABRICKS_HOST or DATABRICKS_TOKEN – sync disabled.")


# ── state push ───────────────────────────────────────────────────────────

def push_state(
    state: dict[str, Any],
    display_settings: dict[str, Any] | None = None,
) -> None:
    """Debounced, non-blocking push of state JSON.  Returns immediately."""
    if not _initialized:
        return
    now = time.monotonic()
    with _state_lock:
        if now - _last_state_push < _STATE_DEBOUNCE_SEC:
            return

    payload = dict(state)
    if display_settings:
        payload["_display_settings"] = display_settings
    payload["_recent_log"] = _read_recent_log()

    snapshot = json.dumps(payload, indent=2).encode("utf-8")
    threading.Thread(target=_do_push_state, args=(snapshot,), daemon=True).start()


def _do_push_state(body: bytes) -> None:
    global _last_state_push
    with _state_lock:
        _last_state_push = time.monotonic()
    try:
        _api_put(_STATE_PATH, body)
    except Exception as exc:
        _emit(f"[DatabricksSync] State push failed: {exc}")
        return

    # Piggyback: check for cloud commands while we're already talking to Databricks
    if _cmd_callback is not None:
        for cmd in poll_commands():
            try:
                _cmd_callback(cmd)
            except Exception:
                pass


# ── log push ─────────────────────────────────────────────────────────────

def push_log(log_path: Path | None = None) -> None:
    """Debounced, non-blocking push of a log file.  Returns immediately."""
    if not _initialized:
        return
    if log_path is None and _log_dir:
        day = dt.datetime.now().strftime("%Y-%m-%d")
        log_path = _log_dir / f"sequencer-{day}.log"
    if log_path is None or not log_path.exists():
        return
    now = time.monotonic()
    with _log_lock:
        if now - _last_log_push < _LOG_DEBOUNCE_SEC:
            return
    threading.Thread(target=_do_push_log, args=(log_path,), daemon=True).start()


def _do_push_log(log_path: Path) -> None:
    global _last_log_push
    with _log_lock:
        _last_log_push = time.monotonic()
    try:
        body = log_path.read_bytes()
        _api_put(f"{_LOGS_DIR}/{log_path.name}", body, timeout=30)
    except Exception as exc:
        _emit(f"[DatabricksSync] Log push failed: {exc}")


# ── cloud commands ───────────────────────────────────────────────────────

def poll_commands() -> list[str]:
    """List command files in the volume, parse, delete, return command strings.

    Command file names::

        pause--<task_id>   →  "pause:<task_id>"
        unpause--<task_id> →  "unpause:<task_id>"
        run--<task_id>     →  "run:<task_id>"
        pull               →  "pull"
        push               →  "push"

    Returns an empty list on error or if sync is disabled.
    """
    if not _initialized:
        return []
    try:
        data = _api_get_json(_COMMANDS_DIR)
    except Exception:
        return []  # directory may not exist yet

    commands: list[str] = []
    for entry in data.get("contents", []):
        if entry.get("is_directory"):
            continue
        name = entry.get("name", "")
        if not name:
            continue

        # Parse command from filename
        if "--" in name:
            action, _, task_id = name.partition("--")
            if action in ("pause", "unpause", "run") and task_id:
                commands.append(f"{action}:{task_id}")
        elif name in ("pull", "push"):
            commands.append(name)

        # Delete after reading
        try:
            _api_delete(f"{_COMMANDS_DIR}/{name}")
        except Exception:
            pass

    if commands:
        _emit(f"[DatabricksSync] Cloud commands: {commands}")
    return commands
