"""YAML-driven scheduler for Python scripts."""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

_LIBS_DIR = Path(__file__).resolve().parent / "libs"
_PROJECT_DIR = Path(__file__).resolve().parent
_UV_EXE = _PROJECT_DIR / "bin" / "uv.exe"
_VENDOR_DIR = _PROJECT_DIR / "vendor"
_PYPROJECT = _PROJECT_DIR / "pyproject.toml"
_BUNDLED_PYTHON_DIR = _PROJECT_DIR / "bin" / "python"


def _scan_lib_paths() -> list[str]:
    if not _LIBS_DIR.is_dir():
        return []
    return sorted(str(p) for p in _LIBS_DIR.iterdir() if p.is_dir())


def _refresh_sys_path() -> None:
    """Ensure every libs/* entry is on sys.path (idempotent)."""
    for p in reversed(_scan_lib_paths()):
        if p not in sys.path:
            sys.path.insert(0, p)


_refresh_sys_path()
import yaml


def _parse_requires_python(pyproject: Path) -> str:
    """Extract the version from requires-python in a pyproject.toml (e.g. '>=3.12' -> '3.12')."""
    import re
    try:
        text = pyproject.read_text(encoding="utf-8")
        match = re.search(r'requires-python\s*=\s*"[><=!~]*(\d+\.\d+)"', text)
        if match:
            return match.group(1)
    except OSError:
        pass
    return "3"


def _find_bundled_python(version: str = "") -> str | None:
    """Locate python.exe inside bin/python/ matching the requested version.

    If *version* is given (e.g. '3.11'), prefer a directory whose name
    contains that version string.  Falls back to any available python.exe.
    """
    if not _BUNDLED_PYTHON_DIR.is_dir():
        return None

    fallback: str | None = None
    for child in sorted(_BUNDLED_PYTHON_DIR.iterdir()):
        candidate = child / "python.exe"
        if not candidate.is_file():
            continue
        # Match e.g. "cpython-3.11" in directory name
        if version and f"-{version}" in child.name:
            return str(candidate)
        if fallback is None:
            fallback = str(candidate)
    return fallback


def _bootstrap_env(project_dir: Path | None = None) -> str | None:
    """Create a .venv and install vendored wheels if bin/uv.exe is present.

    Returns the path to the venv Python interpreter, or None to use the
    system interpreter (when uv.exe or vendor/ are not available).
    """
    if not _UV_EXE.is_file():
        return None

    if project_dir is None:
        project_dir = _PROJECT_DIR

    venv_dir = project_dir / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe"
    pyproject = project_dir / "pyproject.toml"
    vendor_dir = project_dir / "vendor" if (project_dir / "vendor").is_dir() else _VENDOR_DIR

    if not pyproject.is_file():
        return None

    # Create venv if missing
    if not venv_python.is_file():
        required_ver = _parse_requires_python(pyproject)
        bundled = _find_bundled_python(required_ver)
        python_arg = bundled if bundled else required_ver
        print(f"[Bootstrap] Creating .venv in {project_dir} (python={python_arg}) ...")
        subprocess.run(
            [str(_UV_EXE), "venv", str(venv_dir), "--python", python_arg],
            cwd=str(project_dir),
            check=True,
        )

    # Offline install from vendored wheels
    if vendor_dir.is_dir() and any(vendor_dir.glob("*.whl")):
        print(f"[Bootstrap] Installing vendored packages from {vendor_dir} ...")
        subprocess.run(
            [
                str(_UV_EXE), "pip", "install",
                "--no-index",
                "--find-links", str(vendor_dir),
                "-r", str(pyproject),
                "--python", str(venv_python),
            ],
            cwd=str(project_dir),
            check=True,
        )

    return str(venv_python)


def _bootstrap_subprojects(base_dir: Path) -> dict[Path, str]:
    """Find subfolders with their own pyproject.toml and bootstrap each one.

    Returns a mapping of subfolder -> venv python interpreter path.
    """
    interpreters: dict[Path, str] = {}
    if not _UV_EXE.is_file():
        return interpreters
    for child in base_dir.iterdir():
        if child.is_dir() and (child / "pyproject.toml").is_file() and child.name != ".venv":
            interp = _bootstrap_env(child)
            if interp:
                interpreters[child.resolve()] = interp
    return interpreters


def _sync_vendor_packages(project_dir: Path | None = None) -> None:
    """Re-install vendored packages into an existing .venv (e.g. after git pull)."""
    if not _UV_EXE.is_file():
        return
    if project_dir is None:
        project_dir = _PROJECT_DIR
    venv_python = project_dir / ".venv" / "Scripts" / "python.exe"
    pyproject = project_dir / "pyproject.toml"
    vendor_dir = project_dir / "vendor" if (project_dir / "vendor").is_dir() else _VENDOR_DIR
    if not venv_python.is_file() or not pyproject.is_file():
        return
    if not (vendor_dir.is_dir() and any(vendor_dir.glob("*.whl"))):
        return
    log("[Bootstrap] Post-pull: syncing vendored packages...")
    subprocess.run(
        [
            str(_UV_EXE), "pip", "install",
            "--no-index",
            "--find-links", str(vendor_dir),
            "-r", str(pyproject),
            "--python", str(venv_python),
        ],
        cwd=str(project_dir),
        check=True,
    )


def _sync_subproject_packages(base_dir: Path) -> None:
    """Re-sync vendored packages for all subprojects after git pull."""
    if not _UV_EXE.is_file():
        return
    for child in base_dir.iterdir():
        if child.is_dir() and (child / "pyproject.toml").is_file() and child.name != ".venv":
            _sync_vendor_packages(child)


DAY_NAME_TO_INDEX = {name.lower(): idx for idx, name in enumerate(calendar.day_name)}
DAY_ABBR_TO_INDEX = {
    name[:3].lower(): idx for idx, name in enumerate(calendar.day_name)
}
MONTH_NAME_TO_INDEX = {
    name.lower(): idx for idx, name in enumerate(calendar.month_name) if name
}
MONTH_ABBR_TO_INDEX = {
    name.lower(): idx for idx, name in enumerate(calendar.month_abbr) if name
}

LOG_DIRNAME_DEFAULT = "logs"
LOG_FILE_PREFIX = "sequencer-"
LOG_KEEP_COUNT_DEFAULT = 14
LOG_PRUNE_BATCH_DEFAULT = 7

_LOG_LOCK = threading.Lock()
_LOG_DIR = (Path.cwd() / LOG_DIRNAME_DEFAULT).resolve()
_LOG_KEEP_COUNT = LOG_KEEP_COUNT_DEFAULT
_LOG_PRUNE_BATCH = LOG_PRUNE_BATCH_DEFAULT
_LOG_LAST_PRUNE_DAY = ""


def _active_log_path_for_day(day_value: str) -> Path:
    return _LOG_DIR / f"{LOG_FILE_PREFIX}{day_value}.log"


def _prune_logs_for_day(day_value: str) -> None:
    """Prune old rotated logs once per day."""
    global _LOG_LAST_PRUNE_DAY
    if _LOG_LAST_PRUNE_DAY == day_value:
        return
    _LOG_LAST_PRUNE_DAY = day_value
    try:
        candidates = sorted(
            _LOG_DIR.glob(f"{LOG_FILE_PREFIX}*.log"),
            key=lambda path: (path.stat().st_mtime, path.name),
        )
    except OSError:
        return

    if len(candidates) <= _LOG_KEEP_COUNT:
        return

    for path in candidates[:_LOG_PRUNE_BATCH]:
        try:
            path.unlink()
        except OSError:
            continue


def _write_log_line(line: str) -> None:
    now = dt.datetime.now()
    day_value = now.strftime("%Y-%m-%d")
    with _LOG_LOCK:
        log_path = _active_log_path_for_day(day_value)
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        _prune_logs_for_day(day_value)
        try:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")
        except OSError:
            # Keep console logging even if file logging fails temporarily.
            pass

    print(line, flush=True)


def log_raw(message: str) -> None:
    text = str(message)
    if not text:
        return
    lines = text.splitlines()
    if not lines:
        _write_log_line(text)
        return
    for line in lines:
        _write_log_line(line)


def log(message: str) -> None:
    timestamp = dt.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    _write_log_line(f"[{timestamp}] {message}")


class WorkerSlotLimiter:
    """Limits parallel task execution by weighted worker slots."""

    def __init__(self, total_slots: int) -> None:
        self.total_slots = max(1, total_slots)
        self.available_slots = self.total_slots
        self._condition = threading.Condition()

    def acquire(self, slots: int) -> None:
        slots = max(1, slots)
        with self._condition:
            while slots > self.available_slots:
                self._condition.wait()
            self.available_slots -= slots

    def release(self, slots: int) -> None:
        slots = max(1, slots)
        with self._condition:
            self.available_slots += slots
            if self.available_slots > self.total_slots:
                self.available_slots = self.total_slots
            self._condition.notify_all()


class SchedulerContext:
    """Persistent state that survives across daemon ticks."""

    def __init__(self, state_path: Path, max_workers: int) -> None:
        self.state_path = state_path
        self.max_workers = max_workers
        self.state: dict[str, Any] = load_state(state_path)
        self.last_triggered_slot: dict[str, Any] = self.state["last_triggered_slot"]
        self.in_progress: dict[str, Any] = self.state["in_progress"]
        self.profiling: dict[str, Any] = self.state["profiling"]
        self.state_lock = threading.Lock()
        self.wake_event = threading.Event()
        self.git_wake_event = threading.Event()
        self.command_queue: queue.Queue[str] = queue.Queue()
        self.slot_limiter = WorkerSlotLimiter(max_workers)
        self.actively_running: set[str] = set()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="seq-worker",
        )

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def mark_task_started(
        self,
        key: str,
        task_name: str,
        script_path: Path,
        worker_cost: int,
        slot_key_value: str,
        is_recovery: bool,
    ) -> None:
        with self.state_lock:
            self.in_progress[key] = {
                "task_name": task_name,
                "script_path": str(script_path),
                "worker_cost": worker_cost,
                "slot_key": slot_key_value,
                "started_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "is_recovery": is_recovery,
            }
            save_state(self.state_path, self.state)

    def mark_task_finished(self, key: str, slot_key_value: str, success: bool) -> None:
        with self.state_lock:
            self.in_progress.pop(key, None)
            self.actively_running.discard(key)
            now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            prev = self.last_triggered_slot.get(key)
            prev_count = prev.get("retry_count", 0) if isinstance(prev, dict) else 0
            self.last_triggered_slot[key] = {
                "slot": slot_key_value,
                "last_run": now_str,
                "outcome": "success" if success else "failure",
                "retry_count": 0 if success else prev_count + 1,
            }
            save_state(self.state_path, self.state)
        # Trigger immediate git push so logs + state reach remote fast.
        self.command_queue.put("push")
        self.git_wake_event.set()

    def clear_recovery_entry(self, key: str) -> None:
        with self.state_lock:
            if key in self.in_progress:
                self.in_progress.pop(key, None)
                save_state(self.state_path, self.state)

    def mark_slot_consumed_without_run(self, key: str, slot_key_value: str) -> None:
        with self.state_lock:
            self.in_progress.pop(key, None)
            now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.last_triggered_slot[key] = {
                "slot": slot_key_value,
                "last_run": now_str,
                "outcome": "skipped",
            }
            save_state(self.state_path, self.state)

    def get_last_slot(self, key: str) -> str | None:
        entry = self.last_triggered_slot.get(key)
        if isinstance(entry, dict):
            return str(entry.get("slot", "")) or None
        if isinstance(entry, str):
            return entry
        return None

    def is_task_actively_running(self, key: str) -> bool:
        return key in self.actively_running


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Python scripts based on a YAML schedule."
    )
    parser.add_argument(
        "--config",
        default="schedule.yaml",
        help="Path to YAML schedule file (default: schedule.yaml).",
    )
    parser.add_argument(
        "--now",
        default=None,
        help=(
            "Override current timestamp using ISO format for testing, "
            "for example 2026-02-13T09:00."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without starting child scripts.",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously and trigger scheduler passes on minute boundaries.",
    )
    return parser.parse_args()


def parse_now(now_override: str | None) -> dt.datetime:
    if not now_override:
        return dt.datetime.now()
    try:
        return dt.datetime.fromisoformat(now_override)
    except ValueError as exc:
        raise ValueError(
            "Invalid --now value. Use ISO format, e.g. 2026-02-13T09:00"
        ) from exc


def parse_task_datetime(value: Any, field_name: str) -> dt.datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        raise ValueError(
            f"`{field_name}` cannot be empty. "
            "Use datetime format like 2026-03-01 09:00:00."
        )

    normalized = text.replace("T", " ")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"`{field_name}` must be a datetime like 2026-03-01 09:00:00 "
            "(seconds optional)."
        ) from exc

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)

    return parsed


def task_datetime(task: dict[str, Any], field_name: str) -> dt.datetime | None:
    value = task.get(field_name)
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    return parse_task_datetime(value, field_name)


def is_within_task_window(task: dict[str, Any], now: dt.datetime) -> bool:
    start_at = task_datetime(task, "start_at")
    end_at = task_datetime(task, "end_at")

    if start_at and now < start_at:
        return False
    if end_at and now > end_at:
        return False
    return True



GIT_PULL_TRIGGER = ".git_pull_now"


def _git_remote_has_changes(repo_dir: Path) -> bool:
    """Return True if the remote branch has commits not in the local branch."""
    try:
        subprocess.run(
            ["git", "fetch"],
            cwd=str(repo_dir),
            capture_output=True,
            timeout=120,
        )
        result = subprocess.run(
            ["git", "rev-list", "HEAD..@{u}", "--count"],
            cwd=str(repo_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0 and int(result.stdout.strip() or "0") > 0
    except Exception:
        return True  # on error, pull anyway to be safe


def git_pull(repo_dir: Path) -> str:
    """Pull only when the remote has new commits.  Returns a short status string."""
    try:
        if not _git_remote_has_changes(repo_dir):
            return "no changes"
        result = subprocess.run(
            ["git", "pull"],
            cwd=str(repo_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = (result.stdout or "").strip()
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            return f"failed: {err or output}"
        return f"updated: {output.splitlines()[0] if output else 'ok'}"
    except subprocess.TimeoutExpired:
        return "failed: timeout"
    except FileNotFoundError:
        return "failed: git not found"
    except Exception as exc:
        return f"failed: {exc}"


def maybe_git_pull(
    config_path: Path,
    last_pull_time: dt.datetime | None,
    interval_minutes: int,
    triggered: bool = False,
) -> tuple[dt.datetime | None, bool]:
    """Pull if the interval has elapsed or *triggered* is True.

    Returns ``(new_last_pull_time, files_changed)`` where *files_changed*
    is True only when ``git pull`` actually updated files.
    """
    repo_dir = config_path.parent
    now = dt.datetime.now()

    # Also check for leftover trigger file (backward compatibility).
    trigger_file = repo_dir / GIT_PULL_TRIGGER
    if trigger_file.exists():
        triggered = True
        try:
            trigger_file.unlink()
        except OSError:
            pass

    interval_elapsed = (
        interval_minutes > 0
        and (last_pull_time is None or (now - last_pull_time).total_seconds() >= interval_minutes * 60)
    )

    if not triggered and not interval_elapsed:
        return last_pull_time, False

    reason = "on-demand" if triggered else "scheduled"
    status = git_pull(repo_dir)
    log(f"[Git Pull] ({reason}) {status}")
    return now, status.startswith("updated")


GIT_PUSH_TRIGGER = ".git_push_now"

PAUSE_TRIGGER_PREFIX = ".pause_task_"
UNPAUSE_TRIGGER_PREFIX = ".unpause_task_"
RUN_TRIGGER_PREFIX = ".run_task_"

WAKE_UDP_PORT = 19876

# Files the push helper will commit (relative to repo root).
GIT_PUSH_PATHS = [
    "sequencer_state.json",
    "logs/",
]


def git_push(repo_dir: Path) -> str:
    """Stage state + logs, commit, and push.  Returns a short status string."""
    try:
        # Stage only the files we care about
        add_args = ["git", "add", "--"] + GIT_PUSH_PATHS
        subprocess.run(add_args, cwd=str(repo_dir), capture_output=True, text=True, timeout=30)

        # Check if there is anything to commit
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=str(repo_dir),
            capture_output=True,
            timeout=30,
        )
        if diff.returncode == 0:
            return "no changes"

        # Commit
        subprocess.run(
            ["git", "commit", "-m", "auto: sequencer state + logs"],
            cwd=str(repo_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Push
        result = subprocess.run(
            ["git", "push"],
            cwd=str(repo_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            return f"failed: {err or result.stdout or 'unknown'}"
        return "pushed"
    except subprocess.TimeoutExpired:
        return "failed: timeout"
    except FileNotFoundError:
        return "failed: git not found"
    except Exception as exc:
        return f"failed: {exc}"


def maybe_git_push(
    config_path: Path,
    last_push_time: dt.datetime | None,
    interval_minutes: int,
    triggered: bool = False,
) -> dt.datetime | None:
    """Push if the interval has elapsed or *triggered* is True.

    Returns the new last-push timestamp, or *last_push_time* unchanged.
    """
    repo_dir = config_path.parent
    now = dt.datetime.now()

    # Also check for leftover trigger file (backward compatibility).
    trigger_file = repo_dir / GIT_PUSH_TRIGGER
    if trigger_file.exists():
        triggered = True
        try:
            trigger_file.unlink()
        except OSError:
            pass

    interval_elapsed = (
        interval_minutes > 0
        and (last_push_time is None or (now - last_push_time).total_seconds() >= interval_minutes * 60)
    )

    if not triggered and not interval_elapsed:
        return last_push_time

    reason = "on-demand" if triggered else "scheduled"
    status = git_push(repo_dir)
    log(f"[Git Push] ({reason}) {status}")
    return now


def process_commands(cmd_queue: queue.Queue[str], state: dict) -> set[str]:
    """Drain queued commands from the UDP listener.

    Handles pause:<id>, unpause:<id>, and run:<id> commands.
    Returns a set of task IDs that should run immediately.
    """
    paused: list[str] = state.setdefault("paused_tasks", [])
    run_now: set[str] = set()
    while True:
        try:
            cmd = cmd_queue.get_nowait()
        except queue.Empty:
            break
        if cmd.startswith("pause:"):
            task_id = cmd[6:]
            if task_id not in paused:
                paused.append(task_id)
                log(f"[Pause] Task `{task_id}` paused.")
        elif cmd.startswith("unpause:"):
            task_id = cmd[8:]
            if task_id in paused:
                paused.remove(task_id)
                log(f"[Resume] Task `{task_id}` resumed.")
        elif cmd.startswith("run:"):
            task_id = cmd[4:]
            run_now.add(task_id)
            log(f"[Run Now] Task `{task_id}` queued for immediate run.")
    return run_now


def process_triggers(config_path: Path, state: dict) -> set[str]:
    """Consume leftover trigger files (backward compatibility).

    Handles .pause_task_*, .unpause_task_*, and .run_task_* files.
    Returns a set of task IDs that should run immediately.
    """
    repo_dir = config_path.parent
    paused: list[str] = state.setdefault("paused_tasks", [])
    run_now: set[str] = set()
    try:
        entries = list(repo_dir.iterdir())
    except OSError:
        return run_now
    for f in entries:
        if f.name.startswith(PAUSE_TRIGGER_PREFIX):
            task_id = f.name[len(PAUSE_TRIGGER_PREFIX):]
            try:
                f.unlink()
            except OSError:
                pass
            if task_id not in paused:
                paused.append(task_id)
                log(f"[Pause] Task `{task_id}` paused via trigger.")
        elif f.name.startswith(UNPAUSE_TRIGGER_PREFIX):
            task_id = f.name[len(UNPAUSE_TRIGGER_PREFIX):]
            try:
                f.unlink()
            except OSError:
                pass
            if task_id in paused:
                paused.remove(task_id)
                log(f"[Resume] Task `{task_id}` resumed via trigger.")
        elif f.name.startswith(RUN_TRIGGER_PREFIX):
            task_id = f.name[len(RUN_TRIGGER_PREFIX):]
            try:
                f.unlink()
            except OSError:
                pass
            run_now.add(task_id)
            log(f"[Run Now] Task `{task_id}` queued for immediate run via trigger.")
    return run_now


def load_config(config_path: Path) -> dict[str, Any]:
    try:
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {config_path}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError("YAML root must be a dictionary.")

    # Load settings from a separate settings.yaml if it exists alongside the config.
    settings_path = config_path.parent / "settings.yaml"
    if settings_path.exists():
        try:
            settings_data = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML in {settings_path}: {exc}") from exc
        if isinstance(settings_data, dict):
            config["settings"] = settings_data.get("settings", settings_data)

    return config


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        log(f"[Warn] State file {state_path} is unreadable. Resetting it.")
        return {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}

    if not isinstance(state, dict):
        return {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}

    if not isinstance(state.get("last_triggered_slot"), dict):
        state["last_triggered_slot"] = {}
    if not isinstance(state.get("in_progress"), dict):
        state["in_progress"] = {}
    if not isinstance(state.get("profiling"), dict):
        state["profiling"] = {}
    if not isinstance(state.get("paused_tasks"), list):
        state["paused_tasks"] = []
    state.pop("run_now_tasks", None)  # transient; never persisted
    # last_outcomes is now merged into last_triggered_slot, so we can ignore it or clean it up
    if "last_outcomes" in state:
        state.pop("last_outcomes", None)

    return state


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def configure_log_runtime(config_path: Path, settings: dict[str, Any]) -> None:
    global _LOG_DIR, _LOG_KEEP_COUNT, _LOG_PRUNE_BATCH
    log_dir_name = str(settings.get("log_dir", LOG_DIRNAME_DEFAULT)).strip() or LOG_DIRNAME_DEFAULT
    keep_count = max(1, to_int(settings.get("log_keep_count"), LOG_KEEP_COUNT_DEFAULT))
    prune_batch = max(1, to_int(settings.get("log_prune_batch"), LOG_PRUNE_BATCH_DEFAULT))
    log_dir = (config_path.parent / log_dir_name).resolve()

    with _LOG_LOCK:
        _LOG_DIR = log_dir
        _LOG_KEEP_COUNT = keep_count
        _LOG_PRUNE_BATCH = prune_batch
        _LOG_DIR.mkdir(parents=True, exist_ok=True)


def parse_worker_setting(value: Any, default: int = 4) -> int:
    return max(1, to_int(value, default))


def compute_retry_delay(base_delay: float, retry_count: int, max_delay: float) -> float:
    """Exponential backoff: base_delay * 2^retry_count, capped at max_delay."""
    return min(base_delay * (2 ** retry_count), max_delay)


def day_of_week_to_index(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, int):
        if 0 <= value <= 6:
            return value
        raise ValueError("day_of_week integer must be in the range 0-6.")

    text = str(value).strip().lower()
    if text in DAY_NAME_TO_INDEX:
        return DAY_NAME_TO_INDEX[text]
    if text in DAY_ABBR_TO_INDEX:
        return DAY_ABBR_TO_INDEX[text]

    raise ValueError(
        "day_of_week must be a full day name, 3-letter abbreviation, or 0-6."
    )


def day_of_week_to_indices(value: Any) -> set[int]:
    if value is None:
        return {0}

    if isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        text = str(value).strip()
        if not text:
            raise ValueError(
                "day_of_week cannot be empty. Use a day name/number, "
                "comma-separated values, or a YAML list."
            )
        if "," not in text:
            return {day_of_week_to_index(value)}
        parts = [part.strip() for part in text.split(",")]

    if not parts:
        raise ValueError("day_of_week list cannot be empty.")

    indices: set[int] = set()
    for part in parts:
        if part is None or str(part).strip() == "":
            raise ValueError("day_of_week contains an empty value.")
        indices.add(day_of_week_to_index(part))

    return indices


def month_to_number(value: Any) -> int:
    if isinstance(value, int):
        if 1 <= value <= 12:
            return value
        raise ValueError("months integers must be in the range 1-12.")

    text = str(value).strip().lower()
    if text in MONTH_NAME_TO_INDEX:
        return MONTH_NAME_TO_INDEX[text]
    if text in MONTH_ABBR_TO_INDEX:
        return MONTH_ABBR_TO_INDEX[text]

    try:
        month_number = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "months values must be month names, 3-letter abbreviations, or 1-12."
        ) from exc

    if 1 <= month_number <= 12:
        return month_number

    raise ValueError("months integers must be in the range 1-12.")


def months_to_numbers(value: Any) -> set[int]:
    if value is None:
        return set(range(1, 13))

    if isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        text = str(value).strip()
        if not text:
            raise ValueError(
                "months cannot be empty. Use month names/numbers, "
                "comma-separated values, or a YAML list."
            )
        if "," not in text:
            return {month_to_number(value)}
        parts = [part.strip() for part in text.split(",")]

    if not parts:
        raise ValueError("months list cannot be empty.")

    months: set[int] = set()
    for part in parts:
        if part is None or str(part).strip() == "":
            raise ValueError("months contains an empty value.")
        months.add(month_to_number(part))

    return months


def month_days_to_set(value: Any) -> set[int] | None:
    """Parse ``month_day`` field.  Returns *None* when *value* is None (meaning
    ALL days of the month).  Otherwise returns a set of ints in range 1-31."""
    if value is None:
        return None

    if isinstance(value, int):
        if 1 <= value <= 31:
            return {value}
        raise ValueError("month_day integers must be in the range 1-31.")

    if isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        if "," not in text:
            try:
                d = int(text)
            except (TypeError, ValueError) as exc:
                raise ValueError("month_day values must be integers 1-31.") from exc
            if not 1 <= d <= 31:
                raise ValueError("month_day integers must be in the range 1-31.")
            return {d}
        parts = [p.strip() for p in text.split(",")]

    if not parts:
        return None

    days: set[int] = set()
    for part in parts:
        try:
            d = int(str(part).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("month_day values must be integers 1-31.") from exc
        if not 1 <= d <= 31:
            raise ValueError("month_day integers must be in the range 1-31.")
        days.add(d)
    return days


def week_days_to_set(value: Any) -> set[int] | None:
    """Parse ``week_day`` field.  Accepts 1-7 (1=Monday … 7=Sunday), day names,
    or comma-separated values.  Returns a set of Python weekday indices (0-6,
    where 0=Monday) or *None* when *value* is None (meaning all weekdays)."""
    if value is None:
        return None

    def _parse_one(v: Any) -> int:
        if isinstance(v, int):
            if 1 <= v <= 7:
                return v - 1
            raise ValueError("week_day integers must be in the range 1-7 (1=Monday, 7=Sunday).")
        text = str(v).strip()
        try:
            n = int(text)
            if 1 <= n <= 7:
                return n - 1
            raise ValueError("week_day integers must be in the range 1-7 (1=Monday, 7=Sunday).")
        except ValueError:
            pass
        return day_of_week_to_index(text)

    if isinstance(value, (list, tuple, set)):
        parts = list(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        if "," not in text:
            return {_parse_one(value)}
        parts = [p.strip() for p in text.split(",")]

    if not parts:
        return None

    return {_parse_one(p) for p in parts}


def max_day_for_month(month: int) -> int:
    if month == 2:
        return 29
    return calendar.monthrange(2025, month)[1]


def validate_task(task: Any, index: int) -> dict[str, Any]:
    if not isinstance(task, dict):
        raise ValueError(f"Task #{index} is not a dictionary.")

    name = task.get("id")
    path = task.get("path")
    if not name:
        raise ValueError(f"Task #{index} is missing required field `id`.")
    if not path:
        raise ValueError(f"Task `{name}` is missing required field `path`.")

    task_copy = dict(task)
    task_copy["name"] = str(name)
    task_copy["path"] = str(path)

    start_at = parse_task_datetime(task.get("start_at"), "start_at")
    end_at = parse_task_datetime(task.get("end_at"), "end_at")
    if start_at is not None:
        task_copy["start_at"] = start_at
    if end_at is not None:
        task_copy["end_at"] = end_at
    if start_at is not None and end_at is not None and start_at > end_at:
        raise ValueError("`start_at` must be <= `end_at`.")

    # --- Schedule fields ---
    # month: optional, defaults to all months
    task_copy["_months"] = months_to_numbers(task.get("month"))

    # month_day: optional, None = all days of the month
    task_copy["_month_day"] = month_days_to_set(task.get("month_day"))

    # week_day: optional, None = all weekdays
    task_copy["_week_day"] = week_days_to_set(task.get("week_day"))

    # Mutual exclusivity: month_day takes precedence over week_day
    if task_copy["_month_day"] is not None and task_copy["_week_day"] is not None:
        task_copy["_week_day"] = None

    # start_hour: optional
    raw_sh = task.get("start_hour")
    if raw_sh is not None:
        sh = to_int(raw_sh, -1)
        if not (0 <= sh <= 23):
            raise ValueError(f"Task `{name}`: `start_hour` must be in range 0-23.")
        task_copy["_start_hour"] = sh
    else:
        task_copy["_start_hour"] = None

    # start_minute: optional, defaults to 0
    raw_sm = task.get("start_minute")
    if raw_sm is not None:
        sm = to_int(raw_sm, -1)
        if not (0 <= sm <= 59):
            raise ValueError(f"Task `{name}`: `start_minute` must be in range 0-59.")
        task_copy["_start_minute"] = sm
    else:
        task_copy["_start_minute"] = 0

    # frequency_min: optional
    raw_fm = task.get("frequency_min")
    if raw_fm is not None:
        fm = to_int(raw_fm, -1)
        if fm < 1:
            raise ValueError(f"Task `{name}`: `frequency_min` must be >= 1.")
        task_copy["_frequency_min"] = fm
        # Default start_hour to 0 (midnight) when frequency_min is set
        # so interval math works instead of falling into "run every tick"
        if task_copy["_start_hour"] is None:
            task_copy["_start_hour"] = 0
    else:
        task_copy["_frequency_min"] = None

    # end_hour: optional, 0-23. If omitted, no end boundary.
    raw_eh = task.get("end_hour")
    if raw_eh is not None:
        eh = to_int(raw_eh, -1)
        if not (0 <= eh <= 23):
            raise ValueError(f"Task `{name}`: `end_hour` must be in range 0-23.")
        task_copy["_end_hour"] = eh
    else:
        task_copy["_end_hour"] = None

    # end_minute: optional, defaults to 0
    raw_em = task.get("end_minute")
    if raw_em is not None:
        em = to_int(raw_em, -1)
        if not (0 <= em <= 59):
            raise ValueError(f"Task `{name}`: `end_minute` must be in range 0-59.")
        task_copy["_end_minute"] = em
    else:
        task_copy["_end_minute"] = 0

    if (task_copy["_start_hour"] is not None
            and task_copy["_end_hour"] is not None
            and task_copy["_start_hour"] > task_copy["_end_hour"]):
        raise ValueError(f"Task `{name}`: `start_hour` must be <= `end_hour`.")

    # times: optional, list of "HH:MM" strings for running at multiple specific times
    raw_times = task.get("times")
    if raw_times is not None:
        if isinstance(raw_times, str):
            parts = [t.strip() for t in raw_times.split(",") if t.strip()]
        elif isinstance(raw_times, list):
            parts = [str(t).strip() for t in raw_times if str(t).strip()]
        else:
            raise ValueError(f"Task `{name}`: `times` must be a string or list of \"HH:MM\" values.")
        parsed_times: list[tuple[int, int]] = []
        for t in parts:
            if ":" not in t:
                raise ValueError(f"Task `{name}`: invalid time `{t}` in `times` (expected HH:MM).")
            h_str, m_str = t.split(":", 1)
            h, m = to_int(h_str, -1), to_int(m_str, -1)
            if not (0 <= h <= 23) or not (0 <= m <= 59):
                raise ValueError(f"Task `{name}`: invalid time `{t}` in `times`.")
            parsed_times.append((h, m))
        if task_copy["_start_hour"] is not None or task_copy["_frequency_min"] is not None:
            raise ValueError(f"Task `{name}`: `times` cannot be combined with `start_hour`/`frequency_min`.")
        task_copy["_times"] = parsed_times
    else:
        task_copy["_times"] = None

    # Task dependencies.
    depends_on = task.get("depends_on")
    if depends_on is not None:
        if isinstance(depends_on, str):
            depends_on = [d.strip() for d in depends_on.split(",") if d.strip()]
        elif isinstance(depends_on, list):
            depends_on = [str(d).strip() for d in depends_on if str(d).strip()]
        else:
            raise ValueError(f"Task `{name}`: `depends_on` must be a list or comma-separated string.")
        task_copy["_depends_on"] = depends_on
    else:
        task_copy["_depends_on"] = []

    # Task timeout.
    raw_timeout = task.get("timeout_minutes")
    if raw_timeout is not None:
        timeout = to_int(raw_timeout, -1)
        if timeout < 1:
            raise ValueError(f"Task `{name}`: `timeout_minutes` must be >= 1.")
        task_copy["_timeout_minutes"] = timeout
    else:
        task_copy["_timeout_minutes"] = None

    return task_copy


def should_run(task: dict[str, Any], now: dt.datetime) -> bool:
    if not is_within_task_window(task, now):
        return False

    # 1. Month filter
    months = task.get("_months")
    if months and now.month not in months:
        return False

    # 2. Day filter — month_day takes precedence over week_day
    month_day = task.get("_month_day")
    week_day = task.get("_week_day")
    if month_day is not None:
        if now.day not in month_day:
            return False
    elif week_day is not None:
        if now.weekday() not in week_day:
            return False

    # 3. Times shorthand — multiple specific times in one entry
    times = task.get("_times")
    if times is not None:
        return any(now.hour == h and now.minute == m for h, m in times)

    # 4. Time logic
    start_hour = task.get("_start_hour")
    start_minute = task.get("_start_minute", 0)
    frequency_min = task.get("_frequency_min")
    end_hour = task.get("_end_hour")
    end_minute = task.get("_end_minute", 0)

    if start_hour is None:
        # No start_hour → run every tick on matching days/months
        return True

    if frequency_min is None:
        # Run exactly once at start_hour:start_minute
        return now.hour == start_hour and now.minute == start_minute

    # Repeating: every frequency_min minutes from start (to end if set)
    window_start = start_hour * 60 + start_minute
    current_minutes = now.hour * 60 + now.minute

    if current_minutes < window_start:
        return False

    if end_hour is not None:
        window_end = end_hour * 60 + end_minute
        if current_minutes > window_end:
            return False

    elapsed = current_minutes - window_start
    return elapsed % frequency_min == 0


def compute_next_wake_time(
    validated_tasks: list[dict[str, Any]],
    state: dict[str, Any],
    settings: dict[str, Any],
    now: dt.datetime,
    paused_tasks: set[str],
    max_horizon_minutes: int = 60,
) -> dt.datetime:
    """Return the earliest datetime at which the scheduler should next wake.

    Scans forward minute-by-minute up to *max_horizon_minutes* and also
    considers retry timers for failed tasks.  Falls back to
    ``now + max_horizon_minutes`` when nothing is due sooner.
    """
    candidates: list[dt.datetime] = []
    base = now.replace(second=0, microsecond=0)

    # A. Next scheduled-task fire time
    for task in validated_tasks:
        if task.get("id") in paused_tasks:
            continue
        for offset in range(1, max_horizon_minutes + 1):
            candidate = base + dt.timedelta(minutes=offset)
            if should_run(task, candidate):
                candidates.append(candidate)
                break

    # B. Retry timers for failed tasks
    retry_base = float(settings.get("retry_delay_seconds", 60))
    retry_max = float(settings.get("retry_max_delay_seconds", 1800))
    for key, entry in state.get("last_triggered_slot", {}).items():
        if not isinstance(entry, dict) or entry.get("outcome") != "failure":
            continue
        if key in paused_tasks:
            continue
        try:
            last_run_dt = dt.datetime.strptime(entry["last_run"], "%Y-%m-%d %H:%M:%S")
            delay = compute_retry_delay(retry_base, entry.get("retry_count", 0), retry_max)
            candidates.append(last_run_dt + dt.timedelta(seconds=delay))
        except (ValueError, KeyError):
            candidates.append(now)

    if not candidates:
        return now + dt.timedelta(minutes=max_horizon_minutes)

    earliest = min(candidates)
    return max(earliest, now + dt.timedelta(seconds=1))


def run_task(
    task_name: str,
    script_path: Path,
    working_directory: Path,
    dry_run: bool,
    email_config: dict[str, Any] | None = None,
    log_task_output: bool = True,
    lib_pythonpath: str = "",
    interpreter: str | None = None,
    timeout_seconds: int | None = None,
) -> bool:
    interpreter = interpreter or sys.executable
    if dry_run:
        log(f"[Dry Run] Would execute: {task_name} -> {script_path} (python={interpreter})")
        return True

    capture = log_task_output
    try:
        log(f"Executing: {task_name} (python={interpreter})")
        env = os.environ.copy()
        if lib_pythonpath:
            env["PYTHONPATH"] = lib_pythonpath + os.pathsep + env.get("PYTHONPATH", "")
        result = subprocess.run(
            [interpreter, str(script_path)],
            check=True,
            cwd=str(working_directory),
            env=env,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.STDOUT if capture else subprocess.DEVNULL,
            text=capture,
            timeout=timeout_seconds,
        )
        if capture and result.stdout:
            log_raw(result.stdout.rstrip())
        log(f"[Success] {task_name} completed.")
        return True
    except subprocess.TimeoutExpired:
        log(f"[Timeout] {task_name} exceeded {timeout_seconds}s — killed.")
    except subprocess.CalledProcessError as exc:
        if capture and exc.stdout:
            log_raw(exc.stdout.rstrip())
        log(f"[Error] {task_name} failed with exit code {exc.returncode}.")
    except Exception as exc:
        log(f"[Error] {task_name} failed unexpectedly: {exc}")

    if email_config:
        send_failure_email(email_config, task_name, script_path)

    return False


def _try_import_psutil():
    """Lazily import psutil; returns the module or None.

    Temporarily hides embedded-interpreter directories (e.g. libs/python39)
    from sys.path to prevent their DLLs from clashing with the running Python.
    """
    interpreter_dirs = []
    libs_prefix = str(_LIBS_DIR) + os.sep
    for p in list(sys.path):
        if p.startswith(libs_prefix) and (Path(p) / "python.exe").exists():
            interpreter_dirs.append(p)
    for d in interpreter_dirs:
        sys.path.remove(d)
    try:
        import psutil
        return psutil
    except ImportError:
        return None
    finally:
        for d in interpreter_dirs:
            if d not in sys.path:
                sys.path.append(d)


def resolve_dynamic_worker_cost(
    key: str,
    profiling_state: dict[str, Any],
    max_workers: int,
    default_cost: int = 100,
) -> int:
    """Return the learned worker_cost, or default_cost if no profiling data exists."""
    entry = profiling_state.get(key)
    if isinstance(entry, dict):
        learned = entry.get("learned_cost", default_cost)
        return max(1, min(int(learned), max_workers))
    return default_cost


def update_profiling_state(
    key: str,
    profiling_state: dict[str, Any],
    peak_ram_pct: float,
    avg_cpu_pct: float,
    max_workers: int,
) -> None:
    """Persist profiling metrics and compute learned_cost."""
    learned_cost = max(1, min(int(max(peak_ram_pct, avg_cpu_pct)), max_workers))
    profiling_state[key] = {
        "peak_ram_pct": round(peak_ram_pct, 2),
        "avg_cpu_pct": round(avg_cpu_pct, 2),
        "learned_cost": learned_cost,
    }


def run_task_profiled(
    task_name: str,
    script_path: Path,
    working_directory: Path,
    dry_run: bool,
    email_config: dict[str, Any] | None = None,
    log_task_output: bool = True,
    lib_pythonpath: str = "",
    interpreter: str | None = None,
    timeout_seconds: int | None = None,
) -> tuple[bool, float, float]:
    """Run a task and profile its peak RAM % and average CPU %.

    Returns (success, peak_ram_pct, avg_cpu_pct).
    """
    interpreter = interpreter or sys.executable
    if dry_run:
        log(f"[Dry Run] Would execute: {task_name} -> {script_path} (python={interpreter})")
        return True, 0.0, 0.0

    psutil = _try_import_psutil()
    if psutil is None:
        # Fallback: run without profiling.
        success = run_task(
            task_name=task_name,
            script_path=script_path,
            working_directory=working_directory,
            dry_run=dry_run,
            email_config=email_config,
            log_task_output=log_task_output,
            lib_pythonpath=lib_pythonpath,
            interpreter=interpreter,
            timeout_seconds=timeout_seconds,
        )
        return success, 0.0, 0.0

    capture = log_task_output
    peak_ram_pct = 0.0
    cpu_samples: list[float] = []
    stop_event = threading.Event()

    def monitor(pid: int) -> None:
        nonlocal peak_ram_pct
        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        # Prime cpu_percent so the first real sample is meaningful.
        try:
            proc.cpu_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        # Keep persistent Process objects so cpu_percent() counters
        # accumulate across samples instead of resetting each iteration.
        tracked: dict[int, Any] = {pid: proc}

        def _refresh_tracked() -> list:
            """Update tracked dict with current children, prime newcomers."""
            child_pids: set[int] = set()
            try:
                for child in proc.children(recursive=True):
                    child_pids.add(child.pid)
                    if child.pid not in tracked:
                        tracked[child.pid] = child
                        try:
                            child.cpu_percent()  # prime new child
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            # Remove tracked children that no longer exist.
            for dead_pid in set(tracked) - child_pids - {pid}:
                tracked.pop(dead_pid, None)
            return list(tracked.values())

        def _sample() -> None:
            """Collect one RAM + CPU sample from the process tree."""
            nonlocal peak_ram_pct
            all_procs = _refresh_tracked()

            ram_total = 0.0
            cpu_total = 0.0
            for p in all_procs:
                try:
                    ram_total += p.memory_percent()
                    cpu_total += p.cpu_percent()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if ram_total > peak_ram_pct:
                peak_ram_pct = ram_total
            # Normalize to system-wide %: psutil reports 100% = one core,
            # divide by cpu_count so 100% = all cores (matches Task Manager).
            num_cpus = psutil.cpu_count() or 1
            cpu_samples.append(cpu_total / num_cpus)

        # Take an immediate sample so short-lived scripts get at least one reading.
        try:
            _sample()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

        while not stop_event.is_set():
            stop_event.wait(0.5)
            try:
                _sample()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

    try:
        log(f"Executing (profiled): {task_name} (python={interpreter})")
        env = os.environ.copy()
        if lib_pythonpath:
            env["PYTHONPATH"] = lib_pythonpath + os.pathsep + env.get("PYTHONPATH", "")

        proc = subprocess.Popen(
            [interpreter, str(script_path)],
            cwd=str(working_directory),
            env=env,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.STDOUT if capture else subprocess.DEVNULL,
            text=capture or None,
        )

        monitor_thread = threading.Thread(target=monitor, args=(proc.pid,), daemon=True)
        monitor_thread.start()

        try:
            stdout_data, _ = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            log(f"[Timeout] {task_name} exceeded {timeout_seconds}s — killing process.")
            proc.kill()
            proc.wait()
            stdout_data = None
        stop_event.set()
        monitor_thread.join(timeout=2)

        if capture and stdout_data:
            log_raw(stdout_data.rstrip())

        nonzero_cpu = [s for s in cpu_samples if s > 0]
        avg_cpu = (sum(nonzero_cpu) / len(nonzero_cpu)) if nonzero_cpu else 0.0

        if proc.returncode == 0:
            log(
                f"[Success] {task_name} completed. "
                f"peak_ram={peak_ram_pct:.1f}% avg_cpu={avg_cpu:.1f}%"
            )
            return True, peak_ram_pct, avg_cpu
        else:
            log(f"[Error] {task_name} failed with exit code {proc.returncode}.")
    except Exception as exc:
        log(f"[Error] {task_name} failed unexpectedly: {exc}")

    nonzero_cpu = [s for s in cpu_samples if s > 0]
    avg_cpu = (sum(nonzero_cpu) / len(nonzero_cpu)) if nonzero_cpu else 0.0

    if email_config:
        send_failure_email(email_config, task_name, script_path)

    return False, peak_ram_pct, avg_cpu


def run_with_slots(
    task_name: str,
    script_path: Path,
    worker_cost: int,
    slot_limiter: WorkerSlotLimiter,
    working_directory: Path,
    dry_run: bool,
    email_config: dict[str, Any] | None = None,
    log_task_output: bool = True,
    lib_pythonpath: str = "",
    interpreter: str | None = None,
    profiled: bool = False,
    timeout_seconds: int | None = None,
) -> bool | tuple[bool, float, float]:
    slot_limiter.acquire(worker_cost)
    try:
        if profiled:
            return run_task_profiled(
                task_name=task_name,
                script_path=script_path,
                working_directory=working_directory,
                dry_run=dry_run,
                email_config=email_config,
                log_task_output=log_task_output,
                lib_pythonpath=lib_pythonpath,
                interpreter=interpreter,
                timeout_seconds=timeout_seconds,
            )
        return run_task(
            task_name=task_name,
            script_path=script_path,
            working_directory=working_directory,
            dry_run=dry_run,
            email_config=email_config,
            log_task_output=log_task_output,
            lib_pythonpath=lib_pythonpath,
            interpreter=interpreter,
            timeout_seconds=timeout_seconds,
        )
    finally:
        slot_limiter.release(worker_cost)


def send_failure_email(
    email_config: dict[str, Any],
    task_name: str,
    script_path: Path,
) -> None:
    """Send an Outlook email via win32com when a task fails."""
    if not email_config.get("enabled", False):
        return

    to = email_config.get("to", "")
    if not to:
        log("[Email] No recipient configured in failure_email.to — skipping.")
        return

    placeholders = {
        "task_name": task_name,
        "script_path": str(script_path),
        "timestamp": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    subject = email_config.get("subject", "Sequencer Task Failed: {task_name}")
    body = email_config.get(
        "body",
        "Task '{task_name}' failed.\n"
        "Time: {timestamp}\nScript: {script_path}",
    )

    for key, val in placeholders.items():
        subject = subject.replace("{" + key + "}", val)
        body = body.replace("{" + key + "}", val)

    try:
        import time as _time
        import pythoncom
        import win32com.client  # noqa: delayed import
        import win32gui

        pythoncom.CoInitialize()
        try:
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = to
            cc = email_config.get("cc", "")
            if cc:
                mail.CC = cc
            mail.Subject = subject
            mail.Body = body
            mail.Display()
            _time.sleep(1)
            # Find the Outlook compose window and force it to foreground
            hwnd = win32gui.FindWindow("rctrl_renwnd32", None)
            if hwnd:
                win32gui.SetForegroundWindow(hwnd)
                _time.sleep(0.3)
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.SendKeys("%s")  # Alt+S to send
            log(f"[Email] Failure email sent to {to} for task '{task_name}'.")
            del shell, mail, outlook
        finally:
            pythoncom.CoUninitialize()
    except ImportError:
        log("[Email] win32com/pythoncom is not installed — cannot send email.")
    except Exception as exc:
        log(f"[Email] Failed to send email: {exc}")


def send_heartbeat_email(
    email_config: dict[str, Any],
    state_path: Path,
) -> None:
    """Send a periodic heartbeat email with the current in_progress state."""
    if not email_config.get("enabled", False):
        return

    to = email_config.get("to", "")
    if not to:
        log("[Heartbeat] No recipient configured in heartbeat_email.to — skipping.")
        return

    # Read current state for in_progress
    in_progress_text = "(state file not found)"
    try:
        state = load_state(state_path)
        in_progress = state.get("in_progress", {})
        if in_progress:
            in_progress_text = json.dumps(in_progress, indent=2)
        else:
            in_progress_text = "No tasks currently in progress."
    except Exception:
        pass

    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = email_config.get("subject", "Sequencer Heartbeat — {timestamp}")
    body = email_config.get(
        "body",
        "The sequencer is still running.\n\n"
        "Time: {timestamp}\n\n"
        "In Progress:\n{in_progress}",
    )

    placeholders = {
        "timestamp": timestamp,
        "in_progress": in_progress_text,
    }
    for key, val in placeholders.items():
        subject = subject.replace("{" + key + "}", val)
        body = body.replace("{" + key + "}", val)

    try:
        import time as _time
        import pythoncom
        import win32com.client  # noqa: delayed import
        import win32gui

        pythoncom.CoInitialize()
        try:
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = to
            cc = email_config.get("cc", "")
            if cc:
                mail.CC = cc
            mail.Subject = subject
            mail.Body = body
            mail.Display()
            _time.sleep(1)
            # Find the Outlook compose window and force it to foreground
            hwnd = win32gui.FindWindow("rctrl_renwnd32", None)
            if hwnd:
                win32gui.SetForegroundWindow(hwnd)
                _time.sleep(0.3)
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.SendKeys("%s")  # Alt+S to send
            log(f"[Heartbeat] Email sent to {to}.")
            del shell, mail, outlook
        finally:
            pythoncom.CoUninitialize()
    except ImportError:
        log("[Heartbeat] win32com/pythoncom is not installed — cannot send email.")
    except Exception as exc:
        log(f"[Heartbeat] Failed to send email: {exc}")


def task_key(task: dict[str, Any]) -> str:
    configured_id = task.get("id")
    if configured_id:
        return str(configured_id)
    return f"{task['name']}::{task['path']}"


def run_scheduler_pass(
    config_path: Path,
    now: dt.datetime,
    dry_run: bool,
    default_interpreter: str | None = None,
    subproject_interpreters: dict[Path, str] | None = None,
    ctx: SchedulerContext | None = None,
) -> int:
    _refresh_sys_path()
    lib_pythonpath = os.pathsep.join(_scan_lib_paths())

    _sub_interps = subproject_interpreters or {}

    def _resolve_interpreter(task: dict[str, Any], script: Path) -> str | None:
        """Pick the best interpreter: subproject venv > default venv."""
        # Check if script lives inside a bootstrapped subproject
        resolved = script.resolve()
        for proj_dir, interp in _sub_interps.items():
            try:
                resolved.relative_to(proj_dir)
                return interp
            except ValueError:
                continue
        return default_interpreter

    config = load_config(config_path)

    settings = config.get("settings") or {}
    configure_log_runtime(config_path, settings)
    retry_delay_seconds = float(settings.get("retry_delay_seconds", 60))
    retry_max_delay_seconds = float(settings.get("retry_max_delay_seconds", 1800))
    email_config = settings.get("failure_email") or {}
    log_task_output = settings.get("log_task_output", True) is not False
    use_workers_raw = to_int(settings.get("use_workers"), 1)
    default_worker_cost = parse_worker_setting(settings.get("default_worker_cost", 100), 100)
    use_workers = use_workers_raw != 0
    max_workers = parse_worker_setting(settings.get("max_workers", "4"), 4)
    if use_workers:
        psutil_avail = _try_import_psutil() is not None
        if not psutil_avail:
            log("[Warn] psutil is not installed. Parallel mode requires psutil for auto-profiling.")
            use_workers = False
        else:
            log(f"Parallel mode enabled (use_workers={use_workers_raw}). max_workers={max_workers}")
    else:
        log("Running in sequential mode (use_workers=0).")

    # --- State: use SchedulerContext if provided, otherwise local ---
    if ctx is not None:
        state = ctx.state
        last_triggered_slot = ctx.last_triggered_slot
        in_progress = ctx.in_progress
        profiling = ctx.profiling
        state_lock = ctx.state_lock
        state_path = ctx.state_path
        slot_limiter = ctx.slot_limiter

        mark_task_started = ctx.mark_task_started
        mark_task_finished = ctx.mark_task_finished
        clear_recovery_entry = ctx.clear_recovery_entry
        mark_slot_consumed_without_run = ctx.mark_slot_consumed_without_run
        get_last_slot = ctx.get_last_slot
    else:
        state_path = (config_path.parent / "sequencer_state.json").resolve()

        state = load_state(state_path)
        last_triggered_slot = state["last_triggered_slot"]
        in_progress = state["in_progress"]
        profiling = state["profiling"]
        state_lock = threading.Lock()
        slot_limiter = None  # created later if needed

        def mark_task_started(
            key: str,
            task_name: str,
            script_path: Path,
            worker_cost: int,
            slot_key_value: str,
            is_recovery: bool,
        ) -> None:
            with state_lock:
                in_progress[key] = {
                    "task_name": task_name,
                    "script_path": str(script_path),
                    "worker_cost": worker_cost,
                    "slot_key": slot_key_value,
                    "started_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_recovery": is_recovery,
                }
                save_state(state_path, state)

        def get_last_slot(key: str) -> str | None:
            """Retrieve the last slot string, handling both legacy and new formats."""
            entry = last_triggered_slot.get(key)
            if isinstance(entry, dict):
                return str(entry.get("slot", "")) or None
            if isinstance(entry, str):
                return entry
            return None

        def mark_task_finished(key: str, slot_key_value: str, success: bool) -> None:
            with state_lock:
                in_progress.pop(key, None)
                now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                prev = last_triggered_slot.get(key)
                prev_count = prev.get("retry_count", 0) if isinstance(prev, dict) else 0
                last_triggered_slot[key] = {
                    "slot": slot_key_value,
                    "last_run": now_str,
                    "outcome": "success" if success else "failure",
                    "retry_count": 0 if success else prev_count + 1,
                }
                save_state(state_path, state)

        def clear_recovery_entry(key: str) -> None:
            with state_lock:
                if key in in_progress:
                    in_progress.pop(key, None)
                    save_state(state_path, state)

        def mark_slot_consumed_without_run(key: str, slot_key_value: str) -> None:
            with state_lock:
                in_progress.pop(key, None)
                now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                last_triggered_slot[key] = {
                    "slot": slot_key_value,
                    "last_run": now_str,
                    "outcome": "skipped",
                }
                save_state(state_path, state)

    raw_tasks = config.get("tasks") or []
    if not isinstance(raw_tasks, list):
        log("[Error] `tasks` must be a list.")
        return 1

    slot_time = now.replace(second=0, microsecond=0)
    log(f"Heartbeat: {slot_time.strftime('%Y-%m-%d %H:%M')}")

    had_error = False
    slot_key = slot_time.strftime("%Y-%m-%d %H:%M")
    scheduled_keys: set[str] = set()
    recovered_keys: set[str] = set()
    tasks_to_execute: list[dict[str, Any]] = []
    validated_tasks: list[dict[str, Any]] = []
    task_by_key: dict[str, dict[str, Any]] = {}

    for index, raw_task in enumerate(raw_tasks, start=1):
        try:
            task = validate_task(raw_task, index)
        except ValueError as exc:
            log(f"[Error] {exc}")
            had_error = True
            continue

        validated_tasks.append(task)
        key = task_key(task)
        if key not in task_by_key:
            task_by_key[key] = task

    # Validate dependency references.
    all_task_names = {t["name"] for t in validated_tasks}
    for task in validated_tasks:
        for dep in task.get("_depends_on", []):
            if dep not in all_task_names:
                log(f"[Error] Task `{task['name']}` depends on unknown task `{dep}`.")
                had_error = True

    # Process commands from monitor (UDP queue + leftover trigger files).
    if ctx is not None:
        with state_lock:
            run_now_set = process_commands(ctx.command_queue, state)
            run_now_set |= process_triggers(config_path, state)
            save_state(state_path, state)
    else:
        run_now_set = process_triggers(config_path, state)
    paused_tasks_set = set(state.get("paused_tasks", []))

    for key, recovery_entry in list(in_progress.items()):
        # In daemon mode, skip tasks still running from a previous tick
        if ctx is not None and ctx.is_task_actively_running(key):
            log(f"[Skip] `{key}` still running from a previous tick.")
            scheduled_keys.add(key)
            continue

        task = task_by_key.get(key)
        if task is None:
            log(f"[Warn] Dropping recovery entry for unknown task key `{key}`.")
            clear_recovery_entry(key)
            continue

        if key in paused_tasks_set:
            log(f"[Skip] Recovery for paused task `{task['name']}`.")
            clear_recovery_entry(key)
            continue

        script_path = (config_path.parent / task["path"]).resolve()
        if not script_path.exists():
            log(
                f"[Error] Recovery script not found for task `{task['name']}`: "
                f"{script_path}"
            )
            had_error = True
            clear_recovery_entry(key)
            continue

        worker_cost = resolve_dynamic_worker_cost(key, profiling, max_workers, default_worker_cost)
        log(f"[Profile] Recovery task `{task['name']}` using dynamic cost={worker_cost}")
        if worker_cost > max_workers:
            log(
                f"[Error] Recovery task `{task['name']}` requires "
                f"worker_cost={worker_cost}, which exceeds max_workers={max_workers}."
            )
            had_error = True
            clear_recovery_entry(key)
            continue

        previous_slot = ""
        if isinstance(recovery_entry, dict):
            previous_slot = str(recovery_entry.get("slot_key", "")).strip()

        if previous_slot:
            log(
                f"[Recovery] Re-queueing interrupted task `{task['name']}` "
                f"from slot {previous_slot}."
            )
        else:
            log(f"[Recovery] Re-queueing interrupted task `{task['name']}`.")

        tasks_to_execute.append(
            {
                "key": key,
                "task_name": task["name"],
                "script_path": script_path,
                "worker_cost": worker_cost,
                "is_recovery": True,
                "interpreter": _resolve_interpreter(task, script_path),
                "timeout_minutes": task.get("_timeout_minutes"),
            }
        )
        scheduled_keys.add(key)
        recovered_keys.add(key)

    for task in validated_tasks:
        if not should_run(task, now):
            continue

        key = task_key(task)
        if key in scheduled_keys:
            if key in recovered_keys:
                log(
                    f"[Skip] {task['name']} already queued for crash recovery in slot "
                    f"{slot_key}."
                )
            else:
                log(f"[Skip] {task['name']} already triggered in slot {slot_key}.")
            continue

        last_entry = last_triggered_slot.get(key)
        if get_last_slot(key) == slot_key:
            # Allow retry if the previous run failed and retry_delay_seconds has elapsed.
            is_failed = isinstance(last_entry, dict) and last_entry.get("outcome") == "failure"
            if is_failed:
                last_run_str = last_entry.get("last_run", "")
                try:
                    last_run_dt = dt.datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
                    elapsed = (now - last_run_dt).total_seconds()
                    retry_count = last_entry.get("retry_count", 0)
                    delay = compute_retry_delay(retry_delay_seconds, retry_count, retry_max_delay_seconds)
                    if elapsed < delay:
                        continue
                    log(f"[Retry] {task['name']} — attempt #{retry_count + 1} after {delay:.0f}s backoff.")
                except (ValueError, TypeError):
                    pass  # Malformed timestamp; fall through and retry.
            else:
                log(f"[Skip] {task['name']} already triggered in slot {slot_key}.")
                continue

        # Check task dependencies.
        deps = task.get("_depends_on", [])
        if deps:
            all_deps_ok = True
            for dep_name in deps:
                dep_key = next((task_key(t) for t in validated_tasks if t["name"] == dep_name), None)
                if dep_key is None:
                    all_deps_ok = False
                    break
                dep_entry = last_triggered_slot.get(dep_key)
                dep_in_slot = isinstance(dep_entry, dict) and dep_entry.get("slot") == slot_key
                dep_succeeded = isinstance(dep_entry, dict) and dep_entry.get("outcome") == "success"
                if not (dep_in_slot and dep_succeeded):
                    all_deps_ok = False
                    break
            if not all_deps_ok:
                continue

        if key in paused_tasks_set:
            continue

        script_path = (config_path.parent / task["path"]).resolve()
        if not script_path.exists():
            log(f"[Error] Script not found for task `{task['name']}`: {script_path}")
            had_error = True
            mark_slot_consumed_without_run(key, slot_key)
            scheduled_keys.add(key)
            continue

        worker_cost = resolve_dynamic_worker_cost(key, profiling, max_workers, default_worker_cost)
        log(f"[Profile] {task['name']} using dynamic cost={worker_cost}")
        if worker_cost > max_workers:
            log(
                f"[Error] Task `{task['name']}` requires worker_cost={worker_cost}, "
                f"which exceeds max_workers={max_workers}."
            )
            had_error = True
            mark_slot_consumed_without_run(key, slot_key)
            scheduled_keys.add(key)
            continue

        tasks_to_execute.append(
            {
                "key": key,
                "task_name": task["name"],
                "script_path": script_path,
                "worker_cost": worker_cost,
                "is_recovery": False,
                "interpreter": _resolve_interpreter(task, script_path),
                "timeout_minutes": task.get("_timeout_minutes"),
            }
        )
        scheduled_keys.add(key)

    # --- Retry pass: re-queue failed tasks whose retry_delay_seconds has elapsed ---
    for task in validated_tasks:
        key = task_key(task)
        if key in scheduled_keys:
            continue
        last_entry = last_triggered_slot.get(key)
        if not (isinstance(last_entry, dict) and last_entry.get("outcome") == "failure"):
            continue
        last_run_str = last_entry.get("last_run", "")
        try:
            last_run_dt = dt.datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            elapsed = (now - last_run_dt).total_seconds()
            retry_count = last_entry.get("retry_count", 0)
            delay = compute_retry_delay(retry_delay_seconds, retry_count, retry_max_delay_seconds)
            if elapsed < delay:
                continue
        except (ValueError, TypeError):
            pass  # Malformed timestamp; fall through and retry.

        # Check task dependencies.
        deps = task.get("_depends_on", [])
        if deps:
            all_deps_ok = True
            for dep_name in deps:
                dep_key = next((task_key(t) for t in validated_tasks if t["name"] == dep_name), None)
                if dep_key is None:
                    all_deps_ok = False
                    break
                dep_entry = last_triggered_slot.get(dep_key)
                dep_succeeded = isinstance(dep_entry, dict) and dep_entry.get("outcome") == "success"
                if not dep_succeeded:
                    all_deps_ok = False
                    break
            if not all_deps_ok:
                continue

        if key in paused_tasks_set:
            continue

        script_path = (config_path.parent / task["path"]).resolve()
        if not script_path.exists():
            log(f"[Error] Script not found for task `{task['name']}`: {script_path}")
            had_error = True
            continue

        worker_cost = resolve_dynamic_worker_cost(key, profiling, max_workers, default_worker_cost)
        log(f"[Profile] Retry task `{task['name']}` using dynamic cost={worker_cost}")
        if worker_cost > max_workers:
            log(
                f"[Error] Task `{task['name']}` requires worker_cost={worker_cost}, "
                f"which exceeds max_workers={max_workers}."
            )
            had_error = True
            continue

        retry_count = last_entry.get("retry_count", 0)
        delay = compute_retry_delay(retry_delay_seconds, retry_count, retry_max_delay_seconds)
        log(f"[Retry] {task['name']} — attempt #{retry_count + 1} after {delay:.0f}s backoff.")
        tasks_to_execute.append(
            {
                "key": key,
                "task_name": task["name"],
                "script_path": script_path,
                "worker_cost": worker_cost,
                "is_recovery": False,
                "interpreter": _resolve_interpreter(task, script_path),
                "timeout_minutes": task.get("_timeout_minutes"),
            }
        )
        scheduled_keys.add(key)

    # --- Run-now pass: queue tasks triggered on-demand from the monitor ---
    if run_now_set:
        for task in validated_tasks:
            key = task_key(task)
            if key not in run_now_set:
                continue
            if key in scheduled_keys:
                continue
            if key in paused_tasks_set:
                log(f"[Skip] Run-now ignored for paused task `{task['name']}`. Unpause it first.")
                continue

            script_path = (config_path.parent / task["path"]).resolve()
            if not script_path.exists():
                log(f"[Error] Script not found for task `{task['name']}`: {script_path}")
                had_error = True
                continue

            worker_cost = resolve_dynamic_worker_cost(key, profiling, max_workers, default_worker_cost)
            log(f"[Run Now] {task['name']} — triggered from monitor (cost={worker_cost})")
            if worker_cost > max_workers:
                log(
                    f"[Error] Task `{task['name']}` requires worker_cost={worker_cost}, "
                    f"which exceeds max_workers={max_workers}."
                )
                had_error = True
                continue

            tasks_to_execute.append(
                {
                    "key": key,
                    "task_name": task["name"],
                    "script_path": script_path,
                    "worker_cost": worker_cost,
                    "is_recovery": False,
                    "interpreter": _resolve_interpreter(task, script_path),
                    "timeout_minutes": task.get("_timeout_minutes"),
                }
            )
            scheduled_keys.add(key)

    thread_worker_count = min(max_workers, len(tasks_to_execute)) if use_workers else 1
    if thread_worker_count > 1:
        log(
            f"Executing {len(tasks_to_execute)} task(s) with "
            f"slot capacity={max_workers} and thread_workers={thread_worker_count}."
        )

    def _handle_profiled_result(
        task_run: dict[str, Any],
        result: bool | tuple[bool, float, float],
    ) -> bool:
        """If result is a profiled tuple, update profiling state and return success."""
        if isinstance(result, tuple):
            success, peak_ram, avg_cpu = result
            key = task_run["key"]
            with state_lock:
                update_profiling_state(
                    key, profiling,
                    peak_ram, avg_cpu, max_workers,
                )
            learned = profiling.get(key, {}).get("learned_cost", "?")
            log(
                f"[Profile] {task_run['task_name']}: "
                f"peak_ram={peak_ram:.1f}% avg_cpu={avg_cpu:.1f}% "
                f"-> learned_cost={learned}"
            )
            return success
        return result

    # --- Fire-and-forget mode (daemon with SchedulerContext) ---
    if ctx is not None and tasks_to_execute:
        for task_run in tasks_to_execute:
            _key = task_run["key"]
            ctx.actively_running.add(_key)

        def _daemon_task_wrapper(task_run: dict[str, Any]) -> None:
            _key = task_run["key"]
            _timeout_min = task_run.get("timeout_minutes")
            _timeout_sec = _timeout_min * 60 if _timeout_min is not None else None
            try:
                ctx.mark_task_started(
                    key=_key,
                    task_name=task_run["task_name"],
                    script_path=task_run["script_path"],
                    worker_cost=task_run["worker_cost"],
                    slot_key_value=slot_key,
                    is_recovery=task_run["is_recovery"],
                )
                if use_workers:
                    raw_result = run_with_slots(
                        task_name=task_run["task_name"],
                        script_path=task_run["script_path"],
                        worker_cost=task_run["worker_cost"],
                        slot_limiter=ctx.slot_limiter,
                        working_directory=config_path.parent,
                        dry_run=dry_run,
                        email_config=email_config,
                        log_task_output=log_task_output,
                        lib_pythonpath=lib_pythonpath,
                        interpreter=task_run.get("interpreter"),
                        profiled=True,
                        timeout_seconds=_timeout_sec,
                    )
                else:
                    raw_result = run_task_profiled(
                        task_name=task_run["task_name"],
                        script_path=task_run["script_path"],
                        working_directory=config_path.parent,
                        dry_run=dry_run,
                        email_config=email_config,
                        log_task_output=log_task_output,
                        lib_pythonpath=lib_pythonpath,
                        interpreter=task_run.get("interpreter"),
                        timeout_seconds=_timeout_sec,
                    )
                succeeded = _handle_profiled_result(task_run, raw_result)
            except Exception as exc:
                log(f"[Error] {task_run['task_name']} crashed in executor: {exc}")
                succeeded = False
            ctx.mark_task_finished(_key, slot_key, succeeded)
            if not succeeded:
                log(f"[Warn] {task_run['task_name']} finished with failure.")

        for task_run in tasks_to_execute:
            ctx._executor.submit(_daemon_task_wrapper, task_run)

        return 0

    # --- Blocking mode (CLI one-shot or sequential) ---
    if thread_worker_count <= 1:
        for task_run in tasks_to_execute:
            key = task_run["key"]
            task_name = task_run["task_name"]
            script_path = task_run["script_path"]
            worker_cost = task_run["worker_cost"]
            is_recovery = task_run["is_recovery"]

            mark_task_started(
                key=key,
                task_name=task_name,
                script_path=script_path,
                worker_cost=worker_cost,
                slot_key_value=slot_key,
                is_recovery=is_recovery,
            )
            _timeout_min = task_run.get("timeout_minutes")
            _timeout_sec = _timeout_min * 60 if _timeout_min is not None else None
            result = run_task_profiled(
                task_name=task_name,
                script_path=script_path,
                working_directory=config_path.parent,
                dry_run=dry_run,
                email_config=email_config,
                log_task_output=log_task_output,
                lib_pythonpath=lib_pythonpath,
                interpreter=task_run.get("interpreter"),
                timeout_seconds=_timeout_sec,
            )
            succeeded = _handle_profiled_result(task_run, result)
            mark_task_finished(key, slot_key, succeeded)
    else:
        if slot_limiter is None:
            slot_limiter = WorkerSlotLimiter(max_workers)

        def execute_task(task_run: dict[str, Any]) -> bool | tuple[bool, float, float]:
            mark_task_started(
                key=task_run["key"],
                task_name=task_run["task_name"],
                script_path=task_run["script_path"],
                worker_cost=task_run["worker_cost"],
                slot_key_value=slot_key,
                is_recovery=task_run["is_recovery"],
            )
            _timeout_min = task_run.get("timeout_minutes")
            _timeout_sec = _timeout_min * 60 if _timeout_min is not None else None
            return run_with_slots(
                task_name=task_run["task_name"],
                script_path=task_run["script_path"],
                worker_cost=task_run["worker_cost"],
                slot_limiter=slot_limiter,
                working_directory=config_path.parent,
                dry_run=dry_run,
                email_config=email_config,
                log_task_output=log_task_output,
                lib_pythonpath=lib_pythonpath,
                interpreter=task_run.get("interpreter"),
                profiled=True,
                timeout_seconds=_timeout_sec,
            )

        with ThreadPoolExecutor(max_workers=thread_worker_count) as executor:
            future_to_task = {
                executor.submit(
                    execute_task,
                    task_run,
                ): task_run
                for task_run in tasks_to_execute
            }
            for future in as_completed(future_to_task):
                task_run = future_to_task[future]
                key = task_run["key"]
                task_name = task_run["task_name"]
                try:
                    raw_result = future.result()
                    succeeded = _handle_profiled_result(task_run, raw_result)
                except Exception as exc:
                    log(f"[Error] {task_name} crashed in scheduler thread: {exc}")
                    succeeded = False

                mark_task_finished(key, slot_key, succeeded)
                if not succeeded:
                    had_error = True

    save_state(state_path, state)

    if had_error:
        log("Run completed with one or more errors.")
        return 1

    log("Run completed successfully.")
    return 0


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()

    if not config_path.exists():
        log(f"[Error] Config file not found: {config_path}")
        return 1

    try:
        startup_config = load_config(config_path)
    except ValueError as exc:
        log(f"[Error] {exc}")
        return 1
    configure_log_runtime(config_path, startup_config.get("settings") or {})

    # --- Bootstrap portable venv from vendored wheels ---
    try:
        default_interpreter = _bootstrap_env()
        if default_interpreter:
            log(f"[Bootstrap] Using venv interpreter: {default_interpreter}")
    except Exception as exc:
        log(f"[Bootstrap] Warning: venv bootstrap failed ({exc}), using system Python.")
        default_interpreter = None

    try:
        subproject_interpreters = _bootstrap_subprojects(config_path.parent)
        for proj, interp in subproject_interpreters.items():
            log(f"[Bootstrap] Subproject {proj.name}: {interp}")
    except Exception as exc:
        log(f"[Bootstrap] Warning: subproject bootstrap failed ({exc}).")
        subproject_interpreters = {}

    if args.daemon:
        if args.now:
            log("[Error] --now cannot be used together with --daemon.")
            return 1

        startup_settings = startup_config.get("settings") or {}
        last_heartbeat_hour: int | None = None

        # --- Initialize SchedulerContext ---
        state_path = (config_path.parent / "sequencer_state.json").resolve()
        max_workers = parse_worker_setting(startup_settings.get("max_workers", "4"), 4)
        ctx = SchedulerContext(state_path, max_workers)
        log(f"[Daemon] SchedulerContext initialized (max_workers={max_workers}).")

        stop_event = threading.Event()

        # --- Background services thread (git pull/push) ---
        def _background_services() -> None:
            git_pull_interval = max(0, to_int(startup_settings.get("git_pull_interval_minutes"), 0))
            git_push_interval = max(0, to_int(startup_settings.get("git_push_interval_minutes"), 0))
            last_pull_time: dt.datetime | None = dt.datetime.now()
            last_push_time: dt.datetime | None = dt.datetime.now()
            fmt = "%Y-%m-%d %H:%M:%S"

            with ctx.state_lock:
                if "daemon" not in ctx.state:
                    ctx.state["daemon"] = {}
                ctx.state["daemon"]["last_pull_time"] = last_pull_time.strftime(fmt)
                ctx.state["daemon"]["last_push_time"] = last_push_time.strftime(fmt)
                save_state(ctx.state_path, ctx.state)

            while not stop_event.is_set():
                try:
                    # Reload git intervals from config
                    try:
                        cfg = load_config(config_path)
                        cfg_settings = cfg.get("settings") or {}
                        git_pull_interval = max(0, to_int(cfg_settings.get("git_pull_interval_minutes"), git_pull_interval))
                        git_push_interval = max(0, to_int(cfg_settings.get("git_push_interval_minutes"), git_push_interval))
                    except Exception:
                        pass

                    # Drain git commands from queue
                    pull_triggered = False
                    push_triggered = False
                    while True:
                        try:
                            cmd = ctx.command_queue.get_nowait()
                        except queue.Empty:
                            break
                        if cmd == "pull":
                            pull_triggered = True
                        elif cmd == "push":
                            push_triggered = True

                    # Git pull check
                    new_pull_time, pull_changed = maybe_git_pull(config_path, last_pull_time, git_pull_interval, triggered=pull_triggered)
                    last_pull_time = new_pull_time

                    if pull_changed:
                        try:
                            _sync_vendor_packages()
                            _sync_subproject_packages(config_path.parent)
                        except Exception as exc:
                            log(f"[Bootstrap] Post-pull sync failed: {exc}")
                        ctx.wake_event.set()  # config may have changed

                    # Git push check
                    new_push_time = maybe_git_push(config_path, last_push_time, git_push_interval, triggered=push_triggered)
                    push_changed = new_push_time is not last_push_time
                    last_push_time = new_push_time

                    if pull_changed or push_changed:
                        with ctx.state_lock:
                            if "daemon" not in ctx.state:
                                ctx.state["daemon"] = {}
                            if last_pull_time:
                                ctx.state["daemon"]["last_pull_time"] = last_pull_time.strftime(fmt)
                            if last_push_time:
                                ctx.state["daemon"]["last_push_time"] = last_push_time.strftime(fmt)
                            save_state(ctx.state_path, ctx.state)

                except Exception as exc:
                    log(f"[Background] Unexpected error: {exc}")

                # Sleep until the next git operation is due
                now_bg = dt.datetime.now()
                waits: list[float] = []
                if git_pull_interval > 0 and last_pull_time is not None:
                    next_pull = (last_pull_time + dt.timedelta(minutes=git_pull_interval) - now_bg).total_seconds()
                    waits.append(max(1, next_pull))
                if git_push_interval > 0 and last_push_time is not None:
                    next_push = (last_push_time + dt.timedelta(minutes=git_push_interval) - now_bg).total_seconds()
                    waits.append(max(1, next_push))
                sleep_bg = min(waits) if waits else 600
                ctx.git_wake_event.clear()
                ctx.git_wake_event.wait(sleep_bg)
                if stop_event.is_set():
                    break

        def _udp_listener() -> None:
            """Listen for UDP command messages from monitor (zero-CPU blocking wait).

            Protocol: UTF-8 encoded commands, one per packet:
              pull, push, pause:<id>, unpause:<id>, run:<id>
            """
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1.0)
            try:
                sock.bind(("127.0.0.1", WAKE_UDP_PORT))
            except OSError as exc:
                log(f"[UDP] Could not bind port {WAKE_UDP_PORT}: {exc}")
                return
            while not stop_event.is_set():
                try:
                    data, _ = sock.recvfrom(256)
                    cmd = data.decode("utf-8", errors="ignore").strip()
                    if cmd in ("pull", "push"):
                        ctx.command_queue.put(cmd)
                        ctx.git_wake_event.set()
                    elif cmd.startswith(("pause:", "unpause:", "run:")):
                        ctx.command_queue.put(cmd)
                        ctx.wake_event.set()
                except socket.timeout:
                    pass
                except OSError:
                    break
            sock.close()

        bg_thread = threading.Thread(target=_background_services, daemon=True)
        bg_thread.start()
        udp_thread = threading.Thread(target=_udp_listener, daemon=True)
        udp_thread.start()
        log("Starting daemon mode (event-driven sleep).")
        log(f"[Background] Git sync thread started (pull every {to_int(startup_settings.get('git_pull_interval_minutes'), 0)} min, push on task completion).")
        log(f"[Background] UDP wake listener on 127.0.0.1:{WAKE_UDP_PORT}.")

        try:
            while True:
                tick_time = dt.datetime.now()
                try:
                    run_scheduler_pass(
                        config_path=config_path,
                        now=tick_time,
                        dry_run=args.dry_run,
                        default_interpreter=default_interpreter,
                        subproject_interpreters=subproject_interpreters,
                        ctx=ctx,
                    )
                except ValueError as exc:
                    log(f"[Error] {exc}")
                except Exception as exc:
                    log(f"[Error] Daemon tick failed unexpectedly: {exc}")

                # --- heartbeat email check ---
                try:
                    cfg = load_config(config_path)
                    hb_email_cfg = (cfg.get("settings") or {}).get("heartbeat_email") or {}
                    if hb_email_cfg.get("enabled", False):
                        now_hb = dt.datetime.now()
                        hb_hours_raw = hb_email_cfg.get("hours")
                        if hb_hours_raw is not None:
                            allowed_hours = {int(h.strip()) for h in str(hb_hours_raw).split(",") if h.strip()}
                        else:
                            allowed_hours = set(range(24))
                        if now_hb.hour in allowed_hours and last_heartbeat_hour != now_hb.hour:
                            send_heartbeat_email(hb_email_cfg, state_path)
                            last_heartbeat_hour = now_hb.hour
                            with ctx.state_lock:
                                if "daemon" not in ctx.state:
                                    ctx.state["daemon"] = {}
                                ctx.state["daemon"]["last_heartbeat_hour"] = last_heartbeat_hour
                                save_state(ctx.state_path, ctx.state)
                except Exception as exc:
                    log(f"[Heartbeat] Error: {exc}")

                # --- event-driven sleep until next job ---
                try:
                    cfg_sleep = load_config(config_path)
                    raw_tasks_sleep = cfg_sleep.get("tasks") or []
                    validated_sleep = []
                    for i, rt in enumerate(raw_tasks_sleep, 1):
                        try:
                            validated_sleep.append(validate_task(rt, i))
                        except ValueError:
                            pass
                    stgs_sleep = cfg_sleep.get("settings") or {}
                    paused_sleep = set(ctx.state.get("paused_tasks", []))
                    next_wake = compute_next_wake_time(
                        validated_sleep, ctx.state, stgs_sleep,
                        dt.datetime.now(), paused_sleep,
                    )
                    sleep_secs = max(0, (next_wake - dt.datetime.now()).total_seconds())
                except Exception:
                    sleep_secs = 60  # safe fallback

                log(f"[Sleep] Next wake in {sleep_secs:.0f}s")
                ctx.wake_event.clear()
                ctx.wake_event.wait(timeout=sleep_secs)
        except KeyboardInterrupt:
            log("Daemon stopped by user. Waiting for running tasks to finish...")
            stop_event.set()
            ctx.shutdown(wait=True)
            bg_thread.join(timeout=5)
            log("All tasks finished. Exiting.")
            return 0

    try:
        now = parse_now(args.now)
        return run_scheduler_pass(
            config_path=config_path,
            now=now,
            dry_run=args.dry_run,
            default_interpreter=default_interpreter,
            subproject_interpreters=subproject_interpreters,
        )
    except ValueError as exc:
        log(f"[Error] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
