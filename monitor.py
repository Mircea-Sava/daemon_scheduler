"""Live terminal dashboard for the YAML-driven sequencer."""

from __future__ import annotations

import json
import msvcrt
import socket
import sys
import threading
from pathlib import Path

_LIBS_DIR = Path(__file__).resolve().parent / "libs"


def _refresh_sys_path() -> None:
    if not _LIBS_DIR.is_dir():
        return
    for p in sorted(str(d) for d in _LIBS_DIR.iterdir() if d.is_dir()):
        if p not in sys.path:
            sys.path.insert(0, p)


_refresh_sys_path()

try:
    import yaml
except ImportError:
    yaml = None

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("rich is required: pip install rich")
    raise SystemExit(1)

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "sequencer_state.json"
SETTINGS_FILE = BASE_DIR / "settings.yaml"
SCHEDULE_FILE = BASE_DIR / "schedule.yaml"
WAKE_UDP_PORT = 19876


def _send_command(cmd: str) -> None:
    """Send a command to the sequencer via UDP (e.g. 'pull', 'pause:task_id')."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(cmd.encode("utf-8"), ("127.0.0.1", WAKE_UDP_PORT))
        sock.close()
    except OSError:
        pass


REFRESH_SECONDS = 2
PAGE_SIZE = 10
SECTIONS = ["tasks", "profiling", "schedule"]

_quit_event = threading.Event()
_lock = threading.Lock()
_focus_index = 0  # index into SECTIONS
_scroll_offsets = {"tasks": 0, "profiling": 0, "schedule": 0}
_visible_task_keys: list[str] = []  # populated by build_task_table for pause toggle


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def load_settings() -> dict:
    if yaml is None or not SETTINGS_FILE.exists():
        return {}
    try:
        data = yaml.safe_load(SETTINGS_FILE.read_text(encoding="utf-8")) or {}
        return data.get("settings", data) if isinstance(data, dict) else {}
    except Exception:
        return {}



def _clamp_offset(offset: int, total: int, visible: int) -> int:
    max_offset = max(0, total - visible)
    return max(0, min(offset, max_offset))


def build_task_table(state: dict, offset: int, focused: bool) -> tuple[Panel, int]:
    global _visible_task_keys
    last_slots = state.get("last_triggered_slot", {})
    in_progress = state.get("in_progress", {})
    paused_tasks = set(state.get("paused_tasks", []))
    all_keys_set = set(list(last_slots.keys()) + list(in_progress.keys()))

    def _sort_key(k: str) -> tuple[int, str]:
        entry = last_slots.get(k, {})
        outcome = entry.get("outcome", "") if isinstance(entry, dict) else ""
        if outcome == "failure":
            return (0, k)
        if k in in_progress:
            return (1, k)
        if k in paused_tasks:
            return (2, k)
        return (3, k)

    all_keys = sorted(all_keys_set, key=_sort_key)
    total = len(all_keys)

    offset = _clamp_offset(offset, total, PAGE_SIZE)
    visible_keys = all_keys[offset:offset + PAGE_SIZE]
    _visible_task_keys = list(visible_keys)

    # Which row is selected (relative to visible page)
    selected_row = _scroll_offsets.get("tasks", 0) - offset if focused else -1

    table = Table(expand=True)
    table.add_column("Task ID", style="cyan", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("Last Slot", justify="center")
    table.add_column("Outcome", justify="center")
    table.add_column("Last Run", justify="center")

    if not visible_keys:
        table.add_row("(no tasks)", "", "", "", "")
    else:
        for row_idx, key in enumerate(visible_keys):
            is_selected = focused and row_idx == selected_row

            if key in paused_tasks:
                status = Text("PAUSED", style="bold magenta")
            elif key in in_progress:
                status = Text("RUNNING", style="bold yellow")
            else:
                status = Text("Idle", style="dim")

            entry = last_slots.get(key, {})
            if isinstance(entry, dict):
                slot = entry.get("slot", "")
                outcome_raw = entry.get("outcome", "")
                last_run = entry.get("last_run", "")
            else:
                slot = str(entry)
                outcome_raw = ""
                last_run = ""

            if outcome_raw == "success":
                outcome = Text("success", style="bold green")
            elif outcome_raw == "failure":
                outcome = Text("FAILURE", style="bold red")
            elif outcome_raw == "skipped":
                outcome = Text("skipped", style="yellow")
            else:
                outcome = Text(outcome_raw or "-", style="dim")

            # Highlight selected row
            if is_selected:
                key_text = Text(f"> {key}", style="bold white")
            else:
                key_text = Text(f"  {key}", style="cyan")

            table.add_row(key_text, status, slot, outcome, last_run)

    page_info = f" ({offset + 1}-{offset + len(visible_keys)}/{total})" if total > PAGE_SIZE else ""
    border = "bold cyan" if focused else "dim"
    title = f"Task Status{page_info}"
    return Panel(table, title=title, border_style=border), offset


def build_profiling_table(state: dict, offset: int, focused: bool) -> tuple[Panel, int]:
    profiling = state.get("profiling", {})
    all_keys = sorted(k for k, v in profiling.items() if isinstance(v, dict))
    total = len(all_keys)

    offset = _clamp_offset(offset, total, PAGE_SIZE)
    visible_keys = all_keys[offset:offset + PAGE_SIZE]

    table = Table(expand=True)
    table.add_column("Task ID", style="cyan", no_wrap=True)
    table.add_column("Peak RAM %", justify="right")
    table.add_column("Avg CPU %", justify="right")
    table.add_column("Learned Cost", justify="right")

    if not visible_keys:
        table.add_row("(no profiling data)", "", "", "")
    else:
        for key in visible_keys:
            entry = profiling[key]
            ram = f"{entry.get('peak_ram_pct', 0):.1f}%"
            cpu = f"{entry.get('avg_cpu_pct', 0):.1f}%"
            cost = str(entry.get("learned_cost", "?"))
            table.add_row(key, ram, cpu, cost)

    page_info = f" ({offset + 1}-{offset + len(visible_keys)}/{total})" if total > PAGE_SIZE else ""
    border = "bold cyan" if focused else "dim"
    title = f"Profiling{page_info}"
    return Panel(table, title=title, border_style=border), offset


def build_schedule_table(offset: int, focused: bool) -> tuple[Panel, int]:
    """Show today's schedule: which tasks run at which times."""
    import datetime as dt

    now = dt.datetime.now()
    today = now.date()

    # Load schedule.yaml
    if yaml is None or not SCHEDULE_FILE.exists():
        return Panel("(schedule.yaml not found)", title="Today's Schedule", border_style="dim"), 0

    try:
        data = yaml.safe_load(SCHEDULE_FILE.read_text(encoding="utf-8")) or {}
    except Exception:
        return Panel("(failed to read schedule.yaml)", title="Today's Schedule", border_style="dim"), 0

    raw_tasks = data.get("tasks") or []
    if not isinstance(raw_tasks, list):
        return Panel("(no tasks)", title="Today's Schedule", border_style="dim"), 0

    # Validate tasks using sequencer's validate_task
    try:
        from sequencer import validate_task, should_run
    except ImportError:
        return Panel("(cannot import sequencer)", title="Today's Schedule", border_style="dim"), 0

    validated = []
    for i, raw in enumerate(raw_tasks, start=1):
        try:
            validated.append(validate_task(raw, i))
        except (ValueError, Exception):
            continue

    if not validated:
        return Panel("(no valid tasks)", title="Today's Schedule", border_style="dim"), 0

    # Build timeline: check every minute of today
    timeline: list[tuple[str, list[str]]] = []
    seen_slots: dict[str, set[str]] = {}  # time_str -> set of task names

    for hour in range(24):
        for minute in range(60):
            check_time = dt.datetime(today.year, today.month, today.day, hour, minute)
            running = []
            for task in validated:
                if should_run(task, check_time):
                    running.append(task["name"])
            if running:
                time_str = f"{hour:02d}:{minute:02d}"
                task_set = frozenset(running)
                # Collapse consecutive identical entries
                if timeline and timeline[-1][1] == running:
                    continue
                timeline.append((time_str, running))

    total = len(timeline)
    offset = _clamp_offset(offset, total, PAGE_SIZE)
    visible = timeline[offset:offset + PAGE_SIZE]

    table = Table(expand=True)
    table.add_column("Time", style="cyan", no_wrap=True, width=7)
    table.add_column("Tasks", style="white")

    if not visible:
        table.add_row("(none)", "No tasks scheduled today")
    else:
        for time_str, tasks in visible:
            # Highlight past/current/future
            hour, minute = map(int, time_str.split(":"))
            slot_time = dt.datetime(today.year, today.month, today.day, hour, minute)
            if slot_time < now.replace(second=0, microsecond=0):
                style = "dim"
            elif slot_time == now.replace(second=0, microsecond=0):
                style = "bold green"
            else:
                style = ""
            task_list = ", ".join(tasks)
            table.add_row(Text(time_str, style=style), Text(task_list, style=style))

    page_info = f" ({offset + 1}-{offset + len(visible)}/{total})" if total > PAGE_SIZE else ""
    border = "bold cyan" if focused else "dim"
    title = f"Today's Schedule{page_info}"
    return Panel(table, title=title, border_style=border), offset


def _format_countdown(last_time_str: str | None, interval_minutes: int) -> str:
    """Return a human-readable countdown like '12m 34s' or 'now'."""
    import datetime as dt
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


def _next_heartbeat_str(settings: dict) -> str:
    """Return the next scheduled heartbeat hour like 'Next: 17:00'."""
    import datetime as dt
    hb_cfg = settings.get("heartbeat_email", {})
    if not hb_cfg.get("enabled", False):
        return "disabled"
    hours_raw = hb_cfg.get("hours")
    if hours_raw is not None:
        allowed = sorted({int(h.strip()) for h in str(hours_raw).split(",") if h.strip()})
    else:
        allowed = list(range(24))
    if not allowed:
        return "no hours configured"
    now_hour = dt.datetime.now().hour
    # Find next hour strictly after current
    future = [h for h in allowed if h > now_hour]
    next_h = future[0] if future else allowed[0]
    return f"{next_h:02d}:00"


def build_git_panel(settings: dict, state: dict) -> Panel:
    pull_interval = settings.get("git_pull_interval_minutes", 0)

    daemon = state.get("daemon", {})
    last_pull = daemon.get("last_pull_time")
    last_push = daemon.get("last_push_time")

    lines = []
    if pull_interval:
        countdown = _format_countdown(last_pull, pull_interval)
        suffix = countdown if countdown else ("awaiting first sync" if not last_pull else "")
        lines.append(f"Pull: smart, every {pull_interval} min  —  {suffix}" if suffix else f"Pull: smart, every {pull_interval} min")
    else:
        lines.append("Pull: disabled")

    push_status = f"last: {last_push}" if last_push else "awaiting first push"
    lines.append(f"Push: after task completion  —  {push_status}")

    hb_next = _next_heartbeat_str(settings)
    lines.append(f"Heartbeat email: next at {hb_next}" if hb_next not in ("disabled", "no hours configured") else f"Heartbeat email: {hb_next}")

    return Panel("\n".join(lines), title="Git Sync & Heartbeat", border_style="blue")



def build_help_panel() -> Panel:
    return Panel(
        "[bold]Tab[/bold] = switch section  |  "
        "[bold]Up/Down[/bold] = scroll  |  "
        "[bold]Space[/bold] = pause/resume task  |  "
        "[bold]r[/bold] = run task now  |  "
        "[bold]p[/bold] = git pull  |  "
        "[bold]u[/bold] = git push  |  "
        "[bold]q[/bold] = quit",
        title="Controls",
        border_style="dim",
    )


def build_display() -> Layout:
    import datetime as dt

    state = load_json(STATE_FILE)
    settings = load_settings()

    with _lock:
        focus = SECTIONS[_focus_index]
        task_offset = _scroll_offsets["tasks"]
        prof_offset = _scroll_offsets["profiling"]
        sched_offset = _scroll_offsets["schedule"]

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    focus_label = focus.capitalize()
    header = Text(
        f"  Sequencer Monitor  |  {now_str}  |  Focus: {focus_label}  ",
        style="bold white on blue",
    )

    task_panel, task_offset = build_task_table(state, task_offset, focus == "tasks")
    prof_panel, prof_offset = build_profiling_table(state, prof_offset, focus == "profiling")
    sched_panel, sched_offset = build_schedule_table(sched_offset, focus == "schedule")

    with _lock:
        _scroll_offsets["tasks"] = task_offset
        _scroll_offsets["profiling"] = prof_offset
        _scroll_offsets["schedule"] = sched_offset

    layout = Layout()
    layout.split_column(
        Layout(Panel(header, border_style="blue"), name="header", size=3),
        Layout(task_panel, name="tasks"),
        Layout(prof_panel, name="profiling"),
        Layout(sched_panel, name="schedule"),
        Layout(build_git_panel(settings, state), name="git", size=8),
        Layout(build_help_panel(), name="help", size=3),
    )

    return layout



def key_listener() -> None:
    global _focus_index
    while not _quit_event.is_set():
        if msvcrt.kbhit():
            ch = msvcrt.getch()

            # Arrow keys come as two bytes: b'\xe0' or b'\x00' followed by the key code
            if ch in (b"\xe0", b"\x00"):
                if msvcrt.kbhit():
                    arrow = msvcrt.getch()
                    with _lock:
                        section = SECTIONS[_focus_index]
                        if arrow == b"H":  # Up
                            _scroll_offsets[section] = max(0, _scroll_offsets[section] - 1)
                        elif arrow == b"P":  # Down
                            _scroll_offsets[section] += 1
                continue

            decoded = ch.decode("utf-8", errors="ignore").lower()
            if decoded == "q":
                _quit_event.set()
                return
            elif decoded == "\t":
                with _lock:
                    _focus_index = (_focus_index + 1) % len(SECTIONS)
            elif decoded == "p":
                _send_command("pull")
            elif decoded == "u":
                _send_command("push")
            elif decoded == " ":
                with _lock:
                    if SECTIONS[_focus_index] == "tasks" and _visible_task_keys:
                        idx = _scroll_offsets.get("tasks", 0)
                        page_start = _clamp_offset(idx, len(_visible_task_keys) + idx, PAGE_SIZE)
                        row = idx - page_start
                        if 0 <= row < len(_visible_task_keys):
                            task_id = _visible_task_keys[row]
                            state = load_json(STATE_FILE)
                            paused = state.get("paused_tasks", [])
                            if task_id in paused:
                                _send_command(f"unpause:{task_id}")
                            else:
                                _send_command(f"pause:{task_id}")
            elif decoded == "r":
                with _lock:
                    if SECTIONS[_focus_index] == "tasks" and _visible_task_keys:
                        idx = _scroll_offsets.get("tasks", 0)
                        page_start = _clamp_offset(idx, len(_visible_task_keys) + idx, PAGE_SIZE)
                        row = idx - page_start
                        if 0 <= row < len(_visible_task_keys):
                            task_id = _visible_task_keys[row]
                            _send_command(f"run:{task_id}")
        _quit_event.wait(0.5)


def main() -> int:
    console = Console()

    listener = threading.Thread(target=key_listener, daemon=True)
    listener.start()

    try:
        with Live(build_display(), console=console, refresh_per_second=1, screen=True) as live:
            while not _quit_event.is_set():
                live.update(build_display())
                _quit_event.wait(REFRESH_SECONDS)
    except KeyboardInterrupt:
        pass

    console.print("[dim]Monitor stopped.[/dim]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
