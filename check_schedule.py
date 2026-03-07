"""Show when a specific task will run. Double-click check_schedule.bat to use."""

import sys
import datetime as dt
from pathlib import Path

from sequencer import load_config, validate_task, should_run


def find_next_run(task, now, max_days=366):
    """Find the next single run time for a task, scanning up to max_days."""
    base = now.replace(second=0, microsecond=0)
    for offset in range(1, max_days * 24 * 60 + 1):
        candidate = base + dt.timedelta(minutes=offset)
        if should_run(task, candidate):
            return candidate
    return None


def find_all_runs(task, now, horizon_days):
    """Find all run times within the horizon."""
    base = now.replace(second=0, microsecond=0)
    run_times = []
    for offset in range(1, horizon_days * 24 * 60 + 1):
        candidate = base + dt.timedelta(minutes=offset)
        if should_run(task, candidate):
            run_times.append(candidate)
    return run_times


def summarize_day(times):
    """Summarize a list of times for one day.

    If 10 or fewer: return list of "HH:MM" strings.
    If more than 10: return a single summary string.
    """
    if len(times) <= 10:
        return [t.strftime("%H:%M") for t in times]

    first = times[0].strftime("%H:%M")
    last = times[-1].strftime("%H:%M")

    # Detect interval
    if len(times) >= 2:
        gap = (times[1] - times[0]).total_seconds() / 60
        consistent = all(
            (times[i + 1] - times[i]).total_seconds() / 60 == gap
            for i in range(len(times) - 1)
        )
        if consistent and gap == 1:
            return [f"{first} to {last} every minute ({len(times)} runs)"]
        elif consistent:
            return [f"{first} to {last} every {int(gap)} min ({len(times)} runs)"]

    return [f"{first} to {last} ({len(times)} runs)"]


def show_all_tasks(validated, now):
    """Show next run time for every task (one line each)."""
    print()
    print("All tasks - next scheduled run:")
    print()
    max_name = max(len(t.get("id", "?")) for t in validated)
    for j, task in enumerate(validated, 1):
        name = task.get("id", "?")
        next_run = find_next_run(task, now)
        if next_run:
            print(f"  {j}. {name:<{max_name}}  ->  {next_run.strftime('%A %Y-%m-%d %H:%M')}")
        else:
            print(f"  {j}. {name:<{max_name}}  ->  no runs in the next year")
    print()


def show_task_schedule(task, now, horizon_days):
    """Show all runs for a task within the chosen horizon."""
    task_id = task.get("id", "?")

    print()
    print(f"Task: {task_id}")
    print(f"Script: {task.get('path', '?')}")
    print(f"Now: {now.strftime('%A %Y-%m-%d %H:%M')}")
    print()

    run_times = find_all_runs(task, now, horizon_days)

    if not run_times:
        print(f"No runs scheduled in the next {horizon_days} days.")
        print()
        # Show config so they can debug
        show_task_config(task)
        return

    total = len(run_times)
    header = f"Next {horizon_days} days ({total} run{'s' if total != 1 else ''}"
    if total > 50:
        header += ", showing summary"
    header += "):"
    print(header)
    print()

    # Group by day
    days = {}
    for rt in run_times:
        key = rt.date()
        if key not in days:
            days[key] = []
        days[key].append(rt)

    for date, times in days.items():
        day_str = times[0].strftime("%A %Y-%m-%d")
        print(f"  {day_str}")
        lines = summarize_day(times)
        for line in lines:
            print(f"    {line}")

    print()


def show_task_config(task):
    """Show the task's schedule config for debugging."""
    times_field = task.get("_times")
    if times_field:
        print(f"  times: {', '.join(f'{h}:{m:02d}' for h, m in times_field)}")
    else:
        sh = task.get("_start_hour")
        sm = task.get("_start_minute", 0)
        fm = task.get("_frequency_min")
        eh = task.get("_end_hour")
        if sh is not None:
            print(f"  start_hour: {sh}, start_minute: {sm}")
        if fm is not None:
            print(f"  frequency_min: {fm}")
        if eh is not None:
            print(f"  end_hour: {eh}, end_minute: {task.get('_end_minute', 0)}")
    months = task.get("_months")
    if months:
        print(f"  month: {','.join(str(m) for m in sorted(months))}")
    md = task.get("_month_day")
    if md:
        print(f"  month_day: {','.join(str(d) for d in sorted(md))}")
    wd = task.get("_week_day")
    if wd:
        day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
        print(f"  week_day: {','.join(day_names.get(d, str(d)) for d in sorted(wd))}")


def ask_horizon():
    """Ask the user how far ahead to check."""
    print("How far ahead to check?")
    print("  1. Next 7 days")
    print("  2. Next 30 days")
    print("  3. Next 365 days")
    print()
    choice = input("Enter choice (1/2/3): ").strip()
    horizons = {"1": 7, "2": 30, "3": 365}
    return horizons.get(choice)


def main() -> int:
    config_path = Path("schedule.yaml").resolve()
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return 1

    try:
        cfg = load_config(config_path)
    except ValueError as exc:
        print(f"Error loading config: {exc}")
        return 1

    raw_tasks = cfg.get("tasks") or []
    validated = []
    for i, t in enumerate(raw_tasks, 1):
        try:
            validated.append(validate_task(t, i))
        except ValueError as exc:
            print(f"Warning: {exc}")

    if not validated:
        print("No tasks found in schedule.yaml.")
        return 1

    now = dt.datetime.now()

    # Show available tasks
    print()
    print("Available tasks:")
    print()
    for j, task in enumerate(validated, 1):
        print(f"  {j}. {task.get('id')}")
    print(f"  a. All tasks (quick overview)")
    print()

    choice = input("Enter task number, ID, or 'a' for all: ").strip()
    if not choice:
        print("No task selected.")
        return 1

    # "All tasks" overview
    if choice.lower() == "a":
        show_all_tasks(validated, now)
        return 0

    # Find by number or by ID
    match = None
    try:
        idx = int(choice)
        if 1 <= idx <= len(validated):
            match = validated[idx - 1]
    except ValueError:
        pass

    if match is None:
        for task in validated:
            if task.get("id") == choice:
                match = task
                break

    if match is None:
        print(f"Task not found: {choice}")
        return 1

    # Ask horizon
    print()
    horizon = ask_horizon()
    if horizon is None:
        print("Invalid choice.")
        return 1

    show_task_schedule(match, now, horizon)
    return 0


if __name__ == "__main__":
    sys.exit(main())
