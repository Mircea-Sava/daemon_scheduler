"""Show when a specific task will run. Double-click check_schedule.bat to use."""

import sys
import datetime as dt
from pathlib import Path

from sequencer import load_config, validate_task, should_run, is_within_task_window


# ---------------------------------------------------------------------------
# Core scheduling math (direct computation, no minute-by-minute scanning)
# ---------------------------------------------------------------------------

def _day_matches(task, date):
    """Check if a date passes the task's month/day/weekday filters."""
    months = task.get("_months")
    if months and date.month not in months:
        return False
    month_day = task.get("_month_day")
    week_day = task.get("_week_day")
    if month_day is not None:
        if date.day not in month_day:
            return False
    elif week_day is not None:
        if date.weekday() not in week_day:
            return False
    return True


def _day_run_times(task, date):
    """Compute all run times for a task on a given day using direct math.

    Returns a sorted list of datetime objects. Assumes _day_matches already passed.
    """
    times_field = task.get("_times")
    if times_field is not None:
        return sorted(
            dt.datetime(date.year, date.month, date.day, h, m)
            for h, m in times_field
        )

    start_hour = task.get("_start_hour")
    start_minute = task.get("_start_minute", 0)
    frequency_min = task.get("_frequency_min")
    end_hour = task.get("_end_hour")
    end_minute = task.get("_end_minute", 0)

    if start_hour is None:
        # No start_hour = every minute of the day (1440 entries).
        return [
            dt.datetime(date.year, date.month, date.day, h, m)
            for h in range(24) for m in range(60)
        ]

    if frequency_min is None:
        # Single run at start_hour:start_minute.
        return [dt.datetime(date.year, date.month, date.day, start_hour, start_minute)]

    # Repeating with frequency_min.
    window_start = start_hour * 60 + start_minute

    if end_hour is not None:
        window_end = end_hour * 60 + end_minute
        wraps = window_start > window_end
    else:
        window_end = 1439  # end of day
        wraps = False

    results = []

    if wraps:
        # Overnight window: runs from window_start..23:59 and 00:00..window_end.
        total_window = (1440 - window_start) + window_end
        offset = 0
        while offset <= total_window:
            minute_of_day = (window_start + offset) % 1440
            h, m = divmod(minute_of_day, 60)
            if offset < (1440 - window_start):
                results.append(dt.datetime(date.year, date.month, date.day, h, m))
            else:
                next_day = date + dt.timedelta(days=1)
                results.append(dt.datetime(next_day.year, next_day.month, next_day.day, h, m))
            offset += frequency_min
    else:
        minute = window_start
        while minute <= window_end:
            h, m = divmod(minute, 60)
            results.append(dt.datetime(date.year, date.month, date.day, h, m))
            minute += frequency_min

    return results


def find_next_runs(task, now, count=5, max_days=366):
    """Find the next *count* run times for a task."""
    base = now.replace(second=0, microsecond=0)
    candidate_date = base.date()
    results = []

    for _ in range(max_days):
        if _day_matches(task, candidate_date):
            for run_time in _day_run_times(task, candidate_date):
                if run_time > base and is_within_task_window(task, run_time):
                    results.append(run_time)
                    if len(results) >= count:
                        return results
        candidate_date += dt.timedelta(days=1)
    return results


def find_all_runs(task, now, horizon_days):
    """Find all run times within the horizon."""
    base = now.replace(second=0, microsecond=0)
    end = base + dt.timedelta(days=horizon_days)
    candidate_date = base.date()
    run_times = []

    while candidate_date <= end.date():
        if _day_matches(task, candidate_date):
            for run_time in _day_run_times(task, candidate_date):
                if run_time > base and run_time <= end:
                    if is_within_task_window(task, run_time):
                        run_times.append(run_time)
        candidate_date += dt.timedelta(days=1)

    return run_times


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def summarize_day(times):
    """Summarize a list of times for one day.

    If 10 or fewer: return list of "HH:MM" strings.
    If more than 10: return a single summary string.
    """
    if len(times) <= 10:
        return [t.strftime("%H:%M") for t in times]

    first = times[0].strftime("%H:%M")
    last = times[-1].strftime("%H:%M")

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


def _describe_schedule(task):
    """Return a short human-readable description of the task's schedule."""
    dep_only = task.get("_dependency_only", False)
    if dep_only:
        deps = task.get("_depends_on", [])
        return f"runs after: {', '.join(deps)}"

    parts = []

    # Time pattern
    times_field = task.get("_times")
    if times_field:
        time_strs = [f"{h}:{m:02d}" for h, m in sorted(times_field)]
        parts.append(f"at {', '.join(time_strs)}")
    else:
        sh = task.get("_start_hour")
        fm = task.get("_frequency_min")
        eh = task.get("_end_hour")
        sm = task.get("_start_minute", 0)
        em = task.get("_end_minute", 0)

        if sh is None:
            parts.append("every minute")
        elif fm is None:
            parts.append(f"once at {sh}:{sm:02d}")
        else:
            start_str = f"{sh}:{sm:02d}"
            if eh is not None:
                end_str = f"{eh}:{em:02d}"
                parts.append(f"every {fm} min, {start_str}-{end_str}")
            else:
                parts.append(f"every {fm} min from {start_str}")

    # Day filters
    wd = task.get("_week_day")
    md = task.get("_month_day")
    months = task.get("_months")
    day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

    if md is not None:
        parts.append(f"on day {','.join(str(d) for d in sorted(md))}")
    elif wd is not None:
        names = [day_names[d] for d in sorted(wd)]
        if names == ["Mon", "Tue", "Wed", "Thu", "Fri"]:
            parts.append("weekdays")
        elif names == ["Sat", "Sun"]:
            parts.append("weekends")
        else:
            parts.append(', '.join(names))

    if months and months != set(range(1, 13)):
        import calendar
        month_names = [calendar.month_abbr[m] for m in sorted(months)]
        parts.append(f"in {', '.join(month_names)}")

    # Dependencies
    deps = task.get("_depends_on", [])
    if deps:
        parts.append(f"after {', '.join(deps)}")

    return ', '.join(parts) if parts else "always"


def _diagnose_no_runs(task):
    """When no runs are found, explain why."""
    reasons = []

    start_at = task.get("start_at")
    end_at = task.get("end_at")
    if end_at and isinstance(end_at, dt.datetime) and end_at < dt.datetime.now():
        reasons.append(f"  - end_at ({end_at.strftime('%Y-%m-%d %H:%M')}) is in the past")
    if start_at and isinstance(start_at, dt.datetime) and start_at > dt.datetime.now() + dt.timedelta(days=366):
        reasons.append(f"  - start_at ({start_at.strftime('%Y-%m-%d %H:%M')}) is more than a year away")

    months = task.get("_months")
    if months and len(months) == 1:
        import calendar
        m = list(months)[0]
        reasons.append(f"  - only runs in {calendar.month_name[m]}")

    md = task.get("_month_day")
    if md and len(md) == 1:
        d = list(md)[0]
        reasons.append(f"  - only runs on day {d} of the month")

    dep_only = task.get("_dependency_only", False)
    if dep_only:
        deps = task.get("_depends_on", [])
        reasons.append(f"  - dependency-only task (waits for: {', '.join(deps)})")
        reasons.append(f"  - has no schedule of its own, won't show in this tool")

    if not reasons:
        reasons.append("  - check your schedule fields in schedule.yaml")

    return reasons


# ---------------------------------------------------------------------------
# Display modes
# ---------------------------------------------------------------------------

def show_all_tasks(validated, now):
    """Show every task with its schedule description and next run."""
    print()
    max_name = max(len(t.get("id", "?")) for t in validated)

    for j, task in enumerate(validated, 1):
        name = task.get("id", "?")
        desc = _describe_schedule(task)
        next_runs = find_next_runs(task, now, count=1)

        if next_runs:
            next_str = next_runs[0].strftime("%a %Y-%m-%d %H:%M")
            print(f"  {j}. {name:<{max_name}}  |  {desc}")
            print(f"     {'':>{max_name}}  ->  next: {next_str}")
        else:
            dep_only = task.get("_dependency_only", False)
            if dep_only:
                deps = task.get("_depends_on", [])
                print(f"  {j}. {name:<{max_name}}  |  {desc}")
                print(f"     {'':>{max_name}}  ->  (event-driven, no fixed schedule)")
            else:
                print(f"  {j}. {name:<{max_name}}  |  {desc}")
                print(f"     {'':>{max_name}}  ->  no runs in the next year")
        print()


def show_task_detail(task, now):
    """Show detailed schedule for a single task."""
    task_id = task.get("id", "?")
    task_path = task.get("path", "?")
    desc = _describe_schedule(task)

    print()
    print(f"  Task:     {task_id}")
    print(f"  Script:   {task_path}")
    print(f"  Schedule: {desc}")

    # Timeout / depends
    timeout = task.get("_timeout_minutes")
    deps = task.get("_depends_on", [])
    if timeout:
        print(f"  Timeout:  {timeout} minutes")
    if deps:
        print(f"  Depends:  {', '.join(deps)}")

    print()

    # Dependency-only tasks have no schedule to show
    if task.get("_dependency_only", False):
        print("  This task has no schedule of its own.")
        print(f"  It runs automatically when {' and '.join(deps)} succeed.")
        print()
        return

    # Auto-detect a good horizon: find a few runs first to show the pattern
    preview = find_next_runs(task, now, count=5)

    if not preview:
        print("  No runs found in the next year.")
        print()
        print("  Possible reasons:")
        for reason in _diagnose_no_runs(task):
            print(reason)
        print()
        print("  Current config:")
        _show_task_config(task)
        print()
        return

    # Show next 5 runs immediately
    print("  Next runs:")
    for run_time in preview:
        delta = run_time - now
        if delta.days == 0:
            relative = f"in {delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m"
        elif delta.days == 1:
            relative = "tomorrow"
        elif delta.days < 7:
            relative = f"in {delta.days} days"
        else:
            relative = f"in {delta.days} days"
        print(f"    {run_time.strftime('%a %Y-%m-%d %H:%M')}  ({relative})")

    # Detect the pattern and show a summary
    if len(preview) >= 2:
        gap = (preview[1] - preview[0]).total_seconds() / 60
        if gap < 60:
            pattern = f"every {int(gap)} minutes"
        elif gap == 60:
            pattern = "every hour"
        elif gap < 1440:
            hours = gap / 60
            pattern = f"every {hours:.0f} hours" if hours == int(hours) else f"every {hours:.1f} hours"
        elif gap == 1440:
            pattern = "once daily"
        elif gap == 1440 * 7:
            pattern = "once weekly"
        else:
            pattern = f"every {gap / 1440:.0f} days" if gap % 1440 == 0 else None

        if pattern:
            print(f"\n  Pattern: {pattern}")

    # Offer deep dive
    print()
    print("  Want to see a full calendar view?")
    print("    1. Next 7 days")
    print("    2. Next 30 days")
    print("    3. Next 365 days")
    print("    Enter. Skip")
    print()
    try:
        choice = input("  Choice: ").strip()
    except EOFError:
        return

    horizons = {"1": 7, "2": 30, "3": 365}
    horizon = horizons.get(choice)
    if horizon is None:
        return

    run_times = find_all_runs(task, now, horizon)
    if not run_times:
        print(f"\n  No runs in the next {horizon} days.")
        return

    total = len(run_times)
    print(f"\n  {horizon}-day view: {total} run{'s' if total != 1 else ''}")
    print()

    # Group by day
    days = {}
    for rt in run_times:
        key = rt.date()
        if key not in days:
            days[key] = []
        days[key].append(rt)

    for date, times in days.items():
        day_str = times[0].strftime("%a %Y-%m-%d")
        lines = summarize_day(times)
        if len(lines) == 1:
            print(f"    {day_str}  {lines[0]}")
        else:
            print(f"    {day_str}  {', '.join(lines)}")

    print()


def _show_task_config(task):
    """Show the task's raw schedule fields for debugging."""
    times_field = task.get("_times")
    if times_field:
        print(f"    times: {', '.join(f'{h}:{m:02d}' for h, m in times_field)}")
    else:
        sh = task.get("_start_hour")
        sm = task.get("_start_minute", 0)
        fm = task.get("_frequency_min")
        eh = task.get("_end_hour")
        if sh is not None:
            print(f"    start_hour: {sh}, start_minute: {sm}")
        if fm is not None:
            print(f"    frequency_min: {fm}")
        if eh is not None:
            print(f"    end_hour: {eh}, end_minute: {task.get('_end_minute', 0)}")
    months = task.get("_months")
    if months and months != set(range(1, 13)):
        print(f"    month: {','.join(str(m) for m in sorted(months))}")
    md = task.get("_month_day")
    if md:
        print(f"    month_day: {','.join(str(d) for d in sorted(md))}")
    wd = task.get("_week_day")
    if wd:
        day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
        print(f"    week_day: {','.join(day_names.get(d, str(d)) for d in sorted(wd))}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
    errors = []
    for i, t in enumerate(raw_tasks, 1):
        try:
            validated.append(validate_task(t, i))
        except ValueError as exc:
            errors.append((i, t, str(exc)))

    # Show validation errors prominently
    if errors:
        print()
        print(f"  WARNING: {len(errors)} task(s) have config errors:")
        print()
        for idx, raw_task, msg in errors:
            name = raw_task.get("id", f"(task #{idx})") if isinstance(raw_task, dict) else f"(task #{idx})"
            print(f"    {name}: {msg}")
        print()

    if not validated:
        print("No valid tasks found in schedule.yaml.")
        return 1

    now = dt.datetime.now()
    print()
    print(f"  Schedule Checker  |  {now.strftime('%A %Y-%m-%d %H:%M')}")
    print(f"  {len(validated)} task(s) loaded from schedule.yaml")
    print()

    for j, task in enumerate(validated, 1):
        name = task.get("id", "?")
        desc = _describe_schedule(task)
        print(f"    {j}. {name}  ({desc})")
    print(f"    a. All tasks overview")
    print()

    try:
        choice = input("  Select task number, ID, or 'a': ").strip()
    except EOFError:
        return 0
    if not choice:
        print("  No selection.")
        return 1

    if choice.lower() == "a":
        show_all_tasks(validated, now)
        return 0

    # Find by number or ID
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
        print(f"  Task not found: {choice}")
        return 1

    show_task_detail(match, now)
    return 0


if __name__ == "__main__":
    sys.exit(main())
