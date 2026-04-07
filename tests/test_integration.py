"""End-to-end integration tests for the scheduler daemon loop.

These tests simulate multiple scheduler ticks over time, verifying that:
- Tasks run at their scheduled times
- State persists between ticks
- Retry logic works across ticks
- Dependency-only tasks trigger after deps succeed
- Recovery re-queues interrupted tasks
- Paused tasks are skipped
"""

import sys
import json
import datetime as dt
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import (
    run_scheduler_pass,
    compute_next_wake_time,
    validate_task,
    load_state,
)


def _write_config(tmp_path, tasks_yaml, settings_yaml=None):
    config = tmp_path / "schedule.yaml"
    config.write_text(f"tasks:\n{tasks_yaml}", encoding="utf-8")
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        settings_yaml or (
            "settings:\n"
            "  retry_delay_seconds: 60\n"
            "  retry_max_delay_seconds: 1800\n"
            "  use_workers: 0\n"
            "  max_workers: 100\n"
        ),
        encoding="utf-8",
    )
    return config


def _make_script(tmp_path, name="script.py"):
    s = tmp_path / name
    s.write_text("print('ok')\n", encoding="utf-8")
    return s


def _read_state(tmp_path):
    return json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Multi-tick integration tests
# ---------------------------------------------------------------------------

def test_multi_tick_task_runs_once_per_day(tmp_path):
    """A task scheduled at 09:00 should run exactly once, not on every tick."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    start_hour: 9\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)

    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        # Tick 1: task runs
        run_scheduler_pass(config, now, dry_run=False)
        assert mock.call_count == 1

        # Tick 2: same minute, task already ran in this slot
        run_scheduler_pass(config, now, dry_run=False)
        assert mock.call_count == 1

    # Tick 3: next day at same time — task should run again
    tomorrow = dt.datetime(2026, 3, 25, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, tomorrow, dry_run=False)
        assert mock.call_count == 1


def test_multi_tick_frequency_task(tmp_path):
    """A task every 5 minutes should run at each interval."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 5\n')

    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        # Tick at 09:00
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)
        assert mock.call_count == 1

        # Tick at 09:01 — not yet due
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 1, 0), dry_run=False)
        assert mock.call_count == 1

        # Tick at 09:05 — due again
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 5, 0), dry_run=False)
        assert mock.call_count == 2

        # Tick at 09:06 — not yet
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 6, 0), dry_run=False)
        assert mock.call_count == 2

        # Tick at 09:10 — due again
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 10, 0), dry_run=False)
        assert mock.call_count == 3


def test_multi_tick_retry_across_ticks(tmp_path):
    """A failed task should be retried on a later tick after retry_delay_seconds."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')

    # Tick 1: task fails
    with patch("sequencer.run_task_profiled", return_value=(False, 0.0, 0.0)):
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)

    state = _read_state(tmp_path)
    assert state["last_triggered_slot"]["t1"]["outcome"] == "failure"
    assert state["last_triggered_slot"]["t1"]["retry_count"] == 1

    # Tick 2: 30 seconds later — retry delay (60s) not yet elapsed
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 30), dry_run=False)
        assert mock.call_count == 0

    # Tick 3: 90 seconds later — retry delay elapsed, task runs and succeeds
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 1, 30), dry_run=False)
        assert mock.call_count == 1

    state = _read_state(tmp_path)
    assert state["last_triggered_slot"]["t1"]["outcome"] == "success"
    assert state["last_triggered_slot"]["t1"]["retry_count"] == 0


def test_multi_tick_dep_chain(tmp_path):
    """A -> B -> C dependency chain across multiple ticks."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    _make_script(tmp_path, "c.py")
    config = _write_config(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        '    frequency_min: 1\n'
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n'
        '  - id: "C"\n'
        '    path: "c.py"\n'
        '    depends_on: "B"\n',
    )

    # Tick 1: A runs, B and C trigger as dep-only
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)
        assert mock.call_count == 3  # A, B, C all run

    state = _read_state(tmp_path)
    assert state["last_triggered_slot"]["A"]["outcome"] == "success"
    assert state["last_triggered_slot"]["B"]["outcome"] == "success"
    assert state["last_triggered_slot"]["C"]["outcome"] == "success"


def test_multi_tick_recovery_after_crash(tmp_path):
    """Simulate a crash: task is in_progress but never finished. Next tick recovers it."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')

    # Pre-populate state: task was running but crashed
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {
            "t1": {
                "task_name": "t1",
                "script_path": str(tmp_path / "s.py"),
                "worker_cost": 1,
                "slot_key": "2026-03-24 08:59",
                "started_at": "2026-03-24 08:59:00",
                "is_recovery": False,
            }
        },
        "profiling": {},
    }), encoding="utf-8")

    # Tick: recovers and runs the task
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)
        assert mock.call_count == 1

    state = _read_state(tmp_path)
    assert state["in_progress"] == {}  # cleared
    assert state["last_triggered_slot"]["t1"]["outcome"] == "success"


def test_multi_tick_pause_and_resume(tmp_path):
    """Pause a task, verify it's skipped. Resume it, verify it runs."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')

    # Pre-populate state with t1 paused
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "paused_tasks": ["t1"],
    }), encoding="utf-8")

    # Tick: task is skipped
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)
        assert mock.call_count == 0

    # Unpause (remove from paused_tasks)
    state = _read_state(tmp_path)
    state["paused_tasks"] = []
    state_path.write_text(json.dumps(state), encoding="utf-8")

    # Tick: task runs
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, dt.datetime(2026, 3, 24, 9, 0, 0), dry_run=False)
        assert mock.call_count == 1


# ---------------------------------------------------------------------------
# compute_next_wake_time integration
# ---------------------------------------------------------------------------

def test_wake_time_frequency_task(tmp_path):
    """Wake time for a frequency_min: 5 task should be exactly 5 minutes ahead."""
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 5\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    tasks = [validate_task({"id": "t1", "path": "s.py", "frequency_min": 5}, 1)]
    state = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}

    wake = compute_next_wake_time(tasks, state, settings, now, set())
    assert wake == dt.datetime(2026, 3, 24, 9, 5, 0)


def test_wake_time_single_run_past(tmp_path):
    """If the single run time is past, wake should be tomorrow."""
    tasks = [validate_task({"id": "t1", "path": "s.py", "start_hour": 9, "start_minute": 0}, 1)]
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    state = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}

    wake = compute_next_wake_time(tasks, state, settings, now, set())
    assert wake == dt.datetime(2026, 3, 25, 9, 0, 0)


def test_wake_time_no_tasks(tmp_path):
    """With no tasks, should fall back to max_horizon_minutes."""
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    state = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}

    wake = compute_next_wake_time([], state, settings, now, set())
    assert wake == dt.datetime(2026, 3, 24, 10, 0, 0)


def test_wake_time_paused_task(tmp_path):
    """Paused tasks should not affect wake time — fallback to now + horizon."""
    tasks = [validate_task({"id": "t1", "path": "s.py", "start_hour": 9, "start_minute": 0}, 1)]
    now = dt.datetime(2026, 3, 24, 8, 30, 0)
    state = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}

    wake = compute_next_wake_time(tasks, state, settings, now, {"t1"})
    # Task is paused, so no candidate from it — fallback to now + 60 min = 09:30
    assert wake == dt.datetime(2026, 3, 24, 9, 30, 0)


def test_wake_time_frequency_overshoots_end_hour(tmp_path):
    """Frequency interval that overshoots end_hour should skip to the next day."""
    # 45-min frequency from 9:00 to 10:00 → fires at 9:00, 9:45.
    # 9:45 + 45 = 10:30 > 10:00, so next wake should be tomorrow 9:00.
    tasks = [validate_task({
        "id": "t1", "path": "s.py",
        "start_hour": 9, "frequency_min": 45, "end_hour": 10,
    }, 1)]
    now = dt.datetime(2026, 3, 24, 9, 45, 0)
    state = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}

    wake = compute_next_wake_time(tasks, state, settings, now, set())
    # 9:45 + 45 min = 10:30 > end_hour 10:00, so next valid day starts at 9:00
    assert wake == dt.datetime(2026, 3, 25, 9, 0, 0)
