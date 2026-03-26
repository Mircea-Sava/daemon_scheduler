"""Integration tests for run_scheduler_pass."""

import sys
import json
import datetime as dt
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import run_scheduler_pass


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
# Basic pass tests
# ---------------------------------------------------------------------------

def test_pass_runs_simple_task(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0
    assert mock.called


def test_pass_skips_task_wrong_time(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    start_hour: 15\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0
    mock.assert_not_called()


def test_pass_logs_error_missing_script(tmp_path):
    # Script file intentionally NOT created
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "missing.py"\n    frequency_min: 1\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 1


def test_pass_dry_run(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    # In dry_run, run_task_profiled is still called but it internally returns early
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=True)
    assert rc == 0


def test_pass_slot_deduplication(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        run_scheduler_pass(config, now, dry_run=False)
        # Second pass with same time — task already ran in this slot
        run_scheduler_pass(config, now, dry_run=False)
    # Should only be called once (second pass sees slot already consumed)
    assert mock.call_count == 1


def test_pass_paused_task_skipped(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "paused_tasks": ["t1"],
    }), encoding="utf-8")
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    mock.assert_not_called()


def test_pass_empty_task_list(tmp_path):
    config = _write_config(tmp_path, "  []\n")
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0


def test_pass_tasks_not_a_list(tmp_path):
    config = tmp_path / "schedule.yaml"
    config.write_text("tasks: not_a_list\n", encoding="utf-8")
    (tmp_path / "settings.yaml").write_text("settings:\n  use_workers: 0\n", encoding="utf-8")
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 1


def test_pass_invalid_task_does_not_block_valid(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(
        tmp_path,
        '  - id: ""\n'  # invalid: empty id
        '    path: "bad.py"\n'
        '  - id: "t1"\n'
        '    path: "s.py"\n'
        '    frequency_min: 1\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    # Invalid task causes error, but valid task still runs
    assert rc == 1  # had_error due to invalid task
    assert mock.called


# ---------------------------------------------------------------------------
# Retry behavior
# ---------------------------------------------------------------------------

def test_pass_retry_failed_task(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    # Pre-populate state: task failed 120 seconds ago (retry_delay_seconds=60 so it's eligible)
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {
            "t1": {
                "slot": "2026-03-24 09:00",
                "last_run": "2026-03-24 09:00:00",
                "outcome": "failure",
                "retry_count": 0,
            }
        },
        "in_progress": {},
        "profiling": {},
    }), encoding="utf-8")
    # Now is 2 minutes later — retry delay (60s) has elapsed
    now = dt.datetime(2026, 3, 24, 9, 2, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert mock.called
    state = _read_state(tmp_path)
    assert state["last_triggered_slot"]["t1"]["outcome"] == "success"


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def test_pass_recovery_of_in_progress_task(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {
            "t1": {
                "task_name": "t1",
                "script_path": str(tmp_path / "s.py"),
                "worker_cost": 1,
                "slot_key": "2026-03-24 09:00",
                "started_at": "2026-03-24 09:00:00",
                "is_recovery": False,
            }
        },
        "profiling": {},
    }), encoding="utf-8")
    now = dt.datetime(2026, 3, 24, 9, 1, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    # Should have been recovered and run
    assert mock.called


def test_pass_recovery_unknown_task_dropped(tmp_path):
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    frequency_min: 1\n')
    _make_script(tmp_path, "s.py")
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {
            "unknown_task": {
                "task_name": "unknown_task",
                "script_path": str(tmp_path / "unknown.py"),
                "worker_cost": 1,
                "slot_key": "2026-03-24 09:00",
                "started_at": "2026-03-24 09:00:00",
                "is_recovery": False,
            }
        },
        "profiling": {},
    }), encoding="utf-8")
    now = dt.datetime(2026, 3, 24, 9, 1, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    # Unknown task's in_progress entry should be cleaned up
    state = _read_state(tmp_path)
    assert "unknown_task" not in state["in_progress"]


# ---------------------------------------------------------------------------
# Run-now
# ---------------------------------------------------------------------------

def test_pass_run_now_via_trigger_file(tmp_path):
    _make_script(tmp_path, "s.py")
    # Task with a schedule that doesn't match now
    config = _write_config(tmp_path, '  - id: "t1"\n    path: "s.py"\n    start_hour: 23\n')
    # Create run trigger file
    (tmp_path / ".run_task_t1").write_text("", encoding="utf-8")
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert mock.called  # Should run despite wrong time
    assert not (tmp_path / ".run_task_t1").exists()  # Trigger file consumed


# ---------------------------------------------------------------------------
# Validates dependency references
# ---------------------------------------------------------------------------

def test_pass_validates_dependency_references(tmp_path):
    _make_script(tmp_path, "s.py")
    config = _write_config(
        tmp_path,
        '  - id: "t1"\n'
        '    path: "s.py"\n'
        '    frequency_min: 1\n'
        '    depends_on: "ghost"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 1  # Error: unknown dependency
