"""Tests for dependency resolution logic."""

import sys
import json
import datetime as dt
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import (
    run_scheduler_pass,
    validate_task,
    task_key,
    SchedulerContext,
    save_state,
)


def _write_yaml(tmp_path, tasks_yaml):
    config = tmp_path / "schedule.yaml"
    config.write_text(f"tasks:\n{tasks_yaml}", encoding="utf-8")
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "settings:\n"
        "  retry_delay_seconds: 60\n"
        "  retry_max_delay_seconds: 1800\n"
        "  use_workers: 0\n"
        "  max_workers: 100\n",
        encoding="utf-8",
    )
    return config


def _make_script(tmp_path, name):
    script = tmp_path / name
    script.write_text("print('ok')\n", encoding="utf-8")
    return script


# ---------------------------------------------------------------------------
# Dependency-only flag via validate_task
# ---------------------------------------------------------------------------

def test_dep_only_flag_set_when_no_schedule():
    task = validate_task({"id": "B", "path": "b.py", "depends_on": "A"}, 1)
    assert task["_dependency_only"] is True


def test_dep_only_flag_not_set_with_schedule():
    task = validate_task({"id": "B", "path": "b.py", "depends_on": "A", "start_hour": 9}, 1)
    assert task["_dependency_only"] is False


def test_dep_only_flag_not_set_without_deps():
    task = validate_task({"id": "A", "path": "a.py"}, 1)
    assert task["_dependency_only"] is False


# ---------------------------------------------------------------------------
# Blocking-mode dep-only triggering via run_scheduler_pass
# ---------------------------------------------------------------------------

def test_dep_only_task_triggered_after_dep_succeeds(tmp_path):
    """Task B (dep-only) should run after Task A succeeds in blocking mode."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0
    state = json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["A"]["outcome"] == "success"
    assert state["last_triggered_slot"]["B"]["outcome"] == "success"


def test_dep_only_task_not_triggered_when_dep_fails(tmp_path):
    """Task B should NOT run if Task A fails."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(False, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    state = json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["A"]["outcome"] == "failure"
    assert "B" not in state["last_triggered_slot"]


def test_chained_deps_a_b_c(tmp_path):
    """A -> B -> C chain: all three should run in order."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    _make_script(tmp_path, "c.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n'
        '  - id: "C"\n'
        '    path: "c.py"\n'
        '    depends_on: "B"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0
    state = json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["A"]["outcome"] == "success"
    assert state["last_triggered_slot"]["B"]["outcome"] == "success"
    assert state["last_triggered_slot"]["C"]["outcome"] == "success"


def test_multi_deps_all_must_succeed(tmp_path):
    """Task C depends on both A and B. Only runs if both succeed."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    _make_script(tmp_path, "c.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        "    frequency_min: 1\n"
        '  - id: "C"\n'
        '    path: "c.py"\n'
        '    depends_on: "A, B"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    assert rc == 0
    state = json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["C"]["outcome"] == "success"


def test_multi_deps_one_fails_blocks_dependent(tmp_path):
    """Task C depends on A and B. If A fails, C should not run."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    _make_script(tmp_path, "c.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        "    frequency_min: 1\n"
        '  - id: "C"\n'
        '    path: "c.py"\n'
        '    depends_on: "A, B"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    call_count = [0]
    def mock_run(*args, **kwargs):
        call_count[0] += 1
        # First call (A) fails, second call (B) succeeds
        if call_count[0] == 1:
            return (False, 0.0, 0.0)
        return (True, 0.0, 0.0)

    with patch("sequencer.run_task_profiled", side_effect=mock_run):
        rc = run_scheduler_pass(config, now, dry_run=False)
    state = json.loads((tmp_path / "sequencer_state.json").read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["A"]["outcome"] == "failure"
    assert state["last_triggered_slot"]["B"]["outcome"] == "success"
    assert "C" not in state["last_triggered_slot"]


def test_dep_only_task_skipped_when_paused(tmp_path):
    """A paused dependency-only task should not be triggered."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n',
    )
    # Pre-populate state with B paused
    state_path = tmp_path / "sequencer_state.json"
    state_path.write_text(json.dumps({
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "paused_tasks": ["B"],
    }), encoding="utf-8")

    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["last_triggered_slot"]["A"]["outcome"] == "success"
    assert "B" not in state["last_triggered_slot"]


def test_dep_only_task_not_in_normal_schedule_loop(tmp_path):
    """Dep-only tasks should NOT run on normal schedule (they'd match every tick)."""
    _make_script(tmp_path, "b.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n',  # A doesn't exist as a task
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)) as mock:
        rc = run_scheduler_pass(config, now, dry_run=False)
    # B should NOT have been called (A doesn't exist, and B is dep-only so skipped from schedule)
    mock.assert_not_called()


def test_unknown_dependency_logs_error(tmp_path):
    """depends_on referencing a non-existent task should cause error return."""
    _make_script(tmp_path, "a.py")
    _make_script(tmp_path, "b.py")
    config = _write_yaml(
        tmp_path,
        '  - id: "A"\n'
        '    path: "a.py"\n'
        "    frequency_min: 1\n"
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "nonexistent"\n',
    )
    now = dt.datetime(2026, 3, 24, 9, 0, 0)
    with patch("sequencer.run_task_profiled", return_value=(True, 0.0, 0.0)):
        rc = run_scheduler_pass(config, now, dry_run=False)
    # Should return 1 because of the unknown dependency error
    assert rc == 1


# ---------------------------------------------------------------------------
# SchedulerContext._check_and_queue_dependents
# ---------------------------------------------------------------------------

def test_check_and_queue_dependents_calls_callback(tmp_path):
    """When all deps are met, the trigger callback should be called."""
    state_path = tmp_path / "state.json"
    save_state(state_path, {
        "last_triggered_slot": {
            "A": {"slot": "2026-03-24 09:00", "last_run": "2026-03-24 09:00:05", "outcome": "success", "retry_count": 0},
        },
        "in_progress": {},
        "profiling": {},
    })
    ctx = SchedulerContext(state_path, max_workers=4)
    try:
        task_a = {"id": "A", "name": "A", "path": "a.py", "_depends_on": [], "_dependency_only": False}
        task_b = {"id": "B", "name": "B", "path": "b.py", "_depends_on": ["A"], "_dependency_only": True}
        ctx._all_validated_tasks = [task_a, task_b]
        ctx._dep_only_tasks = [task_b]

        triggered = []
        ctx._dep_trigger_callback = lambda task: triggered.append(task["name"])

        ctx._check_and_queue_dependents("A")
        assert triggered == ["B"]
    finally:
        ctx.shutdown(wait=False)


def test_check_and_queue_dependents_prevents_double_queue(tmp_path):
    """Same dep-only task should not be queued twice."""
    state_path = tmp_path / "state.json"
    save_state(state_path, {
        "last_triggered_slot": {
            "A": {"slot": "2026-03-24 09:00", "last_run": "2026-03-24 09:00:05", "outcome": "success", "retry_count": 0},
        },
        "in_progress": {},
        "profiling": {},
    })
    ctx = SchedulerContext(state_path, max_workers=4)
    try:
        task_a = {"id": "A", "name": "A", "path": "a.py", "_depends_on": [], "_dependency_only": False}
        task_b = {"id": "B", "name": "B", "path": "b.py", "_depends_on": ["A"], "_dependency_only": True}
        ctx._all_validated_tasks = [task_a, task_b]
        ctx._dep_only_tasks = [task_b]

        triggered = []
        ctx._dep_trigger_callback = lambda task: triggered.append(task["name"])

        ctx._check_and_queue_dependents("A")
        ctx._check_and_queue_dependents("A")  # second call
        assert triggered == ["B"]  # only once
    finally:
        ctx.shutdown(wait=False)


def test_check_and_queue_dependents_waits_for_all_deps(tmp_path):
    """If C depends on A and B, it should only trigger when both succeed."""
    state_path = tmp_path / "state.json"
    # Only A has succeeded so far
    save_state(state_path, {
        "last_triggered_slot": {
            "A": {"slot": "2026-03-24 09:00", "last_run": "2026-03-24 09:00:05", "outcome": "success", "retry_count": 0},
        },
        "in_progress": {},
        "profiling": {},
    })
    ctx = SchedulerContext(state_path, max_workers=4)
    try:
        task_a = {"id": "A", "name": "A", "path": "a.py", "_depends_on": [], "_dependency_only": False}
        task_b = {"id": "B", "name": "B", "path": "b.py", "_depends_on": [], "_dependency_only": False}
        task_c = {"id": "C", "name": "C", "path": "c.py", "_depends_on": ["A", "B"], "_dependency_only": True}
        ctx._all_validated_tasks = [task_a, task_b, task_c]
        ctx._dep_only_tasks = [task_c]

        triggered = []
        ctx._dep_trigger_callback = lambda task: triggered.append(task["name"])

        # A finishes - but B hasn't yet
        ctx._check_and_queue_dependents("A")
        assert triggered == []

        # Now B succeeds
        with ctx.state_lock:
            ctx.last_triggered_slot["B"] = {
                "slot": "2026-03-24 09:00",
                "last_run": "2026-03-24 09:00:10",
                "outcome": "success",
                "retry_count": 0,
            }

        ctx._check_and_queue_dependents("B")
        assert triggered == ["C"]
    finally:
        ctx.shutdown(wait=False)
