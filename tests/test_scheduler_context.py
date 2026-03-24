"""Tests for the SchedulerContext class."""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import SchedulerContext, save_state, load_state, task_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(**overrides):
    """Return a minimal valid state dict."""
    base = {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}
    base.update(overrides)
    return base


def _write_state(path, state):
    """Persist *state* as JSON to *path*."""
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _read_state(path):
    """Read the JSON state file back."""
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def state_path(tmp_path):
    """Return a state-file path with a minimal empty state already written."""
    p = tmp_path / "state.json"
    _write_state(p, _make_state())
    return p


@pytest.fixture
def ctx(state_path):
    """Create a SchedulerContext and shut it down after the test."""
    context = SchedulerContext(state_path, max_workers=2)
    yield context
    context.shutdown(wait=False)


# ---------------------------------------------------------------------------
# 1. test_init_loads_state
# ---------------------------------------------------------------------------

def test_init_loads_state(tmp_path):
    state_file = tmp_path / "state.json"
    _write_state(state_file, _make_state(
        last_triggered_slot={"k1": {"slot": "2026-03-24T09:00", "outcome": "success"}},
        in_progress={"k2": {"task_name": "t2"}},
    ))

    ctx = SchedulerContext(state_file, max_workers=1)
    try:
        assert "k1" in ctx.last_triggered_slot
        assert ctx.last_triggered_slot["k1"]["outcome"] == "success"
        assert "k2" in ctx.in_progress
    finally:
        ctx.shutdown(wait=False)


# ---------------------------------------------------------------------------
# 2. test_mark_task_started_persists
# ---------------------------------------------------------------------------

def test_mark_task_started_persists(ctx, state_path):
    ctx.mark_task_started(
        key="task-a::script.py",
        task_name="task-a",
        script_path=Path("script.py"),
        worker_cost=1,
        slot_key_value="2026-03-24T10:00",
        is_recovery=False,
    )

    persisted = _read_state(state_path)
    entry = persisted["in_progress"]["task-a::script.py"]
    assert entry["task_name"] == "task-a"
    assert entry["script_path"] == "script.py"
    assert entry["worker_cost"] == 1
    assert entry["slot_key"] == "2026-03-24T10:00"
    assert entry["is_recovery"] is False


# ---------------------------------------------------------------------------
# 3. test_mark_task_finished_success
# ---------------------------------------------------------------------------

def test_mark_task_finished_success(ctx, state_path):
    key = "task-a::script.py"
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:00", is_recovery=False,
    )
    ctx.actively_running.add(key)

    ctx.mark_task_finished(key, "2026-03-24T10:00", success=True)

    persisted = _read_state(state_path)
    assert key not in persisted["in_progress"]
    slot_entry = persisted["last_triggered_slot"][key]
    assert slot_entry["outcome"] == "success"
    assert slot_entry["retry_count"] == 0


# ---------------------------------------------------------------------------
# 4. test_mark_task_finished_failure
# ---------------------------------------------------------------------------

def test_mark_task_finished_failure(ctx, state_path):
    key = "task-a::script.py"
    # First failure.
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:00", is_recovery=False,
    )
    ctx.mark_task_finished(key, "2026-03-24T10:00", success=False)

    persisted = _read_state(state_path)
    assert persisted["last_triggered_slot"][key]["outcome"] == "failure"
    assert persisted["last_triggered_slot"][key]["retry_count"] == 1

    # Second failure -- retry_count should increment.
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:05", is_recovery=False,
    )
    ctx.mark_task_finished(key, "2026-03-24T10:05", success=False)

    persisted = _read_state(state_path)
    assert persisted["last_triggered_slot"][key]["retry_count"] == 2


# ---------------------------------------------------------------------------
# 5. test_mark_task_finished_removes_from_actively_running
# ---------------------------------------------------------------------------

def test_mark_task_finished_removes_from_actively_running(ctx):
    key = "task-a::script.py"
    ctx.actively_running.add(key)
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:00", is_recovery=False,
    )

    ctx.mark_task_finished(key, "2026-03-24T10:00", success=True)

    assert key not in ctx.actively_running


# ---------------------------------------------------------------------------
# 6. test_clear_recovery_entry
# ---------------------------------------------------------------------------

def test_clear_recovery_entry(ctx, state_path):
    key = "task-a::script.py"
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:00", is_recovery=True,
    )
    assert key in ctx.in_progress

    ctx.clear_recovery_entry(key)

    assert key not in ctx.in_progress
    persisted = _read_state(state_path)
    assert key not in persisted["in_progress"]


# ---------------------------------------------------------------------------
# 7. test_mark_slot_consumed_without_run
# ---------------------------------------------------------------------------

def test_mark_slot_consumed_without_run(ctx, state_path):
    key = "task-a::script.py"

    ctx.mark_slot_consumed_without_run(key, "2026-03-24T10:00")

    persisted = _read_state(state_path)
    entry = persisted["last_triggered_slot"][key]
    assert entry["outcome"] == "skipped"
    assert entry["slot"] == "2026-03-24T10:00"


# ---------------------------------------------------------------------------
# 8. test_get_last_slot_dict_format
# ---------------------------------------------------------------------------

def test_get_last_slot_dict_format(ctx):
    key = "task-a::script.py"
    ctx.last_triggered_slot[key] = {
        "slot": "2026-03-24T10:00",
        "outcome": "success",
    }

    assert ctx.get_last_slot(key) == "2026-03-24T10:00"


# ---------------------------------------------------------------------------
# 9. test_get_last_slot_legacy_string
# ---------------------------------------------------------------------------

def test_get_last_slot_legacy_string(ctx):
    key = "task-a::script.py"
    ctx.last_triggered_slot[key] = "2026-03-24T10:00"

    assert ctx.get_last_slot(key) == "2026-03-24T10:00"


# ---------------------------------------------------------------------------
# 10. test_get_last_slot_missing
# ---------------------------------------------------------------------------

def test_get_last_slot_missing(ctx):
    assert ctx.get_last_slot("nonexistent-key") is None


# ---------------------------------------------------------------------------
# 11. test_is_task_actively_running
# ---------------------------------------------------------------------------

def test_is_task_actively_running(ctx):
    key = "task-a::script.py"
    assert ctx.is_task_actively_running(key) is False

    ctx.actively_running.add(key)
    assert ctx.is_task_actively_running(key) is True


# ---------------------------------------------------------------------------
# 12. test_mark_task_finished_queues_push
# ---------------------------------------------------------------------------

def test_mark_task_finished_queues_push(ctx):
    key = "task-a::script.py"
    ctx.mark_task_started(
        key=key, task_name="task-a", script_path=Path("script.py"),
        worker_cost=1, slot_key_value="2026-03-24T10:00", is_recovery=False,
    )

    ctx.mark_task_finished(key, "2026-03-24T10:00", success=True)

    assert not ctx.command_queue.empty()
    assert ctx.command_queue.get_nowait() == "push"


# ---------------------------------------------------------------------------
# 13. test_check_and_queue_dependents_triggers_callback
# ---------------------------------------------------------------------------

def test_check_and_queue_dependents_triggers_callback(tmp_path):
    # Create state where the dependency task has already succeeded.
    dep_task = {
        "id": "dep-task",
        "name": "dep-task",
        "path": "dep_script.py",
        "_depends_on": [],
        "_dependency_only": False,
    }
    dep_only_task = {
        "id": "downstream",
        "name": "downstream",
        "path": "downstream_script.py",
        "_depends_on": ["dep-task"],
        "_dependency_only": True,
    }

    dep_key = task_key(dep_task)      # "dep-task" (uses id)
    downstream_key = task_key(dep_only_task)  # "downstream"

    state_file = tmp_path / "state.json"
    _write_state(state_file, _make_state(
        last_triggered_slot={
            dep_key: {"slot": "2026-03-24T10:00", "outcome": "success", "retry_count": 0},
        },
    ))

    ctx = SchedulerContext(state_file, max_workers=1)
    try:
        ctx._dep_only_tasks = [dep_only_task]
        ctx._all_validated_tasks = [dep_task, dep_only_task]

        triggered = []
        ctx._dep_trigger_callback = lambda task: triggered.append(task)

        ctx._check_and_queue_dependents("dep-task")

        assert len(triggered) == 1
        assert triggered[0]["name"] == "downstream"
        # The downstream key should be recorded in _dep_queued to prevent repeats.
        assert downstream_key in ctx._dep_queued
    finally:
        ctx.shutdown(wait=False)
