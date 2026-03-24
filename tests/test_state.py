import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pytest
from sequencer import load_state, save_state


# ── load_state ──────────────────────────────────────────────────────


def test_load_state_missing_file(tmp_path):
    """File does not exist -> returns default dict with empty sub-dicts."""
    result = load_state(tmp_path / "nonexistent.json")
    assert result == {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
    }


def test_load_state_valid_json(tmp_path):
    """Reads a well-formed state file and returns its contents."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {"task1": "2025-01-01T00:00:00"},
        "in_progress": {"task2": True},
        "profiling": {"task3": {"avg": 1.5}},
        "paused_tasks": ["task4"],
    }
    state_file.write_text(json.dumps(data), encoding="utf-8")
    result = load_state(state_file)
    assert result["last_triggered_slot"] == {"task1": "2025-01-01T00:00:00"}
    assert result["in_progress"] == {"task2": True}
    assert result["profiling"] == {"task3": {"avg": 1.5}}
    assert result["paused_tasks"] == ["task4"]


def test_load_state_corrupt_json(tmp_path):
    """Invalid JSON content -> returns default dict."""
    state_file = tmp_path / "state.json"
    state_file.write_text("{not valid json!!!", encoding="utf-8")
    result = load_state(state_file)
    assert result == {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
    }


def test_load_state_not_a_dict(tmp_path):
    """JSON root is a list instead of dict -> returns default."""
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    result = load_state(state_file)
    assert result == {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
    }


def test_load_state_missing_sub_dicts(tmp_path):
    """JSON dict missing expected keys -> they are initialized to empty."""
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"custom_key": 42}), encoding="utf-8")
    result = load_state(state_file)
    assert result["last_triggered_slot"] == {}
    assert result["in_progress"] == {}
    assert result["profiling"] == {}
    assert result["paused_tasks"] == []
    # original key is still present
    assert result["custom_key"] == 42


def test_load_state_removes_run_now_tasks(tmp_path):
    """run_now_tasks is a transient key and must be popped on load."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "run_now_tasks": ["t1"],
    }
    state_file.write_text(json.dumps(data), encoding="utf-8")
    result = load_state(state_file)
    assert "run_now_tasks" not in result


def test_load_state_removes_last_outcomes(tmp_path):
    """last_outcomes is a legacy key and must be popped on load."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "last_outcomes": {"task1": "ok"},
    }
    state_file.write_text(json.dumps(data), encoding="utf-8")
    result = load_state(state_file)
    assert "last_outcomes" not in result


def test_load_state_preserves_paused_tasks(tmp_path):
    """A valid paused_tasks list is kept as-is."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "paused_tasks": ["task_a", "task_b"],
    }
    state_file.write_text(json.dumps(data), encoding="utf-8")
    result = load_state(state_file)
    assert result["paused_tasks"] == ["task_a", "task_b"]


def test_load_state_fixes_non_list_paused_tasks(tmp_path):
    """paused_tasks that is not a list -> replaced with []."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {},
        "in_progress": {},
        "profiling": {},
        "paused_tasks": "not-a-list",
    }
    state_file.write_text(json.dumps(data), encoding="utf-8")
    result = load_state(state_file)
    assert result["paused_tasks"] == []


# ── save_state ──────────────────────────────────────────────────────


def test_save_state_writes_json(tmp_path):
    """Roundtrip: save then load returns the same data."""
    state_file = tmp_path / "state.json"
    data = {
        "last_triggered_slot": {"t1": "2025-06-01"},
        "in_progress": {"t2": True},
        "profiling": {"t3": {"avg": 2.0}},
        "paused_tasks": ["t4"],
    }
    save_state(state_file, data)
    result = load_state(state_file)
    assert result == data


def test_save_state_creates_file(tmp_path):
    """Writing to a non-existent file creates it."""
    state_file = tmp_path / "brand_new.json"
    assert not state_file.exists()
    save_state(state_file, {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}})
    assert state_file.exists()
    content = json.loads(state_file.read_text(encoding="utf-8"))
    assert content == {"last_triggered_slot": {}, "in_progress": {}, "profiling": {}}


def test_save_state_overwrites(tmp_path):
    """A second save overwrites the first."""
    state_file = tmp_path / "state.json"
    save_state(state_file, {"last_triggered_slot": {"old": "data"}, "in_progress": {}, "profiling": {}})
    save_state(state_file, {"last_triggered_slot": {"new": "data"}, "in_progress": {}, "profiling": {}})
    content = json.loads(state_file.read_text(encoding="utf-8"))
    assert content["last_triggered_slot"] == {"new": "data"}
    assert "old" not in content["last_triggered_slot"]
