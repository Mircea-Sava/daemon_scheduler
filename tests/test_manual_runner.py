"""Tests for bin/_manual_runner.py parsing and ordering logic."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bin"))

from _manual_runner import parse_tasks, _topo_sort, SCHEDULE, ROOT


# ---------------------------------------------------------------------------
# _topo_sort
# ---------------------------------------------------------------------------

def _t(id, deps=None):
    return {"id": id, "path": f"{id}.py", "depends_on": deps or []}


def test_topo_sort_no_deps():
    tasks = [_t("A"), _t("B"), _t("C")]
    result = [t["id"] for t in _topo_sort(tasks)]
    assert result == ["A", "B", "C"]


def test_topo_sort_simple_chain():
    tasks = [_t("C", ["B"]), _t("B", ["A"]), _t("A")]
    result = [t["id"] for t in _topo_sort(tasks)]
    assert result == ["A", "B", "C"]


def test_topo_sort_diamond():
    tasks = [_t("D", ["B", "C"]), _t("B", ["A"]), _t("C", ["A"]), _t("A")]
    result = [t["id"] for t in _topo_sort(tasks)]
    assert result.index("A") < result.index("B")
    assert result.index("A") < result.index("C")
    assert result.index("B") < result.index("D")
    assert result.index("C") < result.index("D")


def test_topo_sort_missing_dep_ignored():
    tasks = [_t("B", ["ghost"]), _t("A")]
    result = [t["id"] for t in _topo_sort(tasks)]
    assert "B" in result and "A" in result


def test_topo_sort_single_task():
    result = _topo_sort([_t("A")])
    assert len(result) == 1 and result[0]["id"] == "A"


def test_topo_sort_preserves_order_when_no_deps():
    tasks = [_t("Z"), _t("M"), _t("A")]
    result = [t["id"] for t in _topo_sort(tasks)]
    assert result == ["Z", "M", "A"]


# ---------------------------------------------------------------------------
# parse_tasks
# ---------------------------------------------------------------------------

def test_parse_tasks_from_schedule(tmp_path, monkeypatch):
    schedule = tmp_path / "schedule.yaml"
    schedule.write_text(
        "tasks:\n"
        '  - id: "A"\n'
        '    path: "a.py"\n'
        '    frequency_min: 1\n'
        '  - id: "B"\n'
        '    path: "b.py"\n'
        '    depends_on: "A"\n',
        encoding="utf-8",
    )
    import _manual_runner
    monkeypatch.setattr(_manual_runner, "SCHEDULE", schedule)
    tasks = _manual_runner.parse_tasks()
    assert len(tasks) == 2
    assert tasks[0]["id"] == "A"
    assert tasks[0]["depends_on"] == []
    assert tasks[1]["id"] == "B"
    assert tasks[1]["depends_on"] == ["A"]


def test_parse_tasks_multi_deps(tmp_path, monkeypatch):
    schedule = tmp_path / "schedule.yaml"
    schedule.write_text(
        "tasks:\n"
        '  - id: "C"\n'
        '    path: "c.py"\n'
        '    depends_on: "A, B"\n',
        encoding="utf-8",
    )
    import _manual_runner
    monkeypatch.setattr(_manual_runner, "SCHEDULE", schedule)
    tasks = _manual_runner.parse_tasks()
    assert tasks[0]["depends_on"] == ["A", "B"]


def test_parse_tasks_skips_comments(tmp_path, monkeypatch):
    schedule = tmp_path / "schedule.yaml"
    schedule.write_text(
        "tasks:\n"
        '  - id: "A"\n'
        '    path: "a.py"\n'
        '#  - id: "hidden"\n'
        '#    path: "hidden.py"\n'
        '  - id: "B"\n'
        '    path: "b.py"\n',
        encoding="utf-8",
    )
    import _manual_runner
    monkeypatch.setattr(_manual_runner, "SCHEDULE", schedule)
    tasks = _manual_runner.parse_tasks()
    ids = [t["id"] for t in tasks]
    assert "hidden" not in ids
    assert ids == ["A", "B"]


def test_parse_tasks_empty_list(tmp_path, monkeypatch):
    schedule = tmp_path / "schedule.yaml"
    schedule.write_text("tasks:\n  []\n", encoding="utf-8")
    import _manual_runner
    monkeypatch.setattr(_manual_runner, "SCHEDULE", schedule)
    tasks = _manual_runner.parse_tasks()
    assert tasks == []
