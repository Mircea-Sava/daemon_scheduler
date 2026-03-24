import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sequencer import task_key


def test_task_key_with_id():
    """When task has a truthy 'id', return it as a string."""
    assert task_key({"id": "my-task", "name": "N", "path": "p.py"}) == "my-task"


def test_task_key_without_id():
    """When 'id' is None, fall back to 'name::path'."""
    assert task_key({"id": None, "name": "My Task", "path": "s.py"}) == "My Task::s.py"


def test_task_key_empty_id():
    """When 'id' is an empty string (falsy), fall back to 'name::path'."""
    assert task_key({"id": "", "name": "N", "path": "p.py"}) == "N::p.py"


def test_task_key_integer_id():
    """When 'id' is an integer, it is converted to string."""
    assert task_key({"id": 42, "name": "N", "path": "p.py"}) == "42"


def test_task_key_deterministic():
    """Same input always produces the same output."""
    task = {"id": "stable", "name": "N", "path": "p.py"}
    results = {task_key(task) for _ in range(100)}
    assert len(results) == 1
    assert results.pop() == "stable"
