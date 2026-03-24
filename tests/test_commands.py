"""Tests for process_commands and process_triggers from sequencer."""

import sys
import queue
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sequencer import process_commands, process_triggers


# ---------------------------------------------------------------------------
# process_commands
# ---------------------------------------------------------------------------

class TestProcessCommands:
    """Tests for process_commands(cmd_queue, state) -> set[str]."""

    @staticmethod
    def _make_queue(*items: str) -> queue.Queue[str]:
        q: queue.Queue[str] = queue.Queue()
        for item in items:
            q.put(item)
        return q

    def test_pause_command(self):
        """'pause:task-1' adds task-1 to paused_tasks."""
        state: dict = {"paused_tasks": []}
        q = self._make_queue("pause:task-1")
        process_commands(q, state)
        assert "task-1" in state["paused_tasks"]

    def test_unpause_command(self):
        """'unpause:task-1' removes task-1 from paused_tasks."""
        state: dict = {"paused_tasks": ["task-1"]}
        q = self._make_queue("unpause:task-1")
        process_commands(q, state)
        assert "task-1" not in state["paused_tasks"]

    def test_run_command(self):
        """'run:task-1' returns {'task-1'}."""
        state: dict = {"paused_tasks": []}
        q = self._make_queue("run:task-1")
        result = process_commands(q, state)
        assert result == {"task-1"}

    def test_pause_idempotent(self):
        """Pausing an already-paused task does not create a duplicate."""
        state: dict = {"paused_tasks": ["task-1"]}
        q = self._make_queue("pause:task-1")
        process_commands(q, state)
        assert state["paused_tasks"].count("task-1") == 1

    def test_unpause_not_paused(self):
        """Unpausing a task that is not paused does not raise an error."""
        state: dict = {"paused_tasks": []}
        q = self._make_queue("unpause:task-1")
        result = process_commands(q, state)
        assert result == set()
        assert state["paused_tasks"] == []

    def test_multiple_commands(self):
        """Multiple commands of the same kind are all processed."""
        state: dict = {"paused_tasks": []}
        q = self._make_queue("pause:a", "pause:b", "pause:c")
        process_commands(q, state)
        assert set(state["paused_tasks"]) == {"a", "b", "c"}

    def test_empty_queue(self):
        """An empty queue returns an empty set and leaves state unchanged."""
        state: dict = {"paused_tasks": []}
        q: queue.Queue[str] = queue.Queue()
        result = process_commands(q, state)
        assert result == set()
        assert state["paused_tasks"] == []

    def test_mixed_commands(self):
        """Pause, unpause, and run in the same batch are all handled."""
        state: dict = {"paused_tasks": ["old"]}
        q = self._make_queue(
            "pause:new",
            "unpause:old",
            "run:immediate",
        )
        result = process_commands(q, state)
        assert "new" in state["paused_tasks"]
        assert "old" not in state["paused_tasks"]
        assert result == {"immediate"}


# ---------------------------------------------------------------------------
# process_triggers
# ---------------------------------------------------------------------------

class TestProcessTriggers:
    """Tests for process_triggers(config_path, state) -> set[str]."""

    def test_pause_trigger_file(self, tmp_path: Path):
        """.pause_task_foo file pauses 'foo' and the file is deleted."""
        trigger = tmp_path / ".pause_task_foo"
        trigger.touch()
        config_path = tmp_path / "config.yaml"
        state: dict = {"paused_tasks": []}

        process_triggers(config_path, state)

        assert "foo" in state["paused_tasks"]
        assert not trigger.exists()

    def test_unpause_trigger_file(self, tmp_path: Path):
        """.unpause_task_foo file unpauses 'foo'."""
        trigger = tmp_path / ".unpause_task_foo"
        trigger.touch()
        config_path = tmp_path / "config.yaml"
        state: dict = {"paused_tasks": ["foo"]}

        process_triggers(config_path, state)

        assert "foo" not in state["paused_tasks"]
        assert not trigger.exists()

    def test_run_trigger_file(self, tmp_path: Path):
        """.run_task_foo file returns {'foo'}."""
        trigger = tmp_path / ".run_task_foo"
        trigger.touch()
        config_path = tmp_path / "config.yaml"
        state: dict = {"paused_tasks": []}

        result = process_triggers(config_path, state)

        assert result == {"foo"}
        assert not trigger.exists()

    def test_trigger_files_cleaned_up(self, tmp_path: Path):
        """All trigger files are deleted after processing."""
        pause_f = tmp_path / ".pause_task_a"
        unpause_f = tmp_path / ".unpause_task_b"
        run_f = tmp_path / ".run_task_c"
        for f in (pause_f, unpause_f, run_f):
            f.touch()
        config_path = tmp_path / "config.yaml"
        state: dict = {"paused_tasks": ["b"]}

        process_triggers(config_path, state)

        assert not pause_f.exists()
        assert not unpause_f.exists()
        assert not run_f.exists()

    def test_no_trigger_files(self, tmp_path: Path):
        """No trigger files returns an empty set."""
        config_path = tmp_path / "config.yaml"
        state: dict = {"paused_tasks": []}

        result = process_triggers(config_path, state)

        assert result == set()
        assert state["paused_tasks"] == []
