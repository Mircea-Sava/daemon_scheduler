"""Tests for resolve_dynamic_worker_cost and update_profiling_state from sequencer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sequencer import resolve_dynamic_worker_cost, update_profiling_state


# ---------------------------------------------------------------------------
# resolve_dynamic_worker_cost
# ---------------------------------------------------------------------------

class TestResolveDynamicWorkerCost:
    """Tests for resolve_dynamic_worker_cost(key, profiling_state, max_workers, default_cost)."""

    def test_no_profiling_data(self):
        """Returns default_cost when the key has no profiling data."""
        result = resolve_dynamic_worker_cost("task-x", {}, max_workers=80, default_cost=100)
        assert result == 100

    def test_with_learned_cost(self):
        """Returns the learned_cost from profiling_state."""
        profiling = {"task-x": {"learned_cost": 25}}
        result = resolve_dynamic_worker_cost("task-x", profiling, max_workers=80)
        assert result == 25

    def test_clamps_to_max_workers(self):
        """learned_cost exceeding max_workers is clamped down."""
        profiling = {"task-x": {"learned_cost": 200}}
        result = resolve_dynamic_worker_cost("task-x", profiling, max_workers=80)
        assert result == 80

    def test_minimum_is_one(self):
        """learned_cost of 0 is clamped up to 1."""
        profiling = {"task-x": {"learned_cost": 0}}
        result = resolve_dynamic_worker_cost("task-x", profiling, max_workers=80)
        assert result == 1

    def test_non_dict_entry(self):
        """A non-dict entry in profiling_state falls back to default_cost."""
        profiling = {"task-x": "invalid"}
        result = resolve_dynamic_worker_cost("task-x", profiling, max_workers=80, default_cost=100)
        assert result == 100


# ---------------------------------------------------------------------------
# update_profiling_state
# ---------------------------------------------------------------------------

class TestUpdateProfilingState:
    """Tests for update_profiling_state(key, profiling_state, peak_ram_pct, avg_cpu_pct, max_workers)."""

    def test_basic_update(self):
        """Sets peak_ram_pct, avg_cpu_pct, and learned_cost in profiling_state."""
        profiling: dict = {}
        update_profiling_state("task-x", profiling, peak_ram_pct=40.0, avg_cpu_pct=30.0, max_workers=80)
        entry = profiling["task-x"]
        assert entry["peak_ram_pct"] == 40.0
        assert entry["avg_cpu_pct"] == 30.0
        assert entry["learned_cost"] == 40

    def test_learned_cost_picks_max(self):
        """learned_cost is max(peak_ram_pct, avg_cpu_pct)."""
        profiling: dict = {}
        update_profiling_state("task-x", profiling, peak_ram_pct=30.0, avg_cpu_pct=50.0, max_workers=80)
        assert profiling["task-x"]["learned_cost"] == 50

    def test_clamped_to_max_workers(self):
        """learned_cost exceeding max_workers is clamped."""
        profiling: dict = {}
        update_profiling_state("task-x", profiling, peak_ram_pct=120.0, avg_cpu_pct=90.0, max_workers=80)
        assert profiling["task-x"]["learned_cost"] == 80

    def test_minimum_one(self):
        """learned_cost is at least 1, even when both values are 0."""
        profiling: dict = {}
        update_profiling_state("task-x", profiling, peak_ram_pct=0.0, avg_cpu_pct=0.0, max_workers=80)
        assert profiling["task-x"]["learned_cost"] == 1

    def test_rounds_percentages(self):
        """peak_ram_pct and avg_cpu_pct are rounded to 2 decimal places."""
        profiling: dict = {}
        update_profiling_state("task-x", profiling, peak_ram_pct=33.33567, avg_cpu_pct=12.98765, max_workers=80)
        entry = profiling["task-x"]
        assert entry["peak_ram_pct"] == 33.34
        assert entry["avg_cpu_pct"] == 12.99
