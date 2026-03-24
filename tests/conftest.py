"""Shared fixtures for the scheduler test suite."""

import sys
import datetime as dt
from pathlib import Path

import pytest

# Ensure project root is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import sequencer


@pytest.fixture
def tmp_config(tmp_path):
    """Create a minimal schedule.yaml and return its Path."""
    config = tmp_path / "schedule.yaml"
    config.write_text(
        "tasks:\n"
        '  - id: "task-a"\n'
        '    path: "script_a.py"\n'
        "    frequency_min: 1\n",
        encoding="utf-8",
    )
    # Create the dummy script so "script not found" checks pass
    (tmp_path / "script_a.py").write_text("print('ok')\n", encoding="utf-8")
    return config


@pytest.fixture
def tmp_settings(tmp_path):
    """Create a settings.yaml alongside the config and return its Path."""
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "settings:\n"
        "  retry_delay_seconds: 60\n"
        "  retry_max_delay_seconds: 1800\n"
        "  max_workers: 4\n",
        encoding="utf-8",
    )
    return settings


@pytest.fixture
def tmp_state_path(tmp_path):
    """Return path to a state JSON file inside tmp_path."""
    return tmp_path / "sequencer_state.json"


@pytest.fixture
def frozen_now():
    """A fixed datetime for deterministic tests."""
    return dt.datetime(2026, 3, 24, 9, 30, 0)


def make_validated_task(**overrides):
    """Factory that produces a minimal validated task dict with all internal fields."""
    defaults = {
        "id": "test-task",
        "name": "test-task",
        "path": "script.py",
        "_months": set(range(1, 13)),
        "_month_day": None,
        "_week_day": None,
        "_start_hour": None,
        "_start_minute": 0,
        "_frequency_min": None,
        "_end_hour": None,
        "_end_minute": 0,
        "_times": None,
        "_depends_on": [],
        "_dependency_only": False,
        "_timeout_minutes": None,
    }
    defaults.update(overrides)
    return defaults
