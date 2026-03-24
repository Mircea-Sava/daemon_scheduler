"""Tests for load_config – YAML loading and settings.yaml merging."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from sequencer import load_config


# ---------------------------------------------------------------------------
# Basic loading
# ---------------------------------------------------------------------------

def test_load_config_basic(tmp_path):
    """A minimal schedule.yaml with one task loads successfully."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text(
        "tasks:\n"
        '  - id: "t1"\n'
        '    path: "s.py"\n',
        encoding="utf-8",
    )
    result = load_config(cfg)
    assert isinstance(result, dict)
    assert "tasks" in result
    assert result["tasks"][0]["id"] == "t1"


def test_load_config_invalid_yaml_raises(tmp_path):
    """Malformed YAML must raise ValueError."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks:\n  - id: [\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid YAML"):
        load_config(cfg)


def test_load_config_not_a_dict_raises(tmp_path):
    """YAML root that is a list (not a dict) must raise ValueError."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("- one\n- two\n", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML root must be a dictionary"):
        load_config(cfg)


def test_load_config_empty_file(tmp_path):
    """An empty YAML file returns an empty dict."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("", encoding="utf-8")
    result = load_config(cfg)
    assert result == {}


# ---------------------------------------------------------------------------
# settings.yaml merging
# ---------------------------------------------------------------------------

def test_load_config_merges_settings_yaml(tmp_path):
    """When settings.yaml lives alongside config, its 'settings' key is merged."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks: []\n", encoding="utf-8")

    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "settings:\n"
        "  retry_delay_seconds: 60\n"
        "  max_workers: 4\n",
        encoding="utf-8",
    )

    result = load_config(cfg)
    assert "settings" in result
    assert result["settings"]["retry_delay_seconds"] == 60
    assert result["settings"]["max_workers"] == 4


def test_load_config_settings_yaml_not_found(tmp_path):
    """Without a settings.yaml, no 'settings' key appears in the config."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks: []\n", encoding="utf-8")

    result = load_config(cfg)
    assert "settings" not in result


def test_load_config_settings_yaml_invalid_raises(tmp_path):
    """Invalid YAML in settings.yaml must raise ValueError."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks: []\n", encoding="utf-8")

    settings = tmp_path / "settings.yaml"
    settings.write_text("key: [\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid YAML"):
        load_config(cfg)


def test_load_config_settings_yaml_not_dict(tmp_path):
    """If settings.yaml parses to a list, isinstance(dict) fails and settings are not merged."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks: []\n", encoding="utf-8")

    settings = tmp_path / "settings.yaml"
    settings.write_text("- a\n- b\n", encoding="utf-8")

    result = load_config(cfg)
    assert "settings" not in result


def test_load_config_settings_yaml_nested(tmp_path):
    """settings.yaml with a 'settings:' wrapper: inner dict is extracted."""
    cfg = tmp_path / "schedule.yaml"
    cfg.write_text("tasks: []\n", encoding="utf-8")

    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "settings:\n"
        "  log_dir: logs\n"
        "  log_keep_count: 50\n",
        encoding="utf-8",
    )

    result = load_config(cfg)
    assert result["settings"]["log_dir"] == "logs"
    assert result["settings"]["log_keep_count"] == 50
