"""Tests for bootstrap functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import _parse_requires_python, _find_bundled_python


# ---------------------------------------------------------------------------
# _parse_requires_python
# ---------------------------------------------------------------------------

def test_parse_requires_python_ge(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('requires-python = ">=3.12"\n', encoding="utf-8")
    assert _parse_requires_python(pyproject) == "3.12"


def test_parse_requires_python_exact(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('requires-python = "==3.11"\n', encoding="utf-8")
    assert _parse_requires_python(pyproject) == "3.11"


def test_parse_requires_python_missing_field(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'test'\n", encoding="utf-8")
    assert _parse_requires_python(pyproject) == "3"


def test_parse_requires_python_file_not_found(tmp_path):
    assert _parse_requires_python(tmp_path / "nonexistent.toml") == "3"


def test_parse_requires_python_tilde(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('requires-python = "~=3.10"\n', encoding="utf-8")
    assert _parse_requires_python(pyproject) == "3.10"


# ---------------------------------------------------------------------------
# _find_bundled_python
# ---------------------------------------------------------------------------

def test_find_bundled_python_no_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("sequencer._BUNDLED_PYTHON_DIR", tmp_path / "nonexistent")
    assert _find_bundled_python("3.12") is None


def test_find_bundled_python_matching_version(tmp_path, monkeypatch):
    monkeypatch.setattr("sequencer._BUNDLED_PYTHON_DIR", tmp_path)
    python_dir = tmp_path / "cpython-3.12-win"
    python_dir.mkdir()
    exe = python_dir / "python.exe"
    exe.write_text("fake", encoding="utf-8")
    result = _find_bundled_python("3.12")
    assert result == str(exe)


def test_find_bundled_python_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr("sequencer._BUNDLED_PYTHON_DIR", tmp_path)
    python_dir = tmp_path / "cpython-3.11-win"
    python_dir.mkdir()
    exe = python_dir / "python.exe"
    exe.write_text("fake", encoding="utf-8")
    # Request 3.12 but only 3.11 exists — should fallback
    result = _find_bundled_python("3.12")
    assert result == str(exe)


def test_find_bundled_python_no_exe(tmp_path, monkeypatch):
    monkeypatch.setattr("sequencer._BUNDLED_PYTHON_DIR", tmp_path)
    python_dir = tmp_path / "cpython-3.12-win"
    python_dir.mkdir()
    # No python.exe inside
    assert _find_bundled_python("3.12") is None
