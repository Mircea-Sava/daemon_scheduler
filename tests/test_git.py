"""Tests for git operations (mocked subprocess)."""

import sys
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sequencer import git_pull, git_push, _git_remote_has_changes


# ---------------------------------------------------------------------------
# _git_remote_has_changes
# ---------------------------------------------------------------------------

def test_remote_no_changes():
    mock_fetch = MagicMock()
    mock_revlist = MagicMock(returncode=0, stdout="0\n")
    with patch("sequencer.subprocess.run", side_effect=[mock_fetch, mock_revlist]):
        assert _git_remote_has_changes(Path(".")) is False


def test_remote_has_changes():
    mock_fetch = MagicMock()
    mock_revlist = MagicMock(returncode=0, stdout="3\n")
    with patch("sequencer.subprocess.run", side_effect=[mock_fetch, mock_revlist]):
        assert _git_remote_has_changes(Path(".")) is True


def test_remote_error_returns_true():
    with patch("sequencer.subprocess.run", side_effect=Exception("network error")):
        assert _git_remote_has_changes(Path(".")) is True


# ---------------------------------------------------------------------------
# git_pull
# ---------------------------------------------------------------------------

def test_pull_no_changes():
    with patch("sequencer._git_remote_has_changes", return_value=False):
        result = git_pull(Path("."))
    assert result == "no changes"


def test_pull_updated():
    mock_result = MagicMock(returncode=0, stdout="Updating abc..def\nFast-forward\n", stderr="")
    with patch("sequencer._git_remote_has_changes", return_value=True), \
         patch("sequencer.subprocess.run", return_value=mock_result):
        result = git_pull(Path("."))
    assert result.startswith("updated")


def test_pull_failed():
    mock_result = MagicMock(returncode=1, stdout="", stderr="merge conflict")
    with patch("sequencer._git_remote_has_changes", return_value=True), \
         patch("sequencer.subprocess.run", return_value=mock_result):
        result = git_pull(Path("."))
    assert result.startswith("failed")


def test_pull_timeout():
    with patch("sequencer._git_remote_has_changes", return_value=True), \
         patch("sequencer.subprocess.run", side_effect=subprocess.TimeoutExpired("git", 120)):
        result = git_pull(Path("."))
    assert "timeout" in result


def test_pull_git_not_found():
    with patch("sequencer._git_remote_has_changes", return_value=True), \
         patch("sequencer.subprocess.run", side_effect=FileNotFoundError()):
        result = git_pull(Path("."))
    assert "git not found" in result


# ---------------------------------------------------------------------------
# git_push
# ---------------------------------------------------------------------------

def test_push_no_changes():
    mock_add = MagicMock()
    mock_diff = MagicMock(returncode=0)  # returncode 0 = no staged changes
    with patch("sequencer.subprocess.run", side_effect=[mock_add, mock_diff]):
        result = git_push(Path("."))
    assert result == "no changes"


def test_push_success():
    mock_add = MagicMock()
    mock_diff = MagicMock(returncode=1)  # returncode 1 = there are staged changes
    mock_commit = MagicMock()
    mock_push = MagicMock(returncode=0, stdout="ok", stderr="")
    with patch("sequencer.subprocess.run", side_effect=[mock_add, mock_diff, mock_commit, mock_push]):
        result = git_push(Path("."))
    assert result == "pushed"


def test_push_failed():
    mock_add = MagicMock()
    mock_diff = MagicMock(returncode=1)
    mock_commit = MagicMock()
    mock_push = MagicMock(returncode=1, stdout="", stderr="rejected")
    with patch("sequencer.subprocess.run", side_effect=[mock_add, mock_diff, mock_commit, mock_push]):
        result = git_push(Path("."))
    assert result.startswith("failed")


def test_push_timeout():
    mock_add = MagicMock()
    with patch("sequencer.subprocess.run", side_effect=[mock_add, subprocess.TimeoutExpired("git", 30)]):
        result = git_push(Path("."))
    assert "timeout" in result


def test_push_git_not_found():
    with patch("sequencer.subprocess.run", side_effect=FileNotFoundError()):
        result = git_push(Path("."))
    assert "git not found" in result
