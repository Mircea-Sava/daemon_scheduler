"""Tests for compute_retry_delay – exponential backoff with cap."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sequencer import compute_retry_delay


def test_first_attempt():
    """retry_count 0 → delay equals base_delay unchanged."""
    assert compute_retry_delay(60, 0, 1800) == 60


def test_second_attempt():
    """retry_count 1 → base_delay * 2."""
    assert compute_retry_delay(60, 1, 1800) == 120


def test_third_attempt():
    """retry_count 2 → base_delay * 4."""
    assert compute_retry_delay(60, 2, 1800) == 240


def test_capped_at_max():
    """Large retry_count must not exceed max_delay."""
    assert compute_retry_delay(60, 10, 1800) == 1800


def test_zero_base():
    """Zero base_delay always produces zero regardless of retry_count."""
    assert compute_retry_delay(0, 5, 1800) == 0


def test_max_equals_base():
    """When max_delay == base_delay the result is always that value."""
    assert compute_retry_delay(60, 0, 60) == 60


def test_full_sequence():
    """Walk through successive retries and verify the entire ramp-up + cap."""
    expected = [60, 120, 240, 480, 960, 1800, 1800, 1800]
    actual = [compute_retry_delay(60, i, 1800) for i in range(len(expected))]
    assert actual == expected
