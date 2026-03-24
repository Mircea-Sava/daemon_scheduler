"""Tests for compute_next_wake_time from sequencer."""

import sys
import datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sequencer import compute_next_wake_time


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _task(**overrides):
    defaults = {
        "id": "t1",
        "_months": set(range(1, 13)),
        "_month_day": None,
        "_week_day": None,
        "_start_hour": None,
        "_start_minute": 0,
        "_frequency_min": None,
        "_end_hour": None,
        "_end_minute": 0,
        "_times": None,
        "_dependency_only": False,
    }
    defaults.update(overrides)
    return defaults


_EMPTY_STATE: dict = {"last_triggered_slot": {}}
_DEFAULT_SETTINGS: dict = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}


# ---------------------------------------------------------------------------
# 1. No tasks -> max horizon
# ---------------------------------------------------------------------------

def test_no_tasks_returns_max_horizon():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    result = compute_next_wake_time([], _EMPTY_STATE, _DEFAULT_SETTINGS, now, set())
    assert result == now + dt.timedelta(minutes=60)


# ---------------------------------------------------------------------------
# 2. Single task fires every minute -> wake at now + 1 min
# ---------------------------------------------------------------------------

def test_single_task_next_minute():
    # _start_hour=None means "run every tick" -> should_run is True for every
    # candidate minute, so the first candidate (base + 1 min) matches.
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    tasks = [_task()]
    result = compute_next_wake_time(tasks, _EMPTY_STATE, _DEFAULT_SETTINGS, now, set())
    expected = now.replace(second=0, microsecond=0) + dt.timedelta(minutes=1)
    assert result == expected


# ---------------------------------------------------------------------------
# 3. Task fires in 30 minutes
# ---------------------------------------------------------------------------

def test_task_fires_in_30_minutes():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    # Task fires once at 10:30
    tasks = [_task(_start_hour=10, _start_minute=30)]
    result = compute_next_wake_time(tasks, _EMPTY_STATE, _DEFAULT_SETTINGS, now, set())
    expected = dt.datetime(2026, 3, 24, 10, 30, 0)
    assert result == expected


# ---------------------------------------------------------------------------
# 4. Skips dependency-only tasks
# ---------------------------------------------------------------------------

def test_skips_dependency_only_tasks():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    # A dependency-only task that would otherwise fire every minute
    tasks = [_task(_dependency_only=True)]
    result = compute_next_wake_time(tasks, _EMPTY_STATE, _DEFAULT_SETTINGS, now, set())
    # No schedulable tasks -> falls back to max horizon
    assert result == now + dt.timedelta(minutes=60)


# ---------------------------------------------------------------------------
# 5. Skips paused tasks
# ---------------------------------------------------------------------------

def test_skips_paused_tasks():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    tasks = [_task(id="paused_task")]
    paused = {"paused_task"}
    result = compute_next_wake_time(tasks, _EMPTY_STATE, _DEFAULT_SETTINGS, now, paused)
    assert result == now + dt.timedelta(minutes=60)


# ---------------------------------------------------------------------------
# 6. Retry timer creates a candidate
# ---------------------------------------------------------------------------

def test_retry_timer_creates_candidate():
    now = dt.datetime(2026, 3, 24, 9, 30, 0)
    state = {
        "last_triggered_slot": {
            "t1": {
                "outcome": "failure",
                "last_run": "2026-03-24 09:29:00",
                "retry_count": 0,
            }
        }
    }
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}
    # No scheduled tasks -- only the retry timer matters.
    result = compute_next_wake_time([], state, settings, now, set())
    # retry_delay = 60 * 2^0 = 60s -> last_run + 60s = 09:30:00
    # But result must be >= now + 1s, so clamp to 09:30:01
    expected = now + dt.timedelta(seconds=1)
    assert result == expected


# ---------------------------------------------------------------------------
# 7. Retry timer already passed -> wake at now + 1 sec
# ---------------------------------------------------------------------------

def test_retry_timer_already_passed():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    state = {
        "last_triggered_slot": {
            "t1": {
                "outcome": "failure",
                "last_run": "2026-03-24 09:29:00",
                "retry_count": 0,
            }
        }
    }
    settings = {"retry_delay_seconds": 60, "retry_max_delay_seconds": 1800}
    # retry fires at 09:30:00, well before now=10:00:00 -> clamp to now+1s
    result = compute_next_wake_time([], state, settings, now, set())
    assert result == now + dt.timedelta(seconds=1)


# ---------------------------------------------------------------------------
# 8. Multiple tasks -> picks the earliest
# ---------------------------------------------------------------------------

def test_multiple_tasks_picks_earliest():
    now = dt.datetime(2026, 3, 24, 10, 0, 0)
    # Task A fires at 10:20, Task B fires at 10:10 -> pick B
    task_a = _task(id="a", _start_hour=10, _start_minute=20)
    task_b = _task(id="b", _start_hour=10, _start_minute=10)
    result = compute_next_wake_time(
        [task_a, task_b], _EMPTY_STATE, _DEFAULT_SETTINGS, now, set()
    )
    assert result == dt.datetime(2026, 3, 24, 10, 10, 0)


# ---------------------------------------------------------------------------
# 9. Wake is always at least 1 second in the future
# ---------------------------------------------------------------------------

def test_wake_at_least_1_second_future():
    now = dt.datetime(2026, 3, 24, 10, 0, 30)
    # Task fires every minute -> next candidate is base(10:00) + 1 min = 10:01
    # 10:01 > now + 1s, so this is fine already.  But let's construct a retry
    # that would fire exactly at `now`:
    state = {
        "last_triggered_slot": {
            "t1": {
                "outcome": "failure",
                "last_run": "2026-03-24 10:00:00",
                "retry_count": 0,
            }
        }
    }
    settings = {"retry_delay_seconds": 30, "retry_max_delay_seconds": 1800}
    # retry fires at 10:00:00 + 30s = 10:00:30 == now -> clamp to now + 1s
    result = compute_next_wake_time([], state, settings, now, set())
    assert result >= now + dt.timedelta(seconds=1)
