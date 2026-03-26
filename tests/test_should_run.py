"""Tests for sequencer.should_run()."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import datetime as dt
import pytest
from sequencer import should_run


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _task(**overrides):
    defaults = {
        "_months": set(range(1, 13)),
        "_month_day": None,
        "_week_day": None,
        "_start_hour": None,
        "_start_minute": 0,
        "_frequency_min": None,
        "_end_hour": None,
        "_end_minute": 0,
        "_times": None,
    }
    defaults.update(overrides)
    return defaults


# Base date: March 24 2026 is a Tuesday (weekday == 1)
BASE = dt.datetime(2026, 3, 24)


# ---------------------------------------------------------------------------
# 1. No filters at all -- should run on every tick
# ---------------------------------------------------------------------------

def test_no_filters_runs_every_tick():
    now = BASE.replace(hour=12, minute=30)
    assert should_run(_task(), now) is True


# ---------------------------------------------------------------------------
# 2-3. Month filter
# ---------------------------------------------------------------------------

def test_month_filter_matches():
    now = BASE.replace(hour=10)  # March
    assert should_run(_task(_months={3}), now) is True


def test_month_filter_no_match():
    now = dt.datetime(2026, 4, 24, 10, 0)  # April
    assert should_run(_task(_months={3}), now) is False


# ---------------------------------------------------------------------------
# 4-5. Month-day filter
# ---------------------------------------------------------------------------

def test_month_day_filter_matches():
    now = dt.datetime(2026, 3, 15, 10, 0)
    assert should_run(_task(_month_day={15}), now) is True


def test_month_day_filter_no_match():
    now = dt.datetime(2026, 3, 16, 10, 0)
    assert should_run(_task(_month_day={15}), now) is False


# ---------------------------------------------------------------------------
# 6-7. Week-day filter  (Tuesday == 1 for our base date)
# ---------------------------------------------------------------------------

def test_week_day_filter_matches():
    # March 23 2026 is Monday (weekday 0)
    now = dt.datetime(2026, 3, 23, 10, 0)
    assert now.weekday() == 0  # sanity check
    assert should_run(_task(_week_day={0}), now) is True


def test_week_day_filter_no_match():
    # March 23 2026 is Monday (weekday 0), filter requires Friday (4)
    now = dt.datetime(2026, 3, 23, 10, 0)
    assert should_run(_task(_week_day={4}), now) is False


# ---------------------------------------------------------------------------
# 8-10. _times shorthand
# ---------------------------------------------------------------------------

def test_times_matches_exact_time():
    now = BASE.replace(hour=9, minute=0)
    assert should_run(_task(_times=[(9, 0), (14, 0)]), now) is True


def test_times_no_match():
    now = BASE.replace(hour=10, minute=0)
    assert should_run(_task(_times=[(9, 0), (14, 0)]), now) is False


def test_times_matches_second_time():
    now = BASE.replace(hour=14, minute=0)
    assert should_run(_task(_times=[(9, 0), (14, 0)]), now) is True


# ---------------------------------------------------------------------------
# 11-12. start_hour only (no frequency) -- fires once at exact time
# ---------------------------------------------------------------------------

def test_start_hour_only_exact_match():
    now = BASE.replace(hour=9, minute=0)
    assert should_run(_task(_start_hour=9, _start_minute=0), now) is True


def test_start_hour_only_no_match():
    now = BASE.replace(hour=9, minute=1)
    assert should_run(_task(_start_hour=9, _start_minute=0), now) is False


# ---------------------------------------------------------------------------
# 13. Frequency every 30 min starting at 09:00
# ---------------------------------------------------------------------------

def test_frequency_every_30_min():
    t = _task(_start_hour=9, _start_minute=0, _frequency_min=30)
    assert should_run(t, BASE.replace(hour=9, minute=0)) is True
    assert should_run(t, BASE.replace(hour=9, minute=15)) is False
    assert should_run(t, BASE.replace(hour=9, minute=30)) is True
    assert should_run(t, BASE.replace(hour=10, minute=0)) is True


# ---------------------------------------------------------------------------
# 14. Frequency before start window returns False
# ---------------------------------------------------------------------------

def test_frequency_before_start_returns_false():
    t = _task(_start_hour=9, _start_minute=0, _frequency_min=30)
    now = BASE.replace(hour=8, minute=59)
    assert should_run(t, now) is False


# ---------------------------------------------------------------------------
# 15. Frequency with end_hour
# ---------------------------------------------------------------------------

def test_frequency_with_end_hour():
    t = _task(
        _start_hour=9, _start_minute=0,
        _frequency_min=30,
        _end_hour=18, _end_minute=0,
    )
    # 18:00 is exactly on end boundary -> should still run (540 min elapsed,
    # 540 % 30 == 0 and current_minutes == window_end)
    assert should_run(t, BASE.replace(hour=18, minute=0)) is True
    # 18:01 exceeds end -> False
    assert should_run(t, BASE.replace(hour=18, minute=1)) is False


# ---------------------------------------------------------------------------
# 16. No end_hour -- frequency runs indefinitely through the day
# ---------------------------------------------------------------------------

def test_frequency_no_end_hour_runs_indefinitely():
    t = _task(_start_hour=9, _start_minute=0, _frequency_min=30)
    # 23:00 -> elapsed = (23*60) - (9*60) = 840 min, 840 % 30 == 0
    assert should_run(t, BASE.replace(hour=23, minute=0)) is True


# ---------------------------------------------------------------------------
# 17. Frequency with start_minute offset
# ---------------------------------------------------------------------------

def test_frequency_with_start_minute_offset():
    t = _task(_start_hour=9, _start_minute=15, _frequency_min=30)
    # 09:15 -> elapsed 0, 0 % 30 == 0 -> True
    assert should_run(t, BASE.replace(hour=9, minute=15)) is True
    # 09:45 -> elapsed 30, 30 % 30 == 0 -> True
    assert should_run(t, BASE.replace(hour=9, minute=45)) is True
    # 09:30 -> elapsed 15, 15 % 30 != 0 -> False
    assert should_run(t, BASE.replace(hour=9, minute=30)) is False


# ---------------------------------------------------------------------------
# 18-19. Combined filters -- month AND weekday
# ---------------------------------------------------------------------------

def test_combined_month_and_weekday():
    # March 23 2026 is Monday (weekday 0)
    now = dt.datetime(2026, 3, 23, 10, 0)
    t = _task(_months={3}, _week_day={0})
    assert should_run(t, now) is True


def test_combined_month_filter_blocks():
    # April 2026, but _months only allows March
    now = dt.datetime(2026, 4, 6, 10, 0)  # a Monday in April
    t = _task(_months={3}, _week_day={0})
    assert should_run(t, now) is False


# ---------------------------------------------------------------------------
# 20. start_at in the future blocks execution
# ---------------------------------------------------------------------------

def test_start_at_blocks_before_window():
    future = dt.datetime(2026, 6, 1, 0, 0)
    now = BASE.replace(hour=10, minute=0)
    t = _task(start_at=future)
    assert should_run(t, now) is False


# ---------------------------------------------------------------------------
# 21. end_at in the past blocks execution
# ---------------------------------------------------------------------------

def test_end_at_blocks_after_window():
    past = dt.datetime(2026, 1, 1, 0, 0)
    now = BASE.replace(hour=10, minute=0)
    t = _task(end_at=past)
    assert should_run(t, now) is False


# ---------------------------------------------------------------------------
# 22. No start_hour + no frequency -> runs every tick
# ---------------------------------------------------------------------------

def test_no_start_hour_no_frequency_runs_every_tick():
    t = _task(_start_hour=None, _frequency_min=None)
    assert should_run(t, BASE.replace(hour=5, minute=17)) is True


# ---------------------------------------------------------------------------
# 23. Frequency of 1 minute fires every minute
# ---------------------------------------------------------------------------

def test_frequency_1_min():
    t = _task(_start_hour=0, _start_minute=0, _frequency_min=1)
    # Should match at every minute since elapsed % 1 == 0 always
    assert should_run(t, BASE.replace(hour=0, minute=0)) is True
    assert should_run(t, BASE.replace(hour=0, minute=1)) is True
    assert should_run(t, BASE.replace(hour=12, minute=34)) is True
    assert should_run(t, BASE.replace(hour=23, minute=59)) is True


# ---------------------------------------------------------------------------
# 24. Midnight -- start_hour=0, now=00:00
# ---------------------------------------------------------------------------

def test_midnight_start_hour_0():
    now = BASE.replace(hour=0, minute=0)
    assert should_run(_task(_start_hour=0, _start_minute=0), now) is True
