"""Tests for utility / parsing functions from the sequencer module."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime as dt

import pytest

from sequencer import (
    to_int,
    parse_now,
    parse_task_datetime,
    task_datetime,
    is_within_task_window,
    day_of_week_to_index,
    day_of_week_to_indices,
    month_to_number,
    months_to_numbers,
    month_days_to_set,
    week_days_to_set,
    parse_worker_setting,
)


# ---------------------------------------------------------------------------
# to_int
# ---------------------------------------------------------------------------

def test_to_int_valid_int():
    assert to_int(5, 0) == 5


def test_to_int_string_int():
    assert to_int("42", 0) == 42


def test_to_int_invalid_string_returns_default():
    assert to_int("abc", 7) == 7


def test_to_int_none_returns_default():
    assert to_int(None, 10) == 10


def test_to_int_float_string_returns_default():
    assert to_int("3.14", 0) == 0


# ---------------------------------------------------------------------------
# parse_now
# ---------------------------------------------------------------------------

def test_parse_now_none_returns_approx_now():
    before = dt.datetime.now()
    result = parse_now(None)
    after = dt.datetime.now()
    assert before <= result <= after


def test_parse_now_valid_iso_parses():
    result = parse_now("2026-03-15T10:30:00")
    assert result == dt.datetime(2026, 3, 15, 10, 30, 0)


def test_parse_now_invalid_raises_value_error():
    with pytest.raises(ValueError, match="Invalid --now value"):
        parse_now("not-a-date")


# ---------------------------------------------------------------------------
# parse_task_datetime
# ---------------------------------------------------------------------------

def test_parse_task_datetime_none_returns_none():
    assert parse_task_datetime(None, "start_at") is None


def test_parse_task_datetime_valid_string_parses():
    result = parse_task_datetime("2026-03-01 09:00:00", "start_at")
    assert result == dt.datetime(2026, 3, 1, 9, 0, 0)


def test_parse_task_datetime_with_t_separator():
    result = parse_task_datetime("2026-03-01T09:00:00", "start_at")
    assert result == dt.datetime(2026, 3, 1, 9, 0, 0)


def test_parse_task_datetime_empty_raises_value_error():
    with pytest.raises(ValueError, match="cannot be empty"):
        parse_task_datetime("", "start_at")


def test_parse_task_datetime_invalid_format_raises_value_error():
    with pytest.raises(ValueError, match="must be a datetime"):
        parse_task_datetime("not-a-date", "end_at")


def test_parse_task_datetime_without_seconds_parses():
    result = parse_task_datetime("2026-03-01 09:00", "start_at")
    assert result == dt.datetime(2026, 3, 1, 9, 0, 0)


def test_parse_task_datetime_with_timezone_strips_tz():
    result = parse_task_datetime("2026-03-01T09:00:00+02:00", "start_at")
    assert result.tzinfo is None


# ---------------------------------------------------------------------------
# task_datetime
# ---------------------------------------------------------------------------

def test_task_datetime_dict_with_string_value():
    task = {"start_at": "2026-04-10 12:00:00"}
    result = task_datetime(task, "start_at")
    assert result == dt.datetime(2026, 4, 10, 12, 0, 0)


def test_task_datetime_dict_with_datetime_object_returns_as_is():
    expected = dt.datetime(2026, 5, 20, 8, 0, 0)
    task = {"start_at": expected}
    result = task_datetime(task, "start_at")
    assert result is expected


def test_task_datetime_missing_field_returns_none():
    task = {"id": "test"}
    assert task_datetime(task, "start_at") is None


# ---------------------------------------------------------------------------
# is_within_task_window
# ---------------------------------------------------------------------------

def test_is_within_task_window_no_bounds_returns_true():
    task = {"id": "t1"}
    now = dt.datetime(2026, 6, 1, 12, 0, 0)
    assert is_within_task_window(task, now) is True


def test_is_within_task_window_before_start_returns_false():
    task = {"start_at": "2026-06-01 12:00:00"}
    now = dt.datetime(2026, 5, 31, 23, 59, 59)
    assert is_within_task_window(task, now) is False


def test_is_within_task_window_after_end_returns_false():
    task = {"end_at": "2026-06-01 12:00:00"}
    now = dt.datetime(2026, 6, 1, 12, 0, 1)
    assert is_within_task_window(task, now) is False


def test_is_within_task_window_in_range_returns_true():
    task = {
        "start_at": "2026-06-01 08:00:00",
        "end_at": "2026-06-01 17:00:00",
    }
    now = dt.datetime(2026, 6, 1, 12, 0, 0)
    assert is_within_task_window(task, now) is True


def test_is_within_task_window_exactly_at_start_returns_true():
    task = {
        "start_at": "2026-06-01 08:00:00",
        "end_at": "2026-06-01 17:00:00",
    }
    now = dt.datetime(2026, 6, 1, 8, 0, 0)
    assert is_within_task_window(task, now) is True


def test_is_within_task_window_exactly_at_end_returns_true():
    task = {
        "start_at": "2026-06-01 08:00:00",
        "end_at": "2026-06-01 17:00:00",
    }
    now = dt.datetime(2026, 6, 1, 17, 0, 0)
    assert is_within_task_window(task, now) is True


# ---------------------------------------------------------------------------
# day_of_week_to_index
# ---------------------------------------------------------------------------

def test_day_of_week_to_index_full_name_monday():
    assert day_of_week_to_index("monday") == 0


def test_day_of_week_to_index_full_name_sunday():
    assert day_of_week_to_index("sunday") == 6


def test_day_of_week_to_index_abbreviation_mon():
    assert day_of_week_to_index("mon") == 0


def test_day_of_week_to_index_abbreviation_fri():
    assert day_of_week_to_index("fri") == 4


def test_day_of_week_to_index_integer_zero():
    assert day_of_week_to_index(0) == 0


def test_day_of_week_to_index_integer_six():
    assert day_of_week_to_index(6) == 6


def test_day_of_week_to_index_out_of_range_raises():
    with pytest.raises(ValueError, match="0-6"):
        day_of_week_to_index(7)


def test_day_of_week_to_index_invalid_string_raises():
    with pytest.raises(ValueError):
        day_of_week_to_index("notaday")


def test_day_of_week_to_index_none_returns_zero():
    assert day_of_week_to_index(None) == 0


# ---------------------------------------------------------------------------
# day_of_week_to_indices
# ---------------------------------------------------------------------------

def test_day_of_week_to_indices_comma_separated():
    assert day_of_week_to_indices("mon,tue,fri") == {0, 1, 4}


def test_day_of_week_to_indices_list_input():
    assert day_of_week_to_indices([0, 3]) == {0, 3}


def test_day_of_week_to_indices_none_returns_set_zero():
    assert day_of_week_to_indices(None) == {0}


def test_day_of_week_to_indices_empty_string_raises():
    with pytest.raises(ValueError, match="cannot be empty"):
        day_of_week_to_indices("")


# ---------------------------------------------------------------------------
# month_to_number
# ---------------------------------------------------------------------------

def test_month_to_number_int_one():
    assert month_to_number(1) == 1


def test_month_to_number_int_twelve():
    assert month_to_number(12) == 12


def test_month_to_number_out_of_range_zero_raises():
    with pytest.raises(ValueError, match="1-12"):
        month_to_number(0)


def test_month_to_number_out_of_range_thirteen_raises():
    with pytest.raises(ValueError, match="1-12"):
        month_to_number(13)


def test_month_to_number_full_name():
    assert month_to_number("january") == 1


def test_month_to_number_abbreviation():
    assert month_to_number("jan") == 1


def test_month_to_number_string_int():
    assert month_to_number("6") == 6


def test_month_to_number_invalid_raises():
    with pytest.raises(ValueError):
        month_to_number("notamonth")


# ---------------------------------------------------------------------------
# months_to_numbers
# ---------------------------------------------------------------------------

def test_months_to_numbers_none_returns_all():
    assert months_to_numbers(None) == set(range(1, 13))


def test_months_to_numbers_single_value():
    assert months_to_numbers(3) == {3}


def test_months_to_numbers_comma_separated():
    assert months_to_numbers("1,6,12") == {1, 6, 12}


def test_months_to_numbers_list():
    assert months_to_numbers([1, 3, 7]) == {1, 3, 7}


def test_months_to_numbers_empty_string_raises():
    with pytest.raises(ValueError, match="cannot be empty"):
        months_to_numbers("")


def test_months_to_numbers_empty_list_raises():
    with pytest.raises(ValueError, match="cannot be empty"):
        months_to_numbers([])


# ---------------------------------------------------------------------------
# month_days_to_set
# ---------------------------------------------------------------------------

def test_month_days_to_set_none_returns_none():
    assert month_days_to_set(None) is None


def test_month_days_to_set_single_int():
    assert month_days_to_set(15) == {15}


def test_month_days_to_set_out_of_range_zero_raises():
    with pytest.raises(ValueError, match="1-31"):
        month_days_to_set(0)


def test_month_days_to_set_out_of_range_32_raises():
    with pytest.raises(ValueError, match="1-31"):
        month_days_to_set(32)


def test_month_days_to_set_comma_separated():
    assert month_days_to_set("1,15,28") == {1, 15, 28}


def test_month_days_to_set_list():
    assert month_days_to_set([1, 31]) == {1, 31}


def test_month_days_to_set_empty_string_returns_none():
    assert month_days_to_set("") is None


def test_month_days_to_set_non_integer_abc_raises():
    with pytest.raises(ValueError, match="integers"):
        month_days_to_set("abc")


# ---------------------------------------------------------------------------
# week_days_to_set
# ---------------------------------------------------------------------------

def test_week_days_to_set_none_returns_none():
    assert week_days_to_set(None) is None


def test_week_days_to_set_single_int():
    # 1 (Monday) maps to index 0
    assert week_days_to_set(1) == {0}


def test_week_days_to_set_out_of_range_zero_raises():
    with pytest.raises(ValueError):
        week_days_to_set(0)


def test_week_days_to_set_out_of_range_eight_raises():
    with pytest.raises(ValueError):
        week_days_to_set(8)


def test_week_days_to_set_comma_separated():
    # 1,2,3,4,5 -> Monday-Friday -> indices 0,1,2,3,4
    assert week_days_to_set("1,2,3,4,5") == {0, 1, 2, 3, 4}


def test_week_days_to_set_list():
    # [1,3,5] -> Mon, Wed, Fri -> indices 0, 2, 4
    assert week_days_to_set([1, 3, 5]) == {0, 2, 4}


def test_week_days_to_set_empty_string_returns_none():
    assert week_days_to_set("") is None


def test_week_days_to_set_day_name():
    assert week_days_to_set("monday") == {0}




# ---------------------------------------------------------------------------
# parse_worker_setting
# ---------------------------------------------------------------------------

def test_parse_worker_setting_valid_string():
    assert parse_worker_setting("8") == 8


def test_parse_worker_setting_zero_becomes_one():
    assert parse_worker_setting(0) == 1


def test_parse_worker_setting_negative_becomes_one():
    assert parse_worker_setting(-5) == 1


def test_parse_worker_setting_none_with_default():
    assert parse_worker_setting(None, default=4) == 4
