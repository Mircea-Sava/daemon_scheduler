import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pytest
from sequencer import validate_task


# ---------------------------------------------------------------------------
# 1. test_minimal_valid_task
# ---------------------------------------------------------------------------
def test_minimal_valid_task():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert isinstance(result["name"], str)
    assert isinstance(result["path"], str)
    assert result["name"] == "t1"
    assert result["path"] == "s.py"


# ---------------------------------------------------------------------------
# 2. test_missing_id_raises
# ---------------------------------------------------------------------------
def test_missing_id_raises():
    with pytest.raises(ValueError):
        validate_task({"path": "s.py"}, 0)


# ---------------------------------------------------------------------------
# 3. test_missing_path_raises
# ---------------------------------------------------------------------------
def test_missing_path_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1"}, 0)


# ---------------------------------------------------------------------------
# 4. test_not_a_dict_raises
# ---------------------------------------------------------------------------
def test_not_a_dict_raises():
    with pytest.raises(ValueError):
        validate_task("not a dict", 0)


# ---------------------------------------------------------------------------
# 5. test_start_hour_valid_range
# ---------------------------------------------------------------------------
def test_start_hour_valid_range():
    r0 = validate_task({"id": "t1", "path": "s.py", "start_hour": 0}, 0)
    assert r0["_start_hour"] == 0
    r23 = validate_task({"id": "t1", "path": "s.py", "start_hour": 23}, 0)
    assert r23["_start_hour"] == 23


# ---------------------------------------------------------------------------
# 6. test_start_hour_out_of_range_raises
# ---------------------------------------------------------------------------
def test_start_hour_out_of_range_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "start_hour": 24}, 0)
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "start_hour": -1}, 0)


# ---------------------------------------------------------------------------
# 7. test_start_hour_none_sets_none
# ---------------------------------------------------------------------------
def test_start_hour_none_sets_none():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_start_hour"] is None


# ---------------------------------------------------------------------------
# 8. test_start_minute_valid
# ---------------------------------------------------------------------------
def test_start_minute_valid():
    r0 = validate_task({"id": "t1", "path": "s.py", "start_minute": 0}, 0)
    assert r0["_start_minute"] == 0
    r59 = validate_task({"id": "t1", "path": "s.py", "start_minute": 59}, 0)
    assert r59["_start_minute"] == 59


# ---------------------------------------------------------------------------
# 9. test_start_minute_out_of_range_raises
# ---------------------------------------------------------------------------
def test_start_minute_out_of_range_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "start_minute": 60}, 0)


# ---------------------------------------------------------------------------
# 10. test_start_minute_defaults_to_zero
# ---------------------------------------------------------------------------
def test_start_minute_defaults_to_zero():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_start_minute"] == 0


# ---------------------------------------------------------------------------
# 11. test_frequency_min_valid
# ---------------------------------------------------------------------------
def test_frequency_min_valid():
    r1 = validate_task({"id": "t1", "path": "s.py", "frequency_min": 1}, 0)
    assert r1["_frequency_min"] == 1
    r30 = validate_task({"id": "t1", "path": "s.py", "frequency_min": 30}, 0)
    assert r30["_frequency_min"] == 30


# ---------------------------------------------------------------------------
# 12. test_frequency_min_zero_raises
# ---------------------------------------------------------------------------
def test_frequency_min_zero_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "frequency_min": 0}, 0)


# ---------------------------------------------------------------------------
# 13. test_frequency_min_negative_raises
# ---------------------------------------------------------------------------
def test_frequency_min_negative_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "frequency_min": -1}, 0)


# ---------------------------------------------------------------------------
# 14. test_frequency_min_sets_default_start_hour
# ---------------------------------------------------------------------------
def test_frequency_min_sets_default_start_hour():
    result = validate_task({"id": "t1", "path": "s.py", "frequency_min": 5}, 0)
    assert result["_start_hour"] == 0


# ---------------------------------------------------------------------------
# 15. test_end_hour_valid
# ---------------------------------------------------------------------------
def test_end_hour_valid():
    r0 = validate_task({"id": "t1", "path": "s.py", "end_hour": 0}, 0)
    assert r0["_end_hour"] == 0
    r23 = validate_task({"id": "t1", "path": "s.py", "end_hour": 23}, 0)
    assert r23["_end_hour"] == 23


# ---------------------------------------------------------------------------
# 16. test_end_hour_out_of_range_raises
# ---------------------------------------------------------------------------
def test_end_hour_out_of_range_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "end_hour": 24}, 0)


# ---------------------------------------------------------------------------
# 17. test_end_hour_none
# ---------------------------------------------------------------------------
def test_end_hour_none():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_end_hour"] is None


# ---------------------------------------------------------------------------
# 18. test_end_minute_valid
# ---------------------------------------------------------------------------
def test_end_minute_valid():
    r0 = validate_task({"id": "t1", "path": "s.py", "end_minute": 0}, 0)
    assert r0["_end_minute"] == 0
    r59 = validate_task({"id": "t1", "path": "s.py", "end_minute": 59}, 0)
    assert r59["_end_minute"] == 59


# ---------------------------------------------------------------------------
# 19. test_end_minute_out_of_range_raises
# ---------------------------------------------------------------------------
def test_end_minute_out_of_range_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "end_minute": 60}, 0)


# ---------------------------------------------------------------------------
# 20. test_end_minute_defaults_to_zero
# ---------------------------------------------------------------------------
def test_end_minute_defaults_to_zero():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_end_minute"] == 0


# ---------------------------------------------------------------------------
# 21. test_start_hour_greater_than_end_hour_raises
# ---------------------------------------------------------------------------
def test_start_hour_greater_than_end_hour_raises():
    with pytest.raises(ValueError):
        validate_task(
            {"id": "t1", "path": "s.py", "start_hour": 18, "end_hour": 9}, 0
        )


# ---------------------------------------------------------------------------
# 22. test_times_valid_string
# ---------------------------------------------------------------------------
def test_times_valid_string():
    result = validate_task(
        {"id": "t1", "path": "s.py", "times": "9:00, 14:00, 17:30"}, 0
    )
    assert result["_times"] == [(9, 0), (14, 0), (17, 30)]


# ---------------------------------------------------------------------------
# 23. test_times_valid_list
# ---------------------------------------------------------------------------
def test_times_valid_list():
    result = validate_task(
        {"id": "t1", "path": "s.py", "times": ["9:00", "14:00"]}, 0
    )
    assert result["_times"] == [(9, 0), (14, 0)]


# ---------------------------------------------------------------------------
# 24. test_times_invalid_format_raises
# ---------------------------------------------------------------------------
def test_times_invalid_format_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "times": "900"}, 0)


# ---------------------------------------------------------------------------
# 25. test_times_out_of_range_raises
# ---------------------------------------------------------------------------
def test_times_out_of_range_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "times": "25:00"}, 0)


# ---------------------------------------------------------------------------
# 26. test_times_with_start_hour_raises
# ---------------------------------------------------------------------------
def test_times_with_start_hour_raises():
    with pytest.raises(ValueError):
        validate_task(
            {"id": "t1", "path": "s.py", "times": "9:00", "start_hour": 8}, 0
        )


# ---------------------------------------------------------------------------
# 27. test_times_with_frequency_min_raises
# ---------------------------------------------------------------------------
def test_times_with_frequency_min_raises():
    with pytest.raises(ValueError):
        validate_task(
            {"id": "t1", "path": "s.py", "times": "9:00", "frequency_min": 5},
            0,
        )


# ---------------------------------------------------------------------------
# 28. test_times_invalid_type_raises
# ---------------------------------------------------------------------------
def test_times_invalid_type_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "times": 42}, 0)


# ---------------------------------------------------------------------------
# 29. test_depends_on_string
# ---------------------------------------------------------------------------
def test_depends_on_string():
    result = validate_task(
        {"id": "t1", "path": "s.py", "depends_on": "task-A, task-B"}, 0
    )
    assert result["_depends_on"] == ["task-A", "task-B"]


# ---------------------------------------------------------------------------
# 30. test_depends_on_list
# ---------------------------------------------------------------------------
def test_depends_on_list():
    result = validate_task(
        {"id": "t1", "path": "s.py", "depends_on": ["task-A"]}, 0
    )
    assert result["_depends_on"] == ["task-A"]


# ---------------------------------------------------------------------------
# 31. test_depends_on_none
# ---------------------------------------------------------------------------
def test_depends_on_none():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_depends_on"] == []


# ---------------------------------------------------------------------------
# 32. test_depends_on_invalid_type_raises
# ---------------------------------------------------------------------------
def test_depends_on_invalid_type_raises():
    with pytest.raises(ValueError):
        validate_task({"id": "t1", "path": "s.py", "depends_on": 42}, 0)


# ---------------------------------------------------------------------------
# 33. test_dependency_only_flag_true
# ---------------------------------------------------------------------------
def test_dependency_only_flag_true():
    result = validate_task(
        {"id": "t1", "path": "s.py", "depends_on": "task-A"}, 0
    )
    assert result["_dependency_only"] is True


# ---------------------------------------------------------------------------
# 34. test_dependency_only_flag_false_with_schedule
# ---------------------------------------------------------------------------
def test_dependency_only_flag_false_with_schedule():
    result = validate_task(
        {"id": "t1", "path": "s.py", "depends_on": "task-A", "start_hour": 9},
        0,
    )
    assert result["_dependency_only"] is False


# ---------------------------------------------------------------------------
# 35. test_dependency_only_flag_false_no_deps
# ---------------------------------------------------------------------------
def test_dependency_only_flag_false_no_deps():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_dependency_only"] is False


# ---------------------------------------------------------------------------
# 36. test_month_day_and_week_day_mutual_exclusion
# ---------------------------------------------------------------------------
def test_month_day_and_week_day_mutual_exclusion():
    result = validate_task(
        {"id": "t1", "path": "s.py", "month_day": "1,15", "week_day": "1,2"},
        0,
    )
    assert result["_month_day"] is not None
    assert result["_week_day"] is None


# ---------------------------------------------------------------------------
# 37. test_timeout_minutes_valid
# ---------------------------------------------------------------------------
def test_timeout_minutes_valid():
    result = validate_task(
        {"id": "t1", "path": "s.py", "timeout_minutes": 10}, 0
    )
    assert result["_timeout_minutes"] == 10


# ---------------------------------------------------------------------------
# 38. test_timeout_minutes_zero_raises
# ---------------------------------------------------------------------------
def test_timeout_minutes_zero_raises():
    with pytest.raises(ValueError):
        validate_task(
            {"id": "t1", "path": "s.py", "timeout_minutes": 0}, 0
        )


# ---------------------------------------------------------------------------
# 39. test_timeout_minutes_negative_raises
# ---------------------------------------------------------------------------
def test_timeout_minutes_negative_raises():
    with pytest.raises(ValueError):
        validate_task(
            {"id": "t1", "path": "s.py", "timeout_minutes": -1}, 0
        )


# ---------------------------------------------------------------------------
# 40. test_timeout_minutes_none
# ---------------------------------------------------------------------------
def test_timeout_minutes_none():
    result = validate_task({"id": "t1", "path": "s.py"}, 0)
    assert result["_timeout_minutes"] is None


# ---------------------------------------------------------------------------
# 41. test_start_at_valid
# ---------------------------------------------------------------------------
def test_start_at_valid():
    import datetime as dt

    result = validate_task(
        {"id": "t1", "path": "s.py", "start_at": "2026-03-01 09:00:00"}, 0
    )
    assert result["start_at"] == dt.datetime(2026, 3, 1, 9, 0, 0)


# ---------------------------------------------------------------------------
# 42. test_end_at_valid
# ---------------------------------------------------------------------------
def test_end_at_valid():
    import datetime as dt

    result = validate_task(
        {"id": "t1", "path": "s.py", "end_at": "2026-12-31 23:59:59"}, 0
    )
    assert result["end_at"] == dt.datetime(2026, 12, 31, 23, 59, 59)


# ---------------------------------------------------------------------------
# 43. test_start_at_after_end_at_raises
# ---------------------------------------------------------------------------
def test_start_at_after_end_at_raises():
    with pytest.raises(ValueError):
        validate_task(
            {
                "id": "t1",
                "path": "s.py",
                "start_at": "2026-12-31 23:59:59",
                "end_at": "2026-01-01 00:00:00",
            },
            0,
        )


# ---------------------------------------------------------------------------
# 44. test_months_field_parsing
# ---------------------------------------------------------------------------
def test_months_field_parsing():
    result = validate_task(
        {"id": "t1", "path": "s.py", "month": "1,6"}, 0
    )
    assert result["_months"] == {1, 6}


# ---------------------------------------------------------------------------
# 45. test_week_day_field_parsing
# ---------------------------------------------------------------------------
def test_week_day_field_parsing():
    result = validate_task(
        {"id": "t1", "path": "s.py", "week_day": "1,2,3,4,5"}, 0
    )
    # week_days_to_set converts 1-7 (1=Monday) to 0-6 (0=Monday)
    assert result["_week_day"] == {0, 1, 2, 3, 4}


# ---------------------------------------------------------------------------
# 46. test_month_day_field_parsing
# ---------------------------------------------------------------------------
def test_month_day_field_parsing():
    result = validate_task(
        {"id": "t1", "path": "s.py", "month_day": "1,15"}, 0
    )
    assert result["_month_day"] == {1, 15}
