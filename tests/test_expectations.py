import pytest

from src.quality.expectations import (
    DataQualityError,
    check_min_row_count,
    check_no_negative_values,
    check_null_ratio,
    check_row_count_within_tolerance,
    run_checks,
)


def test_check_min_row_count_pass():
    result = check_min_row_count(row_count=1000, minimum=100, table_name="bronze.store_sales")
    assert result.passed


def test_check_min_row_count_fail():
    result = check_min_row_count(row_count=5, minimum=100, table_name="bronze.store_sales")
    assert not result.passed
    assert "bronze.store_sales" in result.message


def test_check_null_ratio_pass():
    result = check_null_ratio(null_count=2, total_count=1000, max_ratio=0.01, column_name="customer_id")
    assert result.passed


def test_check_null_ratio_fail():
    result = check_null_ratio(null_count=200, total_count=1000, max_ratio=0.01, column_name="customer_id")
    assert not result.passed


def test_check_null_ratio_zero_total():
    result = check_null_ratio(null_count=0, total_count=0, max_ratio=0.01, column_name="customer_id")
    assert not result.passed


def test_check_no_negative_values_pass():
    result = check_no_negative_values(min_value=0.0, column_name="sale_amount")
    assert result.passed


def test_check_no_negative_values_fail():
    result = check_no_negative_values(min_value=-15.5, column_name="sale_amount")
    assert not result.passed


def test_check_row_count_within_tolerance_pass():
    result = check_row_count_within_tolerance(
        upstream_count=10000, downstream_count=9800, max_drop_ratio=0.05, stage_name="silver_join"
    )
    assert result.passed


def test_check_row_count_within_tolerance_fail():
    result = check_row_count_within_tolerance(
        upstream_count=10000, downstream_count=6000, max_drop_ratio=0.05, stage_name="silver_join"
    )
    assert not result.passed


def test_run_checks_raises_on_any_failure():
    results = [
        check_min_row_count(1000, 100, "table_a"),
        check_no_negative_values(-1, "sale_amount"),
    ]
    with pytest.raises(DataQualityError):
        run_checks(results, stage_name="silver")


def test_run_checks_passes_when_all_pass():
    results = [
        check_min_row_count(1000, 100, "table_a"),
        check_no_negative_values(5, "sale_amount"),
    ]
    run_checks(results, stage_name="silver")  # should not raise
