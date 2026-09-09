"""
Data quality checks used between pipeline layers.

These are deliberately pure Python (no PySpark import) so they can be unit
tested quickly in CI without spinning up a Spark session. The PySpark
transform scripts call these functions with plain numbers/dicts they've
already computed from a DataFrame (e.g. via .count(), .agg(), etc.).
"""

from dataclasses import dataclass


class DataQualityError(Exception):
    """Raised when a data quality check fails. A pipeline task should let
    this propagate so the Databricks Job task fails loudly rather than
    silently passing bad data downstream."""


@dataclass
class CheckResult:
    passed: bool
    message: str


def check_min_row_count(row_count: int, minimum: int, table_name: str = "table") -> CheckResult:
    """Fail if a table has fewer rows than expected (e.g. an empty or
    truncated load)."""
    if row_count < minimum:
        return CheckResult(
            passed=False,
            message=f"{table_name}: expected at least {minimum} rows, got {row_count}",
        )
    return CheckResult(passed=True, message=f"{table_name}: row count OK ({row_count})")


def check_null_ratio(null_count: int, total_count: int, max_ratio: float, column_name: str = "column") -> CheckResult:
    """Fail if the fraction of nulls in a column exceeds max_ratio."""
    if total_count == 0:
        return CheckResult(passed=False, message=f"{column_name}: total_count is 0, cannot evaluate null ratio")

    ratio = null_count / total_count
    if ratio > max_ratio:
        return CheckResult(
            passed=False,
            message=f"{column_name}: null ratio {ratio:.2%} exceeds max allowed {max_ratio:.2%}",
        )
    return CheckResult(passed=True, message=f"{column_name}: null ratio OK ({ratio:.2%})")


def check_no_negative_values(min_value: float, column_name: str = "column") -> CheckResult:
    """Fail if a numeric column (e.g. sale_amount, quantity) contains
    negative values, which usually signals a join or unit-conversion bug."""
    if min_value < 0:
        return CheckResult(
            passed=False,
            message=f"{column_name}: found negative value(s), min={min_value}",
        )
    return CheckResult(passed=True, message=f"{column_name}: no negative values")


def check_row_count_within_tolerance(
    upstream_count: int, downstream_count: int, max_drop_ratio: float = 0.05, stage_name: str = "stage"
) -> CheckResult:
    """Fail if a transform (e.g. a join) silently drops more rows than
    expected. A join going from an inner to accidentally-cross join, or a
    bad join key, often shows up first as a row count anomaly."""
    if upstream_count == 0:
        return CheckResult(passed=False, message=f"{stage_name}: upstream_count is 0")

    drop_ratio = (upstream_count - downstream_count) / upstream_count
    if drop_ratio > max_drop_ratio:
        return CheckResult(
            passed=False,
            message=(
                f"{stage_name}: row count dropped by {drop_ratio:.2%} "
                f"({upstream_count} -> {downstream_count}), exceeds tolerance {max_drop_ratio:.2%}"
            ),
        )
    return CheckResult(passed=True, message=f"{stage_name}: row count change within tolerance")


def run_checks(results: list[CheckResult], stage_name: str = "stage") -> None:
    """Run a batch of checks; raise DataQualityError listing every failure
    if any check failed. Call this at the end of a bronze/silver/gold
    notebook so a single failing check fails the whole Databricks task."""
    failures = [r.message for r in results if not r.passed]
    if failures:
        raise DataQualityError(f"{stage_name} failed {len(failures)} check(s):\n" + "\n".join(failures))
