"""
Lightweight Data Quality Validation Module
===========================================

Design: each check_*() function runs one assertion and returns a plain dict:
    {"check": "<name>", "status": "PASS" | "FAIL", "detail": "<message>"}

run_checks() collects results, prints a summary, and optionally raises if any
check fails — stopping the pipeline at the stage where data is bad.

No external frameworks needed. Pure Python + standard PySpark/Pandas APIs.
Easy to explain in any interview: "each check is a function that returns a
dict, we collect them, log the report, and raise on failure."

Supports both PySpark DataFrames and Pandas DataFrames via duck typing:
    _is_spark(df) checks for the .rdd attribute which only PySpark has.
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Union

logger = logging.getLogger("data_quality")


# ──────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────

def _is_spark(df) -> bool:
    """True if df is a PySpark DataFrame, False if Pandas."""
    return hasattr(df, "rdd")


def _result(check: str, passed: bool, detail: str) -> dict:
    status = "PASS" if passed else "FAIL"
    msg = "[DQ] %-5s | %-40s | %s" % (status, check, detail)
    if passed:
        logger.info(msg)
    else:
        logger.error(msg)
    return {"check": check, "status": status, "detail": detail}


# ──────────────────────────────────────────────────────────────
# File-level checks
# ──────────────────────────────────────────────────────────────

def check_file_exists(path: str) -> dict:
    """Verify a file or directory exists at the given path."""
    passed = os.path.exists(path)
    return _result("file_exists", passed, f"path={path}")


# ──────────────────────────────────────────────────────────────
# DataFrame checks — work with PySpark or Pandas
# ──────────────────────────────────────────────────────────────

def check_row_count(df, min_rows: int = 1, label: str = "") -> dict:
    """Row count must be >= min_rows."""
    count = df.count() if _is_spark(df) else len(df)
    passed = count >= min_rows
    return _result("row_count", passed, f"{label} count={count} min_required={min_rows}")


def check_columns_exist(df, expected_cols: List[str], label: str = "") -> dict:
    """All expected column names must be present in the DataFrame."""
    actual = set(df.columns)
    missing = sorted(set(expected_cols) - actual)
    return _result("columns_exist", len(missing) == 0, f"{label} missing={missing}")


def check_not_null(df, col_name: str, label: str = "") -> dict:
    """A specific column must contain no null values."""
    if _is_spark(df):
        from pyspark.sql.functions import col
        null_count = df.filter(col(col_name).isNull()).count()
    else:
        null_count = int(df[col_name].isna().sum())
    return _result("not_null", null_count == 0, f"{label} col={col_name} null_count={null_count}")


def check_no_fully_null_rows(df, critical_cols: List[str], label: str = "") -> dict:
    """No row should be entirely null across all listed critical columns."""
    present = [c for c in critical_cols if c in df.columns]
    if not present:
        return _result("no_fully_null_rows", True, f"{label} no critical cols found — skipped")

    if _is_spark(df):
        from pyspark.sql.functions import col
        # Build condition: ALL critical columns are null
        condition = col(present[0]).isNull()
        for c in present[1:]:
            condition = condition & col(c).isNull()
        null_rows = df.filter(condition).count()
    else:
        null_rows = int(df[present].isnull().all(axis=1).sum())

    return _result("no_fully_null_rows", null_rows == 0, f"{label} fully_null_rows={null_rows}")


def check_positive(df, col_name: str, label: str = "") -> dict:
    """All non-null values in col_name must be > 0."""
    if _is_spark(df):
        from pyspark.sql.functions import col
        violations = df.filter(col(col_name).isNotNull() & (col(col_name) <= 0)).count()
    else:
        series = df[col_name].dropna()
        violations = int((series <= 0).sum())
    return _result("positive", violations == 0, f"{label} col={col_name} non_positive_count={violations}")


def check_non_negative(df, col_name: str, label: str = "") -> dict:
    """All non-null values in col_name must be >= 0."""
    if _is_spark(df):
        from pyspark.sql.functions import col
        violations = df.filter(col(col_name).isNotNull() & (col(col_name) < 0)).count()
    else:
        series = df[col_name].dropna()
        violations = int((series < 0).sum())
    return _result("non_negative", violations == 0, f"{label} col={col_name} negative_count={violations}")


def check_value_range(df, col_name: str, min_val, max_val, label: str = "") -> dict:
    """All non-null values must fall within [min_val, max_val] (inclusive)."""
    if _is_spark(df):
        from pyspark.sql.functions import col
        violations = df.filter(
            col(col_name).isNotNull()
            & ((col(col_name) < min_val) | (col(col_name) > max_val))
        ).count()
    else:
        series = df[col_name].dropna()
        violations = int(((series < min_val) | (series > max_val)).sum())
    return _result(
        "value_range", violations == 0,
        f"{label} col={col_name} range=[{min_val},{max_val}] violations={violations}",
    )


def check_categorical(df, col_name: str, valid_values: List, label: str = "") -> dict:
    """All non-null values in col_name must be one of valid_values."""
    if _is_spark(df):
        invalid = df.filter(
            df[col_name].isNotNull() & ~df[col_name].isin(valid_values)
        ).count()
    else:
        invalid = int((~df[col_name].isin(valid_values) & df[col_name].notna()).sum())
    return _result(
        "categorical", invalid == 0,
        f"{label} col={col_name} valid={valid_values} invalid_count={invalid}",
    )


def check_unique(df, cols: Union[str, List[str]], label: str = "") -> dict:
    """No duplicate values (or combinations) allowed in the given column(s)."""
    if isinstance(cols, str):
        cols = [cols]

    if _is_spark(df):
        total = df.count()
        distinct = df.select(*cols).distinct().count()
    else:
        total = len(df)
        distinct = df[cols].drop_duplicates().shape[0]

    duplicates = total - distinct
    return _result(
        "unique", duplicates == 0,
        f"{label} cols={cols} total={total} distinct={distinct} duplicates={duplicates}",
    )


def check_is_numeric_type(df, col_name: str, label: str = "") -> dict:
    """Column must already be a numeric dtype (not a string)."""
    if _is_spark(df):
        from pyspark.sql.types import NumericType
        field = next((f for f in df.schema.fields if f.name == col_name), None)
        is_numeric = field is not None and isinstance(field.dataType, NumericType)
    else:
        is_numeric = str(df[col_name].dtype).startswith(("float", "int"))
    return _result(
        "is_numeric_type", is_numeric,
        f"{label} col={col_name} is_numeric={is_numeric}",
    )


# ──────────────────────────────────────────────────────────────
# Warehouse checks — SQL-based, using SQLAlchemy engine
# ──────────────────────────────────────────────────────────────

def check_warehouse_row_count(engine, table: str, min_rows: int = 1) -> dict:
    """Warehouse table must contain at least min_rows rows after load."""
    from sqlalchemy import text
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    return _result(
        "warehouse_row_count", count >= min_rows,
        f"table={table} count={count} min_required={min_rows}",
    )


def check_warehouse_unique(engine, table: str, col: str) -> dict:
    """A column in a warehouse table must have no duplicate values."""
    from sqlalchemy import text
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        distinct = conn.execute(text(f"SELECT COUNT(DISTINCT {col}) FROM {table}")).scalar()
    duplicates = total - distinct
    return _result(
        "warehouse_unique", duplicates == 0,
        f"table={table} col={col} total={total} distinct={distinct} duplicates={duplicates}",
    )


def check_warehouse_referential_integrity(
    engine, fact_table: str, dim_table: str, fk_col: str, pk_col: str
) -> dict:
    """Every FK value in fact_table must exist in dim_table (no orphan rows)."""
    from sqlalchemy import text
    with engine.connect() as conn:
        orphans = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM {fact_table} f
            LEFT JOIN {dim_table} d ON f.{fk_col} = d.{pk_col}
            WHERE d.{pk_col} IS NULL
        """)).scalar()
    return _result(
        "referential_integrity", orphans == 0,
        f"fact={fact_table}.{fk_col} → dim={dim_table}.{pk_col} orphans={orphans}",
    )


def check_warehouse_recommendations_exist(engine, user_id: str) -> dict:
    """At least one recommendation must exist in the warehouse for the given user."""
    from sqlalchemy import text
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM fact_food_recommendation WHERE user_id = :uid"),
            {"uid": user_id},
        ).scalar()
    return _result(
        "recommendations_exist", count > 0,
        f"user_id={user_id} recommendation_count={count}",
    )


# ──────────────────────────────────────────────────────────────
# Report runner
# ──────────────────────────────────────────────────────────────

def run_checks(checks: List[dict], stage: str, fail_on_error: bool = True) -> dict:
    """
    Summarise a list of check results and optionally raise on failures.

    Args:
        checks:        list of dicts returned by check_*() functions
        stage:         human-readable pipeline stage name (for logging)
        fail_on_error: if True, raises ValueError when any check fails
                       set to False to log-and-continue instead of halting

    Returns:
        report dict with total/passed/failed counts and per-check results
    """
    failed = [c for c in checks if c["status"] == "FAIL"]
    passed_count = len(checks) - len(failed)

    report = {
        "stage": stage,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total": len(checks),
        "passed": passed_count,
        "failed": len(failed),
        "results": checks,
    }

    logger.info(
        "[DQ] ── Stage: %-30s │ %d/%d checks passed ──",
        stage,
        passed_count,
        len(checks),
    )

    if failed:
        logger.error(
            "[DQ] FAILED checks at stage '%s': %s",
            stage,
            [c["check"] for c in failed],
        )
        if fail_on_error:
            raise ValueError(
                f"Data quality failed at stage '{stage}': "
                f"{len(failed)} check(s) failed — {[c['check'] for c in failed]}"
            )

    return report
