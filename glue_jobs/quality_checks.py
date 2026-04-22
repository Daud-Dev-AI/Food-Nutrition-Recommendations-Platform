"""
Glue-compatible Data Quality Validation Module
===============================================
Identical to the local quality_checks.py except check_file_exists
supports s3:// URIs via boto3 (local os.path.exists doesn't work in Glue).

Works with both PySpark DataFrames and Pandas DataFrames.
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Union

logger = logging.getLogger("data_quality")


def _is_spark(df) -> bool:
    return hasattr(df, "rdd")


def _result(check: str, passed: bool, detail: str) -> dict:
    status = "PASS" if passed else "FAIL"
    msg = "[DQ] %-5s | %-40s | %s" % (status, check, detail)
    if passed:
        logger.info(msg)
    else:
        logger.error(msg)
    return {"check": check, "status": status, "detail": detail}


def check_file_exists(path: str) -> dict:
    """Check existence — handles both local paths and s3:// URIs."""
    if path.startswith("s3://"):
        import boto3
        without_scheme = path[5:]
        parts = without_scheme.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        try:
            boto3.client("s3").head_object(Bucket=bucket, Key=key)
            passed = True
        except Exception:
            # Could also be a prefix (directory) — try listing
            try:
                resp = boto3.client("s3").list_objects_v2(
                    Bucket=bucket, Prefix=key.rstrip("/") + "/", MaxKeys=1
                )
                passed = resp.get("KeyCount", 0) > 0
            except Exception:
                passed = False
    else:
        passed = os.path.exists(path)
    return _result("file_exists", passed, f"path={path}")


def check_row_count(df, min_rows: int = 1, label: str = "") -> dict:
    count = df.count() if _is_spark(df) else len(df)
    passed = count >= min_rows
    return _result("row_count", passed, f"{label} count={count} min_required={min_rows}")


def check_columns_exist(df, expected_cols: List[str], label: str = "") -> dict:
    actual = set(df.columns)
    missing = sorted(set(expected_cols) - actual)
    return _result("columns_exist", len(missing) == 0, f"{label} missing={missing}")


def check_not_null(df, col_name: str, label: str = "") -> dict:
    if _is_spark(df):
        from pyspark.sql.functions import col
        null_count = df.filter(col(col_name).isNull()).count()
    else:
        null_count = int(df[col_name].isna().sum())
    return _result("not_null", null_count == 0, f"{label} col={col_name} null_count={null_count}")


def check_no_fully_null_rows(df, critical_cols: List[str], label: str = "") -> dict:
    present = [c for c in critical_cols if c in df.columns]
    if not present:
        return _result("no_fully_null_rows", True, f"{label} no critical cols found — skipped")
    if _is_spark(df):
        from pyspark.sql.functions import col
        condition = col(present[0]).isNull()
        for c in present[1:]:
            condition = condition & col(c).isNull()
        null_rows = df.filter(condition).count()
    else:
        null_rows = int(df[present].isnull().all(axis=1).sum())
    return _result("no_fully_null_rows", null_rows == 0, f"{label} fully_null_rows={null_rows}")


def check_positive(df, col_name: str, label: str = "") -> dict:
    if _is_spark(df):
        from pyspark.sql.functions import col
        violations = df.filter(col(col_name).isNotNull() & (col(col_name) <= 0)).count()
    else:
        series = df[col_name].dropna()
        violations = int((series <= 0).sum())
    return _result("positive", violations == 0, f"{label} col={col_name} non_positive_count={violations}")


def check_non_negative(df, col_name: str, label: str = "") -> dict:
    if _is_spark(df):
        from pyspark.sql.functions import col
        violations = df.filter(col(col_name).isNotNull() & (col(col_name) < 0)).count()
    else:
        series = df[col_name].dropna()
        violations = int((series < 0).sum())
    return _result("non_negative", violations == 0, f"{label} col={col_name} negative_count={violations}")


def check_value_range(df, col_name: str, min_val, max_val, label: str = "") -> dict:
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


def run_checks(checks: List[dict], stage: str, fail_on_error: bool = True) -> dict:
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
        stage, passed_count, len(checks),
    )
    if failed:
        logger.error(
            "[DQ] FAILED checks at stage '%s': %s",
            stage, [c["check"] for c in failed],
        )
        if fail_on_error:
            raise ValueError(
                f"Data quality failed at stage '{stage}': "
                f"{len(failed)} check(s) failed — {[c['check'] for c in failed]}"
            )
    return report
