"""
Daily Nutrition Batch Pipeline DAG
====================================

Why Airflow with a static dataset?
-----------------------------------
Real production batch pipelines run on a schedule regardless of whether the
source data changes on any given day. Including Airflow here demonstrates:

  - Dependency-ordered task execution (raw → bronze → silver → gold → postgres)
  - Automatic retry on transient failures (spark cluster blip, DB timeout)
  - A web UI for monitoring run history and inspecting logs
  - Easy backfill if a run needs to be replayed

When this project is extended to a live data source (e.g. a daily vendor drop
or a database CDC export), only the schedule and source path need updating.

Schedule: Daily at midnight UTC (0 0 * * *)
Executor: LocalExecutor (single-node, no Redis/Celery needed for a local demo)

Task breakdown:
  raw_to_bronze        — Spark: normalise CSV, cast types, write Parquet
  bronze_to_silver     — Spark: rename columns, derive bands/flags, write Parquet
  silver_to_gold       — Spark: cross-join users × foods, score, rank top 10
  load_gold_to_postgres — Python: truncate & reload warehouse tables, run
                          warehouse quality checks

Each Spark task uses SparkSubmitOperator which submits the job to the Spark
cluster (spark://spark-master:7077) via the `spark_default` Airflow connection.
The load step runs as a BashOperator (plain Python, no Spark needed).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# ──────────────────────────────────────────────────────────────
# Default arguments — applied to every task unless overridden
# ──────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# Shared environment variables injected into every Spark job.
# DATA_BASE_PATH points to the mounted project data directory.
SPARK_ENV = {
    "DATA_BASE_PATH":  "/opt/project/data",
    "PROJECT_ROOT":    "/opt/project",
    "LOG_LEVEL":       "INFO",
}

# Environment for the Python load script — adds DB connection details
LOAD_ENV = {
    **SPARK_ENV,
    "POSTGRES_HOST":     "postgres",
    "POSTGRES_PORT":     "5432",
    "POSTGRES_DB":       "nutrition_dw",
    "POSTGRES_USER":     "de_user",
    "POSTGRES_PASSWORD": "de_pass",
}

# ──────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id="nutrition_batch_pipeline",
    description="Daily batch: raw → bronze → silver → gold → postgres (with DQ checks)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 * * *",   # midnight UTC every day
    default_args=default_args,
    catchup=False,                   # don't replay missed historical runs
    tags=["nutrition", "batch", "medallion"],
) as dag:

    # ── Task 1: raw → bronze ──────────────────────────────────
    # Reads nutrition.csv, normalises column names, casts numeric types,
    # drops duplicates, writes bronze Parquet. Includes DQ checks.
    raw_to_bronze = SparkSubmitOperator(
        task_id="raw_to_bronze",
        application="/opt/project/spark_jobs/batch/raw_to_bronze.py",
        conn_id="spark_default",          # connection configured via env var
        name="raw_to_bronze",
        env_vars=SPARK_ENV,
        verbose=False,
    )

    # ── Task 2: bronze → silver ───────────────────────────────
    # Renames columns to business names, derives nutritional bands and flags,
    # writes silver Parquet. Includes DQ checks on incoming bronze data.
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/project/spark_jobs/batch/bronze_to_silver.py",
        conn_id="spark_default",
        name="bronze_to_silver",
        env_vars=SPARK_ENV,
        verbose=False,
    )

    # ── Task 3: silver → gold ─────────────────────────────────
    # Cross-joins users × foods, applies goal-specific scoring formulas,
    # deduplicates by food_name, ranks top 10 per user, writes gold Parquet.
    # Includes DQ checks on silver inputs and gold outputs.
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/project/spark_jobs/batch/silver_to_gold_recommendations.py",
        conn_id="spark_default",
        name="silver_to_gold",
        env_vars=SPARK_ENV,
        verbose=False,
    )

    # ── Task 4: gold → postgres ───────────────────────────────
    # Truncates and reloads dim_food, dim_user_profile, fact_food_recommendation.
    # Runs warehouse DQ checks (row counts, uniqueness, referential integrity).
    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=(
            "cd /opt/project && "
            + " ".join(f"{k}={v}" for k, v in LOAD_ENV.items())
            + " python scripts/ingestion/load_gold_to_postgres.py"
        ),
    )

    # ── Dependency chain — enforces execution order ───────────
    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> load_gold_to_postgres
