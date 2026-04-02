"""
Daily Nutrition Batch Pipeline DAG
====================================

Why Airflow with a static dataset?
-----------------------------------
Real production batch pipelines run on a schedule regardless of whether the
source data changes on any given day. Including Airflow here demonstrates:

  - Dependency-ordered task execution (raw → bronze → silver → gold → postgres)
  - Automatic retry on transient failures
  - A web UI for monitoring run history and inspecting logs
  - Easy backfill if a run needs to be replayed

When this project is extended to a live data source (e.g. a daily vendor drop),
only the schedule and source path need updating.

Schedule: Daily at midnight UTC (0 0 * * *)
Executor: LocalExecutor (single-node, no Redis/Celery needed for a local demo)

All tasks use BashOperator running `python` directly inside the Airflow
container. PySpark is installed in the image and runs in local[*] mode —
no need to connect to the external Spark standalone cluster from within
the Airflow container. This is the simplest reliable setup for a local demo.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# Environment variables injected into every task subprocess
ENV = (
    "DATA_BASE_PATH=/opt/project/data "
    "PROJECT_ROOT=/opt/project "
    "LOG_LEVEL=INFO "
    "POSTGRES_HOST=postgres "
    "POSTGRES_PORT=5432 "
    "POSTGRES_DB=nutrition_dw "
    "POSTGRES_USER=de_user "
    "POSTGRES_PASSWORD=de_pass "
)

with DAG(
    dag_id="nutrition_batch_pipeline",
    description="Daily batch: raw → bronze → silver → gold → postgres -> to minIO (with DQ checks)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
    tags=["nutrition", "batch", "medallion"],
) as dag:

    # ── Task 1: raw → bronze ──────────────────────────────────
    # Reads nutrition.csv, normalises column names, casts numeric types,
    # drops duplicates, writes bronze Parquet. DQ checks run inline.
    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=f"cd /opt/project && {ENV} python spark_jobs/batch/raw_to_bronze.py",
    )

    # ── Task 2: bronze → silver ───────────────────────────────
    # Renames columns, derives nutritional bands and flags, writes silver Parquet.
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"cd /opt/project && {ENV} python spark_jobs/batch/bronze_to_silver.py",
    )

    # ── Task 3: silver → gold ─────────────────────────────────
    # Cross-joins users × foods, scores, ranks top 10 per user, writes gold Parquet.
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"cd /opt/project && {ENV} python spark_jobs/batch/silver_to_gold_recommendations.py",
    )

    # ── Task 4: gold → postgres ───────────────────────────────
    # Truncates and reloads warehouse tables, runs warehouse DQ checks.
    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=f"cd /opt/project && {ENV} python scripts/ingestion/load_gold_to_postgres.py",
    )

    # ── Dependency chain ──────────────────────────────────────
    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> load_gold_to_postgres
