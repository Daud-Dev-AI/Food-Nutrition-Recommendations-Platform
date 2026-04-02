import os
import sys
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace
from pyspark.sql.types import DoubleType

# Make the project root importable so quality_checks resolves correctly
sys.path.insert(0, os.environ.get("PROJECT_ROOT", "/opt/project"))
from spark_jobs.data_quality.quality_checks import (
    check_file_exists,
    check_row_count,
    check_columns_exist,
    check_not_null,
    run_checks,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
)
logger = logging.getLogger("raw_to_bronze")

BASE_PATH   = os.environ.get("DATA_BASE_PATH", "/opt/project/data")
input_path  = os.path.join(BASE_PATH, "raw", "nutrition.csv")
output_path = os.path.join(BASE_PATH, "bronze", "food_nutrition")

# ── Stage: raw (file-level) ────────────────────────────────────
run_checks([
    check_file_exists(input_path),
], stage="raw_file_check")

spark = (
    SparkSession.builder
    .appName("raw-to-bronze-food-nutrition")
    # umask 000 ensures output files are world-writable so any container
    # user (spark uid 185, airflow uid 50000, etc.) can overwrite them.
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

logger.info("Reading raw CSV from %s", input_path)
df = spark.read.option("header", True).csv(input_path)

# ── Stage: raw (schema + row count) ───────────────────────────
# Check original column names before normalisation
EXPECTED_RAW_COLS = ["label", "weight", "calories", "protein",
                      "carbohydrates", "fats", "fiber", "sugars", "sodium"]
run_checks([
    check_row_count(df, min_rows=1, label="raw"),
    check_columns_exist(df, EXPECTED_RAW_COLS, label="raw"),
], stage="raw_schema_check")

# ── Transformations ────────────────────────────────────────────

# Normalise column names: strip, lowercase, spaces/hyphens → underscores
new_names = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
for old, new in zip(df.columns, new_names):
    df = df.withColumnRenamed(old, new)

text_cols = ["label", "weight"]
for c in text_cols:
    if c in df.columns:
        df = df.withColumn(c, trim(lower(col(c))))

numeric_cols = ["calories", "protein", "carbohydrates", "fats", "fiber", "sugars", "sodium"]
for c in numeric_cols:
    if c in df.columns:
        df = df.withColumn(c, regexp_replace(trim(col(c)), ",", ""))
        df = df.withColumn(c, col(c).cast(DoubleType()))

df = df.dropDuplicates()

# ── Stage: bronze (post-transform) ────────────────────────────
# Verify the label column (food name) survived normalisation with no nulls
run_checks([
    check_row_count(df, min_rows=1, label="bronze"),
    check_not_null(df, "label", label="bronze"),
], stage="bronze_post_transform")

shutil.rmtree(output_path, ignore_errors=True)
df.write.mode("overwrite").parquet(output_path)

row_count = df.count()
logger.info("Bronze write complete — output=%s rows=%d", output_path, row_count)

spark.stop()
