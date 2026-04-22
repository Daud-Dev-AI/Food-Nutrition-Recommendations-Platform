"""
Glue Job: raw_to_bronze
=======================
Reads the raw nutrition.csv from S3, normalises column names and data types,
and writes the result as Parquet to the bronze zone.

Adapted from spark_jobs/batch/raw_to_bronze.py for AWS Glue 4.0 (PySpark 3.3).
Changes from local version:
  - Paths are S3 URIs resolved from the DATA_BASE_PATH job argument
  - shutil.rmtree removed (Spark mode=overwrite handles S3 cleanup)
  - check_file_exists uses boto3 (local os.path.exists doesn't reach S3)
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace
from pyspark.sql.types import DoubleType

from quality_checks import (
    check_file_exists,
    check_row_count,
    check_columns_exist,
    check_not_null,
    run_checks,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("glue_raw_to_bronze")

args = getResolvedOptions(sys.argv, ["DATA_BASE_PATH"])
BASE_PATH   = args["DATA_BASE_PATH"].rstrip("/")
input_path  = f"{BASE_PATH}/raw/nutrition.csv"
output_path = f"{BASE_PATH}/bronze/food_nutrition"

# ── Stage: raw (file-level) ────────────────────────────────────
run_checks([
    check_file_exists(input_path),
], stage="raw_file_check")

spark = (
    SparkSession.builder
    .appName("glue-raw-to-bronze-food-nutrition")
    .getOrCreate()
)

logger.info("Reading raw CSV from %s", input_path)
df = spark.read.option("header", True).csv(input_path)

# ── Stage: raw (schema + row count) ───────────────────────────
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
run_checks([
    check_row_count(df, min_rows=1, label="bronze"),
    check_not_null(df, "label", label="bronze"),
], stage="bronze_post_transform")

df.write.mode("overwrite").parquet(output_path)

row_count = df.count()
logger.info("Bronze write complete — output=%s rows=%d", output_path, row_count)

spark.stop()
