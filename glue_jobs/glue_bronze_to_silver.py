"""
Glue Job: bronze_to_silver
==========================
Reads bronze Parquet, applies column renames, band derivations, and binary
feature flags, and writes the cleaned data to the silver zone.

Adapted from spark_jobs/batch/bronze_to_silver.py for AWS Glue 4.0 (PySpark 3.3).
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_extract, trim, lower
from pyspark.sql.types import DoubleType

from quality_checks import (
    check_row_count,
    check_columns_exist,
    check_not_null,
    check_non_negative,
    check_positive,
    check_categorical,
    check_no_fully_null_rows,
    check_is_numeric_type,
    run_checks,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("glue_bronze_to_silver")

args = getResolvedOptions(sys.argv, ["DATA_BASE_PATH"])
BASE_PATH   = args["DATA_BASE_PATH"].rstrip("/")
input_path  = f"{BASE_PATH}/bronze/food_nutrition"
output_path = f"{BASE_PATH}/silver/food_nutrition_clean"

spark = (
    SparkSession.builder
    .appName("glue-bronze-to-silver-food-nutrition")
    .getOrCreate()
)

logger.info("Reading bronze parquet from %s", input_path)
df = spark.read.parquet(input_path)

# ── Stage: bronze (incoming checks) ───────────────────────────
BRONZE_NUMERIC_COLS = ["calories", "protein", "carbohydrates", "fats", "fiber", "sugars", "sodium"]
run_checks([
    check_row_count(df, min_rows=1, label="bronze_input"),
    check_no_fully_null_rows(df, ["label"] + BRONZE_NUMERIC_COLS, label="bronze_input"),
    check_is_numeric_type(df, "calories", label="bronze_input"),
    check_is_numeric_type(df, "protein",  label="bronze_input"),
], stage="bronze_incoming_check")

# ── Transformations ────────────────────────────────────────────

df = df.dropDuplicates()

if "label" in df.columns:
    df = df.withColumn("label", trim(lower(col("label"))))
if "weight" in df.columns:
    df = df.withColumn("weight", trim(lower(col("weight"))))
if "weight" in df.columns:
    df = df.withColumn(
        "serving_size_value",
        regexp_extract(col("weight"), r"([0-9]+(\.[0-9]+)?)", 1).cast(DoubleType()),
    )

rename_map = {
    "label":         "food_name",
    "weight":        "serving_description",
    "fats":          "fat_g",
    "sugars":        "sugar_g",
    "protein":       "protein_g",
    "fiber":         "fiber_g",
    "carbohydrates": "carbohydrates_g",
    "calories":      "calories_kcal",
    "sodium":        "sodium_mg",
}
for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

if "calories_kcal" in df.columns:
    df = df.withColumn(
        "calorie_band",
        when(col("calories_kcal") < 150, lit("low"))
        .when((col("calories_kcal") >= 150) & (col("calories_kcal") < 300), lit("medium"))
        .otherwise(lit("high")),
    )

if "protein_g" in df.columns:
    df = df.withColumn(
        "protein_band",
        when(col("protein_g") < 5, lit("low"))
        .when((col("protein_g") >= 5) & (col("protein_g") < 15), lit("medium"))
        .otherwise(lit("high")),
    )

if "carbohydrates_g" in df.columns:
    df = df.withColumn(
        "carb_band",
        when(col("carbohydrates_g") < 10, lit("low"))
        .when((col("carbohydrates_g") >= 10) & (col("carbohydrates_g") < 25), lit("medium"))
        .otherwise(lit("high")),
    )

if "fat_g" in df.columns:
    df = df.withColumn(
        "fat_band",
        when(col("fat_g") < 5, lit("low"))
        .when((col("fat_g") >= 5) & (col("fat_g") < 15), lit("medium"))
        .otherwise(lit("high")),
    )

if "protein_g" in df.columns and "calories_kcal" in df.columns:
    df = df.withColumn(
        "is_high_protein_low_calorie",
        when((col("protein_g") >= 10) & (col("calories_kcal") <= 250), lit(1)).otherwise(lit(0)),
    )

if "fiber_g" in df.columns:
    df = df.withColumn(
        "is_high_fiber",
        when(col("fiber_g") >= 5, lit(1)).otherwise(lit(0)),
    )

if "sugar_g" in df.columns:
    df = df.withColumn(
        "is_low_sugar",
        when(col("sugar_g") <= 8, lit(1)).otherwise(lit(0)),
    )

# ── Stage: silver (post-transform checks) ─────────────────────
VALID_BANDS = ["low", "medium", "high"]
run_checks([
    check_row_count(df, min_rows=1, label="silver"),
    check_not_null(df, "food_name",       label="silver"),
    check_positive(df, "calories_kcal",   label="silver"),
    check_non_negative(df, "protein_g",       label="silver"),
    check_non_negative(df, "carbohydrates_g", label="silver"),
    check_non_negative(df, "fat_g",           label="silver"),
    check_non_negative(df, "fiber_g",         label="silver"),
    check_non_negative(df, "sugar_g",         label="silver"),
    check_categorical(df, "calorie_band", VALID_BANDS, label="silver"),
    check_categorical(df, "protein_band", VALID_BANDS, label="silver"),
    check_categorical(df, "carb_band",    VALID_BANDS, label="silver"),
    check_categorical(df, "fat_band",     VALID_BANDS, label="silver"),
], stage="silver_post_transform")

df.write.mode("overwrite").parquet(output_path)

row_count = df.count()
logger.info("Silver write complete — output=%s rows=%d", output_path, row_count)

spark.stop()
