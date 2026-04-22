"""
Glue Job: silver_to_gold
=========================
Cross-joins users × foods, scores each food per user goal (weight_loss /
weight_gain / maintenance), ranks and keeps top-10 per user, then writes
gold-zone Parquet for recommendations and enriched user profiles.

Adapted from spark_jobs/batch/silver_to_gold_recommendations.py for Glue 4.0.
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, row_number, round as spark_round,
    abs as spark_abs, datediff, current_date, floor as spark_floor,
)
from pyspark.sql.window import Window

from quality_checks import (
    check_row_count,
    check_not_null,
    check_non_negative,
    check_positive,
    check_value_range,
    check_unique,
    run_checks,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("glue_silver_to_gold")

args = getResolvedOptions(sys.argv, ["DATA_BASE_PATH"])
BASE_PATH        = args["DATA_BASE_PATH"].rstrip("/")
silver_input     = f"{BASE_PATH}/silver/food_nutrition_clean"
user_input       = f"{BASE_PATH}/raw/user_profiles.csv"
gold_output      = f"{BASE_PATH}/gold/food_recommendations"
gold_user_output = f"{BASE_PATH}/gold/user_profiles_enriched"

spark = (
    SparkSession.builder
    .appName("glue-silver-to-gold-recommendations")
    .getOrCreate()
)

logger.info("Reading silver foods from %s", silver_input)
foods = spark.read.parquet(silver_input)

# ── Stage: silver (incoming food checks) ──────────────────────
run_checks([
    check_row_count(foods, min_rows=1, label="silver_foods"),
    check_not_null(foods, "food_name",     label="silver_foods"),
    check_positive(foods, "calories_kcal", label="silver_foods"),
    check_non_negative(foods, "protein_g",       label="silver_foods"),
    check_non_negative(foods, "carbohydrates_g", label="silver_foods"),
    check_non_negative(foods, "fat_g",           label="silver_foods"),
    check_non_negative(foods, "fiber_g",         label="silver_foods"),
    check_non_negative(foods, "sugar_g",         label="silver_foods"),
], stage="silver_food_input_check")

logger.info("Reading user profiles from %s", user_input)
users = spark.read.option("header", True).csv(user_input)

users = (
    users.withColumn("height_cm",         col("height_cm").cast("double"))
         .withColumn("current_weight_lb", col("current_weight_lb").cast("double"))
         .withColumn("target_weight_lb",  col("target_weight_lb").cast("double"))
)

if "birth_date" in users.columns:
    users = users.withColumn(
        "birth_date", col("birth_date").cast("date")
    ).withColumn(
        "age",
        spark_floor(datediff(current_date(), col("birth_date")) / lit(365.25)).cast("integer"),
    )

users = users.withColumn(
    "goal_type",
    when(col("target_weight_lb") < col("current_weight_lb"), lit("weight_loss"))
    .when(col("target_weight_lb") > col("current_weight_lb"), lit("weight_gain"))
    .otherwise(lit("maintenance")),
)

run_checks([
    check_row_count(users, min_rows=1, label="users"),
    check_not_null(users, "user_id",  label="users"),
    check_unique(users, "user_id",    label="users"),
], stage="user_input_check")

# ── Scoring: cross-join users × foods ─────────────────────────
logger.info("Scoring foods for %d users across %d foods", users.count(), foods.count())

candidate_df = users.crossJoin(foods)

candidate_df = candidate_df.withColumn(
    "recommendation_score",
    when(
        col("goal_type") == "weight_loss",
        col("protein_g") * lit(2.5)
        + col("fiber_g") * lit(2.0)
        - col("calories_kcal") * lit(0.015)
        - col("sugar_g") * lit(0.8)
        - col("fat_g") * lit(0.2),
    ).when(
        col("goal_type") == "weight_gain",
        col("calories_kcal") * lit(0.02)
        + col("protein_g") * lit(2.0)
        + col("carbohydrates_g") * lit(0.8)
        + col("fat_g") * lit(0.5),
    ).otherwise(
        col("protein_g") * lit(2.0)
        + col("fiber_g") * lit(1.5)
        - col("sugar_g") * lit(0.7)
        - spark_abs(col("calories_kcal") - lit(400)) * lit(0.01)
    ),
)

candidate_df = candidate_df.withColumn(
    "recommendation_score",
    spark_round(col("recommendation_score"), 2),
)

candidate_df = candidate_df.withColumn(
    "recommendation_reason",
    when(col("goal_type") == "weight_loss",
         lit("Higher protein/fiber with relatively lower calories and sugar"))
    .when(col("goal_type") == "weight_gain",
          lit("Higher calories and protein for weight gain support"))
    .otherwise(lit("Balanced nutrition profile for maintenance")),
)

# Deduplicate per (user_id, food_name): keep highest score
food_dedup_window = Window.partitionBy("user_id", "food_name").orderBy(
    col("recommendation_score").desc()
)
deduped_df = (
    candidate_df.withColumn("food_row_num", row_number().over(food_dedup_window))
                .filter(col("food_row_num") == 1)
                .drop("food_row_num")
)

# Rank top 10 per user
final_rank_window = Window.partitionBy("user_id").orderBy(col("recommendation_score").desc())
ranked_df = deduped_df.withColumn("recommendation_rank", row_number().over(final_rank_window))
top_recommendations = ranked_df.filter(col("recommendation_rank") <= 10)

# ── Stage: gold (recommendation checks) ───────────────────────
run_checks([
    check_row_count(top_recommendations, min_rows=1, label="gold_recs"),
    check_not_null(top_recommendations, "recommendation_score", label="gold_recs"),
    check_value_range(top_recommendations, "recommendation_rank", min_val=1, max_val=10, label="gold_recs"),
    check_unique(top_recommendations, ["user_id", "recommendation_rank"], label="gold_recs"),
    check_unique(top_recommendations, ["user_id", "food_name"],           label="gold_recs"),
], stage="gold_recommendations_check")

users.write.mode("overwrite").parquet(gold_user_output)
top_recommendations.write.mode("overwrite").parquet(gold_output)

rec_count = top_recommendations.count()
logger.info("Gold write complete — recommendations=%d output=%s", rec_count, gold_output)
logger.info("User profiles enriched — output=%s", gold_user_output)

spark.stop()
