import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, row_number, round, abs as spark_abs
from pyspark.sql.window import Window

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
)
logger = logging.getLogger("silver_to_gold")

BASE_PATH = os.environ.get("DATA_BASE_PATH", "/opt/project/data")
silver_input = os.path.join(BASE_PATH, "silver", "food_nutrition_clean")
user_input = os.path.join(BASE_PATH, "input", "user_profiles.csv")
gold_output = os.path.join(BASE_PATH, "gold", "food_recommendations")
gold_user_output = os.path.join(BASE_PATH, "gold", "user_profiles_enriched")

spark = (
    SparkSession.builder
    .appName("silver-to-gold-recommendations")
    .getOrCreate()
)

logger.info("Reading silver foods from %s", silver_input)
foods = spark.read.parquet(silver_input)

logger.info("Reading user profiles from %s", user_input)
users = spark.read.option("header", True).csv(user_input)

users = (
    users.withColumn("height_cm", col("height_cm").cast("double"))
         .withColumn("current_weight_lb", col("current_weight_lb").cast("double"))
         .withColumn("target_weight_lb", col("target_weight_lb").cast("double"))
)

users = users.withColumn(
    "goal_type",
    when(col("target_weight_lb") < col("current_weight_lb"), lit("weight_loss"))
    .when(col("target_weight_lb") > col("current_weight_lb"), lit("weight_gain"))
    .otherwise(lit("maintenance")),
)

logger.info("Scoring foods for %d users across %d foods", users.count(), foods.count())

candidate_df = users.crossJoin(foods)

candidate_df = candidate_df.withColumn(
    "recommendation_score",
    when(
        col("goal_type") == "weight_loss",
        (
            col("protein_g") * lit(2.5)
            + col("fiber_g") * lit(2.0)
            - col("calories_kcal") * lit(0.015)
            - col("sugar_g") * lit(0.8)
            - col("fat_g") * lit(0.2)
        ),
    ).when(
        col("goal_type") == "weight_gain",
        (
            col("calories_kcal") * lit(0.02)
            + col("protein_g") * lit(2.0)
            + col("carbohydrates_g") * lit(0.8)
            + col("fat_g") * lit(0.5)
        ),
    ).otherwise(
        (
            col("protein_g") * lit(2.0)
            + col("fiber_g") * lit(1.5)
            - col("sugar_g") * lit(0.7)
            - spark_abs(col("calories_kcal") - lit(400)) * lit(0.01)
        )
    ),
)

candidate_df = candidate_df.withColumn(
    "recommendation_score",
    round(col("recommendation_score"), 2),
)

candidate_df = candidate_df.withColumn(
    "recommendation_reason",
    when(
        col("goal_type") == "weight_loss",
        lit("Higher protein/fiber with relatively lower calories and sugar"),
    ).when(
        col("goal_type") == "weight_gain",
        lit("Higher calories and protein for weight gain support"),
    ).otherwise(
        lit("Balanced nutrition profile for maintenance")
    ),
)

food_dedup_window = Window.partitionBy("user_id", "food_name").orderBy(col("recommendation_score").desc())

deduped_df = (
    candidate_df.withColumn("food_row_num", row_number().over(food_dedup_window))
                .filter(col("food_row_num") == 1)
                .drop("food_row_num")
)

final_rank_window = Window.partitionBy("user_id").orderBy(col("recommendation_score").desc())

ranked_df = deduped_df.withColumn(
    "recommendation_rank",
    row_number().over(final_rank_window),
)

top_recommendations = ranked_df.filter(col("recommendation_rank") <= 10)

users.write.mode("overwrite").parquet(gold_user_output)
top_recommendations.write.mode("overwrite").parquet(gold_output)

rec_count = top_recommendations.count()
logger.info("Gold write complete — recommendations=%d output=%s", rec_count, gold_output)
logger.info("User profiles enriched — output=%s", gold_user_output)

spark.stop()
