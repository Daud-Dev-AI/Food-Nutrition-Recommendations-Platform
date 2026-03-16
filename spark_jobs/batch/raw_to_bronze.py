import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace
from pyspark.sql.types import DoubleType

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
)
logger = logging.getLogger("raw_to_bronze")

BASE_PATH = os.environ.get("DATA_BASE_PATH", "/opt/project/data")
input_path = os.path.join(BASE_PATH, "raw", "nutrition.csv")
output_path = os.path.join(BASE_PATH, "bronze", "food_nutrition")

spark = (
    SparkSession.builder
    .appName("raw-to-bronze-food-nutrition")
    .getOrCreate()
)

logger.info("Reading raw CSV from %s", input_path)
df = spark.read.option("header", True).csv(input_path)

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

df.write.mode("overwrite").parquet(output_path)

row_count = df.count()
logger.info("Bronze write complete — output=%s rows=%d", output_path, row_count)

spark.stop()
