from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_extract, trim, lower
)
from pyspark.sql.types import DoubleType

spark = (
    SparkSession.builder
    .appName("bronze-to-silver-food-nutrition")
    .getOrCreate()
)

input_path = "/opt/project/data/bronze/food_nutrition"
output_path = "/opt/project/data/silver/food_nutrition_clean"

df = spark.read.parquet(input_path)

# Standard cleanup
df = df.dropDuplicates()

if "label" in df.columns:
    df = df.withColumn("label", trim(lower(col("label"))))

if "weight" in df.columns:
    df = df.withColumn("weight", trim(lower(col("weight"))))

# Extract serving size number if possible from weight text
if "weight" in df.columns:
    df = df.withColumn(
        "serving_size_value",
        regexp_extract(col("weight"), r"([0-9]+(\.[0-9]+)?)", 1).cast(DoubleType())
    )

# Rename columns to make them more business-friendly
rename_map = {
    "label": "food_name",
    "weight": "serving_description",
    "fats": "fat_g",
    "sugars": "sugar_g",
    "protein": "protein_g",
    "fiber": "fiber_g",
    "carbohydrates": "carbohydrates_g",
    "calories": "calories_kcal",
    "sodium": "sodium_mg"
}

for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# Derived business tags
if "calories_kcal" in df.columns:
    df = df.withColumn(
        "calorie_band",
        when(col("calories_kcal") < 150, lit("low"))
        .when((col("calories_kcal") >= 150) & (col("calories_kcal") < 300), lit("medium"))
        .otherwise(lit("high"))
    )

if "protein_g" in df.columns:
    df = df.withColumn(
        "protein_band",
        when(col("protein_g") < 5, lit("low"))
        .when((col("protein_g") >= 5) & (col("protein_g") < 15), lit("medium"))
        .otherwise(lit("high"))
    )

if "carbohydrates_g" in df.columns:
    df = df.withColumn(
        "carb_band",
        when(col("carbohydrates_g") < 10, lit("low"))
        .when((col("carbohydrates_g") >= 10) & (col("carbohydrates_g") < 25), lit("medium"))
        .otherwise(lit("high"))
    )

if "fat_g" in df.columns:
    df = df.withColumn(
        "fat_band",
        when(col("fat_g") < 5, lit("low"))
        .when((col("fat_g") >= 5) & (col("fat_g") < 15), lit("medium"))
        .otherwise(lit("high"))
    )

# Add simple recommendation-friendly flags
if "protein_g" in df.columns and "calories_kcal" in df.columns:
    df = df.withColumn(
        "is_high_protein_low_calorie",
        when((col("protein_g") >= 10) & (col("calories_kcal") <= 250), lit(1)).otherwise(lit(0))
    )

if "fiber_g" in df.columns:
    df = df.withColumn(
        "is_high_fiber",
        when(col("fiber_g") >= 5, lit(1)).otherwise(lit(0))
    )

if "sugar_g" in df.columns:
    df = df.withColumn(
        "is_low_sugar",
        when(col("sugar_g") <= 8, lit(1)).otherwise(lit(0))
    )

df.write.mode("overwrite").parquet(output_path)

print("Silver write complete")
print("Row count:", df.count())
df.printSchema()
df.show(10, truncate=False)

spark.stop()