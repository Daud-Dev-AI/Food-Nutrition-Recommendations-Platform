from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace
from pyspark.sql.types import DoubleType

spark = (
    SparkSession.builder
    .appName("raw-to-bronze-food-nutrition")
    .getOrCreate()
)

input_path = "/opt/project/data/raw/nutrition.csv"
output_path = "/opt/project/data/bronze/food_nutrition"

df = spark.read.option("header", True).csv(input_path)

# Clean column names
new_names = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
for old, new in zip(df.columns, new_names):
    df = df.withColumnRenamed(old, new)

# Clean text columns
text_cols = ["label", "weight"]
for c in text_cols:
    if c in df.columns:
        df = df.withColumn(c, trim(lower(col(c))))

# Clean numeric-like columns that may contain stray spaces
numeric_cols = ["calories", "protein", "carbohydrates", "fats", "fiber", "sugars", "sodium"]
for c in numeric_cols:
    if c in df.columns:
        df = df.withColumn(c, regexp_replace(trim(col(c)), ",", ""))
        df = df.withColumn(c, col(c).cast(DoubleType()))

df = df.dropDuplicates()

df.write.mode("overwrite").parquet(output_path)

print("Bronze write complete")
print("Row count:", df.count())
df.printSchema()

spark.stop()