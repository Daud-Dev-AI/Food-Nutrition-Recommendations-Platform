import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import create_engine, text
import pandas as pd

from app.config import DATABASE_URL
from app.logger import get_logger
from spark_jobs.data_quality.quality_checks import (
    check_row_count,
    check_not_null,
    check_value_range,
    check_unique,
    check_warehouse_row_count,
    check_warehouse_unique,
    check_warehouse_referential_integrity,
    run_checks,
)

logger = get_logger("load_gold_to_postgres")

engine = create_engine(DATABASE_URL)

BASE_PATH = os.environ.get("DATA_BASE_PATH", "data")

logger.info("Reading parquet layers from %s", BASE_PATH)

food_df = pd.read_parquet(os.path.join(BASE_PATH, "silver", "food_nutrition_clean"))
user_df = pd.read_parquet(os.path.join(BASE_PATH, "gold", "user_profiles_enriched"))
rec_df  = pd.read_parquet(os.path.join(BASE_PATH, "gold", "food_recommendations"))

logger.info(
    "Loaded parquets — food=%d rows, users=%d rows, recommendations=%d rows",
    len(food_df), len(user_df), len(rec_df),
)

# ── Stage: parquet validation (before loading) ────────────────
run_checks([
    check_row_count(food_df, min_rows=1, label="food_parquet"),
    check_not_null(food_df, "food_name",     label="food_parquet"),
    check_row_count(user_df, min_rows=1, label="user_parquet"),
    check_not_null(user_df, "user_id",       label="user_parquet"),
    check_unique(user_df, "user_id",         label="user_parquet"),
    check_row_count(rec_df, min_rows=1, label="rec_parquet"),
    check_not_null(rec_df, "recommendation_score", label="rec_parquet"),
    check_value_range(rec_df, "recommendation_rank", min_val=1, max_val=10, label="rec_parquet"),
    check_unique(rec_df, ["user_id", "food_name"],         label="rec_parquet"),
    check_unique(rec_df, ["user_id", "recommendation_rank"], label="rec_parquet"),
], stage="parquet_pre_load_check")

# ── Select columns for each warehouse table ────────────────────

dim_food_cols = [
    "food_name", "serving_description", "serving_size_value",
    "calories_kcal", "protein_g", "carbohydrates_g", "fat_g",
    "fiber_g", "sugar_g", "sodium_mg",
    "calorie_band", "protein_band", "carb_band", "fat_band",
    "is_high_protein_low_calorie", "is_high_fiber", "is_low_sugar",
]
dim_food = food_df[[c for c in dim_food_cols if c in food_df.columns]].copy()

# New user fields (user_name, gender, birth_date, age) are optional in the
# batch parquet because user_profiles.csv predates the richer schema.
# We include them if present; otherwise they default to NULL in Postgres.
dim_user_base_cols  = ["user_id", "height_cm", "current_weight_lb", "target_weight_lb", "goal_type"]
dim_user_extra_cols = ["user_name", "gender", "birth_date", "age"]
dim_user_select = dim_user_base_cols + [c for c in dim_user_extra_cols if c in user_df.columns]
dim_user = user_df[dim_user_select].copy()

# Inject SCD Type 2 fields for the initial batch load.
# The batch pipeline always represents version 1 (the baseline snapshot).
# Subsequent updates via the API / Kafka consumer will create version 2, 3, …
import pandas as _pd
dim_user["effective_start"] = _pd.Timestamp.utcnow().replace(tzinfo=None)
dim_user["effective_end"]   = None
dim_user["is_current"]      = True
dim_user["version_number"]  = 1

fact_rec_cols = [
    "user_id", "food_name", "goal_type",
    "recommendation_score", "recommendation_rank", "recommendation_reason",
]
fact_rec = rec_df[[c for c in fact_rec_cols if c in rec_df.columns]].copy()

# ── Truncate and reload warehouse tables ───────────────────────
logger.info("Truncating warehouse tables")
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE fact_food_recommendation RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_food RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_user_profile RESTART IDENTITY;"))

dim_user.to_sql("dim_user_profile",        engine, if_exists="append", index=False)
logger.info("Loaded dim_user_profile: %d rows", len(dim_user))

dim_food.to_sql("dim_food",                engine, if_exists="append", index=False)
logger.info("Loaded dim_food: %d rows", len(dim_food))

fact_rec.to_sql("fact_food_recommendation", engine, if_exists="append", index=False)
logger.info("Loaded fact_food_recommendation: %d rows", len(fact_rec))

# ── Stage: warehouse validation (after loading) ────────────────
run_checks([
    check_warehouse_row_count(engine, "dim_food",                min_rows=len(dim_food)),
    check_warehouse_row_count(engine, "dim_user_profile",        min_rows=len(dim_user)),
    check_warehouse_row_count(engine, "fact_food_recommendation", min_rows=len(fact_rec)),
    check_warehouse_unique(engine, "dim_user_profile", "user_id"),
    check_warehouse_referential_integrity(
        engine,
        fact_table="fact_food_recommendation",
        dim_table="dim_user_profile",
        fk_col="user_id",
        pk_col="user_id",
    ),
], stage="warehouse_post_load_check")

logger.info("Warehouse load complete")
