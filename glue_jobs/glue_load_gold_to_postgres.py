"""
Glue Python Shell Job: load_gold_to_postgres
=============================================
Reads gold-zone Parquet from S3 using pandas, then upserts into
PostgreSQL running on EC2. Runs inside the VPC so it can reach the
EC2 private IP on port 5432.

Adapted from scripts/ingestion/load_gold_to_postgres.py.
Key change: skip-if-exists for dim_user_profile — preserves API-created
users and their SCD2 history across pipeline reruns.

Required --additional-python-modules: psycopg2-binary,sqlalchemy,pyarrow
"""

import sys
import logging
import pandas as pd

from awsglue.utils import getResolvedOptions
from sqlalchemy import create_engine, text

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("glue_load_gold_to_postgres")

args = getResolvedOptions(sys.argv, [
    "DATA_BASE_PATH",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
])

BASE_PATH = args["DATA_BASE_PATH"].rstrip("/")
DATABASE_URL = (
    f"postgresql+psycopg2://{args['POSTGRES_USER']}:{args['POSTGRES_PASSWORD']}"
    f"@{args['POSTGRES_HOST']}:{args['POSTGRES_PORT']}/{args['POSTGRES_DB']}"
)

logger.info("Connecting to PostgreSQL at %s:%s/%s",
            args["POSTGRES_HOST"], args["POSTGRES_PORT"], args["POSTGRES_DB"])
engine = create_engine(DATABASE_URL)

# ── Read gold-zone Parquet from S3 ────────────────────────────
logger.info("Reading gold-zone parquets from %s", BASE_PATH)
food_df = pd.read_parquet(f"{BASE_PATH}/silver/food_nutrition_clean")
user_df = pd.read_parquet(f"{BASE_PATH}/gold/user_profiles_enriched")
rec_df  = pd.read_parquet(f"{BASE_PATH}/gold/food_recommendations")

logger.info("Loaded parquets — food=%d rows, users=%d rows, recommendations=%d rows",
            len(food_df), len(user_df), len(rec_df))

# ── Select warehouse columns ───────────────────────────────────
dim_food_cols = [
    "food_name", "serving_description", "serving_size_value",
    "calories_kcal", "protein_g", "carbohydrates_g", "fat_g",
    "fiber_g", "sugar_g", "sodium_mg",
    "calorie_band", "protein_band", "carb_band", "fat_band",
    "is_high_protein_low_calorie", "is_high_fiber", "is_low_sugar",
]
dim_food = food_df[[c for c in dim_food_cols if c in food_df.columns]].copy()

dim_user_base  = ["user_id", "height_cm", "current_weight_lb", "target_weight_lb", "goal_type"]
dim_user_extra = ["user_name", "gender", "birth_date", "age"]
dim_user = user_df[dim_user_base + [c for c in dim_user_extra if c in user_df.columns]].copy()

dim_user["effective_start"] = pd.Timestamp.now("UTC").replace(tzinfo=None)
dim_user["effective_end"]   = None
dim_user["is_current"]      = True
dim_user["version_number"]  = 1

fact_rec_cols = [
    "user_id", "food_name", "goal_type",
    "recommendation_score", "recommendation_rank", "recommendation_reason",
]
fact_rec = rec_df[[c for c in fact_rec_cols if c in rec_df.columns]].copy()

# ── Skip-if-exists for dim_user_profile ───────────────────────
# Preserves API-created users and SCD2 history across pipeline reruns.
with engine.connect() as conn:
    existing_ids = {
        row[0] for row in conn.execute(text("SELECT DISTINCT user_id FROM dim_user_profile"))
    }

new_users   = dim_user[~dim_user["user_id"].isin(existing_ids)].copy()
new_user_ids = set(new_users["user_id"])
new_recs    = fact_rec[fact_rec["user_id"].isin(new_user_ids)].copy()

logger.info("dim_user_profile: %d already in DB (skipped), %d new from batch",
            len(existing_ids), len(new_users))

# ── Reload dim_food, patch recommendations for new users ──────
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dim_food RESTART IDENTITY;"))
    if new_user_ids:
        ids_literal = ", ".join(f"'{uid}'" for uid in new_user_ids)
        conn.execute(text(
            f"DELETE FROM fact_food_recommendation WHERE user_id IN ({ids_literal})"
        ))

dim_food.to_sql("dim_food", engine, if_exists="append", index=False)
logger.info("Loaded dim_food: %d rows", len(dim_food))

if not new_users.empty:
    new_users.to_sql("dim_user_profile", engine, if_exists="append", index=False)
    logger.info("Loaded dim_user_profile: %d new rows", len(new_users))
else:
    logger.info("No new users from batch — dim_user_profile unchanged")

if not new_recs.empty:
    new_recs.to_sql("fact_food_recommendation", engine, if_exists="append", index=False)
    logger.info("Loaded fact_food_recommendation: %d rows for new users", len(new_recs))
else:
    logger.info("No new recommendations to load for batch users")

logger.info("Warehouse load complete")
