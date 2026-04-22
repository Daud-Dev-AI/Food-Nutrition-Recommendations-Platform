"""
Lambda Function: daud_nutrition_load_gold_to_postgres
=====================================================
Reads gold-zone Parquet from S3 using pandas, then upserts into
PostgreSQL running on EC2. Runs inside the VPC so it can reach the
EC2 private IP on port 5432.

Same skip-if-exists logic as the Glue Python Shell job it replaces:
- dim_food: TRUNCATE + RELOAD (only batch writes to it)
- dim_user_profile: skip-if-exists (preserve API-created users + SCD2 history)
- fact_food_recommendation: delete + reload only for new batch users

Environment variables (set on the Lambda function):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
  DATA_BASE_PATH  — e.g. s3://daud-nutrition-platform-data
"""

import io
import os
import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("lambda_load_gold_to_postgres")

POSTGRES_HOST     = os.environ["POSTGRES_HOST"]
POSTGRES_PORT     = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.environ["POSTGRES_DB"]
POSTGRES_USER     = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DATA_BASE_PATH    = os.environ["DATA_BASE_PATH"].rstrip("/")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


def _read_s3_parquet(s3_client, bucket: str, prefix: str) -> pd.DataFrame:
    """Download all .parquet files under an S3 prefix and return as DataFrame."""
    prefix = prefix.rstrip("/") + "/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    frames = []
    for key in keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(frames, ignore_index=True)


def handler(event, context):
    logger.info("Lambda load_gold_to_postgres starting")
    logger.info("Connecting to PostgreSQL at %s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

    engine = create_engine(DATABASE_URL)

    # Parse bucket and optional base prefix from DATA_BASE_PATH (s3://bucket or s3://bucket/prefix)
    s3_path = DATA_BASE_PATH.removeprefix("s3://")
    bucket, _, base_prefix = s3_path.partition("/")
    base_prefix = base_prefix.strip("/")  # may be empty string when path is just s3://bucket

    def s3_prefix(*parts) -> str:
        """Join S3 prefix segments, ignoring empty ones."""
        return "/".join(p.strip("/") for p in [base_prefix, *parts] if p)

    s3 = boto3.client("s3")

    # ── Read gold-zone Parquet from S3 via boto3 ──────────────────
    logger.info("Reading parquets from bucket=%s prefix=%s", bucket, base_prefix or "(root)")
    food_df = _read_s3_parquet(s3, bucket, s3_prefix("silver", "food_nutrition_clean"))
    user_df = _read_s3_parquet(s3, bucket, s3_prefix("gold", "user_profiles_enriched"))
    rec_df  = _read_s3_parquet(s3, bucket, s3_prefix("gold", "food_recommendations"))

    logger.info(
        "Loaded parquets — food=%d rows, users=%d rows, recommendations=%d rows",
        len(food_df), len(user_df), len(rec_df),
    )

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
    dim_user = user_df[
        dim_user_base + [c for c in dim_user_extra if c in user_df.columns]
    ].copy()

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

    new_users    = dim_user[~dim_user["user_id"].isin(existing_ids)].copy()
    new_user_ids = set(new_users["user_id"])
    new_recs     = fact_rec[fact_rec["user_id"].isin(new_user_ids)].copy()

    logger.info(
        "dim_user_profile: %d already in DB (skipped), %d new from batch",
        len(existing_ids), len(new_users),
    )

    # ── Reload dim_food; patch recommendations for new users ──────
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
    return {
        "status": "success",
        "new_users_loaded": len(new_users),
        "dim_food_rows": len(dim_food),
        "new_recs_loaded": len(new_recs),
    }
