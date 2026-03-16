import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import create_engine, text
import pandas as pd

from app.config import DATABASE_URL
from app.logger import get_logger

logger = get_logger("load_gold_to_postgres")

engine = create_engine(DATABASE_URL)

BASE_PATH = os.environ.get("DATA_BASE_PATH", "data")

logger.info("Reading parquet layers from %s", BASE_PATH)

food_df = pd.read_parquet(os.path.join(BASE_PATH, "silver", "food_nutrition_clean"))
user_df = pd.read_parquet(os.path.join(BASE_PATH, "gold", "user_profiles_enriched"))
rec_df = pd.read_parquet(os.path.join(BASE_PATH, "gold", "food_recommendations"))

logger.info(
    "Loaded parquets — food=%d rows, users=%d rows, recommendations=%d rows",
    len(food_df), len(user_df), len(rec_df),
)

dim_food = food_df[[
    "food_name",
    "serving_description",
    "serving_size_value",
    "calories_kcal",
    "protein_g",
    "carbohydrates_g",
    "fat_g",
    "fiber_g",
    "sugar_g",
    "sodium_mg",
    "calorie_band",
    "protein_band",
    "carb_band",
    "fat_band",
    "is_high_protein_low_calorie",
    "is_high_fiber",
    "is_low_sugar",
]].copy()

dim_user = user_df[[
    "user_id",
    "height_cm",
    "current_weight_lb",
    "target_weight_lb",
    "goal_type",
]].copy()

fact_rec = rec_df[[
    "user_id",
    "food_name",
    "goal_type",
    "recommendation_score",
    "recommendation_rank",
    "recommendation_reason",
]].copy()

logger.info("Truncating warehouse tables")
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE fact_food_recommendation RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_food RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_user_profile RESTART IDENTITY;"))

dim_user.to_sql("dim_user_profile", engine, if_exists="append", index=False)
logger.info("Loaded dim_user_profile: %d rows", len(dim_user))

dim_food.to_sql("dim_food", engine, if_exists="append", index=False)
logger.info("Loaded dim_food: %d rows", len(dim_food))

fact_rec.to_sql("fact_food_recommendation", engine, if_exists="append", index=False)
logger.info("Loaded fact_food_recommendation: %d rows", len(fact_rec))

logger.info("Warehouse load complete")
