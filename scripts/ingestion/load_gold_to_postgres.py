from sqlalchemy import create_engine, text
import pandas as pd

POSTGRES_URL = "postgresql+psycopg2://de_user:de_pass@localhost:5432/nutrition_dw"

engine = create_engine(POSTGRES_URL)

food_df = pd.read_parquet("data/silver/food_nutrition_clean")
user_df = pd.read_parquet("data/gold/user_profiles_enriched")
rec_df = pd.read_parquet("data/gold/food_recommendations")

# Keep only columns needed for warehouse tables
dim_food = food_df[
    [
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
        "is_low_sugar"
    ]
].copy()

dim_user = user_df[
    [
        "user_id",
        "height_cm",
        "current_weight_lb",
        "target_weight_lb",
        "goal_type"
    ]
].copy()

fact_rec = rec_df[
    [
        "user_id",
        "food_name",
        "goal_type",
        "recommendation_score",
        "recommendation_rank",
        "recommendation_reason"
    ]
].copy()

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE fact_food_recommendation RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_food RESTART IDENTITY;"))
    conn.execute(text("TRUNCATE TABLE dim_user_profile RESTART IDENTITY;"))

dim_user.to_sql("dim_user_profile", engine, if_exists="append", index=False)
dim_food.to_sql("dim_food", engine, if_exists="append", index=False)
fact_rec.to_sql("fact_food_recommendation", engine, if_exists="append", index=False)

print("Loaded dim_user_profile:", len(dim_user))
print("Loaded dim_food:", len(dim_food))
print("Loaded fact_food_recommendation:", len(fact_rec))