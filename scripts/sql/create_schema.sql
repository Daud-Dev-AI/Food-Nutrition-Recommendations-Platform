DROP TABLE IF EXISTS fact_food_recommendation;
DROP TABLE IF EXISTS dim_food;
DROP TABLE IF EXISTS dim_user_profile;

CREATE TABLE dim_user_profile (
    user_profile_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    height_cm NUMERIC(10,2),
    current_weight_lb NUMERIC(10,2),
    target_weight_lb NUMERIC(10,2),
    goal_type VARCHAR(50)
);

CREATE TABLE dim_food (
    food_key SERIAL PRIMARY KEY,
    food_name VARCHAR(255) NOT NULL,
    serving_description VARCHAR(255),
    serving_size_value NUMERIC(10,2),
    calories_kcal NUMERIC(10,2),
    protein_g NUMERIC(10,2),
    carbohydrates_g NUMERIC(10,2),
    fat_g NUMERIC(10,2),
    fiber_g NUMERIC(10,2),
    sugar_g NUMERIC(10,2),
    sodium_mg NUMERIC(10,2),
    calorie_band VARCHAR(20),
    protein_band VARCHAR(20),
    carb_band VARCHAR(20),
    fat_band VARCHAR(20),
    is_high_protein_low_calorie INTEGER,
    is_high_fiber INTEGER,
    is_low_sugar INTEGER
);

CREATE TABLE fact_food_recommendation (
    recommendation_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    food_name VARCHAR(255) NOT NULL,
    goal_type VARCHAR(50),
    recommendation_score NUMERIC(10,2),
    recommendation_rank INTEGER,
    recommendation_reason TEXT
);