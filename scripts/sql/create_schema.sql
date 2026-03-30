-- =============================================================
-- Nutrition Data Platform — Warehouse Schema
-- =============================================================
-- Run order matters: drop fact before dims (FK direction).
-- Re-running this script is safe — it drops and recreates all objects.
-- =============================================================

DROP VIEW  IF EXISTS vw_recommendation_report;
DROP TABLE IF EXISTS fact_food_recommendation;
DROP TABLE IF EXISTS dim_food;
DROP TABLE IF EXISTS dim_user_profile;
DROP TABLE IF EXISTS stg_user_profile_event;

-- -------------------------------------------------------------
-- STAGING: raw event log (append-only, one row per API call)
-- event_type: create | update | delete
-- -------------------------------------------------------------
CREATE TABLE stg_user_profile_event (
    event_id           SERIAL PRIMARY KEY,
    user_id            VARCHAR(50)  NOT NULL,
    event_type         VARCHAR(20)  NOT NULL DEFAULT 'create',
    user_name          VARCHAR(100),
    gender             VARCHAR(30),             -- male | female | other | prefer_not_to_say
    birth_date         DATE,
    height_cm          NUMERIC(10,2),
    current_weight_lb  NUMERIC(10,2),
    target_weight_lb   NUMERIC(10,2),
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------
-- DIMENSION: one row per user — SCD Type 1 (last-write-wins)
-- age is stored as an integer derived from birth_date at write
-- time; recalculated whenever the consumer processes an update.
-- -------------------------------------------------------------
CREATE TABLE dim_user_profile (
    user_profile_key   SERIAL PRIMARY KEY,
    user_id            VARCHAR(50)  UNIQUE NOT NULL,
    user_name          VARCHAR(100),
    gender             VARCHAR(30),
    birth_date         DATE,
    age                INTEGER,                 -- derived: years since birth_date
    height_cm          NUMERIC(10,2),
    current_weight_lb  NUMERIC(10,2),
    target_weight_lb   NUMERIC(10,2),
    goal_type          VARCHAR(50),             -- weight_loss | weight_gain | maintenance
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------
-- DIMENSION: food catalogue — loaded from silver parquet layer
-- -------------------------------------------------------------
CREATE TABLE dim_food (
    food_key                    SERIAL PRIMARY KEY,
    food_name                   VARCHAR(255) NOT NULL,
    serving_description         VARCHAR(255),
    serving_size_value          NUMERIC(10,2),
    calories_kcal               NUMERIC(10,2),
    protein_g                   NUMERIC(10,2),
    carbohydrates_g             NUMERIC(10,2),
    fat_g                       NUMERIC(10,2),
    fiber_g                     NUMERIC(10,2),
    sugar_g                     NUMERIC(10,2),
    sodium_mg                   NUMERIC(10,2),
    calorie_band                VARCHAR(20),    -- low | medium | high
    protein_band                VARCHAR(20),
    carb_band                   VARCHAR(20),
    fat_band                    VARCHAR(20),
    is_high_protein_low_calorie INTEGER,        -- 0 | 1 flag
    is_high_fiber               INTEGER,
    is_low_sugar                INTEGER
);

-- -------------------------------------------------------------
-- FACT: top-10 food recommendations per user
-- Rebuilt in full on every create/update event.
-- Removed entirely on delete event (cascade via consumer).
-- -------------------------------------------------------------
CREATE TABLE fact_food_recommendation (
    recommendation_key   SERIAL PRIMARY KEY,
    user_id              VARCHAR(50)   NOT NULL,
    food_name            VARCHAR(255)  NOT NULL,
    goal_type            VARCHAR(50),
    recommendation_score NUMERIC(10,2),
    recommendation_rank  INTEGER,
    recommendation_reason TEXT,
    created_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------
-- BI REPORTING VIEW
-- Join all layers into one flat table for Power BI / Tableau.
-- Connect any BI tool to PostgreSQL and query this view directly.
-- No extra ETL needed — the view reflects current warehouse state.
-- -------------------------------------------------------------
CREATE VIEW vw_recommendation_report AS
SELECT
    u.user_id,
    u.user_name,
    u.gender,
    u.age,
    u.goal_type,
    u.height_cm,
    u.current_weight_lb,
    u.target_weight_lb,
    r.food_name,
    r.recommendation_score,
    r.recommendation_rank,
    r.recommendation_reason,
    f.calories_kcal,
    f.protein_g,
    f.fiber_g,
    f.carbohydrates_g,
    f.fat_g,
    f.sugar_g,
    f.calorie_band,
    f.protein_band,
    f.is_high_protein_low_calorie,
    f.is_high_fiber,
    f.is_low_sugar
FROM fact_food_recommendation r
JOIN  dim_user_profile u ON r.user_id   = u.user_id
LEFT JOIN dim_food     f ON r.food_name = f.food_name;
