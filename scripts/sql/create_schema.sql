-- =============================================================
-- Nutrition Data Platform — Warehouse Schema
-- =============================================================
-- Drop order respects foreign-key direction: fact → dims → staging.
-- Re-running this script is safe — it drops and recreates everything.
-- =============================================================

DROP VIEW  IF EXISTS vw_user_progress_history;
DROP VIEW  IF EXISTS vw_food_nutrition_profile;
DROP VIEW  IF EXISTS vw_user_goal_summary;
DROP VIEW  IF EXISTS vw_recommendation_report;
DROP TABLE IF EXISTS fact_food_recommendation;
DROP TABLE IF EXISTS dim_food;
DROP TABLE IF EXISTS dim_user_profile;
DROP TABLE IF EXISTS stg_user_profile_event;

-- -------------------------------------------------------------
-- STAGING: append-only event log (one row per API call)
-- event_type: create | update | delete
-- -------------------------------------------------------------
CREATE TABLE stg_user_profile_event (
    event_id           SERIAL PRIMARY KEY,
    user_id            VARCHAR(50)  NOT NULL,
    event_type         VARCHAR(20)  NOT NULL DEFAULT 'create',
    user_name          VARCHAR(100),
    gender             VARCHAR(30),
    birth_date         DATE,
    height_cm          NUMERIC(10,2),
    current_weight_lb  NUMERIC(10,2),
    target_weight_lb   NUMERIC(10,2),
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------------
-- DIMENSION: dim_user_profile — SCD Type 2
--
-- Why SCD Type 2?
--   Every PUT /users/{id} creates a new version of the user's
--   profile. The old row is expired (effective_end stamped,
--   is_current set to FALSE). This preserves full history so
--   Tableau can plot weight over time.
--
-- Surrogate key: user_profile_key (SERIAL) — unique per row/version.
-- Business key:  user_id — NOT globally unique; many rows share it.
--
-- Current state:  WHERE is_current = TRUE
-- Full history:   WHERE user_id = :id  ORDER BY version_number
-- State at time:  WHERE user_id = :id
--                 AND effective_start <= :ts
--                 AND (effective_end > :ts OR effective_end IS NULL)
-- -------------------------------------------------------------
CREATE TABLE dim_user_profile (
    user_profile_key   SERIAL       PRIMARY KEY,   -- surrogate key
    user_id            VARCHAR(50)  NOT NULL,       -- business key
    user_name          VARCHAR(100),
    gender             VARCHAR(30),
    birth_date         DATE,
    age                INTEGER,                     -- derived from birth_date at write time
    height_cm          NUMERIC(10,2),
    current_weight_lb  NUMERIC(10,2),
    target_weight_lb   NUMERIC(10,2),
    goal_type          VARCHAR(50),                 -- weight_loss | weight_gain | maintenance
    -- SCD Type 2 tracking fields
    effective_start    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end      TIMESTAMP,                   -- NULL = row is currently active
    is_current         BOOLEAN      NOT NULL DEFAULT TRUE,
    version_number     INTEGER      NOT NULL DEFAULT 1,
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Enforce: only one active row per user at any time
CREATE UNIQUE INDEX uq_dim_user_profile_current
    ON dim_user_profile (user_id)
    WHERE (is_current = TRUE);

-- Speed up history queries and Tableau joins
CREATE INDEX idx_dim_user_profile_history
    ON dim_user_profile (user_id, effective_start);

-- -------------------------------------------------------------
-- DIMENSION: food catalogue — loaded from silver Parquet layer
-- No SCD needed — food data is static for this dataset.
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
    calorie_band                VARCHAR(20),
    protein_band                VARCHAR(20),
    carb_band                   VARCHAR(20),
    fat_band                    VARCHAR(20),
    is_high_protein_low_calorie INTEGER,
    is_high_fiber               INTEGER,
    is_low_sugar                INTEGER
);

-- -------------------------------------------------------------
-- FACT: current top-10 recommendations per user
-- Rebuilt in full on every create/update event (current state only).
-- Deleted in full on delete event.
-- Recommendation *history* is not stored here — the SCD2 table
-- provides the progress timeline needed for Tableau analysis.
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

-- =============================================================
-- REPORTING VIEWS
-- =============================================================

-- -------------------------------------------------------------
-- VIEW 1: vw_recommendation_report
-- Flat join for Power BI / Tableau recommendation dashboards.
-- Filters is_current = TRUE so only the latest user profile
-- appears — historical rows in dim_user_profile are excluded.
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
JOIN  dim_user_profile u ON r.user_id   = u.user_id  AND u.is_current = TRUE
LEFT JOIN dim_food     f ON r.food_name = f.food_name;

-- -------------------------------------------------------------
-- VIEW 2: vw_user_goal_summary
-- Current-state user overview for demographic/goal charts.
-- -------------------------------------------------------------
CREATE VIEW vw_user_goal_summary AS
SELECT
    user_id,
    user_name,
    gender,
    age,
    goal_type,
    height_cm,
    current_weight_lb,
    target_weight_lb,
    ROUND(current_weight_lb - target_weight_lb, 1) AS weight_delta_lb
FROM dim_user_profile
WHERE is_current = TRUE;

-- -------------------------------------------------------------
-- VIEW 3: vw_food_nutrition_profile
-- One row per food for nutritional comparison charts.
-- -------------------------------------------------------------
CREATE VIEW vw_food_nutrition_profile AS
SELECT
    food_name,
    calories_kcal,
    protein_g,
    carbohydrates_g,
    fat_g,
    fiber_g,
    sugar_g,
    sodium_mg,
    calorie_band,
    protein_band,
    carb_band,
    fat_band,
    is_high_protein_low_calorie,
    is_high_fiber,
    is_low_sugar
FROM dim_food;

-- -------------------------------------------------------------
-- VIEW 4: vw_user_progress_history  (PRIMARY TABLEAU VIEW)
--
-- One row per SCD version — the full timeline of every user's
-- profile changes. Designed for Tableau progress line charts.
--
-- Key fields for Tableau:
--   changed_at          → x-axis (time dimension)
--   current_weight_lb   → primary measure (weight over time)
--   target_weight_lb    → reference line
--   weight_delta_lb     → positive = above target, negative = below
--   progress_status     → colour dimension (improving/not_improving)
--   is_current          → filter to latest snapshot if needed
--
-- progress_status logic:
--   'initial'       — first recorded version (no prior to compare)
--   'improving'     — weight moving toward target
--   'not_improving' — weight moving away from target
--   'unchanged'     — weight identical to prior version
-- -------------------------------------------------------------
CREATE VIEW vw_user_progress_history AS
SELECT
    user_profile_key,
    user_id,
    user_name,
    gender,
    age,
    goal_type,
    height_cm,
    current_weight_lb,
    target_weight_lb,
    ROUND(current_weight_lb - target_weight_lb, 1)           AS weight_delta_lb,
    effective_start                                           AS changed_at,
    effective_end,
    is_current,
    version_number,
    -- Progress status compared to the immediately preceding version
    CASE
        WHEN LAG(current_weight_lb) OVER w IS NULL
            THEN 'initial'
        WHEN goal_type = 'weight_loss'
             AND current_weight_lb < LAG(current_weight_lb) OVER w
            THEN 'improving'
        WHEN goal_type = 'weight_gain'
             AND current_weight_lb > LAG(current_weight_lb) OVER w
            THEN 'improving'
        WHEN goal_type = 'maintenance'
             AND ABS(current_weight_lb - target_weight_lb)
               < ABS(LAG(current_weight_lb) OVER w - LAG(target_weight_lb) OVER w)
            THEN 'improving'
        WHEN current_weight_lb = LAG(current_weight_lb) OVER w
            THEN 'unchanged'
        ELSE 'not_improving'
    END                                                       AS progress_status
FROM dim_user_profile
WINDOW w AS (PARTITION BY user_id ORDER BY version_number);
