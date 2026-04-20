-- =============================================================
-- Seed: Maria Chen (U004) — SCD Type 2 History
-- =============================================================
-- Purpose: Inserts 5 historical profile versions for U004 to
--          demonstrate user progress tracking in Tableau.
--          The batch pipeline (load_gold_to_postgres.py) always
--          loads U004 as version_number=1, is_current=TRUE with
--          her latest weight (199 lb). This script:
--            1. Corrects that row to version_number=6 and
--               backdates effective_start to 2026-04-01.
--            2. Inserts versions 1–5 as expired historical rows.
--
-- IMPORTANT: Run this AFTER load_gold_to_postgres.py, not before.
--            The batch load TRUNCATEs dim_user_profile, wiping any
--            previously seeded history. Re-run this script each
--            time you re-run the batch pipeline.
--
-- Journey summary (weight_loss goal throughout):
--   v1  2025-11-01  220 lb  →  initial baseline
--   v2  2025-11-28  215 lb  →  improving
--   v3  2025-12-20  218 lb  →  not_improving (holiday setback)
--   v4  2026-01-15  211 lb  →  improving (new year)
--   v5  2026-02-20  205 lb  →  improving
--   v6  2026-04-01  199 lb  →  improving (current)
-- =============================================================

BEGIN;

-- Step 1 ── Fix the batch-loaded row (version_number=1 → 6)
--           and backdate its effective_start to the real date.
UPDATE dim_user_profile
SET
    version_number  = 6,
    effective_start = '2026-04-01 09:00:00'
WHERE user_id    = 'U004'
  AND is_current = TRUE;

-- Step 2 ── Insert the 5 historical (expired) versions.
--           All have is_current=FALSE so the partial unique index
--           (WHERE is_current=TRUE) is never violated.

-- v1 — Baseline: first time Maria logged her profile (Nov 2025)
INSERT INTO dim_user_profile (
    user_id, user_name, gender, birth_date, age,
    height_cm, current_weight_lb, target_weight_lb,
    goal_type,
    effective_start, effective_end, is_current, version_number
) VALUES (
    'U004', 'Maria Chen', 'female', '1990-03-22', 35,
    163, 220.00, 160.00,
    'weight_loss',
    '2025-11-01 08:00:00', '2025-11-28 10:15:00', FALSE, 1
);

-- v2 — First update: good early progress (-5 lb)
INSERT INTO dim_user_profile (
    user_id, user_name, gender, birth_date, age,
    height_cm, current_weight_lb, target_weight_lb,
    goal_type,
    effective_start, effective_end, is_current, version_number
) VALUES (
    'U004', 'Maria Chen', 'female', '1990-03-22', 35,
    163, 215.00, 160.00,
    'weight_loss',
    '2025-11-28 10:15:00', '2025-12-20 19:45:00', FALSE, 2
);

-- v3 — Holiday setback: weight crept back up (+3 lb over December)
INSERT INTO dim_user_profile (
    user_id, user_name, gender, birth_date, age,
    height_cm, current_weight_lb, target_weight_lb,
    goal_type,
    effective_start, effective_end, is_current, version_number
) VALUES (
    'U004', 'Maria Chen', 'female', '1990-03-22', 35,
    163, 218.00, 160.00,
    'weight_loss',
    '2025-12-20 19:45:00', '2026-01-15 08:30:00', FALSE, 3
);

-- v4 — New Year recovery: strong bounce back (-7 lb)
INSERT INTO dim_user_profile (
    user_id, user_name, gender, birth_date, age,
    height_cm, current_weight_lb, target_weight_lb,
    goal_type,
    effective_start, effective_end, is_current, version_number
) VALUES (
    'U004', 'Maria Chen', 'female', '1990-03-22', 35,
    163, 211.00, 160.00,
    'weight_loss',
    '2026-01-15 08:30:00', '2026-02-20 11:00:00', FALSE, 4
);

-- v5 — Continued improvement (-6 lb in February)
INSERT INTO dim_user_profile (
    user_id, user_name, gender, birth_date, age,
    height_cm, current_weight_lb, target_weight_lb,
    goal_type,
    effective_start, effective_end, is_current, version_number
) VALUES (
    'U004', 'Maria Chen', 'female', '1990-03-22', 36,
    163, 205.00, 160.00,
    'weight_loss',
    '2026-02-20 11:00:00', '2026-04-01 09:00:00', FALSE, 5
);

-- Verify the full timeline looks correct before committing.
-- Expected: 6 rows for U004, version 6 is_current=TRUE.
SELECT
    version_number,
    current_weight_lb,
    ROUND(current_weight_lb - target_weight_lb, 1) AS delta_to_goal_lb,
    effective_start,
    effective_end,
    is_current
FROM dim_user_profile
WHERE user_id = 'U004'
ORDER BY version_number;

COMMIT;
