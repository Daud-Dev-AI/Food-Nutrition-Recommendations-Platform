# Nutrition Recommendation Data Platform

A local end-to-end data engineering platform demonstrating batch pipeline processing, event-driven streaming, orchestration, and data quality — using a medallion architecture on a nutrition dataset.

The system ingests a nutrition CSV, processes it through Raw → Bronze → Silver → Gold layers via Apache Spark, stores curated data in PostgreSQL, and serves personalised food recommendations via FastAPI. Users can be created, updated, and deleted at runtime via the API; events flow through Kafka and are processed by a consumer that keeps the warehouse in sync in near real time.

---

## Architecture

### Batch Pipeline (Airflow-orchestrated, daily at midnight)

```
nutrition.csv (raw)
       │
       ▼ DQ: file exists, row count, expected columns
 [Spark] raw_to_bronze.py
       │  - normalise column names
       │  - cast numeric types
       │  - drop duplicates
       │ DQ: row count, food_name not null
       ▼
 data/bronze/food_nutrition  (Parquet)
       │
       ▼ DQ: numeric types, no fully-null rows
 [Spark] bronze_to_silver.py
       │  - rename columns to business names
       │  - derive calorie / protein / carb / fat bands
       │  - derive recommendation flags
       │ DQ: food_name not null, calories > 0, nutrients non-negative, band values valid
       ▼
 data/silver/food_nutrition_clean  (Parquet)
       │
       ▼ DQ: silver food checks, user uniqueness
 [Spark] silver_to_gold_recommendations.py
       │  - cross-join users × foods
       │  - score per goal_type (weight_loss / weight_gain / maintenance)
       │  - deduplicate, rank top 10 per user
       │ DQ: score not null, ranks 1–10, no duplicates per user
       ▼
 data/gold/food_recommendations  (Parquet)
 data/gold/user_profiles_enriched  (Parquet)
       │
       ▼ DQ: parquet row counts, uniqueness, referential integrity
 [Python] load_gold_to_postgres.py
       │  - truncate warehouse tables
       │  - load dim_food, dim_user_profile, fact_food_recommendation
       │ DQ: warehouse row counts, dim_user unique, FK integrity
       ▼
 PostgreSQL — nutrition_dw

       │  (optional, run after load)
       ▼
 [Python] archive_to_minio.py
       │  - upload bronze / silver / gold Parquet files
       ▼
 MinIO — nutrition-platform bucket  (S3-compatible object storage)
```

### Streaming Pipeline (event-driven, near real-time)

```
POST /users          PUT /users/{id}        DELETE /users/{id}
       │                    │                       │
       └──────────┬─────────┘                       │
                  ▼                                  ▼
     stg_user_profile_event            stg_user_profile_event
           (event_type=create|update)       (event_type=delete)
                  │                                  │
                  └──────────────┬───────────────────┘
                                 ▼
                    Kafka topic: user_profiles
                                 │
                                 ▼
                           consumer.py
                    ┌────────────┴─────────────────┐
              create/update                      delete
                    │                              │
                    ▼                              ▼
        SCD2: expire current row        hard-delete all SCD versions
        insert new version              DELETE fact_food_recommendation
        score foods + rebuild
        fact_food_recommendation
                    │
                    ▼
       GET /recommendations/{user_id}  →  fresh results
       GET /users/{user_id}/history    →  full version timeline
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Pipeline orchestration | Apache Airflow 2.9.3 (LocalExecutor) |
| Batch processing | Apache Spark 3.5.2 (PySpark) |
| Stream processing | Apache Kafka + Python consumer |
| Data quality | Custom Python module (`quality_checks.py`) |
| Serving database | PostgreSQL 16 |
| API | FastAPI + SQLAlchemy |
| Object storage | MinIO (S3-compatible) |
| Containerisation | Docker Compose |
| Runtime | Python 3.12, WSL2 |

---

## Directory Structure

```
nutrition-data-platform/
├── app/
│   ├── config.py                          # Central config — reads .env
│   ├── logger.py                          # Shared structured logger
│   ├── main.py                            # FastAPI: POST/PUT/DELETE /users, GET /recs
│   └── consumer.py                        # Kafka consumer: create/update/delete routing
├── spark_jobs/
│   ├── batch/
│   │   ├── raw_to_bronze.py
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold_recommendations.py
│   └── data_quality/
│       └── quality_checks.py              # Reusable DQ check functions
├── dags/
│   └── nutrition_pipeline_dag.py          # Airflow DAG (daily batch pipeline)
├── scripts/
│   ├── ingestion/
│   │   └── load_gold_to_postgres.py
│   ├── archival/
│   │   └── archive_to_minio.py
│   └── sql/
│       ├── create_schema.sql              # Full DDL + vw_recommendation_report view
│       └── create_airflow_db.sql          # Airflow metadata DB init
├── airflow/
│   └── Dockerfile                         # Custom image: Airflow + Java + PySpark
├── data/
│   ├── raw/                               # Source CSV
│   ├── bronze/                            # Parquet — technical cleansing
│   ├── silver/                            # Parquet — business transformation
│   ├── gold/                              # Parquet — scored recommendations
│   └── input/                             # Static user profiles (batch)
├── docker-compose.yml
├── requirements.txt
├── .env                                   # Local config (gitignored)
└── .env.example
```

---

## Prerequisites

- Docker Desktop with WSL2 integration enabled
- Python 3.10+ (for local scripts run outside Docker)

---

## Initial Setup

### 1. Start the core stack (first time)

```bash
docker compose up -d postgres zookeeper kafka minio spark-master spark-worker
```

On first start, Docker creates the `postgres_data` volume and runs the init script which creates both `nutrition_dw` and `airflow_db`.

### 2. Initialise the warehouse schema

```bash
docker exec -i de_postgres psql -U de_user -d nutrition_dw < scripts/sql/create_schema.sql
```

### 3. Python environment (for scripts run locally)

```bash
python -m venv .venv
source .venv/bin/activate    # Windows WSL: same command
pip install -r requirements.txt
```

### 4. Copy and review environment config

```bash
cp .env.example .env
```

Default values match Docker Compose — no changes needed for local development.

---

## Running the Batch Pipeline (Manual)

Run steps in order via the Spark master container:

```bash
# Step 1: raw → bronze
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/raw_to_bronze.py

# Step 2: bronze → silver
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/bronze_to_silver.py

# Step 3: silver → gold
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/silver_to_gold_recommendations.py

# Step 4: gold → postgres
python scripts/ingestion/load_gold_to_postgres.py
```

Each Spark job logs data quality check results inline. A `FAIL` on any critical check raises an exception and stops the job.

### Step 5 — Archive Parquet layers to MinIO (optional)

Run after Step 4. Uploads all bronze, silver, and gold Parquet files to the `nutrition-platform` bucket in MinIO for object storage demo:

```bash
python scripts/archival/archive_to_minio.py
```

Browse the uploaded files at `http://localhost:9001` (login: `minio` / `minio123`) → Buckets → `nutrition-platform`.

---

## MinIO Object Storage

MinIO provides S3-compatible object storage for archiving the processed Parquet layers. It is included to demonstrate a realistic production pattern where intermediate and final datasets are stored durably in object storage — separate from the compute layer.

### What gets archived

| Layer | Path in MinIO |
|---|---|
| Bronze | `bronze/food_nutrition/*.parquet` |
| Silver | `silver/food_nutrition_clean/*.parquet` |
| Gold | `gold/food_recommendations/*.parquet` |
| Gold | `gold/user_profiles_enriched/*.parquet` |

### Running the archive script

```bash
# Ensure .env is loaded (MINIO_ENDPOINT, MINIO_ACCESS_KEY, etc.)
python scripts/archival/archive_to_minio.py
```

The script:
1. Connects to MinIO at `http://localhost:9000`
2. Creates the `nutrition-platform` bucket if it does not exist
3. Recursively finds all `.parquet` files in `data/bronze`, `data/silver`, `data/gold`
4. Uploads each file preserving the relative path as the S3 key

### Browsing MinIO

| URL | Credentials |
|---|---|
| `http://localhost:9001` | `minio` / `minio123` |

Go to **Buckets → nutrition-platform** to browse and download the archived Parquet files.

---

## Running Airflow

Airflow orchestrates the same four batch steps on a daily schedule (midnight UTC).

### One-time setup (if postgres_data volume already existed before Airflow was added)

```bash
docker exec de_postgres psql -U de_user -c "CREATE DATABASE airflow_db;"
```

Skip this if you started fresh — the init script already created `airflow_db`.

### Build and initialise

```bash
# Build the custom Airflow image (Java + PySpark — takes ~5 min on first build)
docker compose build airflow-init

# Initialise the Airflow metadata DB and create the admin user
docker compose up airflow-init
# Wait for it to exit cleanly (exit code 0), then:

# Start the webserver and scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

### Access the Airflow UI

```
http://localhost:8082
Username: admin
Password: admin
```

The `nutrition_batch_pipeline` DAG appears automatically. Toggle it on to enable scheduling. Click **Trigger DAG** to run it manually.

### Trigger a manual run from the CLI

```bash
docker exec de_airflow_scheduler airflow dags trigger nutrition_batch_pipeline
```

### Why Airflow with static data?

The DAG demonstrates that you understand dependency-ordered orchestration, retry logic, and observability — skills that apply directly to any real pipeline. When the source changes to a live feed, only the schedule and source path need updating. The architecture remains identical.

---

## Streaming API — Running the FastAPI App

```bash
uvicorn app.main:app --reload
```

Available at `http://localhost:8000`. The Kafka consumer starts automatically as a background thread.

---

## API Reference

### GET /health

```
200 OK  {"status": "ok"}
```

### GET /recommendations/{user_id}

Returns top 10 personalised food recommendations.

```json
{
  "user_id": "U001",
  "recommendations": [
    {
      "food_name": "chicken_breast",
      "goal_type": "weight_loss",
      "recommendation_score": 42.15,
      "recommendation_rank": 1,
      "recommendation_reason": "Higher protein/fiber with relatively lower calories and sugar"
    }
  ]
}
```

Returns `404` if no recommendations exist for the user.

### POST /users — Create user

```json
{
  "user_name": "Alice Smith",
  "gender": "female",
  "birth_date": "1990-05-15",
  "height_cm": 165,
  "current_weight_lb": 160,
  "target_weight_lb": 140
}
```

- `gender`: one of `male`, `female`, `other`, `prefer_not_to_say`
- `birth_date`: ISO format `YYYY-MM-DD` — `age` is derived automatically
- `user_id` is auto-generated (`U001`, `U002`, …)

Returns `201` with the generated `user_id`.

### GET /users/{user_id} — Get current profile

Returns the latest (active) version of the user's profile.

```json
{
  "user_id": "U001",
  "user_name": "James Carter",
  "gender": "male",
  "birth_date": "1988-04-12",
  "age": 37,
  "height_cm": 175.0,
  "current_weight_lb": 210.0,
  "target_weight_lb": 185.0,
  "goal_type": "weight_loss",
  "effective_start": "2026-04-01T10:00:00",
  "version_number": 1
}
```

Returns `404` if the user does not exist.

### GET /users/{user_id}/history — Get full version history

Returns all SCD Type 2 versions in chronological order. Use this endpoint to reconstruct the full timeline of a user's profile changes.

```json
{
  "user_id": "U001",
  "total_versions": 2,
  "history": [
    {
      "version_number": 1,
      "current_weight_lb": 210.0,
      "target_weight_lb": 185.0,
      "goal_type": "weight_loss",
      "effective_start": "2026-04-01T10:00:00",
      "effective_end": "2026-04-01T11:30:00",
      "is_current": false
    },
    {
      "version_number": 2,
      "current_weight_lb": 205.0,
      "target_weight_lb": 185.0,
      "goal_type": "weight_loss",
      "effective_start": "2026-04-01T11:30:00",
      "effective_end": null,
      "is_current": true
    }
  ]
}
```

Returns `404` if the user does not exist.

### PUT /users/{user_id} — Update user

Send only the fields you want to change. All other fields are merged from the existing record.

```json
{
  "target_weight_lb": 135,
  "gender": "female"
}
```

Publishes an `update` Kafka event. Consumer **expires the current row** (stamps `effective_end`, sets `is_current = FALSE`) and **inserts a new version** with an incremented `version_number`. Recommendations are regenerated from the new profile state.

### DELETE /users/{user_id} — Delete user

No body required.

Publishes a `delete` Kafka event. Consumer hard-deletes **all SCD versions** for this user from `dim_user_profile`, plus all rows from `fact_food_recommendation` and `stg_user_profile_event`. No history is retained after deletion.

---

## Testing Create / Update / Delete / History

```bash
BASE=http://localhost:8000

# Create — note the returned user_id (e.g. U004)
curl -s -X POST $BASE/users \
  -H "Content-Type: application/json" \
  -d '{"user_name":"Bob Jones","gender":"male","birth_date":"1985-03-20","height_cm":180,"current_weight_lb":200,"target_weight_lb":175}' \
  | python3 -m json.tool

# Fetch current profile (version 1)
curl -s $BASE/users/U004 | python3 -m json.tool

# Fetch recommendations (version 1 goal_type)
curl -s $BASE/recommendations/U004 | python3 -m json.tool

# Update — change weight, triggering SCD2 version 2
curl -s -X PUT $BASE/users/U004 \
  -H "Content-Type: application/json" \
  -d '{"current_weight_lb": 190}' \
  | python3 -m json.tool

# Verify recommendations were regenerated with the updated profile
curl -s $BASE/recommendations/U004 | python3 -m json.tool

# View the full version history — both versions should appear
curl -s $BASE/users/U004/history | python3 -m json.tool
# version 1: effective_end is set, is_current = false
# version 2: effective_end is null, is_current = true

# Delete — hard-removes all versions and recommendations
curl -s -X DELETE $BASE/users/U004 | python3 -m json.tool

# Confirm gone (should return 404)
curl -s $BASE/users/U004
```

---

## Data Quality Checks

Every pipeline stage runs checks using `spark_jobs/data_quality/quality_checks.py`.

Each `check_*()` function returns a plain dict:

```python
{"check": "row_count", "status": "PASS", "detail": "silver count=250 min_required=1"}
```

`run_checks(checks, stage=...)` collects results, logs a summary, and raises `ValueError` on any `FAIL` — stopping the pipeline at the bad stage rather than writing invalid data downstream.

| Stage | Checks |
|---|---|
| **raw** | file exists, row count > 0, expected columns present |
| **bronze** | row count, food_name not null |
| **silver (incoming)** | numeric column types, no fully-null rows |
| **silver (output)** | food_name not null, calories > 0, nutrients ≥ 0, band categories valid |
| **gold** | score not null, rank in 1–10, ranks unique per user, foods unique per user |
| **warehouse** | row counts match load, dim_user unique by user_id, FK integrity |

To run checks in log-and-continue mode (useful for debugging), call:

```python
run_checks(checks, stage="my_stage", fail_on_error=False)
```

---

## User Schema

**Business fields**

| Field | Type | Notes |
|---|---|---|
| `user_id` | VARCHAR(50) | Auto-generated: U001, U002, … |
| `user_name` | VARCHAR(100) | Full display name |
| `gender` | VARCHAR(30) | `male` \| `female` \| `other` \| `prefer_not_to_say` |
| `birth_date` | DATE | ISO date; stored as-is |
| `age` | INTEGER | Derived from `birth_date` at write time; recalculated on each version |
| `height_cm` | NUMERIC | > 0, ≤ 300 |
| `current_weight_lb` | NUMERIC | > 0, ≤ 800 |
| `target_weight_lb` | NUMERIC | > 0, ≤ 800 |
| `goal_type` | VARCHAR(50) | Derived: `weight_loss` \| `weight_gain` \| `maintenance` |

**SCD Type 2 tracking fields** (managed automatically — do not set manually)

| Field | Type | Notes |
|---|---|---|
| `user_profile_key` | SERIAL | Surrogate primary key — unique per row/version |
| `effective_start` | TIMESTAMP | When this version became active |
| `effective_end` | TIMESTAMP | When this version was superseded (`NULL` = currently active) |
| `is_current` | BOOLEAN | `TRUE` for exactly one row per `user_id` at any time |
| `version_number` | INTEGER | Monotonically increasing: 1, 2, 3, … |

---

## BI / Dashboard Connectivity

A flat reporting view is pre-built in PostgreSQL:

```sql
SELECT * FROM vw_recommendation_report;
```

It joins `dim_user_profile + fact_food_recommendation + dim_food` into one denormalised row per recommendation — ready for any BI tool.

### Connect Power BI

1. Open Power BI Desktop → **Get Data** → **PostgreSQL database**
2. Server: `localhost`, Database: `nutrition_dw`
3. Username: `de_user`, Password: `de_pass`
4. Select `vw_recommendation_report` (or individual tables)
5. Build visuals: recommendation rank distribution, top foods by goal type, user demographics, etc.

### Connect Tableau

1. **Connect** → **PostgreSQL**
2. Server: `localhost:5432`, Database: `nutrition_dw`
3. Username: `de_user`, Password: `de_pass`
4. Drag `vw_recommendation_report` to the canvas

### Useful analytics fields in the view

| Field | Use |
|---|---|
| `user_id`, `user_name`, `gender`, `age` | User segmentation |
| `goal_type` | Filter / group by goal |
| `food_name`, `recommendation_rank` | Top foods bar chart |
| `recommendation_score` | Score distribution |
| `calories_kcal`, `protein_g`, `fiber_g` | Nutritional comparison |
| `calorie_band`, `protein_band` | Band-based filtering |
| `is_high_fiber`, `is_high_protein_low_calorie` | Flag-based KPIs |

---

## Recommendation Scoring

Goal type is derived from submitted weights:

| Condition | goal_type |
|---|---|
| target < current | weight_loss |
| target > current | weight_gain |
| target = current | maintenance |

Scoring formulas:

**Weight loss:** `protein_g × 2.5 + fiber_g × 2.0 − calories_kcal × 0.015 − sugar_g × 0.8 − fat_g × 0.2`

**Weight gain:** `calories_kcal × 0.02 + protein_g × 2.0 + carbohydrates_g × 0.8 + fat_g × 0.5`

**Maintenance:** `protein_g × 2.0 + fiber_g × 1.5 − sugar_g × 0.7 − |calories_kcal − 400| × 0.01`

Top 10 foods per user are kept after deduplication by food name.

---

## SCD Type 2 — User Progress Tracking

Every `PUT /users/{id}` call creates a new **version** of the user's profile rather than overwriting the previous one. This is the [SCD Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) pattern — the full history of every profile change is retained in `dim_user_profile`, making it possible to plot a user's weight journey over time in Tableau.

### How it works

1. **Create** — a `POST /users` call inserts version 1 with `effective_end = NULL`, `is_current = TRUE`.
2. **Update** — a `PUT /users/{id}` call does two things in a single transaction:
   - Stamps `effective_end = NOW()` and `is_current = FALSE` on the current row (expires it).
   - Inserts a new row with the updated fields, `version_number` incremented, and `is_current = TRUE`.
3. **Delete** — hard-deletes all SCD versions for the user, plus all recommendations and staging events.

A partial unique index (`WHERE is_current = TRUE`) enforces that exactly one active row exists per `user_id` at all times — violated by any bug that would create two concurrent active versions.

### Querying the history

```sql
-- Current state only
SELECT * FROM dim_user_profile WHERE user_id = 'U001' AND is_current = TRUE;

-- Full version timeline
SELECT user_id, version_number, current_weight_lb, effective_start, effective_end, is_current
FROM dim_user_profile
WHERE user_id = 'U001'
ORDER BY version_number;

-- State at a specific point in time
SELECT * FROM dim_user_profile
WHERE user_id = 'U001'
  AND effective_start <= '2026-04-01 12:00:00'
  AND (effective_end > '2026-04-01 12:00:00' OR effective_end IS NULL);
```

### Schema migration (re-run after pulling this change)

If you have an existing `postgres_data` volume from before SCD2 was added, re-initialise the schema:

```bash
docker exec -i de_postgres psql -U de_user -d nutrition_dw < scripts/sql/create_schema.sql
```

Then re-run the full batch pipeline to reload `dim_user_profile` with the SCD2 columns:

```bash
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/raw_to_bronze.py

docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/bronze_to_silver.py

docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/silver_to_gold_recommendations.py

python scripts/ingestion/load_gold_to_postgres.py
```

### Tableau — progress tracking view

Connect to `vw_user_progress_history` for weight-over-time charts:

| Field | Tableau role |
|---|---|
| `changed_at` | X-axis (continuous date) |
| `current_weight_lb` | Primary measure |
| `target_weight_lb` | Reference line |
| `weight_delta_lb` | Gap to goal (positive = over target) |
| `progress_status` | Colour dimension (`initial` / `improving` / `not_improving` / `unchanged`) |
| `user_id`, `user_name` | Filter / detail |
| `is_current` | Optional filter to show only the latest snapshot |

Suggested chart: **dual-line chart** with `current_weight_lb` and `target_weight_lb` on the Y-axis, `changed_at` on X, coloured by `progress_status`. Each point represents one profile version; hover details show `version_number` and `goal_type`.

---

## Database Schema

**nutrition_dw**

| Table / View | Description |
|---|---|
| `stg_user_profile_event` | Append-only event log (create / update / delete) |
| `dim_user_profile` | SCD Type 2 user dimension — one row per version; current state filtered by `is_current = TRUE` |
| `dim_food` | Cleaned and enriched food catalogue from silver layer |
| `fact_food_recommendation` | Scored top-10 recommendations per user (current state only) |
| `vw_recommendation_report` | Flat BI view joining all layers; filters `is_current = TRUE` |
| `vw_user_goal_summary` | Current-state user overview for demographic/goal charts |
| `vw_food_nutrition_profile` | One row per food for nutritional comparison charts |
| `vw_user_progress_history` | Full SCD2 timeline per user with `progress_status` — primary Tableau view |

---

## Infrastructure Ports

| Service | Port | Credentials |
|---|---|---|
| PostgreSQL | 5432 | de_user / de_pass |
| Kafka | 9092 | — |
| Zookeeper | 2181 | — |
| MinIO API | 9000 | — |
| MinIO Console | 9001 | minio / minio123 |
| Spark Master UI | 8080 | — |
| Spark Worker UI | 8081 | — |
| FastAPI | 8000 | — |
| Airflow Web UI | 8082 | admin / admin |

---

## Configuration Reference

All values read from environment / `.env`:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `nutrition_dw` | Warehouse database name |
| `POSTGRES_USER` | `de_user` | DB user |
| `POSTGRES_PASSWORD` | `de_pass` | DB password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC` | `user_profiles` | Topic for user events |
| `KAFKA_GROUP_ID` | `nutrition-consumer-group` | Consumer group |
| `LOG_LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |
| `DATA_BASE_PATH` | `data` | Base path for parquet layers |
