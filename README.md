# Nutrition Recommendation Data Platform

A local data engineering platform demonstrating both batch pipeline processing and event-driven streaming using a medallion architecture. The system ingests a nutrition dataset, processes it through Raw → Bronze → Silver → Gold layers using Apache Spark, stores curated data in PostgreSQL, and serves personalised food recommendations via a FastAPI service.

New users can be submitted at runtime via the API. The event is published to Kafka and consumed by a streaming consumer that generates fresh recommendations without rerunning the batch pipeline.

---

## Architecture

### Batch Pipeline

```
nutrition.csv (raw)
       │
       ▼
 [Spark] raw_to_bronze.py
       │  - normalise column names
       │  - cast numeric types
       │  - drop duplicates
       │
       ▼
 data/bronze/food_nutrition  (Parquet)
       │
       ▼
 [Spark] bronze_to_silver.py
       │  - rename columns to business names
       │  - derive calorie / protein / carb / fat bands
       │  - derive recommendation flags
       │
       ▼
 data/silver/food_nutrition_clean  (Parquet)
       │
       ▼
 [Spark] silver_to_gold_recommendations.py
       │  - cross-join users × foods
       │  - score per goal_type (weight_loss / weight_gain / maintenance)
       │  - deduplicate, rank top 10 per user
       │
       ▼
 data/gold/food_recommendations  (Parquet)
 data/gold/user_profiles_enriched  (Parquet)
       │
       ▼
 [Python] load_gold_to_postgres.py
       │  - truncate warehouse tables
       │  - load dim_food, dim_user_profile, fact_food_recommendation
       │
       ▼
 PostgreSQL — nutrition_dw
```

### Streaming Pipeline (event-driven user onboarding)

```
POST /users
       │
       ▼
 stg_user_profile_event  (staging table)
       │
       ▼
 Kafka topic: user_profiles
       │
       ▼
 consumer.py
       │  - derive goal_type
       │  - upsert dim_user_profile
       │  - score foods from dim_food
       │  - delete + insert fact_food_recommendation
       │
       ▼
 GET /recommendations/{user_id}  →  returns fresh results
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Orchestration | Docker Compose |
| Batch processing | Apache Spark 3.5 (PySpark) |
| Stream processing | Apache Kafka + Python consumer |
| Serving database | PostgreSQL 16 |
| API | FastAPI + SQLAlchemy |
| Object storage | MinIO (provisioned, available for future use) |
| Runtime | Python 3.12, WSL2 |

---

## Directory Structure

```
nutrition-data-platform/
├── app/
│   ├── config.py           # Central config — reads from environment / .env
│   ├── logger.py           # Shared structured logger
│   ├── main.py             # FastAPI application
│   └── consumer.py         # Kafka consumer
├── spark_jobs/
│   └── batch/
│       ├── raw_to_bronze.py
│       ├── bronze_to_silver.py
│       └── silver_to_gold_recommendations.py
├── scripts/
│   ├── ingestion/
│   │   └── load_gold_to_postgres.py
│   └── sql/
│       └── create_schema.sql
├── data/
│   ├── raw/                # Source CSV files
│   ├── bronze/             # Parquet — technical cleansing
│   ├── silver/             # Parquet — business transformation
│   ├── gold/               # Parquet — scored recommendations
│   └── input/              # Static user profiles for batch runs
├── docker-compose.yml
├── requirements.txt
├── .env                    # Local config (gitignored)
└── .env.example            # Config template
```

---

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ with a virtual environment
- WSL2 (if running on Windows)

---

## Setup

### 1. Start the Docker stack

```bash
docker compose up -d
```

This starts PostgreSQL, Kafka, Zookeeper, MinIO, Spark master, and Spark worker.

Verify all containers are running:

```bash
docker compose ps
```

### 2. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment

Copy the example env file and adjust if needed:

```bash
cp .env.example .env
```

Default values match the Docker Compose configuration and require no changes for local development.

### 4. Initialise the database schema

```bash
docker exec -i de_postgres psql -U de_user -d nutrition_dw < scripts/sql/create_schema.sql
```

---

## Running the Batch Pipeline

Each Spark job must be submitted to the Spark master container. Run them in order.

### Step 1 — Raw to Bronze

```bash
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/raw_to_bronze.py
```

### Step 2 — Bronze to Silver

```bash
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/bronze_to_silver.py
```

### Step 3 — Silver to Gold

```bash
docker exec de_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/project/spark_jobs/batch/silver_to_gold_recommendations.py
```

### Step 4 — Load to PostgreSQL

Run from the project root (not inside Docker):

```bash
python scripts/ingestion/load_gold_to_postgres.py
```

---

## Running the Streaming Components

### Start the API

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`.

### Start the Kafka Consumer

In a separate terminal:

```bash
python -m app.consumer
```

The consumer will block and listen for incoming user profile events.

---

## API Reference

### GET /health

Returns service status.

```
200 OK
{"status": "ok"}
```

### GET /recommendations/{user_id}

Returns top 10 food recommendations for a user.

```
200 OK
{
  "user_id": "U001",
  "recommendations": [
    {
      "food_name": "chicken_breast",
      "goal_type": "weight_loss",
      "recommendation_score": 42.15,
      "recommendation_rank": 1,
      "recommendation_reason": "Higher protein/fiber with relatively lower calories and sugar"
    },
    ...
  ]
}
```

Returns `404` if the user has no recommendations.

### POST /users

Submit a new user profile. Triggers Kafka event and recommendation generation.

**Request body:**

```json
{
  "user_id": "U020",
  "height_cm": 175,
  "current_weight_lb": 210,
  "target_weight_lb": 185
}
```

Field constraints:
- `user_id`: 1–50 characters
- `height_cm`: > 0, ≤ 300
- `current_weight_lb`: > 0, ≤ 800
- `target_weight_lb`: > 0, ≤ 800

```
200 OK
{
  "message": "User profile received",
  "user_profile": { ... },
  "kafka_topic": "user_profiles"
}
```

---

## Recommendation Scoring

Goal type is derived from the submitted weights:

| Condition | Goal Type |
|---|---|
| target < current | weight_loss |
| target > current | weight_gain |
| target == current | maintenance |

Scoring formula per goal type:

**Weight loss**
```
score = protein_g × 2.5 + fiber_g × 2.0 − calories_kcal × 0.015 − sugar_g × 0.8 − fat_g × 0.2
```

**Weight gain**
```
score = calories_kcal × 0.02 + protein_g × 2.0 + carbohydrates_g × 0.8 + fat_g × 0.5
```

**Maintenance**
```
score = protein_g × 2.0 + fiber_g × 1.5 − sugar_g × 0.7 − |calories_kcal − 400| × 0.01
```

Top 10 foods per user are kept after deduplication by food name.

---

## Database Schema

**nutrition_dw**

| Table | Description |
|---|---|
| `stg_user_profile_event` | Raw event log from POST /users |
| `dim_user_profile` | Current user dimension, upserted on each event |
| `dim_food` | Cleaned and enriched food nutrition data |
| `fact_food_recommendation` | Scored and ranked recommendations per user |

---

## Configuration Reference

All configuration is read from environment variables. A `.env` file in the project root is loaded automatically by the application.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `nutrition_dw` | Database name |
| `POSTGRES_USER` | `de_user` | Database user |
| `POSTGRES_PASSWORD` | `de_pass` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `user_profiles` | Topic for user profile events |
| `KAFKA_GROUP_ID` | `nutrition-consumer-group` | Consumer group ID |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `DATA_BASE_PATH` | `data` | Base path for parquet layers (used by load script) |

---

## Infrastructure Ports

| Service | Port |
|---|---|
| PostgreSQL | 5432 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| MinIO API | 9000 |
| MinIO Console | 9001 |
| Spark Master UI | 8080 |
| Spark Worker UI | 8081 |
| FastAPI | 8000 |
