# Nutrition Data Platform — AWS Data Engineering Portfolio

A full end-to-end data engineering project built on AWS, demonstrating real-time data ingestion, a medallion-architecture ETL pipeline, a REST API, and a live frontend — all deployed to production AWS services.

**Live demo:** https://d3hw515tgtfmn6.cloudfront.net

---

## Architecture Overview

```
[Browser/Frontend]
  CloudFront (HTTPS CDN)          ← S3 static site hosting
       │
  API Gateway (HTTPS proxy)
       │
  FastAPI on EC2 (t3.micro)
       │
  ┌────┴────────────────────────────────────────┐
  │  EC2 Services                               │
  │  ├── PostgreSQL 15  (port 5432)             │
  │  ├── Kafka 3.7 KRaft (port 9092)            │
  │  └── FastAPI/uvicorn (port 8000)            │
  └────────────────────────────────────────────┘
       │
  [ETL Pipeline — triggered on demand]
  Glue Spark Job: raw → bronze
       → Glue Spark Job: bronze → silver
            → Glue Spark Job: silver → gold (S3)
                 → Lambda: load gold → PostgreSQL
  [Orchestrated by local Airflow DAG]
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Vanilla JS, Chart.js, S3 + CloudFront |
| API | FastAPI (Python), API Gateway HTTP API v2 |
| Compute | EC2 t3.micro (Amazon Linux 2023), Elastic IP |
| Streaming | Apache Kafka 3.7 (KRaft mode, self-hosted on EC2) |
| ETL | AWS Glue Spark (PySpark), Lambda (Python 3.12) |
| Orchestration | Apache Airflow (local, DAG triggers Glue + Lambda via boto3) |
| Data Warehouse | PostgreSQL 15 (on EC2) |
| Storage | S3 (data lake: raw/bronze/silver/gold) |
| Catalog | AWS Glue Data Catalog + Athena (Phase 7) |
| IaC / Config | AWS CLI, systemd, bootstrap script |

---

## Data Pipeline — Medallion Architecture

### S3 Zones

| Zone | Path | Description |
|------|------|-------------|
| Raw | `s3://daud-nutrition-platform-data/raw/` | Original CSV source files |
| Bronze | `.../bronze/food_nutrition/` | Parquet, schema enforced, no logic |
| Silver | `.../silver/food_nutrition_clean/` | Cleaned, validated, null-handled |
| Gold | `.../gold/food_recommendations/` | Scored recommendations per user |
| Gold | `.../gold/user_profiles_enriched/` | Users with derived fields (age, goal_type) |

### Glue ETL Jobs

1. **`daud_nutrition_raw_to_bronze`** — reads raw CSV, enforces schema, writes Parquet
2. **`daud_nutrition_bronze_to_silver`** — deduplication, null handling, column standardisation, data quality flags
3. **`daud_nutrition_silver_to_gold`** — joins user profiles with nutrition data, scores and ranks top-10 food recommendations per user per goal type
4. **Lambda `daud_nutrition_load_gold_to_postgres`** — reads gold Parquet via boto3, upserts into PostgreSQL warehouse

### SCD Type 2 — User Progress History

User profile updates create new versioned rows in `dim_user_profile` (never overwrite). Each version has `effective_start`, `effective_end`, `is_current`, and `version_number`. The `vw_user_progress_history` view adds `weight_delta_lb` and `progress_status` (improving / not_improving / unchanged / initial). This powers the Progress History tab in the frontend.

---

## Kafka Event Stream

User CRUD operations flow through Kafka before hitting PostgreSQL, demonstrating a real-time event-driven pattern:

```
POST /users → stg_user_profile_event (staging) → Kafka topic: user_profiles
                                                        ↓
                                              Kafka Consumer Thread
                                                        ↓
                                     dim_user_profile + fact_food_recommendation
```

Events: `create`, `update`, `delete`. Consumer runs as a daemon thread inside FastAPI.

---

## PostgreSQL Schema

```sql
dim_food                  -- 500+ foods with nutritional bands
dim_user_profile          -- SCD2 versioned user profiles
fact_food_recommendation  -- top-10 foods per user per goal type
stg_user_profile_event    -- Kafka event staging table
vw_user_progress_history  -- SCD2 history view with progress metrics
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/users` | Create user, publish Kafka event, return user_id |
| GET | `/users/{user_id}` | Fetch current user profile |
| PUT | `/users/{user_id}` | Update profile fields (Kafka → SCD2) |
| DELETE | `/users/{user_id}` | Delete user (Kafka event) |
| GET | `/users/{user_id}/history` | Full SCD2 version timeline |
| GET | `/recommendations/{user_id}` | Top-10 food recommendations |

Base URL: `https://1y8p8hsuyg.execute-api.us-east-1.amazonaws.com`

---

## Frontend Features

- **Create User tab** — form with real-time goal preview, submits to API, polls for recommendations, shows results alongside a toast notification with the assigned User ID in bold
- **Look Up User tab** — fetch profile + recommendations by User ID, inline profile edit, delete with confirmation
- **Progress History tab** — Chart.js weight journey line chart with colour-coded progress dots, full SCD2 version table

---

## User ID Assignment

User IDs follow a sequential format: `U001`, `U002`, `U003`, …

IDs are assigned using `MAX(existing numeric suffix) + 1` across both the live profile table and the event staging table. This means:
- Deleted users' IDs are **never reused**
- IDs are assigned in monotonically increasing order regardless of deletions
- The next new user always gets a fresh ID above all previously issued IDs

---

## Orchestration — Local Airflow

Because MWAA is blocked by company SCP policy, orchestration runs locally:

```bash
pip install apache-airflow apache-airflow-providers-amazon
export AIRFLOW_HOME=~/airflow
airflow db init
cp airflow/dags/daud_nutrition_dag.py ~/airflow/dags/
airflow standalone      # UI at http://localhost:8080 (user: admin)
```

The DAG (`daud_nutrition_pipeline`) triggers the 4 pipeline steps in sequence. Glue jobs run in AWS — Airflow just waits for completion via `wait_for_completion=True`. No VPC needed from local.

For live demo, expose with: `ngrok http 8080`

---

## AWS Resources (us-east-1, account 866934333672)

| Resource | ID / Endpoint |
|----------|---------------|
| EC2 | `i-01d4c65f6f3b80420`, EIP `44.213.0.246` |
| S3 (data lake) | `daud-nutrition-platform-data` |
| S3 (frontend) | `daud-nutrition-platform-frontend` |
| CloudFront | `E8FL5QHZZGTRJ` → `d3hw515tgtfmn6.cloudfront.net` |
| API Gateway | `1y8p8hsuyg.execute-api.us-east-1.amazonaws.com` |
| Lambda | `daud_nutrition_load_gold_to_postgres` |
| Glue Role | `daud_GlueNutritionRole` |
| EC2 Role | `daud_nutrition_ec2_role` |

---

## Repository Structure

```
nutrition-data-platform/
├── app/                        # FastAPI application
│   ├── main.py                 # Routes, Kafka producer, SCD2 logic
│   ├── consumer.py             # Kafka consumer daemon thread
│   ├── config.py               # Environment-based config
│   └── logger.py
├── glue_jobs/                  # PySpark ETL scripts
│   ├── glue_raw_to_bronze.py
│   ├── glue_bronze_to_silver.py
│   ├── glue_silver_to_gold.py
│   └── quality_checks.py
├── lambda/
│   └── lambda_load_gold_to_postgres.py
├── airflow/
│   └── dags/
│       └── daud_nutrition_dag.py
├── scripts/
│   ├── ingestion/
│   │   └── load_gold_to_postgres.py
│   └── ec2_bootstrap.sh
├── frontend/
│   ├── index.html
│   ├── style.css
│   └── app.js
└── data/                       # Local Spark output (not committed)
```
