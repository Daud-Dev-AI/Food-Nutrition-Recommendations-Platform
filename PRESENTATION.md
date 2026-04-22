# Presentation Guide — Nutrition Data Platform

Everything you need to demo the platform live, explain the architecture, and shut down resources afterwards.

---

## Pre-Demo Checklist (do 10 minutes before)

### 1. Start EC2 (if stopped)

```bash
aws.exe ec2 start-instances --instance-ids i-01d4c65f6f3b80420
```

Wait ~60 seconds for the instance to boot, then verify services are up:

```bash
# SSH in
ssh -i ~/.ssh/daud_nutrition_key.pem ec2-user@44.213.0.246

# Check all 3 services
sudo systemctl status kafka nutrition-api postgresql
```

All three should show `active (running)`. If `nutrition-api` is still waiting on Kafka (normal on cold boot — ExecStartPre waits up to ~90s), just wait.

### 2. Verify the API is reachable

```bash
curl https://1y8p8hsuyg.execute-api.us-east-1.amazonaws.com/health
# Expected: {"status":"ok"}
```

### 3. Open the frontend

Navigate to: **https://d3hw515tgtfmn6.cloudfront.net**

The top-right badge should show **API Online** within a few seconds.

### 4. Start Airflow (for pipeline orchestration demo)

Open a WSL2 terminal and run:

```bash
export AIRFLOW_HOME=~/airflow
nohup bash -c 'export AIRFLOW_HOME=~/airflow; export PATH=~/airflow-env/bin:$PATH; airflow standalone' \
  > ~/airflow/standalone.log 2>&1 &
```

Wait ~15 seconds, then open **http://localhost:8080** in your browser.

- Username: `admin`
- Password: `kYQCySHSsFZXTvyS`

The `daud_nutrition_pipeline` DAG should already be there and unpaused. If it shows as paused, run:

```bash
export AIRFLOW_HOME=~/airflow && export PATH=~/airflow-env/bin:$PATH
airflow dags unpause -y daud_nutrition_pipeline
```

If the DAG is missing from the list, copy it in:

```bash
cp ~/projects/nutrition-data-platform/airflow/dags/daud_nutrition_dag.py ~/airflow/dags/
```

> **Note:** Airflow only needs to run locally during the demo. It triggers Glue jobs and Lambda in AWS via boto3 — no VPC or remote server required.

---

## Demo Flow (~15 minutes)

### Act 1 — Architecture Walkthrough (3 min)

Talk through the architecture diagram (whiteboard or READMEV2.md):

> "Data starts as raw CSVs in S3. Three Glue Spark jobs process it through the medallion architecture — raw, bronze, silver, gold — applying schema enforcement, cleaning, and scoring. A Lambda function then loads the gold zone into a PostgreSQL data warehouse running on EC2. All of this is orchestrated by Airflow."

> "The frontend is a static site on S3, served via CloudFront over HTTPS. The API is FastAPI running on EC2, exposed through API Gateway so we have HTTPS end-to-end without managing SSL certificates."

> "User actions — create, update, delete — are streamed through Kafka before hitting the database, demonstrating an event-driven ingestion pattern."

---

### Act 2 — Live User Creation (4 min)

1. Go to the **Create User** tab
2. Fill in details (use your own or a made-up profile):
   - Name: e.g. "Sarah Ahmed"
   - Gender, DOB, Height, Current/Target weight
3. Point out the **goal preview** updating live as you type weights
4. Click **Get My Recommendations**
5. Wait 2–3 seconds — point out: *"The frontend is polling the API while the Kafka consumer processes the event and writes recommendations to the database"*
6. Show the **toast notification** that appears with the User ID in bold
7. Show the **top 10 recommendations** rendered in the results panel

> "Every user gets personalised food recommendations based on their goal type — weight loss, weight gain, or maintenance — scored using nutritional bands computed during the Glue silver-to-gold job."

**Write down the User ID** (e.g. U005) — you'll use it in the next steps.

---

### Act 3 — Look Up & Edit (3 min)

1. Switch to the **Look Up User** tab
2. Start typing a name (e.g. "James") or User ID in the search box — show the **live dropdown** filtering results by name and ID simultaneously
3. Click a user from the dropdown — it auto-fills and triggers the lookup instantly
4. Show the profile card: stats grid, goal badge, recommendations
4. Click **Edit Profile** → change the current weight by a few pounds
5. Click **Save Changes** — point out: *"this publishes an update event to Kafka"*
6. After 2 seconds the profile auto-refreshes — show the updated weight
7. Optionally demonstrate **Delete** and confirm the confirmation dialog

---

### Act 4 — Progress History / SCD Type 2 (3 min)

1. Switch to the **Progress History** tab
2. Enter a User ID that has multiple versions (e.g. `U001` — batch-loaded user who has been updated)
3. Show the **Chart.js weight journey** line chart:
   - Blue line = actual weight over versions
   - Orange dashed = target weight
   - Coloured dots = progress status (green = improving, red = not improving)
4. Show the **Version History table** below the chart

> "This demonstrates SCD Type 2 — Slowly Changing Dimension. Every profile update creates a new versioned row rather than overwriting. We preserve the full history of a user's weight journey, and the API surfaces that as a timeline."

---

### Act 5 — Airflow Orchestration (3 min)

Open **http://localhost:8080** (already running from the pre-demo checklist).

1. Show the `daud_nutrition_pipeline` DAG in the DAG list
2. Click into it — show the **Graph view** with 4 tasks linked in sequence:
   ```
   raw_to_bronze → bronze_to_silver → silver_to_gold → load_gold_to_postgres
   ```
3. Click **Trigger DAG** (the play button, top right)
4. Watch tasks turn from white → yellow (running) → green (success) one by one
5. Click any completed task → **Logs** to show the Glue job run ID in the output

> "Airflow orchestrates the full ETL pipeline. Each step is a separate AWS Glue Spark job — Airflow triggers it via boto3 and waits for completion before moving to the next. The final step is a Lambda function that loads the processed data from S3 into PostgreSQL. This would normally run on Amazon MWAA, but the company account has that service blocked by policy — so we run it locally instead, which is identical from AWS's perspective since Airflow just calls the same APIs."

**Expected runtime:** ~4 minutes total (Glue spins up a cluster for each job).

**If asked why it's local and not on MWAA:** Company SCP (Service Control Policy) restricts managed orchestration services. The architecture is identical — only the Airflow runtime location differs. In a production account this would be a one-line config change.

> **Key talking point:** "Notice that the Glue jobs and Lambda are running entirely in AWS — my laptop is just the control plane. The actual compute happens on AWS-managed Spark clusters."

---

### Act 6 — (Bonus) Glue Data Catalog (1 min)

Open the **AWS Console → Glue → Databases → daud_nutrition_catalog → Tables**.

Show the 4 auto-discovered tables:

| Table | Zone | Rows |
|-------|------|------|
| `food_nutrition` | Bronze | 505 |
| `food_nutrition_clean` | Silver | 505 |
| `food_recommendations` | Gold | 40 |
| `user_profiles_enriched` | Gold | 4 |

Click any table → **Schema** to show the inferred column types.

> "The Glue Crawler automatically scanned all S3 zones and inferred the schema from the Parquet files — no manual DDL. These tables are now available as a queryable catalog. In a full production account this would feed directly into Athena for ad-hoc SQL analytics — that service is restricted here by company policy, but the catalog itself is fully built."

---

## Audience Participation

If audience members want to create their own profiles during the demo:

1. Send them to: **https://d3hw515tgtfmn6.cloudfront.net**
2. They fill in the Create User form themselves
3. Tell them to note their User ID from the toast popup
4. They can look themselves up and see their recommendations immediately

No accounts needed — the form is public and all IDs are auto-assigned.

---

## Resource Cost Summary

| Resource | Cost when running | Cost when stopped |
|----------|-------------------|-------------------|
| EC2 t3.micro | ~$0.011/hr ($8/month) | $0 (but see EIP) |
| Elastic IP | Free while attached to running EC2 | **$0.005/hr ($3.60/month) if EC2 stopped but EIP kept** |
| S3 (data + frontend) | ~$0.023/GB/month (negligible) | Same |
| CloudFront | Free tier (first 1TB/month free) | Same |
| API Gateway | Free tier (first 1M calls/month free) | Same |
| Lambda | Free tier (first 1M invocations/month free) | $0 |
| Glue jobs | $0.44/DPU-hr — only when running | $0 |
| Airflow | Local machine only | $0 |

**Primary ongoing cost: EC2 (~$8/month) + EIP when EC2 is stopped (~$3.60/month)**

---

## Post-Demo Teardown

### Option A — Stop EC2 (cheapest active option, keeps data)

```bash
# Stop EC2 — no compute charge; EIP still billed at $0.005/hr
aws.exe ec2 stop-instances --instance-ids i-01d4c65f6f3b80420
```

The EBS root volume (8GB) persists — all PostgreSQL data, Kafka config, and the app are preserved. Start it again anytime with `start-instances`.

### Option B — Stop EC2 + release EIP (zero ongoing cost, but EIP is gone)

```bash
aws.exe ec2 stop-instances --instance-ids i-01d4c65f6f3b80420

# Wait for stop, then disassociate + release
aws.exe ec2 disassociate-address --association-id eipassoc-06bc13a7edbb85b6a
aws.exe ec2 release-address --allocation-id eipalloc-0fccea19a2b89a531
```

> WARNING: After releasing the EIP, you'll need to update app.js `API_BASE`, re-upload to S3, and invalidate CloudFront when you restart. You'll also get a new public IP every boot.

### Option C — Full teardown (portfolio done, remove everything)

```bash
# 1. Terminate EC2 (EBS DeleteOnTermination=false, so volume persists — delete separately if desired)
aws.exe ec2 terminate-instances --instance-ids i-01d4c65f6f3b80420

# 2. Delete EIP
aws.exe ec2 disassociate-address --association-id eipassoc-06bc13a7edbb85b6a
aws.exe ec2 release-address --allocation-id eipalloc-0fccea19a2b89a531

# 3. Delete Lambda
aws.exe lambda delete-function --function-name daud_nutrition_load_gold_to_postgres

# 4. Delete Glue jobs
aws.exe glue delete-job --job-name daud_nutrition_raw_to_bronze
aws.exe glue delete-job --job-name daud_nutrition_bronze_to_silver
aws.exe glue delete-job --job-name daud_nutrition_silver_to_gold

# 5. Delete CloudFront distribution (must disable first, then wait ~15 min, then delete)
aws.exe cloudfront update-distribution --id E8FL5QHZZGTRJ --distribution-config file://disable-cf.json
# (then after disabled)
aws.exe cloudfront delete-distribution --id E8FL5QHZZGTRJ --if-match <ETag>

# 6. Delete API Gateway
aws.exe apigatewayv2 delete-api --api-id 1y8p8hsuyg

# 7. Empty + delete S3 buckets
aws.exe s3 rb s3://daud-nutrition-platform-data --force
aws.exe s3 rb s3://daud-nutrition-platform-frontend --force
aws.exe s3 rb s3://daud-nutrition-platform-airflow --force
```

### Recommended for post-presentation (while keeping portfolio live)

```bash
# Just stop EC2 — keeps everything intact, costs ~$8/month to restart
aws.exe ec2 stop-instances --instance-ids i-01d4c65f6f3b80420
```

The frontend, CloudFront, and API Gateway have no charges at rest. Only EC2 + EIP matter.

---

## Airflow Day-to-Day Reference

### Start Airflow
```bash
export AIRFLOW_HOME=~/airflow
nohup bash -c 'export AIRFLOW_HOME=~/airflow; export PATH=~/airflow-env/bin:$PATH; airflow standalone' \
  > ~/airflow/standalone.log 2>&1 &
# Open: http://localhost:8080  |  user: admin  |  pass: kYQCySHSsFZXTvyS
```

### Trigger pipeline from CLI
```bash
export AIRFLOW_HOME=~/airflow && export PATH=~/airflow-env/bin:$PATH
airflow dags trigger daud_nutrition_pipeline
```

### Check task states
```bash
# Replace <RUN_ID> with the ID printed by the trigger command
airflow tasks states-for-dag-run daud_nutrition_pipeline <RUN_ID>
```

### If a task fails — read the log
```bash
cat ~/airflow/logs/dag_id=daud_nutrition_pipeline/run_id=<RUN_ID>/task_id=<TASK_NAME>/attempt=1.log
```

### Stop Airflow
```bash
kill $(pgrep -f airflow)
```

### Update the DAG after code changes
```bash
cp ~/projects/nutrition-data-platform/airflow/dags/daud_nutrition_dag.py ~/airflow/dags/
# Scheduler picks it up automatically within ~30 seconds
```

### Known gotcha — `verbose=True` causes AccessDeniedException
The `GlueJobOperator` with `verbose=True` tries to stream CloudWatch logs via `logs:FilterLogEvents`, which the company IAM blocks. The DAG is set to `verbose=False` — this skips log streaming but Glue jobs still run and complete normally in AWS.

---

## Quick Reference

| Thing | Value |
|-------|-------|
| Frontend URL | https://d3hw515tgtfmn6.cloudfront.net |
| API base | https://1y8p8hsuyg.execute-api.us-east-1.amazonaws.com |
| EC2 IP (EIP) | 44.213.0.246 |
| EC2 SSH key | ~/.ssh/daud_nutrition_key.pem |
| EC2 instance ID | i-01d4c65f6f3b80420 |
| PostgreSQL | host: 172.31.45.44 (private), db: nutrition_dw, user: de_user |
| Airflow UI | http://localhost:8080 (local WSL2 only) |
| Airflow login | user: admin / pass: kYQCySHSsFZXTvyS |
| Airflow home | ~/airflow |
| Airflow venv | ~/airflow-env |
| Kafka topic | user_profiles (port 9092, KRaft) |
| Glue Catalog DB | daud_nutrition_catalog |
