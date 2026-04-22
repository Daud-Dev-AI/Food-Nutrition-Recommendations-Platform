"""
Nutrition Platform — Airflow ETL DAG
=====================================
Chains the 4 pipeline steps in order:
  1. Glue: raw → bronze
  2. Glue: bronze → silver
  3. Glue: silver → gold
  4. Lambda: load gold → PostgreSQL

Schedule: manual (schedule=None) — trigger via UI or CLI.
AWS credentials are read from ~/.aws/credentials (aws_default Airflow connection).

Setup:
  pip install apache-airflow apache-airflow-providers-amazon
  export AIRFLOW_HOME=~/airflow
  airflow db init
  airflow standalone          # starts webserver + scheduler on localhost:8080
  # Copy or symlink this file to ~/airflow/dags/
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

AWS_CONN_ID  = "aws_default"   # uses ~/.aws/credentials automatically
AWS_REGION   = "us-east-1"

default_args = {
    "owner":        "daud",
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="daud_nutrition_pipeline",
    description="Nutrition ETL: raw → bronze → silver → gold → PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,    # manual trigger only
    catchup=False,
    tags=["nutrition", "etl", "glue", "lambda"],
) as dag:

    raw_to_bronze = GlueJobOperator(
        task_id="raw_to_bronze",
        job_name="daud_nutrition_raw_to_bronze",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=False,  # logs:FilterLogEvents not granted; poll state only
    )

    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name="daud_nutrition_bronze_to_silver",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=False,  # logs:FilterLogEvents not granted; poll state only
    )

    silver_to_gold = GlueJobOperator(
        task_id="silver_to_gold",
        job_name="daud_nutrition_silver_to_gold",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=False,  # logs:FilterLogEvents not granted; poll state only
    )

    load_to_postgres = LambdaInvokeFunctionOperator(
        task_id="load_gold_to_postgres",
        function_name="daud_nutrition_load_gold_to_postgres",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        payload=json.dumps({}),
        qualifier="$LATEST",
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> load_to_postgres
