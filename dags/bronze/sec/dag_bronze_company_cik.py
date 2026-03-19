# /dags/bronze/sec/dag_bronze_company_cik.py

"""
This DAG is responsible for fetching every CIK from SEC
First step in the pipeline
"""

from datetime import datetime, timedelta
import json
import requests
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.dag_defaults import default_args, sla_miss_callback

import boto3


logger = logging.getLogger(__name__)


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="monthly_sec_company_cik",
    description="Fetches the CIK for every company in SEC EDGAR database once per month",
    schedule="0 0 1 * *",  # Monthly on the 1st at midnight
    start_date=datetime(2026, 3, 18),
    catchup=False,
    default_args=default_args,
    tags=["companies", "stocks", "monthly", "sec", "cik"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ============================================================
    # TASK FUNCTIONS
    # Core logic lives here — plain Python, no Airflow imports needed
    # ============================================================
    def fetch_and_store_sec_company_tickers(**context):
        """Fetches CIK data from SEC and stores raw JSON to S3 bronze layer"""

        # Fetch
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {
            "User-Agent": Variable.get("SEC_USER_AGENT", default_var=None)  # SEC requires this
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data)} company tickers from SEC")
        # Store
        s3 = boto3.client('s3')
        now = datetime.now()
        s3_key = f"bronze/sec/company_tickers/year={now.year}/month={now.month:02d}/company_tickers.json"
        s3.put_object(
            Bucket="stock-pipeline-365642143593",
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        logger.info(f"Stored to s3://{s3_key}")

    # ============================================================
    # TASKS
    # ============================================================
    task_get_and_store_cik = PythonOperator(
        task_id="get_and_store_cik",
        python_callable=fetch_and_store_sec_company_tickers,
    )