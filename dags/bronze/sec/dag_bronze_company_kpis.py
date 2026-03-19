# /dags/bronze/sec/dag_bronze_company_kpis.py

"""
This DAG is responsible for fetching every KPI available through the SEC API
"""
# Python imports
from datetime import datetime, timedelta
import json
import requests
import logging
# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.dag_defaults import default_args, sla_miss_callback
# S3 import
import boto3


logger = logging.getLogger(__name__)


# ============================================================
# Task Functions
# ============================================================

def stream_zip_to_s3(**context):
    """Streams zip from SEC directly to S3"""
    # URL where zip files lives
    url = Variable.get("SEC_COMPANY_HISTORICAL_KPIS_URL")
    headers = {"User-Agent": Variable.get("SEC_USER_AGENT")}
    s3 = boto3.client('s3')
    now = datetime.now()
    # Storing zip directly in s3
    s3_key = f"bronze/sec/company_facts/year={now.year}/month={now.month:02d}/companyfacts.zip"

    with requests.get(url, headers=headers, stream=True) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, "stock-pipeline-365642143593", s3_key)

    logger.info(f"Streamed zip to s3://{s3_key}")


def extract_zip_to_bronze(**context):
    """Downloads zip from S3 to disk, extracts each JSON back to S3"""
    import zipfile, os

    s3 = boto3.client('s3')
    now = datetime.utcnow()
    zip_key = f"bronze/sec/company_facts/year={now.year}/month={now.month:02d}/companyfacts.zip"
    tmp_zip = "/tmp/companyfacts.zip"

    s3.download_file("stock-pipeline-365642143593", zip_key, tmp_zip)

    with zipfile.ZipFile(tmp_zip) as zf:
        for filename in zf.namelist():
            with zf.open(filename) as f:
                s3_key = f"bronze/sec/company_facts/year={now.year}/month={now.month:02d}/extracted/{filename}"
                s3.upload_fileobj(f, "stock-pipeline-365642143593", s3_key)
                logger.info(f"Extracted {filename} to s3://{s3_key}")


def delete_tmp_zip(**context):
    """Deletes the temporary zip file from disk"""
    import os
    tmp_zip = "/tmp/companyfacts.zip"
    if os.path.exists(tmp_zip):
        os.remove(tmp_zip)
        logger.info(f"Deleted {tmp_zip}")
    else:
        logger.warning(f"{tmp_zip} not found — already deleted?")

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="monthly_sec_company_kpis",
    description="Fetches the KPIs for every company in SEC EDGAR database once per month",
    schedule="30 0 1,15 * *",  # Monthly on the 1st and 15th at 00:30 AM
    start_date=datetime(2026, 3, 18),
    catchup=False,
    default_args=default_args,
    tags=["companies", "stocks", "monthly", "sec", "kpis"],
    sla_miss_callback=sla_miss_callback,
) as dag:
    
        task_stream_zip = PythonOperator(
            task_id="stream_zip_to_s3",
            python_callable=stream_zip_to_s3,
        )

        task_extract_zip = PythonOperator(
            task_id="extract_zip_to_bronze",
            python_callable=extract_zip_to_bronze,
        )

        task_delete_tmp = PythonOperator(
            task_id="delete_tmp_zip",
            python_callable=delete_tmp_zip,
        )

        # ==============================================
        # Dependencies
        # ==============================================

        task_stream_zip >> task_extract_zip >> task_delete_tmp