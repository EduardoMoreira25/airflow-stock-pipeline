# /dags/bronze/sec/dag_bronze_company_cik.py

"""
This DAG is responsible for fetching every CIK from SEC
First step in the pipeline
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import json
import requests
import logging

logger = logging.getLogger(__name__)

# ============================================================
# CALLBACKS
# Defined outside the DAG — they're utility functions, not tasks
# ============================================================
def on_failure_callback(context):
    """
    Called automatically when a task fails all retries.
    Logs a structured failure report — in production you'd
    send this to Slack or PagerDuty.
    """
    from airflow.models import Variable

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    logical_date = context["logical_date"]
    run_id = context["run_id"]
    exception = context.get("exception")
    try_number = context["ti"].try_number

    message = (
        f"\n{'='*60}\n"
        f"PIPELINE FAILURE ALERT\n"
        f"{'='*60}\n"
        f"DAG:          {dag_id}\n"
        f"Task:         {task_id}\n"
        f"Logical Date: {logical_date.date()}\n"
        f"Run ID:       {run_id}\n"
        f"Try Number:   {try_number}\n"
        f"Exception:    {exception}\n"
        f"{'='*60}"
    )

    logger.error(message)

    # Send email alert using Airflow's built-in email utility
    try:
        from airflow.utils.email import send_email
        alert_email = Variable.get("ALERT_EMAIL", default_var=None)

        if alert_email:
            send_email(
                to=alert_email,
                subject=f"[Airflow] FAILED: {dag_id}.{task_id} — {logical_date.date()}",
                html_content=f"<pre>{message}</pre>",
            )
            logger.info(f"Alert email sent to {alert_email}")
        else:
            logger.warning("ALERT_EMAIL variable not set — skipping email alert")

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        # Never let the callback itself crash — that causes confusing errors


def on_retry_callback(context):
    """Called on each retry attempt — useful for logging retry patterns."""
    task_id = context["task"].task_id
    try_number = context["ti"].try_number
    exception = context.get("exception")
    logger.warning(
        f"RETRYING task '{task_id}' "
        f"(attempt {try_number}) — {exception}"
    )

# ============================================================
# DEFAULT ARGS
# ============================================================
default_args = {
    "owner": "miendes",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,   # We use our own callback instead
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback, # Specified here
    "on_retry_callback": on_retry_callback, # And here
}

# ============================================================
# SLA MISSED CALLBACK
# ============================================================
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.warning(
        f"SLA MISSED on {dag.dag_id}: "
        f"tasks {task_list} exceeded their SLA"
    )

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
        response = requests.get(url)
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

        