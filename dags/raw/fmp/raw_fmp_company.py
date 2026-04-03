# dags/raw/fmp/raw_fmp_company.py

"""
Raw FMP Company DAG

Fetches company universe and profile data from FMP and backs them up as raw JSON.

Pipeline:
    fetch_and_store_stock_list → fetch_and_store_profiles

Backup layout:
    /opt/airflow/backups/fmp/raw/companies/list/{YYYY-MM-DD}.json
    /opt/airflow/backups/fmp/raw/companies/profile/symbol={SYMBOL}/{YYYY-MM-DD}.json
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = "/opt/airflow/backups/fmp/raw/companies"


# ============================================================
# TASK FUNCTIONS
# ============================================================

def fetch_and_store_stock_list(**context):
    """
    Fetch the full stock list from FMP and write it as a dated JSON file.
    Returns the list of symbols via XCom for the profile task.
    """
    import json
    import os
    from datetime import date

    from fmp.client import FMPClient  # type: ignore

    client = FMPClient()
    today = str(date.today())

    list_dir = os.path.join(BACKUP_BASE, "list")
    os.makedirs(list_dir, exist_ok=True)

    out_path = os.path.join(list_dir, f"{today}.json")

    if os.path.exists(out_path):
        logger.info(f"Stock list for {today} already exists — loading from disk")
        with open(out_path) as fh:
            records = json.load(fh)
    else:
        logger.info("Fetching stock list from FMP")
        records = client.get_stock_list()
        with open(out_path, "w") as fh:
            json.dump(records, fh)
        logger.info(f"Wrote {len(records)} records to {out_path}")

    symbols = [r["symbol"] for r in records if r.get("symbol")]
    logger.info(f"Returning {len(symbols)} symbols for profile fetch")
    return symbols


def fetch_and_store_profiles(**context):
    """
    For each symbol, fetch the company profile from FMP and write it as a
    dated JSON file under the symbol's folder. Skips symbols already fetched
    today (idempotent).
    """
    import json
    import os
    import time
    from datetime import date

    from fmp.client import FMPClient  # type: ignore

    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="fetch_and_store_stock_list")

    if not symbols:
        raise ValueError("No symbols returned from fetch_and_store_stock_list task")

    logger.info(f"Fetching profiles for {len(symbols)} symbols")

    client = FMPClient()
    today = str(date.today())

    saved = 0
    skipped = 0
    errors = 0

    for idx, symbol in enumerate(symbols, 1):
        profile_dir = os.path.join(BACKUP_BASE, "profile", f"symbol={symbol}")
        out_path = os.path.join(profile_dir, f"{today}.json")

        if os.path.exists(out_path):
            skipped += 1
            continue

        try:
            data = client.get_profile_raw(symbol)
            if not data:
                logger.warning(f"[{idx}/{len(symbols)}] {symbol}: empty response, skipping")
                errors += 1
                continue

            os.makedirs(profile_dir, exist_ok=True)
            with open(out_path, "w") as fh:
                json.dump(data, fh)

            saved += 1
            if idx % 100 == 0 or idx == len(symbols):
                logger.info(f"[{idx}/{len(symbols)}] saved={saved}, skipped={skipped}, errors={errors}")

        except Exception as exc:
            logger.error(f"[{idx}/{len(symbols)}] {symbol}: ERROR — {exc}")
            errors += 1

        time.sleep(0.1)

    logger.info(f"Complete — saved: {saved}, skipped: {skipped}, errors: {errors}")

    if errors > 0 and saved == 0 and skipped == 0:
        raise Exception(f"All {errors} profile fetches failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="raw_fmp_company",
    description="Backs up FMP stock list and company profiles to raw JSON files",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "raw", "company", "backup"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_stock_list = PythonOperator(
        task_id="fetch_and_store_stock_list",
        python_callable=fetch_and_store_stock_list,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    task_profiles = PythonOperator(
        task_id="fetch_and_store_profiles",
        python_callable=fetch_and_store_profiles,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=4),
    )

    task_stock_list >> task_profiles
