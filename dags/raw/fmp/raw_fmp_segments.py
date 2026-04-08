# dags/raw/fmp/raw_fmp_segments.py

"""
Raw FMP Revenue Segments DAG

Incrementally backs up geographic and product revenue segment data from FMP
to disk as raw JSON files. For each company, checks what date files already
exist and only fetches missing ones.

Pipeline:
    get_symbols → fetch_and_store_segments → trigger_bronze_fmp_segments

Backup layout:
    /opt/airflow/backups/fmp/raw/revenue_segments/symbol={SYMBOL}/{YYYY-MM-DD}.json

Each file contains a list of flat segment records (geography + product combined).
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = Variable.get("BACKUP_SEGMENTS")


# ============================================================
# TASK FUNCTIONS
# ============================================================

def get_symbols():
    from financial.db import get_companies
    return get_companies()


def fetch_and_store_segments(**context):
    """
    For each symbol, fetch geographic and product revenue segments from FMP
    and write any missing date files to the backup directory.
    """
    import json
    import os
    import time
    from datetime import datetime as dt, timezone

    from fmp.client import FMPClient  # type: ignore

    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    logger.info(f"Processing {len(symbols)} symbols")

    client = FMPClient()
    total_saved = 0
    total_errors = 0

    def _existing_dates(directory):
        if not os.path.isdir(directory):
            return set()
        return {f[:-5] for f in os.listdir(directory) if f.endswith(".json")}

    def _flatten_segments(api_records, segment_type, symbol, now_ts):
        """
        FMP returns [{date: YYYY-MM-DD, SegmentName: value, ...}, ...]
        Converts to flat records grouped by date.
        Returns dict: {date: [records...]}
        """
        by_date = {}
        for item in api_records:
            date = item.get("date")
            if not date:
                continue
            records = []
            for key, value in item.items():
                if key == "date" or value is None:
                    continue
                records.append({
                    "symbol": symbol,
                    "date": date,
                    "period": "annual",
                    "segment_type": segment_type,
                    "segment_name": key,
                    "revenue": value,
                    "fiscal_year": int(date[:4]),
                    "quarter": "FY",
                    "created_at": now_ts,
                })
            if records:
                by_date[date] = by_date.get(date, []) + records
        return by_date

    for idx, symbol in enumerate(symbols, 1):
        backup_dir = os.path.join(BACKUP_BASE, f"symbol={symbol}")
        existing = _existing_dates(backup_dir)
        now_ts = dt.now(timezone.utc).isoformat()
        new_files = 0

        try:
            try:
                geo_raw = client.get_geographic_segment(symbol)
            except Exception as exc:
                logger.warning(f"[{idx}/{len(symbols)}] {symbol}: get_geographic_segment failed — {exc}")
                geo_raw = []

            try:
                prod_raw = client.get_product_revenue(symbol)
            except Exception as exc:
                logger.warning(f"[{idx}/{len(symbols)}] {symbol}: get_product_revenue failed — {exc}")
                prod_raw = []

            geo_by_date  = _flatten_segments(geo_raw,  "geography", symbol, now_ts)
            prod_by_date = _flatten_segments(prod_raw, "product",   symbol, now_ts)

            all_dates = set(geo_by_date) | set(prod_by_date)
            for date in sorted(all_dates):
                if date in existing:
                    continue
                combined = geo_by_date.get(date, []) + prod_by_date.get(date, [])
                if not combined:
                    continue
                os.makedirs(backup_dir, exist_ok=True)
                path = os.path.join(backup_dir, f"{date}.json")
                with open(path, "w") as fh:
                    json.dump(combined, fh)
                existing.add(date)
                new_files += 1

            total_saved += new_files
            logger.info(f"[{idx}/{len(symbols)}] {symbol}: +{new_files} new files")

        except Exception as exc:
            logger.error(f"[{idx}/{len(symbols)}] {symbol}: ERROR — {exc}")
            total_errors += 1

        time.sleep(0.2)

    logger.info(f"Complete — {total_saved} files written, {total_errors} symbol errors")

    if total_errors > 0 and total_saved == 0:
        raise Exception(f"All {total_errors} symbols failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="raw_fmp_segments",
    description="Incrementally backs up FMP revenue segment data to raw JSON files",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "raw", "segments", "backup"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_get_symbols = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_segments",
        python_callable=fetch_and_store_segments,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=2),
    )

    task_trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_fmp_segments",
        trigger_dag_id="bronze_fmp_segments",
        wait_for_completion=True,
    )

    task_get_symbols >> task_fetch_and_store >> task_trigger_bronze
