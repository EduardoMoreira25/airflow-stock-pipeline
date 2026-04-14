# dags/raw/fmp/raw_fmp_financials.py

"""
Raw FMP Financials DAG

Incrementally backs up FMP financial statement data to disk as raw JSON files.

For each company in the DB, checks what files already exist in the backup
directory, fetches only missing records from the FMP API, and writes each
report as an individual JSON file named by report date.

Pipeline:
    get_symbols → fetch_and_store_financials

Backup layout:
    /opt/airflow/backups/fmp/raw/{statement_type}/symbol={SYMBOL}/{YYYY-MM-DD}.json
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = Variable.get("BACKUP_FINANCIALS")

# (folder_name, client_method_name)
FINANCIAL_STATEMENTS = [
    ("financial_statement_balance", "get_balance_sheet"),
    ("financial_statement_cashflow", "get_cashflow_statement"),
    ("financial_statement_income", "get_income_statement"),
]

PERIODS = ["quarter", "annual"]


# ============================================================
# TASK FUNCTIONS
# ============================================================

def get_symbols():
    from financial.db import get_companies # type: ignore
    return get_companies()


def fetch_and_store_financials(**context):
    """
    For each symbol, fetch all missing financial statement records from FMP
    and write them to the backup directory as individual JSON files.
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
        """Return set of date strings (without .json) for files already on disk."""
        if not os.path.isdir(directory):
            return set()
        return {f[:-5] for f in os.listdir(directory) if f.endswith(".json")}

    def _write_json(path, data):
        """Write data as a JSON array to path, creating parent dirs as needed."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            json.dump(data, fh)

    def _flatten_segments(api_records, segment_type, symbol, now_ts):
        """
        Transform FMP segment API response into individual flat records.

        FMP returns a list of dicts: [{"date": "YYYY-MM-DD", "SegmentA": val, ...}, ...]
        Each non-date key becomes a separate record in the output.
        Returns a dict: {date: [records...]}
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
        new_balance = new_cashflow = new_income = new_segments = 0

        try:
            # ── Financial statements (balance / cashflow / income) ────────────
            for folder_name, method_name in FINANCIAL_STATEMENTS:
                backup_dir = os.path.join(BACKUP_BASE, folder_name, f"symbol={symbol}")
                existing = _existing_dates(backup_dir)
                fetcher = getattr(client, method_name)

                for period in PERIODS:
                    try:
                        records = fetcher(symbol, period, limit=120)
                    except Exception as exc:
                        logger.warning(f"[{idx}/{len(symbols)}] {symbol}: {method_name}({period}) failed — {exc}")
                        continue

                    if not records:
                        continue

                    for record in records:
                        date = record.get("date")
                        if not date or date in existing:
                            continue
                        path = os.path.join(backup_dir, f"{date}.json")
                        _write_json(path, [record])
                        existing.add(date)

                        if folder_name == "financial_statement_balance":
                            new_balance += 1
                        elif folder_name == "financial_statement_cashflow":
                            new_cashflow += 1
                        elif folder_name == "financial_statement_income":
                            new_income += 1

            # ── Revenue segments ──────────────────────────────────────────────
            seg_backup_dir = os.path.join(BACKUP_BASE, "revenue_segments", f"symbol={symbol}")
            seg_existing = _existing_dates(seg_backup_dir)
            now_ts = dt.now(timezone.utc).isoformat()

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

            geo_by_date = _flatten_segments(geo_raw, "geography", symbol, now_ts)
            prod_by_date = _flatten_segments(prod_raw, "product", symbol, now_ts)

            all_dates = set(geo_by_date) | set(prod_by_date)
            for date in sorted(all_dates):
                if date in seg_existing:
                    continue
                combined = geo_by_date.get(date, []) + prod_by_date.get(date, [])
                if not combined:
                    continue
                path = os.path.join(seg_backup_dir, f"{date}.json")
                _write_json(path, combined)
                seg_existing.add(date)
                new_segments += 1

            total_saved += new_balance + new_cashflow + new_income + new_segments
            logger.info(
                f"[{idx}/{len(symbols)}] {symbol}: "
                f"+{new_balance} balance, +{new_income} income, "
                f"+{new_cashflow} cashflow, +{new_segments} segments"
            )

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
    dag_id="raw_fmp_financials",
    description="Incrementally backs up FMP financial statements to raw JSON files",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "raw", "financials", "backup"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_get_symbols = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_financials",
        python_callable=fetch_and_store_financials,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )

    task_trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_fmp_financials",
        trigger_dag_id="bronze_fmp_financials",
        wait_for_completion=False,
    )

    task_get_symbols >> task_fetch_and_store >> task_trigger_bronze
