# dags/raw/fmp/raw_fmp_stock_prices_daily.py

"""
Raw FMP Stock Prices Daily DAG

Incrementally backs up daily OHLCV price data from FMP to disk as raw JSON.
For each symbol, finds the latest date already on disk and fetches only the
missing records from the API. Each trading day is stored as its own file.

Pipeline:
    get_symbols → fetch_and_store_prices → trigger_bronze_fmp_stock_prices_daily

Backup layout:
    {BACKUP_STOCK_PRICES}/symbol={SYMBOL}/{YYYY-MM-DD}.json
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)


# ============================================================
# TASK FUNCTIONS
# ============================================================

def get_symbols():
    from financial.db import get_companies
    return get_companies()


def fetch_and_store_prices(**context):
    """
    For each symbol:
    - No existing data → fetch full history from 2010-01-01
    - Existing data    → fetch last 6 months and overwrite those files,
                         ensuring gaps and bad data are corrected on every run
    """
    import json
    import os
    from datetime import date, timezone, datetime as dt

    from fmp.client import FMPClient  # type: ignore

    BACKUP_BASE = Variable.get("BACKUP_STOCK_PRICES")

    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    logger.info(f"Processing {len(symbols)} symbols")

    client = FMPClient()
    today = date.today()
    today_str = str(today)
    six_months_ago = str(today - timedelta(days=182))
    total_saved = 0
    total_errors = 0

    def _has_data(sym_dir):
        """Return True if the symbol folder exists and contains at least one file."""
        if not os.path.isdir(sym_dir):
            return False
        return any(f.endswith(".json") for f in os.listdir(sym_dir))

    for idx, symbol in enumerate(symbols, 1):
        sym_dir = os.path.join(BACKUP_BASE, f"symbol={symbol}")

        if _has_data(sym_dir):
            # Existing data: refresh last 6 months, overwriting files to fix gaps/bad data
            date_from = six_months_ago
        else:
            # No data yet: pull full history
            date_from = "2010-01-01"

        new_files = 0

        try:
            records = client.get_historical_price(symbol, dateFrom=date_from, dateTo=today_str)

            if not records:
                logger.warning(f"[{idx}/{len(symbols)}] {symbol}: no data returned")
                continue

            os.makedirs(sym_dir, exist_ok=True)
            ingestion_ts = dt.now(timezone.utc).isoformat()

            for rec in records:
                rec_date = rec.get("date")
                if not rec_date:
                    continue

                # Normalise camelCase key from API
                if "changePercent" in rec:
                    rec["change_percent"] = rec.pop("changePercent")

                rec.setdefault("market_cap", None)
                rec["ingestion_timestamp"] = ingestion_ts

                # Always write — overwrites existing file to fix gaps or bad data
                out_path = os.path.join(sym_dir, f"{rec_date}.json")
                with open(out_path, "w") as fh:
                    json.dump([rec], fh)
                new_files += 1

            total_saved += new_files
            if idx % 100 == 0 or new_files > 0:
                logger.info(f"[{idx}/{len(symbols)}] {symbol}: {new_files} files written (from {date_from})")

        except Exception as exc:
            logger.error(f"[{idx}/{len(symbols)}] {symbol}: ERROR — {exc}")
            total_errors += 1

    logger.info(f"Complete — {total_saved} files written, {total_errors} symbol errors")

    if total_errors > 0 and total_saved == 0:
        raise Exception(f"All {total_errors} symbols failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="raw_fmp_stock_prices_daily",
    description="Incrementally backs up FMP daily stock prices to raw JSON files",
    schedule="30 21 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "raw", "prices", "backup"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_get_symbols = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_prices",
        python_callable=fetch_and_store_prices,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=10),
    )

    task_trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_fmp_stock_prices_daily",
        trigger_dag_id="bronze_fmp_stock_prices_daily",
        wait_for_completion=False,
    )

    task_get_symbols >> task_fetch_and_store >> task_trigger_bronze