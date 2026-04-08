# dags/bronze/fmp/bronze_fmp_stock_prices_daily.py

"""
Bronze FMP Stock Prices Daily DAG

Loads raw daily stock price JSON files into bronze.stock_prices_daily.
For each symbol, queries the latest date already in the table and only
inserts newer records. Fully idempotent via ON CONFLICT DO NOTHING.

Pipeline:
    load_prices

Source:
    {BACKUP_STOCK_PRICES}/symbol={SYMBOL}/{YYYY-MM-DD}.json

Target:
    postgres_financial → bronze.stock_prices_daily
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

INSERT_SQL = """
    INSERT INTO bronze.stock_prices_daily
        (symbol, date, open, high, low, close, volume,
         change, change_percent, vwap, market_cap, _loaded_at, _source_file)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
    ON CONFLICT (symbol, date) DO NOTHING
"""


# ============================================================
# TASK FUNCTIONS
# ============================================================

def load_prices():
    """
    Walk the stock prices backup directory. For each symbol, skip files
    at or before the latest date already in the table, insert the rest.
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    BACKUP_BASE = Variable.get("BACKUP_STOCK_PRICES")

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    inserted = skipped = errors = 0

    try:
        # Latest loaded date per symbol
        max_dates = {}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, MAX(date) FROM bronze.stock_prices_daily GROUP BY symbol"
            )
            for symbol, max_date in cur.fetchall():
                max_dates[symbol] = max_date

        logger.info(f"{len(max_dates)} symbols already have data in bronze.stock_prices_daily")

        if not os.path.isdir(BACKUP_BASE):
            raise FileNotFoundError(f"Backup directory not found: {BACKUP_BASE}")

        for symbol_dir in sorted(os.listdir(BACKUP_BASE)):
            if not symbol_dir.startswith("symbol="):
                continue
            symbol = symbol_dir[len("symbol="):]
            sym_path = os.path.join(BACKUP_BASE, symbol_dir)
            max_date = max_dates.get(symbol)

            for filename in sorted(os.listdir(sym_path)):
                if not filename.endswith(".json"):
                    continue

                file_date_str = filename[:-5]
                if max_date and file_date_str <= str(max_date):
                    skipped += 1
                    continue

                source_file = os.path.join(sym_path, filename)
                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    rec = raw[0] if isinstance(raw, list) and raw else raw

                    # Handle both old files (change_percent) and API camelCase
                    change_pct = rec.get("change_percent") or rec.get("changePercent")

                    with conn.cursor() as cur:
                        cur.execute(INSERT_SQL, (
                            rec.get("symbol") or symbol,
                            rec.get("date") or file_date_str,
                            rec.get("open"),
                            rec.get("high"),
                            rec.get("low"),
                            rec.get("close"),
                            rec.get("volume"),
                            rec.get("change"),
                            change_pct,
                            rec.get("vwap"),
                            rec.get("market_cap"),
                            source_file,
                        ))
                        was_inserted = cur.rowcount > 0

                    conn.commit()
                    inserted += was_inserted
                    skipped += not was_inserted

                except Exception as exc:
                    logger.error(f"{source_file}: ERROR — {exc}")
                    conn.rollback()
                    errors += 1

    finally:
        conn.close()

    logger.info(f"Complete — inserted: {inserted}, skipped: {skipped}, errors: {errors}")

    if errors > 0 and inserted == 0 and skipped == 0:
        raise Exception(f"All {errors} files failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="bronze_fmp_stock_prices_daily",
    description="Loads raw FMP daily stock price JSON files into bronze.stock_prices_daily",
    schedule=None,  # triggered by raw_fmp_stock_prices_daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "bronze", "prices"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_load_prices = PythonOperator(
        task_id="load_prices",
        python_callable=load_prices,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=4),
    )