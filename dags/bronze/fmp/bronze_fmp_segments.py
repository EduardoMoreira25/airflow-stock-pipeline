# dags/bronze/fmp/bronze_fmp_segments.py

"""
Bronze FMP Segments DAG

Loads raw revenue segment JSON files from disk into bronze.revenue_segments.
For each symbol, checks the latest date already in the table and only inserts
newer records. Each backup file holds multiple rows (one per segment name),
so the primary key is (symbol, date, segment_type, segment_name).

Pipeline:
    load_segments

Source:
    /opt/airflow/backups/fmp/raw/revenue_segments/symbol={SYMBOL}/{YYYY-MM-DD}.json

Target:
    postgres_financial → bronze.revenue_segments
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = "/opt/airflow/backups/fmp/raw/revenue_segments"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bronze.revenue_segments (
    symbol       VARCHAR(20)  NOT NULL,
    date         DATE         NOT NULL,
    period       VARCHAR(10),
    segment_type VARCHAR(20)  NOT NULL,
    segment_name TEXT         NOT NULL,
    revenue      BIGINT,
    fiscal_year  INT,
    quarter      VARCHAR(10),
    _loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _source_file TEXT         NOT NULL,
    CONSTRAINT bronze_revenue_segments_pkey
        PRIMARY KEY (symbol, date, segment_type, segment_name)
);
"""

INSERT_SQL = """
    INSERT INTO bronze.revenue_segments
        (symbol, date, period, segment_type, segment_name, revenue, fiscal_year, quarter, _source_file)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, date, segment_type, segment_name) DO NOTHING
"""


# ============================================================
# TASK FUNCTIONS
# ============================================================

def create_table():
    """Create bronze.revenue_segments if it doesn't exist."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("bronze.revenue_segments table ready")
    finally:
        conn.close()


def load_segments():
    """
    Walk the revenue_segments backup directory. For each symbol, skip files
    whose date is already fully loaded (date <= max loaded date for that symbol).
    Insert all records from newer files.
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    inserted = skipped = errors = 0

    try:
        # Latest loaded date per symbol
        max_dates = {}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, MAX(date) FROM bronze.revenue_segments GROUP BY symbol"
            )
            for symbol, max_date in cur.fetchall():
                max_dates[symbol] = max_date

        logger.info(f"{len(max_dates)} symbols already have data in bronze.revenue_segments")

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
                        records = json.load(fh)

                    if not isinstance(records, list):
                        records = [records]

                    with conn.cursor() as cur:
                        for rec in records:
                            cur.execute(INSERT_SQL, (
                                rec.get("symbol"),
                                rec.get("date"),
                                rec.get("period"),
                                rec.get("segment_type"),
                                rec.get("segment_name"),
                                rec.get("revenue"),
                                rec.get("fiscal_year"),
                                rec.get("quarter"),
                                source_file,
                            ))
                            inserted += cur.rowcount

                    conn.commit()

                except Exception as exc:
                    logger.error(f"{source_file}: ERROR — {exc}")
                    conn.rollback()
                    errors += 1

    finally:
        conn.close()

    logger.info(f"Complete — inserted: {inserted}, skipped (files): {skipped}, errors: {errors}")

    if errors > 0 and inserted == 0 and skipped == 0:
        raise Exception(f"All {errors} files failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="bronze_fmp_segments",
    description="Loads raw FMP revenue segment JSON files into bronze.revenue_segments",
    schedule=None,  # triggered by raw_fmp_segments
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "bronze", "segments"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    task_load_segments = PythonOperator(
        task_id="load_segments",
        python_callable=load_segments,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(hours=2),
    )

    task_create_table >> task_load_segments
