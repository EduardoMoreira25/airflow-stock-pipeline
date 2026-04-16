# dags/bronze/fmp/bronze_fmp_company.py

"""
Bronze FMP Company DAG

Loads raw company profile JSON files from disk into the bronze.companies
Postgres table. Scans all files under the backup directory and inserts any
that have not been loaded yet. Fully idempotent — safe to re-run.

Pipeline:
    create_table → load_profiles

Source:
    /opt/airflow/backups/fmp/raw/companies/profile/symbol={SYMBOL}/{YYYY-MM-DD}.json

Target:
    postgres_financial → bronze.companies
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

PROFILE_BASE = Variable.get("BACKUP_PROFILE")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bronze.companies (
    symbol      VARCHAR(20)  NOT NULL,
    date        DATE         NOT NULL,
    payload     JSONB        NOT NULL,
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_file TEXT         NOT NULL,
    CONSTRAINT bronze_companies_pkey PRIMARY KEY (symbol, date)
);
"""


# ============================================================
# TASK FUNCTIONS
# ============================================================

def create_table():
    """Create bronze schema and bronze.companies table if they don't exist."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("bronze.companies table ready")
    finally:
        conn.close()


def load_profiles():
    """
    Walk the profile backup directory and insert any files not yet in the table.
    Each file becomes one row: symbol + date (from path) + payload (JSONB) + source_file.
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    INSERT_SQL = """
        INSERT INTO bronze.companies (symbol, date, payload, source_file)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO NOTHING
    """

    inserted = 0
    skipped = 0
    errors = 0

    try:
        if not os.path.isdir(PROFILE_BASE):
            raise FileNotFoundError(f"Profile backup directory not found: {PROFILE_BASE}")

        symbol_dirs = sorted(os.listdir(PROFILE_BASE))
        logger.info(f"Found {len(symbol_dirs)} symbol directories")

        for symbol_dir in symbol_dirs:
            # Folder names are like "symbol=AAPL"
            if not symbol_dir.startswith("symbol="):
                continue
            symbol = symbol_dir[len("symbol="):]
            dir_path = os.path.join(PROFILE_BASE, symbol_dir)

            for filename in sorted(os.listdir(dir_path)):
                if not filename.endswith(".json"):
                    continue

                date_str = filename[:-5]  # strip .json
                source_file = os.path.join(dir_path, filename)

                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    # Profile endpoint returns [{...}] — take the first element
                    payload = raw[0] if isinstance(raw, list) and raw else raw

                    with conn.cursor() as cur:
                        cur.execute(INSERT_SQL, (
                            symbol,
                            date_str,
                            json.dumps(payload),
                            source_file,
                        ))
                        was_inserted = cur.rowcount > 0

                    conn.commit()

                    if was_inserted:
                        inserted += 1
                    else:
                        skipped += 1

                except Exception as exc:
                    logger.error(f"{source_file}: ERROR — {exc}")
                    conn.rollback()
                    errors += 1

    finally:
        conn.close()

    logger.info(f"Complete — inserted: {inserted}, skipped (already loaded): {skipped}, errors: {errors}")

    if errors > 0 and inserted == 0 and skipped == 0:
        raise Exception(f"All {errors} files failed to load. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="bronze_fmp_company",
    description="Loads raw FMP company profile JSON files into bronze.companies",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "bronze", "company"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    task_load_profiles = PythonOperator(
        task_id="load_profiles",
        python_callable=load_profiles,
        retries=3,
        retry_delay=timedelta(minutes=3),
    )

    task_create_table >> task_load_profiles
