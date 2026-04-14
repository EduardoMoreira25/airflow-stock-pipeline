# dags/bronze/fmp/bronze_fmp_financials.py

"""
Bronze FMP Financials DAG

Loads raw financial statement JSON files from disk into the bronze Postgres
tables. Each record is stored as a payload jsonb column alongside key columns
for partitioning and deduplication.

Fully idempotent via ON CONFLICT DO NOTHING.

Pipeline:
    load_balance → load_income → load_cashflow → load_segments

Source:
    /opt/airflow/backups/fmp/raw/{statement_type}/symbol={SYMBOL}/{YYYY-MM-DD}_{PERIOD}.json

Target tables (already exist):
    bronze.financial_statement_balance   — PK: (symbol, date, period)
    bronze.financial_statement_income    — PK: (symbol, date, period)
    bronze.financial_statement_cashflow  — PK: (symbol, date, period)
    bronze.revenue_segments              — PK: (symbol, date, segment_type, segment_name)

Table schema (all 3 financial statement tables):
    symbol       text
    date         date
    period       text
    fiscal_year  text
    payload      jsonb
    _loaded_at   timestamp
    _source_file text

Segments table schema:
    symbol       text
    date         date
    period       text
    fiscal_year  text
    segment_type text
    segment_name text
    payload      jsonb
    _loaded_at   timestamp
    _source_file text
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = Variable.get("BACKUP_FINANCIALS")

STATEMENTS = [
    {
        "backup_folder": "financial_statement_balance",
        "table":         "bronze.financial_statement_balance",
        "task_id":       "load_balance",
    },
    {
        "backup_folder": "financial_statement_income",
        "table":         "bronze.financial_statement_income",
        "task_id":       "load_income",
    },
    {
        "backup_folder": "financial_statement_cashflow",
        "table":         "bronze.financial_statement_cashflow",
        "task_id":       "load_cashflow",
    },
]


# ============================================================
# HELPERS
# ============================================================

def _parse_filename(filename):
    """
    Parse filename into (date_str, period).
    Handles both:
      - New format: YYYY-MM-DD_{PERIOD}.json  e.g. 2024-09-28_Q4.json
      - Old format: YYYY-MM-DD.json           e.g. 2024-09-28.json (fallback)
    Returns (date_str, period) or (None, None) if unparseable.
    """
    stem = filename[:-5]  # strip .json
    parts = stem.split("_", 1)
    if len(parts) == 2:
        date_str, period = parts
        if len(date_str) == 10:  # YYYY-MM-DD
            return date_str, period
    # Fallback: old format with no period in filename
    if len(stem) == 10:
        return stem, None
    return None, None


# ============================================================
# SHARED FINANCIAL STATEMENT LOADER
# ============================================================

def _load_statement(backup_folder, table):
    """
    Generic loader for a single financial statement type.
    1. Queries DB for existing (symbol, date, period) keys.
    2. Walks the backup directory, skipping already loaded records.
    3. Inserts new records as payload jsonb with ON CONFLICT DO NOTHING.
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    insert_sql = f"""
        INSERT INTO {table} (symbol, date, period, fiscal_year, payload, _loaded_at, _source_file)
        VALUES (%s, %s, %s, %s, %s::jsonb, NOW(), %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
    """

    backup_dir = os.path.join(BACKUP_BASE, backup_folder)
    inserted = skipped = errors = 0

    try:
        # ── Existing (symbol, date, period) keys already in the table ─────
        existing_keys = set()
        with conn.cursor() as cur:
            cur.execute(f"SELECT symbol, date::text, period FROM {table}")
            for symbol, date, period in cur.fetchall():
                existing_keys.add((symbol, date, period))

        logger.info(f"[{table}] {len(existing_keys)} records already loaded")

        if not os.path.isdir(backup_dir):
            logger.warning(f"Backup directory not found: {backup_dir}")
            return

        for symbol_dir in sorted(os.listdir(backup_dir)):
            if not symbol_dir.startswith("symbol="):
                continue
            symbol = symbol_dir[len("symbol="):]
            sym_path = os.path.join(backup_dir, symbol_dir)

            for filename in sorted(os.listdir(sym_path)):
                if not filename.endswith(".json"):
                    continue

                date_str, period = _parse_filename(filename)
                if not date_str:
                    logger.warning(f"Could not parse filename: {filename}, skipping")
                    continue

                # If period not in filename, read from record
                source_file = os.path.join(sym_path, filename)

                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    record = raw[0] if isinstance(raw, list) and raw else raw

                    # Fall back to period from record if not in filename
                    if not period:
                        period = record.get("period")
                    if not period:
                        logger.warning(f"No period found for {source_file}, skipping")
                        skipped += 1
                        continue

                    key = (symbol, date_str, period)
                    if key in existing_keys:
                        skipped += 1
                        continue

                    fiscal_year = str(record.get("fiscalYear") or record.get("fiscal_year") or "")
                    payload     = json.dumps(record)

                    with conn.cursor() as cur:
                        cur.execute(insert_sql, [symbol, date_str, period, fiscal_year, payload, source_file])
                        was_inserted = cur.rowcount > 0

                    conn.commit()

                    if was_inserted:
                        existing_keys.add(key)
                        inserted += 1
                    else:
                        skipped += 1

                except Exception as exc:
                    logger.error(f"{source_file}: ERROR — {exc}")
                    conn.rollback()
                    errors += 1

    finally:
        conn.close()

    logger.info(f"[{table}] inserted: {inserted}, skipped: {skipped}, errors: {errors}")

    if errors > 0 and inserted == 0 and skipped == 0:
        raise Exception(f"[{table}] All {errors} files failed. Check logs.")


# ============================================================
# SEGMENTS LOADER
# ============================================================

def _load_segments():
    """
    Loads revenue segment JSON files into bronze.revenue_segments.
    Each file contains multiple records (one per segment name/type).
    PK: (symbol, date, segment_type, segment_name)
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    table = "bronze.revenue_segments"

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    insert_sql = f"""
        INSERT INTO {table} (symbol, date, period, fiscal_year, segment_type, segment_name, payload, _loaded_at, _source_file)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), %s)
        ON CONFLICT (symbol, date, segment_type, segment_name) DO NOTHING
    """

    backup_dir = os.path.join(BACKUP_BASE, "revenue_segments")
    inserted = skipped = errors = 0

    try:
        # ── Existing (symbol, date, segment_type, segment_name) keys ──────
        existing_keys = set()
        with conn.cursor() as cur:
            cur.execute(f"SELECT symbol, date::text, segment_type, segment_name FROM {table}")
            for row in cur.fetchall():
                existing_keys.add(tuple(row))

        logger.info(f"[{table}] {len(existing_keys)} records already loaded")

        if not os.path.isdir(backup_dir):
            logger.warning(f"Backup directory not found: {backup_dir}")
            return

        for symbol_dir in sorted(os.listdir(backup_dir)):
            if not symbol_dir.startswith("symbol="):
                continue
            symbol = symbol_dir[len("symbol="):]
            sym_path = os.path.join(backup_dir, symbol_dir)

            for filename in sorted(os.listdir(sym_path)):
                if not filename.endswith(".json"):
                    continue

                date_str, period = _parse_filename(filename)
                if not date_str:
                    logger.warning(f"Could not parse filename: {filename}, skipping")
                    continue

                source_file = os.path.join(sym_path, filename)

                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    records = raw if isinstance(raw, list) else [raw]

                    for record in records:
                        segment_type = record.get("segment_type")
                        segment_name = record.get("segment_name")
                        fiscal_year  = str(record.get("fiscal_year") or "")
                        rec_period   = period or record.get("period") or record.get("quarter")

                        if not all([segment_type, segment_name, rec_period]):
                            logger.warning(
                                f"{source_file}: missing segment_type/segment_name/period, skipping record"
                            )
                            skipped += 1
                            continue

                        key = (symbol, date_str, segment_type, segment_name)
                        if key in existing_keys:
                            skipped += 1
                            continue

                        payload = json.dumps(record)

                        with conn.cursor() as cur:
                            cur.execute(insert_sql, [
                                symbol, date_str, rec_period, fiscal_year,
                                segment_type, segment_name, payload, source_file
                            ])
                            was_inserted = cur.rowcount > 0

                        conn.commit()

                        if was_inserted:
                            existing_keys.add(key)
                            inserted += 1
                        else:
                            skipped += 1

                except Exception as exc:
                    logger.error(f"{source_file}: ERROR — {exc}")
                    conn.rollback()
                    errors += 1

    finally:
        conn.close()

    logger.info(f"[{table}] inserted: {inserted}, skipped: {skipped}, errors: {errors}")

    if errors > 0 and inserted == 0 and skipped == 0:
        raise Exception(f"[{table}] All {errors} files failed. Check logs.")


# ============================================================
# TASK FUNCTIONS
# ============================================================

def load_balance():
    s = STATEMENTS[0]
    _load_statement(s["backup_folder"], s["table"])


def load_income():
    s = STATEMENTS[1]
    _load_statement(s["backup_folder"], s["table"])


def load_cashflow():
    s = STATEMENTS[2]
    _load_statement(s["backup_folder"], s["table"])


def load_segments():
    _load_segments()


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="bronze_fmp_financials",
    description="Loads raw FMP financial statement JSON files into bronze tables",
    schedule=None,  # triggered by raw_fmp_financials
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "bronze", "financials"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_load_balance = PythonOperator(
        task_id="load_balance",
        python_callable=load_balance,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(hours=2),
    )

    task_load_income = PythonOperator(
        task_id="load_income",
        python_callable=load_income,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(hours=2),
    )

    task_load_cashflow = PythonOperator(
        task_id="load_cashflow",
        python_callable=load_cashflow,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(hours=2),
    )

    task_load_segments = PythonOperator(
        task_id="load_segments",
        python_callable=load_segments,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(hours=2),
    )

    task_load_balance >> task_load_income >> task_load_cashflow >> task_load_segments