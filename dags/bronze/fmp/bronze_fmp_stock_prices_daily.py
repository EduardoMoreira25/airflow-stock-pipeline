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

Target table schema:
    symbol       text
    date         date
    payload      jsonb
    _loaded_at   timestamp
    _source_file text

PK: (symbol, date)

Idempotency guarantees:
    1. Per-symbol MAX(date) lookup skips files already covered.
    2. Malformed filenames (non YYYY-MM-DD) are skipped safely.
    3. ON CONFLICT (symbol, date) DO NOTHING handles any race or overlap.
    4. Each symbol's batch is committed in its own transaction, so a
       mid-run failure leaves prior symbols durably loaded and the next
       run resumes from the new MAX(date).
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)


# ============================================================
# TASK FUNCTIONS
# ============================================================

def load_prices():
    """
    Walk the stock prices backup directory. For each symbol:
    1. Query the latest date already loaded in bronze.
    2. Collect all files strictly newer than that date.
    3. Insert them in a single batched statement per symbol.
    Idempotent via ON CONFLICT (symbol, date) DO NOTHING.
    """
    import json
    import os
    from datetime import datetime as dt

    from psycopg2.extras import execute_values  # type: ignore
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    BACKUP_BASE = Variable.get("BACKUP_STOCK_PRICES")
    BATCH_PAGE_SIZE = 500       # rows per server round-trip within execute_values
    PROGRESS_EVERY = 50         # log progress every N symbols

    insert_sql = """
        INSERT INTO bronze.stock_prices_daily
            (symbol, date, payload, _loaded_at, _source_file)
        VALUES %s
        ON CONFLICT (symbol, date) DO NOTHING
    """
    # Template binds each row tuple; NOW() is evaluated server-side per row,
    # which is fine since rows within one batch commit at the same instant anyway.
    insert_template = "(%s, %s, %s::jsonb, NOW(), %s)"

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    inserted = skipped = errors = 0
    symbols_processed = 0
    symbols_with_new_data = 0

    try:
        # ── Latest loaded date per symbol ──────────────────────────────────
        max_dates = {}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, MAX(date) FROM bronze.stock_prices_daily GROUP BY symbol"
            )
            for symbol, max_date in cur.fetchall():
                max_dates[symbol] = max_date  # date object or None

        logger.info(
            f"{len(max_dates)} symbols already have data in bronze.stock_prices_daily"
        )

        if not os.path.isdir(BACKUP_BASE):
            raise FileNotFoundError(f"Backup directory not found: {BACKUP_BASE}")

        symbol_dirs = sorted(
            d for d in os.listdir(BACKUP_BASE) if d.startswith("symbol=")
        )
        total_symbols = len(symbol_dirs)
        logger.info(f"Found {total_symbols} symbol directories to scan")

        # ── Per symbol: collect new rows, then flush in one batch ──────────
        for idx, symbol_dir in enumerate(symbol_dirs, start=1):
            symbol = symbol_dir[len("symbol="):]
            sym_path = os.path.join(BACKUP_BASE, symbol_dir)
            max_date = max_dates.get(symbol)  # None if symbol not yet in DB

            rows = []  # (symbol, date_str, payload_json, source_file)

            try:
                filenames = sorted(os.listdir(sym_path))
            except OSError as exc:
                logger.error(f"Cannot list {sym_path}: {exc}")
                errors += 1
                continue

            for filename in filenames:
                if not filename.endswith(".json"):
                    continue

                file_date_str = filename[:-5]  # YYYY-MM-DD

                # Parse date defensively; malformed filenames are skipped.
                try:
                    file_date = dt.strptime(file_date_str, "%Y-%m-%d").date()
                except ValueError:
                    logger.warning(f"Skipping non-date filename: {sym_path}/{filename}")
                    continue

                # Skip files already covered by max_date
                if max_date is not None and file_date <= max_date:
                    skipped += 1
                    continue

                source_file = os.path.join(sym_path, filename)
                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    rec = raw[0] if isinstance(raw, list) and raw else raw
                    if not isinstance(rec, dict):
                        logger.warning(f"{source_file}: unexpected payload shape, skipping")
                        errors += 1
                        continue

                    # Normalise camelCase keys from older files
                    if "changePercent" in rec:
                        rec["change_percent"] = rec.pop("changePercent")

                    date_str = rec.get("date") or file_date_str
                    payload = json.dumps(rec)

                    rows.append((symbol, date_str, payload, source_file))

                except Exception as exc:
                    logger.error(f"{source_file}: parse error — {exc}")
                    errors += 1

            # ── Flush this symbol's batch ──────────────────────────────────
            if rows:
                symbols_with_new_data += 1
                try:
                    with conn.cursor() as cur:
                        execute_values(
                            cur,
                            insert_sql,
                            rows,
                            template=insert_template,
                            page_size=BATCH_PAGE_SIZE,
                        )
                        # rowcount reflects actual inserted rows (ON CONFLICT skips
                        # are not counted by Postgres under DO NOTHING).
                        batch_inserted = cur.rowcount if cur.rowcount >= 0 else len(rows)
                    conn.commit()
                    inserted += batch_inserted
                    skipped += len(rows) - batch_inserted  # conflict-skipped
                except Exception as exc:
                    logger.error(f"Batch insert failed for {symbol}: {exc}")
                    conn.rollback()
                    errors += len(rows)

            symbols_processed += 1

            # ── Heartbeat-friendly progress log ────────────────────────────
            if symbols_processed % PROGRESS_EVERY == 0:
                logger.info(
                    f"progress {symbols_processed}/{total_symbols} symbols | "
                    f"inserted={inserted} skipped={skipped} errors={errors} | "
                    f"symbols_with_new_data={symbols_with_new_data}"
                )

    finally:
        conn.close()

    logger.info(
        f"Complete — symbols_processed={symbols_processed}/{total_symbols}, "
        f"symbols_with_new_data={symbols_with_new_data}, "
        f"inserted={inserted}, skipped={skipped}, errors={errors}"
    )

    # Fail loudly only if nothing succeeded and we had errors.
    if errors > 0 and inserted == 0 and symbols_with_new_data == 0:
        raise Exception(f"All new-file attempts failed ({errors} errors). Check logs.")


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
    )