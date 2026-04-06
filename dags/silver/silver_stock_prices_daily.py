# dags/silver/silver_stock_prices_daily.py

"""
Stock Prices Daily DAG

Migrated from: scripts/daily_updates/stock_prices_yfinance.py
Original cron: 30 12,21 * * 1-5

Pipeline:
    get_symbols → fetch_and_store_prices
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback



logger = logging.getLogger(__name__)

# ============================================================
# TASK FUNCTIONS
# Core logic lives here — plain Python, no Airflow imports needed
# ============================================================

def get_symbols():
    """
    Fetch all ticker symbols from the companies table.
    Returns a list of symbols for downstream tasks via XCom.
    (We cover XCom properly in Topic 6 — for now just note the return value)
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM gold.companies ORDER BY symbol")
            symbols = [row[0] for row in cur.fetchall()]
        logger.info(f"Found {len(symbols)} symbols to process")
        return symbols  # Airflow stores this automatically as an XCom value
    finally:
        conn.close()

def fetch_and_store_prices(**context):
    """
    Fetches OHLCV data for all symbols in batched requests and stores to Postgres.
    Symbols are split into batches to avoid Yahoo Finance rate limits.
    """
    import time
    import yfinance as yf # type: ignore
    import pandas as pd # type: ignore

    # Fix cache permission issue in Docker
    yf.set_tz_cache_location("/tmp/yfinance_cache")

    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    logger.info(f"Processing {len(symbols)} symbols")

    BATCH_SIZE = 100
    BATCH_SLEEP_SECONDS = 20

    def insert_stock_price(conn, symbol, data):
        """Upsert stock price record."""
        query = """
            INSERT INTO silver.stock_prices_daily (
                symbol, date, open, high, low, close, volume,
                change, change_percent, vwap, market_cap
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                change = EXCLUDED.change,
                change_percent = EXCLUDED.change_percent,
                vwap = EXCLUDED.vwap,
                market_cap = EXCLUDED.market_cap,
                ingestion_timestamp = NOW()
        """
        with conn.cursor() as cur:
            cur.execute(query, (
                symbol,
                data["date"],
                data.get("open"),
                data.get("high"),
                data.get("low"),
                data.get("close"),
                data.get("volume"),
                data.get("change"),
                data.get("change_percent"),
                data.get("vwap"),
                data.get("market_cap"),
            ))

    success = 0
    errors = 0

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    try:
        batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
        logger.info(f"Split into {len(batches)} batches of up to {BATCH_SIZE} symbols each")

        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Downloading batch {batch_num}/{len(batches)} ({len(batch)} symbols)")

            try:
                raw = yf.download(
                    tickers=" ".join(batch),
                    start="2026-01-01", #period="5d"
                    group_by="ticker",
                    auto_adjust=True,
                    threads=True,
                    progress=False,
                )
            except Exception as e:
                logger.error(f"Batch {batch_num} download failed: {e}")
                errors += len(batch)
                time.sleep(BATCH_SLEEP_SECONDS)
                continue

            for i, symbol in enumerate(batch, 1):
                overall_i = (batch_num - 1) * BATCH_SIZE + i
                try:
                    # ── Extract this symbol's data from the batch result ───
                    if len(batch) == 1:
                        ticker_df = raw
                    else:
                        if symbol not in raw.columns.get_level_values(0):
                            logger.warning(f"[{overall_i}/{len(symbols)}] {symbol}: Not found in batch result")
                            errors += 1
                            continue
                        ticker_df = raw[symbol]

                    if ticker_df.empty or len(ticker_df) == 0:
                        logger.warning(f"[{overall_i}/{len(symbols)}] {symbol}: No data returned")
                        errors += 1
                        continue

                    # Latest row
                    latest = ticker_df.iloc[-1]
                    close_price = float(latest["Close"])
                    date = ticker_df.index[-1].date()

                    # Change from previous close if we have 2 rows
                    change = None
                    change_percent = None
                    if len(ticker_df) >= 2:
                        prev_close = float(ticker_df.iloc[-2]["Close"])
                        change = round(close_price - prev_close, 4)
                        change_percent = round((change / prev_close) * 100, 4)

                    # VWAP approximation
                    high = float(latest["High"]) if not pd.isna(latest["High"]) else None
                    low = float(latest["Low"]) if not pd.isna(latest["Low"]) else None
                    vwap = round((high + low + close_price) / 3, 4) if high and low else None

                    data = {
                        "date": date,
                        "open": round(float(latest["Open"]), 4) if not pd.isna(latest["Open"]) else None,
                        "high": high,
                        "low": low,
                        "close": round(close_price, 4),
                        "volume": int(latest["Volume"]) if not pd.isna(latest["Volume"]) else None,
                        "change": change,
                        "change_percent": change_percent,
                        "vwap": vwap,
                        "market_cap": None,  # not available in batch download
                    }

                    insert_stock_price(conn, symbol, data)
                    conn.commit()
                    success += 1
                    logger.info(f"[{overall_i}/{len(symbols)}] {symbol}: close={data['close']}, change={data['change_percent']}%")

                except Exception as e:
                    logger.error(f"[{overall_i}/{len(symbols)}] {symbol}: ERROR - {e}")
                    conn.rollback()
                    errors += 1

            if batch_num < len(batches):
                logger.info(f"Batch {batch_num} done. Sleeping {BATCH_SLEEP_SECONDS}s before next batch...")
                time.sleep(BATCH_SLEEP_SECONDS)

    finally:
        conn.close()

    logger.info(f"Complete — success: {success}, errors: {errors}, total: {len(symbols)}")

    if errors > 0 and success == 0:
        raise Exception(f"All {errors} symbols failed. Check logs.")

# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="silver_stock_prices_daily",
    description="Fetches daily OHLCV data via yfinance, stores to Postgres",
    schedule="30 21 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["stocks", "daily", "yfinance"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ============================================================
    # TASKS
    # ============================================================
    task_get_symbols = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_prices",
        python_callable=fetch_and_store_prices,
        # ⭐ Override retries for this specific task — it hits external APIs
        # so we want more patience than the default
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=2),  # fail if it runs longer than 2hrs
    )

    # ============================================================
    # DEPENDENCIES
    # ============================================================
    task_get_symbols >> task_fetch_and_store