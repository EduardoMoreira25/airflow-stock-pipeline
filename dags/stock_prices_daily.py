# dags/stock_prices_daily.py

"""
Stock Prices Daily DAG

Migrated from: scripts/daily_updates/stock_prices_yfinance.py
Original cron: 30 12,21 * * 1-5

Pipeline:
    get_symbols → fetch_and_store_prices
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="postgres_testes")
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM companies ORDER BY symbol")
            symbols = [row[0] for row in cur.fetchall()]
        logger.info(f"Found {len(symbols)} symbols to process")
        return symbols  # Airflow stores this automatically as an XCom value
    finally:
        conn.close()

def fetch_and_store_prices(**context):
    """
    Fetches OHLCV data for all symbols and stores to Postgres.

    Pulled symbols from XCom (set by get_symbols task).
    Uses the same logic as stock_prices_yfinance.py.
    """
    import yfinance as yf
    import pandas as pd

    # Pull symbols from the previous task via XCom
    # ti = task instance — the object that lets us talk to Airflow
    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    logger.info(f"Processing {len(symbols)} symbols")

    def fetch_single_stock(symbol):
        """Fetch stock data for a single symbol."""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")

            if hist.empty:
                return None

            row = hist.iloc[-1]
            info = ticker.info
            market_cap = info.get("marketCap")
            prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")
            close_price = float(row["Close"])

            change = None
            change_percent = None
            if prev_close and close_price:
                change = round(close_price - prev_close, 4)
                change_percent = round((change / prev_close) * 100, 4)

            typical_price = (float(row["High"]) + float(row["Low"]) + close_price) / 3
            vwap = round(typical_price, 4)

            return {
                "date": hist.index[-1].date(),
                "open": round(float(row["Open"]), 4) if not pd.isna(row["Open"]) else None,
                "high": round(float(row["High"]), 4) if not pd.isna(row["High"]) else None,
                "low": round(float(row["Low"]), 4) if not pd.isna(row["Low"]) else None,
                "close": round(close_price, 4),
                "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else None,
                "change": change,
                "change_percent": change_percent,
                "vwap": vwap,
                "market_cap": market_cap,
            }
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None

    def insert_stock_price(conn, symbol, data):
        """Upsert stock price record — same query as original script."""
        query = """
            INSERT INTO public.stock_prices_daily (
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

    # Main loop — same logic as original main()
    success = 0
    errors = 0

    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_testes")
        conn = hook.get_conn()
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{len(symbols)}] Fetching {symbol}...")
                data = fetch_single_stock(symbol)

                if data:
                    insert_stock_price(conn, symbol, data)
                    conn.commit()
                    success += 1
                    logger.info(f"  {symbol}: close={data['close']}, change={data['change_percent']}%")
                else:
                    logger.warning(f"  {symbol}: No data returned")
                    errors += 1

            except Exception as e:
                logger.error(f"  {symbol}: ERROR - {e}")
                conn.rollback()
                errors += 1
    finally:
        conn.close()

    logger.info(f"Complete — success: {success}, errors: {errors}, total: {len(symbols)}")

    # ⭐ Raise if too many errors — fail the task loudly rather than silently
    if errors > 0 and success == 0:
        raise Exception(f"All {errors} symbols failed. Check logs.")

# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="stock_prices_daily",
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