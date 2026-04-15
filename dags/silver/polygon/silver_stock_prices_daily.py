# dags/silver/polygon/silver_polygon_stock_prices_daily.py

"""
Stock Prices Daily DAG

Pipeline:
    get_symbols → fetch_and_store_prices
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
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
    Fetches daily OHLCV data from Polygon.io grouped daily endpoint.
    Covers the last 3 trading days to handle weekends / late runs safely.
    One request per trading day returns the full US market snapshot,
    which is then filtered to our symbols and upserted into Postgres.
    """
    import time
    import requests
    from datetime import date, timedelta

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    POLYGON_API_KEY  = Variable.get("polygon_api_key")
    REQUEST_DELAY    = 120   # seconds — free tier = 5 req/min

    ti      = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    symbols = set(symbols)
    logger.info(f"Processing {len(symbols)} symbols")

    # ── Date helpers ───────────────────────────────────────────────────────────

    def last_n_trading_days(n: int) -> list[date]:
        """Returns the last N weekdays up to and including yesterday."""
        days    = []
        current = date.today() - timedelta(days=1)
        while len(days) < n:
            if current.weekday() < 5:   # Mon–Fri
                days.append(current)
            current -= timedelta(days=1)
        return sorted(days)             # chronological order

    # ── Polygon fetch ──────────────────────────────────────────────────────────

    def fetch_grouped_daily(trading_date: date) -> list[dict] | None:
        url    = (
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
            f"{trading_date.isoformat()}"
        )
        params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"Request failed for {trading_date}: {e}")
            return None

        status = data.get("status")
        if status == "NOT_FOUND":
            logger.info(f"{trading_date}: market closed / no data")
            return None
        if status != "OK":
            logger.warning(f"{trading_date}: unexpected status '{status}'")
            return None

        return data.get("results") or None

    # ── Row builder ────────────────────────────────────────────────────────────

    def build_rows(results: list[dict], trading_date: date) -> list[dict]:
        """
        Filter Polygon results to our symbols and map to DB row format.
        Polygon fields: T=ticker, o=open, h=high, l=low, c=close,
                        v=volume, vw=vwap
        """
        rows = []
        for r in results:
            ticker = r.get("T")
            if ticker not in symbols:
                continue

            close  = r.get("c")
            high   = r.get("h")
            low    = r.get("l")
            open_  = r.get("o")
            volume = r.get("v")
            vwap   = r.get("vw")

            if close is None:
                continue

            rows.append({
                "symbol":         ticker,
                "date":           trading_date,
                "open":           round(float(open_),  4) if open_  is not None else None,
                "high":           round(float(high),   4) if high   is not None else None,
                "low":            round(float(low),    4) if low    is not None else None,
                "close":          round(float(close),  4),
                "volume":         int(volume)              if volume is not None else None,
                "change":         None,
                "vwap":           round(float(vwap),   4) if vwap   is not None else None,
                "market_cap":     None,
                "year":           trading_date.year,
                "month":          trading_date.month,
            })

        return rows

    # ── DB upsert ──────────────────────────────────────────────────────────────

    def upsert_rows(conn, rows: list[dict]):
        query = """
            INSERT INTO silver.s_stock_prices_daily (
                symbol, date, open, high, low, close, volume,
                change, vwap, market_cap, year, month
            )
            VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, %(change)s, %(vwap)s, %(market_cap)s, %(year)s, %(month)s
            )
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open              = EXCLUDED.open,
                high              = EXCLUDED.high,
                low               = EXCLUDED.low,
                close             = EXCLUDED.close,
                volume            = EXCLUDED.volume,
                change            = EXCLUDED.change,
                vwap              = EXCLUDED.vwap,
                market_cap        = EXCLUDED.market_cap,
                year              = EXCLUDED.year,
                month             = EXCLUDED.month,
                _loaded_at        = NOW()
        """
        with conn.cursor() as cur:
            cur.executemany(query, rows)
        conn.commit()

    # ── Main loop ──────────────────────────────────────────────────────────────

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    trading_days = last_n_trading_days(3)
    logger.info(f"Fetching {len(trading_days)} trading days: {trading_days[0]} → {trading_days[-1]}")

    total_rows = 0
    errors     = 0

    try:
        for i, trading_date in enumerate(trading_days, 1):
            logger.info(f"[{i}/{len(trading_days)}] {trading_date} — fetching from Polygon...")

            results = fetch_grouped_daily(trading_date)

            if results is None:
                errors += 1
                time.sleep(REQUEST_DELAY)
                continue

            rows = build_rows(results, trading_date)

            if not rows:
                logger.warning(f"[{i}/{len(trading_days)}] {trading_date}: no matching tickers")
                time.sleep(REQUEST_DELAY)
                continue

            try:
                upsert_rows(conn, rows)
                total_rows += len(rows)
                logger.info(f"[{i}/{len(trading_days)}] {trading_date}: upserted {len(rows)} rows")
            except Exception as e:
                logger.error(f"[{i}/{len(trading_days)}] {trading_date}: DB error — {e}")
                conn.rollback()
                errors += 1

            if i < len(trading_days):
                time.sleep(REQUEST_DELAY)

    finally:
        conn.close()

    logger.info(f"Complete — {total_rows} rows upserted, {errors} days failed")

    if errors > 0 and total_rows == 0:
        raise Exception(f"All {errors} days failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="silver_stock_prices_daily",
    description="Fetches daily OHLCV data via Polygon.io, stores to Postgres",
    schedule="0 8 * * 2-6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["stocks", "daily", "polygon"],
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
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=1),
    )

    task_trigger_dbt_mkt_cap = TriggerDagRunOperator(
        task_id="dbt_market_cap_update",
        trigger_dag_id="dbt_macro_update_market_cap",
        wait_for_completion=True,
    )

    task_get_symbols >> task_fetch_and_store >> task_trigger_dbt_mkt_cap