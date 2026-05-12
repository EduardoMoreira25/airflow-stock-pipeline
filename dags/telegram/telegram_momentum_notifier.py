# dags/analytics/continuation_momentum_scanner.py

"""
Continuation Momentum Scanner DAG

Finds stocks with +5% gains on the most recent trading day that ALSO
had +5% gains in the past 15 days. Sends results via Telegram.

Triggered manually.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

GAIN_THRESHOLD = 5.0
LOOKBACK_DAYS = 15


# ============================================================
# TASK FUNCTIONS
# ============================================================

def find_and_notify():
    import time
    import requests
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID   = Variable.get("TELEGRAM_CHAT_ID")

    def send_telegram(message):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        try:
            response = requests.post(url, data=payload)
            if response.status_code != 200:
                logger.error(f"Telegram error: {response.text}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    try:
        # ── Get most recent trading date ───────────────────────────────────
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM silver.s_stock_prices_daily")
            target_date = cur.fetchone()[0]

        if not target_date:
            send_telegram("❌ Continuation Momentum Scanner: No data found in silver.s_stock_prices_daily.")
            return

        logger.info(f"Scanning for continuation momentum on {target_date}")

        # ── Step 1: Get yesterday's gainers ───────────────────────────────
        query_gainers = """
            SELECT
                symbol, date, open, high, low, close,
                volume, change, change_percent, market_cap
            FROM silver.s_stock_prices_daily
            WHERE date = %s
              AND change_percent >= %s
            ORDER BY change_percent DESC
        """
        df_yesterday = pd.read_sql_query(query_gainers, conn, params=(target_date, GAIN_THRESHOLD))

        if df_yesterday.empty:
            logger.info(f"No stocks with +{GAIN_THRESHOLD}% on {target_date}")
            send_telegram(
                f"📊 <b>Continuation Momentum Scanner</b>\n\n"
                f"No stocks found with +{GAIN_THRESHOLD}% gains on {target_date}."
            )
            return

        logger.info(f"Found {len(df_yesterday)} gainers on {target_date}")

        # ── Step 2: Find which had prior momentum ─────────────────────────
        symbols = df_yesterday["symbol"].tolist()
        lookback_start = pd.to_datetime(target_date) - timedelta(days=LOOKBACK_DAYS * 2)

        query_historical = """
            SELECT symbol, date, change_percent
            FROM silver.s_stock_prices_daily
            WHERE symbol = ANY(%s)
              AND date < %s
              AND date >= %s
              AND change_percent >= %s
            ORDER BY symbol, date DESC
        """
        df_historical = pd.read_sql_query(
            query_historical, conn,
            params=(symbols, target_date, lookback_start.date(), GAIN_THRESHOLD)
        )

        symbols_with_momentum = set(df_historical["symbol"].unique())
        df_continuation = df_yesterday[df_yesterday["symbol"].isin(symbols_with_momentum)].copy()

        if df_continuation.empty:
            send_telegram(
                f"📊 <b>Continuation Momentum Scanner</b>\n\n"
                f"Found {len(df_yesterday)} stocks with +{GAIN_THRESHOLD}% on {target_date}, "
                f"but none had prior momentum in the last {LOOKBACK_DAYS} days."
            )
            return

        # ── Step 3: Enrich with momentum metadata ─────────────────────────
        target_date_dt = pd.to_datetime(target_date)
        df_continuation["last_5pct_day"]           = None
        df_continuation["days_since_last"]          = None
        df_continuation["num_5pct_days_in_lookback"] = None

        for idx, row in df_continuation.iterrows():
            symbol    = row["symbol"]
            prev      = df_historical[df_historical["symbol"] == symbol]["date"].tolist()
            if prev:
                most_recent = max(prev)
                df_continuation.at[idx, "last_5pct_day"]            = most_recent
                df_continuation.at[idx, "days_since_last"]           = (target_date_dt - pd.to_datetime(most_recent)).days
                df_continuation.at[idx, "num_5pct_days_in_lookback"] = len(prev)

        logger.info(f"Continuation momentum stocks: {len(df_continuation)}")

        # ── Step 4: Send Telegram notifications ───────────────────────────
        summary = (
            f"🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔\n🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔\n <b>Continuation Momentum Scanner</b>\n🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔\n🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔\n\n"
            f"Found <b>{len(df_continuation)}</b> stocks on {target_date}\n\n"
            f"Average gain: {df_continuation['change_percent'].mean():.2f}%\n"
            f"Biggest gainer: {df_continuation.loc[df_continuation['change_percent'].idxmax(), 'symbol']} "
            f"(+{df_continuation['change_percent'].max():.2f}%)"
        )
        send_telegram(summary)

        for _, row in df_continuation.iterrows():
            symbol       = row["symbol"]
            change_pct   = row["change_percent"]
            volume       = row["volume"]
            market_cap   = row["market_cap"]
            last_day     = row["last_5pct_day"] or "N/A"
            days_since   = int(row["days_since_last"]) if pd.notna(row["days_since_last"]) else "N/A"
            num_days     = int(row["num_5pct_days_in_lookback"]) if pd.notna(row["num_5pct_days_in_lookback"]) else "N/A"

            url = f"http://192.168.1.102/stocks/company/{symbol}"

            message = (
                f"<b>{symbol}</b> +<b>{change_pct:.2f}%</b> on {target_date}\n\n"
                f"<a href=\"{url}\">📊 View {symbol} Details</a>\n\n"
                f"- Volume: {int(volume):,}\n"
                f"- Market Cap: ${market_cap:,.0f}\n\n"
                f"- Last 5% day: {last_day}\n"
                f"- Days since last 5%: {days_since}\n"
                f"- # of 5% days in past {LOOKBACK_DAYS}d: {num_days}"
            )
            send_telegram(message)
            time.sleep(0.5)

        logger.info(f"Sent {len(df_continuation) + 1} Telegram notifications")

    finally:
        conn.close()


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="telegram_momentum_alert",
    description="Finds continuation momentum stocks and sends Telegram alerts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "momentum", "telegram"],
) as dag:

    telegram_scan = PythonOperator(
        task_id="find_and_notify",
        python_callable=find_and_notify,
        execution_timeout=timedelta(minutes=10),
    )

    task_trigger_watchlist_alerts = TriggerDagRunOperator(
        task_id="trigger_watchlist_alerts",
        trigger_dag_id="telegram_watchlist_notifier",
        wait_for_completion=False,
    )

    telegram_scan >> task_trigger_watchlist_alerts