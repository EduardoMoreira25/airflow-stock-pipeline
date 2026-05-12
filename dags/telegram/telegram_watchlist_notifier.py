# dags/telegram/telegram_watchlist_notifier.py

"""
Watchlist Notifier DAG

Triggered by silver_stock_prices_daily after prices are loaded.
Reads public.watchlist from postgres_insights_db and compares each
symbol's current price (from silver.s_stock_prices_daily) against
buy/sell targets.

Sends a BUY alert  when current_price <= buy_price
Sends a SELL alert when current_price >= sell_price
Sends nothing if no targets are set or no conditions are met.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


# ============================================================
# TASK FUNCTIONS
# ============================================================

def check_watchlist_and_notify():
    import requests
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID   = Variable.get("TELEGRAM_CHAT_ID")

    def send_telegram(message):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            response = requests.post(url, data=payload)
            if response.status_code != 200:
                logger.error(f"Telegram error: {response.text}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")

    # ── Load watchlist entries that have at least one target price ─────────
    insights_hook = PostgresHook(postgres_conn_id="postgres_insights_db")
    with insights_hook.get_conn() as insights_conn:
        with insights_conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, buy_price, sell_price, note
                FROM public.watchlist
                WHERE buy_price IS NOT NULL OR sell_price IS NOT NULL
            """)
            watchlist = cur.fetchall()

    if not watchlist:
        logger.info("No watchlist entries with buy/sell targets — nothing to check.")
        return

    symbols = [row[0] for row in watchlist]
    logger.info(f"Checking {len(symbols)} watchlist symbols: {symbols}")

    # ── Load most recent prices from silver ───────────────────────────────
    financial_hook = PostgresHook(postgres_conn_id="postgres_financial")
    with financial_hook.get_conn() as fin_conn:
        with fin_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (symbol)
                    symbol, close, date
                FROM silver.s_stock_prices_daily
                WHERE symbol = ANY(%s)
                ORDER BY symbol, date DESC
            """, (symbols,))
            price_rows = cur.fetchall()

    prices = {row[0]: {"close": float(row[1]), "date": row[2]} for row in price_rows}

    # ── Evaluate each watchlist entry ─────────────────────────────────────
    alerts_sent = 0

    for symbol, buy_price, sell_price, note in watchlist:
        if symbol not in prices:
            logger.warning(f"{symbol}: no recent price data found, skipping.")
            continue

        current_price = prices[symbol]["close"]
        price_date    = prices[symbol]["date"]

        logger.info(
            f"{symbol}: current={current_price}, buy_target={buy_price}, sell_target={sell_price}"
        )

        note_line = f"\n📝 Note: {note}" if note else ""

        if buy_price is not None and current_price <= float(buy_price):
            message = (
                f"🟢 <b>BUY ALERT — {symbol}</b>\n\n"
                f"Current price <b>${current_price:.2f}</b> is at or below your buy target of <b>${float(buy_price):.2f}</b>\n\n"
                f"📅 Price date: {price_date}"
                f"{note_line}"
            )
            send_telegram(message)
            alerts_sent += 1
            logger.info(f"BUY alert sent for {symbol}")

        if sell_price is not None and current_price >= float(sell_price):
            message = (
                f"🔴 <b>SELL ALERT — {symbol}</b>\n\n"
                f"Current price <b>${current_price:.2f}</b> is at or above your sell target of <b>${float(sell_price):.2f}</b>\n\n"
                f"📅 Price date: {price_date}"
                f"{note_line}"
            )
            send_telegram(message)
            alerts_sent += 1
            logger.info(f"SELL alert sent for {symbol}")

    logger.info(f"Watchlist check complete — {alerts_sent} alert(s) sent.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="telegram_watchlist_notifier",
    description="Checks watchlist buy/sell targets against latest prices and sends Telegram alerts",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["watchlist", "telegram", "alerts"],
) as dag:

    task_check_watchlist = PythonOperator(
        task_id="check_watchlist_and_notify",
        python_callable=check_watchlist_and_notify,
        execution_timeout=timedelta(minutes=5),
    )
