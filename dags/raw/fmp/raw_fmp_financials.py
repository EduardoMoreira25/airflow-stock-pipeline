# dags/raw/fmp/raw_fmp_financials.py

"""
Stock Prices Daily DAG

Migrated from: scripts/daily_updates/stock_prices_yfinance.py
Original cron: 30 12,21 * * 1-5

Pipeline:
    get_latest_ → fetch_and_store_prices
"""

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback