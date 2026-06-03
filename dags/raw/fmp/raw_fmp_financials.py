# dags/raw/fmp/raw_fmp_financials.py

"""
Raw FMP Financials DAG

Incrementally backs up FMP financial statement data to disk as raw JSON files.

For each company in the DB, checks what files already exist in the backup
directory, fetches only missing records from the FMP API, and writes each
report as an individual JSON file named by report date and period.

Pipeline:
    get_symbols → fetch_and_store_financials → trigger_bronze_fmp_financials

Backup layout:
    /opt/airflow/backups/fmp/raw/{statement_type}/symbol={SYMBOL}/{YYYY-MM-DD}_{PERIOD}.json

Period values:
    Financial statements : Q1, Q2, Q3, Q4, FY  (mapped from FMP period field)
    Revenue segments     : Q1, Q2, Q3, Q4, FY  (inferred from quarter field or date)
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = Variable.get("BACKUP_FINANCIALS")

# (folder_name, client_method_name)
FINANCIAL_STATEMENTS = [
    ("financial_statement_balance", "get_balance_sheet"),
    ("financial_statement_cashflow", "get_cashflow_statement"),
    ("financial_statement_income", "get_income_statement"),
]

PERIODS = ["quarter", "annual"]

# Map FMP API period values to clean labels used in filenames
FMP_PERIOD_MAP = {
    "Q1": "Q1",
    "Q2": "Q2",
    "Q3": "Q3",
    "Q4": "Q4",
    "FY": "FY",
    "annual": "FY",
    "quarter": None,  # need to infer from date — see _infer_quarter()
}


# ============================================================
# TASK FUNCTIONS
# ============================================================

def get_symbols():
    from financial.db import get_companies  # type: ignore
    return get_companies()


def fetch_and_store_financials(**context):
    """
    For each symbol, fetch all missing financial statement records from FMP
    and write them to the backup directory as individual JSON files named
    {YYYY-MM-DD}_{PERIOD}.json.
    """
    import json
    import os
    import time
    from datetime import datetime as dt, timezone

    from fmp.client import FMPClient  # type: ignore

    ti = context["ti"]
    symbols = ti.xcom_pull(task_ids="get_symbols")

    if not symbols:
        raise ValueError("No symbols returned from get_symbols task")

    logger.info(f"Processing {len(symbols)} symbols")

    client = FMPClient()
    total_saved = 0
    total_errors = 0

    def _infer_quarter(date_str):
        """
        Infer quarter label from date string (YYYY-MM-DD).
        Used when FMP returns period='quarter' without a specific quarter label.
        """
        try:
            month = dt.strptime(date_str, "%Y-%m-%d").month
        except ValueError:
            return None
        if month <= 3:
            return "Q1"
        elif month <= 6:
            return "Q2"
        elif month <= 9:
            return "Q3"
        else:
            return "Q4"

    def _resolve_period(record):
        """
        Resolve the period label to use in the filename from a financial
        statement record. Checks 'period' field first, falls back to inferring
        from date if needed.
        Returns a string like 'Q1', 'Q2', 'Q3', 'Q4', 'FY' or None if unknown.
        """
        raw_period = record.get("period")
        if not raw_period:
            return None

        mapped = FMP_PERIOD_MAP.get(raw_period)
        if mapped:
            return mapped

        # period == "quarter" — need to infer from date
        if raw_period == "quarter":
            return _infer_quarter(record.get("date", ""))

        return None

    def _existing_keys(directory):
        """
        Return set of '{date}_{period}' strings (without .json) for files
        already on disk. Handles both old format (YYYY-MM-DD.json) and new
        format (YYYY-MM-DD_{PERIOD}.json) gracefully.
        """
        if not os.path.isdir(directory):
            return set()
        keys = set()
        for f in os.listdir(directory):
            if not f.endswith(".json"):
                continue
            stem = f[:-5]
            keys.add(stem)
        return keys

    def _write_json(path, data):
        """Write data as a JSON array to path, creating parent dirs as needed."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            json.dump(data, fh)

    # FMP API response fields that are metadata, not segment revenue values
    _SEGMENT_META_KEYS = {
        "date", "period", "calendarYear", "fiscalYear",
        "symbol", "reportedCurrency", "acceptedDate",
        "fillingDate", "cik", "link", "finalLink",
    }

    def _flatten_segments(api_records, segment_type, symbol, now_ts):
        """
        Transform FMP segment API response into flat records grouped by
        (date, quarter).

        FMP returns: [{"date": "YYYY-MM-DD", "SegmentA": val, ...}, ...]
        Each non-date key becomes a separate record in the output.

        Returns dict: {(date, quarter): [records...]}
        """
        by_key = {}
        for item in api_records:
            date = item.get("date")
            if not date:
                continue

            # Determine quarter label
            raw_period = item.get("period", "annual")
            quarter = item.get("calendarYear")  # sometimes present in newer API

            if not quarter:
                if raw_period == "annual":
                    quarter = "FY"
                else:
                    quarter = _infer_quarter(date)

            if not quarter:
                logger.warning(f"{symbol}: could not determine quarter for segment date {date}, skipping")
                continue

            records = []
            for key, value in item.items():
                if key in _SEGMENT_META_KEYS or value is None:
                    continue
                records.append({
                    "symbol": symbol,
                    "date": date,
                    "period": raw_period,
                    "segment_type": segment_type,
                    "segment_name": key,
                    "revenue": value,
                    "fiscal_year": int(date[:4]),
                    "quarter": quarter,
                    "created_at": now_ts,
                })

            if records:
                key = (date, quarter)
                by_key[key] = by_key.get(key, []) + records

        return by_key

    for idx, symbol in enumerate(symbols, 1):
        new_balance = new_cashflow = new_income = new_segments = 0

        try:
            # ── Financial statements (balance / cashflow / income) ────────────
            for folder_name, method_name in FINANCIAL_STATEMENTS:
                backup_dir = os.path.join(BACKUP_BASE, folder_name, f"symbol={symbol}")
                existing = _existing_keys(backup_dir)
                fetcher = getattr(client, method_name)

                for period in PERIODS:
                    try:
                        records = fetcher(symbol, period, limit=120)
                    except Exception as exc:
                        logger.warning(
                            f"[{idx}/{len(symbols)}] {symbol}: "
                            f"{method_name}({period}) failed — {exc}"
                        )
                        continue

                    if not records:
                        continue

                    for record in records:
                        date = record.get("date")
                        if not date:
                            continue

                        period_label = _resolve_period(record)
                        if not period_label:
                            logger.warning(
                                f"[{idx}/{len(symbols)}] {symbol}: "
                                f"could not resolve period for {folder_name} "
                                f"date={date}, skipping"
                            )
                            continue

                        file_key = f"{date}_{period_label}"
                        if file_key in existing:
                            continue

                        path = os.path.join(backup_dir, f"{file_key}.json")
                        _write_json(path, [record])
                        existing.add(file_key)

                        if folder_name == "financial_statement_balance":
                            new_balance += 1
                        elif folder_name == "financial_statement_cashflow":
                            new_cashflow += 1
                        elif folder_name == "financial_statement_income":
                            new_income += 1

            # ── Revenue segments ──────────────────────────────────────────────
            seg_backup_dir = os.path.join(BACKUP_BASE, "revenue_segments", f"symbol={symbol}")
            seg_existing = _existing_keys(seg_backup_dir)
            now_ts = dt.now(timezone.utc).isoformat()

            try:
                geo_raw = client.get_geographic_segment(symbol)
            except Exception as exc:
                logger.warning(
                    f"[{idx}/{len(symbols)}] {symbol}: "
                    f"get_geographic_segment failed — {exc}"
                )
                geo_raw = []

            try:
                prod_raw = client.get_product_revenue(symbol)
            except Exception as exc:
                logger.warning(
                    f"[{idx}/{len(symbols)}] {symbol}: "
                    f"get_product_revenue failed — {exc}"
                )
                prod_raw = []

            geo_by_key  = _flatten_segments(geo_raw,  "geography", symbol, now_ts)
            prod_by_key = _flatten_segments(prod_raw, "product",   symbol, now_ts)

            all_keys = set(geo_by_key) | set(prod_by_key)
            for (date, quarter) in sorted(all_keys):
                file_key = f"{date}_{quarter}"
                if file_key in seg_existing:
                    continue
                combined = geo_by_key.get((date, quarter), []) + prod_by_key.get((date, quarter), [])
                if not combined:
                    continue
                path = os.path.join(seg_backup_dir, f"{file_key}.json")
                _write_json(path, combined)
                seg_existing.add(file_key)
                new_segments += 1

            total_saved += new_balance + new_cashflow + new_income + new_segments
            logger.info(
                f"[{idx}/{len(symbols)}] {symbol}: "
                f"+{new_balance} balance, +{new_income} income, "
                f"+{new_cashflow} cashflow, +{new_segments} segments"
            )

        except Exception as exc:
            logger.error(f"[{idx}/{len(symbols)}] {symbol}: ERROR — {exc}")
            total_errors += 1

        time.sleep(0.2)

    logger.info(f"Complete — {total_saved} files written, {total_errors} symbol errors")

    if total_errors > 0 and total_saved == 0:
        raise Exception(f"All {total_errors} symbols failed. Check logs.")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="raw_fmp_financials",
    description="Incrementally backs up FMP financial statements to raw JSON files",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=daily_args,
    tags=["fmp", "raw", "financials", "backup"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    task_get_symbols = PythonOperator(
        task_id="get_symbols",
        python_callable=get_symbols,
    )

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_financials",
        python_callable=fetch_and_store_financials,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )

    task_trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_fmp_financials",
        trigger_dag_id="bronze_fmp_financials",
        wait_for_completion=False,
    )

    task_get_symbols >> task_fetch_and_store >> task_trigger_bronze