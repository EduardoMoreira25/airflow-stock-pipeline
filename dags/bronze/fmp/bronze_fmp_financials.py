# dags/bronze/fmp/bronze_fmp_financials.py

"""
Bronze FMP Financials DAG

Loads raw financial statement JSON files from disk into the bronze Postgres
tables. For each table, checks the latest date already loaded per symbol and
only inserts newer records. Fully idempotent via ON CONFLICT DO NOTHING.

Pipeline:
    load_balance → load_income → load_cashflow   (run in sequence to share one DB connection pool)

Source:
    /opt/airflow/backups/fmp/raw/{statement_type}/symbol={SYMBOL}/{YYYY-MM-DD}.json

Target tables (already exist):
    bronze.financial_statement_balance
    bronze.financial_statement_income
    bronze.financial_statement_cashflow
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from utils.dag_defaults import daily_args, sla_miss_callback


logger = logging.getLogger(__name__)

BACKUP_BASE = Variable.get("BACKUP_FINANCIALS")

# Columns to extract from each JSON record (order matches INSERT statement).
# Excludes JSON-only fields: cik, accepted_date, created_at, updated_at.
# _loaded_at and _source_file are added by the loader, not from the JSON.

BALANCE_COLS = [
    "symbol", "date", "period", "fiscal_year", "filing_date",
    "goodwill", "net_debt", "prepaids", "inventory", "tax_assets",
    "total_debt", "common_stock", "other_assets", "tax_payables",
    "total_assets", "total_equity", "long_term_debt", "other_payables",
    "total_payables", "treasury_stock", "net_receivables", "preferred_stock",
    "short_term_debt", "account_payables", "accrued_expenses", "deferred_revenue",
    "intangible_assets", "minority_interest", "other_liabilities", "other_receivables",
    "reported_currency", "retained_earnings", "total_investments", "total_liabilities",
    "accounts_receivables", "other_current_assets", "total_current_assets",
    "long_term_investments", "short_term_investments", "other_non_current_assets",
    "total_non_current_assets", "capital_lease_obligations", "cash_and_cash_equivalents",
    "other_current_liabilities", "total_current_liabilities", "total_stockholders_equity",
    "additional_paid_in_capital", "deferred_revenue_non_current",
    "property_plant_equipment_net", "other_non_current_liabilities",
    "total_non_current_liabilities", "goodwill_and_intangible_assets",
    "cash_and_short_term_investments", "other_total_stockholders_equity",
    "capital_lease_obligations_current", "total_liabilities_and_total_equity",
    "deferred_tax_liabilities_non_current", "accumulated_other_comprehensive_income_loss",
]

INCOME_COLS = [
    "symbol", "date", "period", "fiscal_year", "filing_date",
    "eps", "ebit", "ebitda", "revenue", "net_income", "eps_diluted",
    "gross_profit", "other_expenses", "cost_of_revenue", "interest_income",
    "interest_expense", "operating_income", "cost_and_expenses", "income_before_tax",
    "reported_currency", "income_tax_expense", "operating_expenses",
    "net_interest_income", "net_income_deductions", "bottom_line_net_income",
    "weighted_average_shs_out", "weighted_average_shs_out_dil",
    "depreciation_and_amortization", "selling_and_marketing_expenses",
    "other_adjustments_to_net_income", "total_other_income_expenses_net",
    "research_and_development_expenses", "general_and_administrative_expenses",
    "net_income_from_continuing_operations", "net_income_from_discontinued_operations",
    "non_operating_income_excluding_interest", "selling_general_and_administrative_expenses",
]

CASHFLOW_COLS = [
    "symbol", "date", "period", "fiscal_year", "filing_date",
    "net_income", "interest_paid", "free_cash_flow", "acquisitions_net",
    "accounts_payables", "income_taxes_paid", "net_debt_issuance",
    "reported_currency", "net_change_in_cash", "net_dividends_paid",
    "net_stock_issuance", "capital_expenditure", "deferred_income_tax",
    "operating_cash_flow", "accounts_receivables", "other_non_cash_items",
    "cash_at_end_of_period", "common_dividends_paid", "common_stock_issuance",
    "other_working_capital", "common_stock_repurchased", "preferred_dividends_paid",
    "purchases_of_investments", "stock_based_compensation", "change_in_working_capital",
    "net_common_stock_issuance", "other_financing_activities", "other_investing_activities",
    "cash_at_beginning_of_period", "long_term_net_debt_issuance",
    "net_preferred_stock_issuance", "short_term_net_debt_issuance",
    "depreciation_and_amortization", "effect_of_forex_changes_on_cash",
    "sales_maturities_of_investments", "net_cash_provided_by_financing_activities",
    "net_cash_provided_by_investing_activities", "net_cash_provided_by_operating_activities",
    "investments_in_property_plant_and_equipment",
]

STATEMENTS = [
    {
        "backup_folder": "financial_statement_balance",
        "table":         "bronze.financial_statement_balance",
        "columns":       BALANCE_COLS,
        "task_id":       "load_balance",
    },
    {
        "backup_folder": "financial_statement_income",
        "table":         "bronze.financial_statement_income",
        "columns":       INCOME_COLS,
        "task_id":       "load_income",
    },
    {
        "backup_folder": "financial_statement_cashflow",
        "table":         "bronze.financial_statement_cashflow",
        "columns":       CASHFLOW_COLS,
        "task_id":       "load_cashflow",
    },
]


# ============================================================
# SHARED LOADER
# ============================================================

def _load_statement(backup_folder, table, columns):
    """
    Generic loader for a single financial statement type.
    1. Queries DB for the max loaded date per symbol.
    2. Walks the backup directory, skipping files already covered.
    3. Inserts new records with ON CONFLICT (symbol, date, period) DO NOTHING.
    """
    import json
    import os

    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    hook = PostgresHook(postgres_conn_id="postgres_financial")
    conn = hook.get_conn()

    col_list   = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"""
        INSERT INTO {table} ({col_list}, _loaded_at, _source_file)
        VALUES ({placeholders}, NOW(), %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
    """

    backup_dir = os.path.join(BACKUP_BASE, backup_folder)
    inserted = skipped = errors = 0

    try:
        # ── Latest date per symbol already in the table ────────────────────
        max_dates = {}
        with conn.cursor() as cur:
            cur.execute(f"SELECT symbol, MAX(date) FROM {table} GROUP BY symbol")
            for symbol, max_date in cur.fetchall():
                max_dates[symbol] = max_date  # date object or None

        logger.info(f"[{table}] {len(max_dates)} symbols already have data")

        if not os.path.isdir(backup_dir):
            logger.warning(f"Backup directory not found: {backup_dir}")
            return

        symbol_dirs = sorted(os.listdir(backup_dir))

        for symbol_dir in symbol_dirs:
            if not symbol_dir.startswith("symbol="):
                continue
            symbol = symbol_dir[len("symbol="):]
            sym_path = os.path.join(backup_dir, symbol_dir)
            max_date = max_dates.get(symbol)  # None if symbol not in DB yet

            for filename in sorted(os.listdir(sym_path)):
                if not filename.endswith(".json"):
                    continue

                file_date_str = filename[:-5]  # "YYYY-MM-DD"

                # Skip files whose date is already covered
                if max_date and file_date_str <= str(max_date):
                    skipped += 1
                    continue

                source_file = os.path.join(sym_path, filename)
                try:
                    with open(source_file) as fh:
                        raw = json.load(fh)

                    record = raw[0] if isinstance(raw, list) and raw else raw
                    values = [record.get(col) for col in columns]

                    with conn.cursor() as cur:
                        cur.execute(insert_sql, values + [source_file])
                        was_inserted = cur.rowcount > 0

                    conn.commit()
                    inserted += was_inserted
                    skipped  += not was_inserted

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
# TASK FUNCTIONS  (one per statement type for independent retries)
# ============================================================

def load_balance():
    s = STATEMENTS[0]
    _load_statement(s["backup_folder"], s["table"], s["columns"])


def load_income():
    s = STATEMENTS[1]
    _load_statement(s["backup_folder"], s["table"], s["columns"])


def load_cashflow():
    s = STATEMENTS[2]
    _load_statement(s["backup_folder"], s["table"], s["columns"])


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

    task_load_balance >> task_load_income >> task_load_cashflow
