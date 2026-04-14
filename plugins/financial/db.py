# /plugins/financial/db.py

from airflow.providers.postgres.hooks.postgres import PostgresHook

CON_PARAM = 'postgres_financial'

def get_companies():
    hook = PostgresHook(postgres_conn_id=CON_PARAM)
    result = hook.get_records("""
        SELECT symbol FROM gold.g_company
        ORDER BY symbol
    """)
    return [row[0] for row in result]