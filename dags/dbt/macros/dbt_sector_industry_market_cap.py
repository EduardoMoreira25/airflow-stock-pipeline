from airflow import DAG
from cosmos.operators.local import DbtRunLocalOperator
from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

profile_config=ProfileConfig(
    profile_name="stock_pipeline",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_financial",
        profile_args={
            "schema": "silver",
            "dbname": "financial",
        },
    )
)

with DAG(
    dag_id="dbt_g_sector_and_industry",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["dbt", "model"],
) as dag:


    run_sector_model = DbtRunLocalOperator(
        task_id="run_g_sector_market_cap_history",
        project_dir="/opt/airflow/dbt",
        profile_config=profile_config,
        models="g_sector_market_cap",   # model name (no path, no extension)
        emit_datasets=False,
    )
