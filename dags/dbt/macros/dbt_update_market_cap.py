from airflow import DAG
from cosmos.operators.local import DbtRunOperationLocalOperator
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
    dag_id="dbt_macro_update_market_cap",
    start_date=datetime(2024,1,1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["dbt", "macro"],
) as dag:

    run_macro = DbtRunOperationLocalOperator(
        task_id="run_macro",
        project_dir="/opt/airflow/dbt",
        profile_config=profile_config,
        macro_name="calculate_market_cap",
        emit_datasets=False,
    )
