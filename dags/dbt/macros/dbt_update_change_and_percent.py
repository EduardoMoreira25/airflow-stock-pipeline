from airflow import DAG
from cosmos.operators.local import DbtRunOperationLocalOperator
from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    dag_id="dbt_macro_update_change_and_percent",
    start_date=datetime(2024,1,1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["dbt", "macro"],
) as dag:

    run_macro_change_and_percent = DbtRunOperationLocalOperator(
        task_id="run_macro_change_and_percent",
        project_dir="/opt/airflow/dbt",
        profile_config=profile_config,
        macro_name="calculate_change",
        emit_datasets=False,
    )

    task_trigger_telegram_alert = TriggerDagRunOperator(
        task_id="telegram_scan",
        trigger_dag_id="telegram_momentum_alert",
        wait_for_completion=True,
    )

    task_trigger_sector_industry_insert = TriggerDagRunOperator(
        task_id="sector_and_industry_insert",
        trigger_dag_id="dbt_g_sector_and_industry",
        wait_for_completion=True,
    )

    run_macro_change_and_percent >> [task_trigger_telegram_alert,task_trigger_sector_industry_insert]
