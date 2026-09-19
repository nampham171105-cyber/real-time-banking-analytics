from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

RAW_LOADED_DATASET = Dataset("snowflake://banking/raw/loaded")

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2 — tự động chạy ngay khi minio_to_snowflake_banking load xong dữ liệu mới",
    schedule=[RAW_LOADED_DATASET],   # thay cho schedule_interval cố định — chạy đúng lúc có dữ liệu mới, không sớm không muộn
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/banking_dbt && /opt/airflow/dbt_venv/bin/dbt run --select staging --profiles-dir /home/airflow/.dbt"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/banking_dbt && /opt/airflow/dbt_venv/bin/dbt test --select staging --profiles-dir /home/airflow/.dbt"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && /opt/airflow/dbt_venv/bin/dbt snapshot --profiles-dir /home/airflow/.dbt"
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/airflow/banking_dbt && /opt/airflow/dbt_venv/bin/dbt run --select intermediate --profiles-dir /home/airflow/.dbt"
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command="cd /opt/airflow/banking_dbt && /opt/airflow/dbt_venv/bin/dbt run --select mart --profiles-dir /home/airflow/.dbt"
    )

    dbt_run_staging >> dbt_test >> dbt_snapshot >> dbt_run_intermediate >> dbt_run_mart