from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    "owner": "airflow",
    "depend_on_past": False,
    "retries": 1,
    "retries_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    #schedule_interval="@daily",     # or "@hourly" depending on your needs
    schedule_interval=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

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


    dbt_test >> dbt_snapshot >> dbt_run_intermediate