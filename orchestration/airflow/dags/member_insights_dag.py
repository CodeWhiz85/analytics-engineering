from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="member_insights_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["analytics", "dbt", "duckdb"],
) as dag:

    run_flow = BashOperator(
        task_id="run_flow",
        bash_command="python {{ var.value.repo_path }}/orchestration/flow.py",
    )

    run_flow
