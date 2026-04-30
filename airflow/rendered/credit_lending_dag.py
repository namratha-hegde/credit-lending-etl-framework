"""
Auto-generated Airflow DAG.
DO NOT EDIT MANUALLY.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineering",
    "retries": 1,
}

with DAG(
    dag_id="credit_lending_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["credit", "dq", "etl"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline started'",
    )

    process_clients = BashOperator(
        task_id="process_clients",
        bash_command="python main.py --dataset clients --env dev",
    )

    process_credits = BashOperator(
        task_id="process_credits",
        bash_command="python main.py --dataset credits --env dev",
    )

    process_collaterals = BashOperator(
        task_id="process_collaterals",
        bash_command="python main.py --dataset collaterals --env dev",
    )

    process_market_information = BashOperator(
        task_id="process_market_information",
        bash_command="python main.py --dataset market_information --env dev",
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline finished'",
    )

    start >> [
        process_clients,
        process_credits,
        process_collaterals,
        process_market_information
    ] >> end
