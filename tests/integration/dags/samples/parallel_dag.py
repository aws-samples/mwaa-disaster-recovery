from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "parallel_dag", default_args=default_args, schedule_interval=None, catchup=False
) as dag:

    extract_a = BashOperator(task_id="extract_a", bash_command="sleep 1")

    extract_b = BashOperator(
        task_id="extract_b",
        bash_command="sleep 1",
    )

    load_a = BashOperator(task_id="load_a", bash_command="sleep 1")

    load_b = BashOperator(task_id="load_b", bash_command="sleep 1")

    transform = BashOperator(task_id="transform", bash_command="sleep 1")

    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform
