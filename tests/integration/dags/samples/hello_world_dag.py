from airflow import version
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow import settings
from airflow.models import Variable

from datetime import datetime


def print_conf(**context):
    print(f"Airflow Version: {version.version}")


def print_table(**context):
    session = settings.Session()
    rows = session.query(Variable).all()
    print(rows)


def print_storage_type(**context):
    storage_type = Variable.get("STORAGE_TYPE", "S3")
    print(f"Storage Type: {storage_type}")


with DAG(
    "hello_world",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    print_conf = PythonOperator(
        task_id="print_conf",
        python_callable=print_conf,
        provide_context=True,
    )

    print_table = PythonOperator(
        task_id="print_table",
        python_callable=print_table,
        provide_context=True,
    )

    print_storage_type = PythonOperator(
        task_id="print_storage_type",
        python_callable=print_storage_type,
        provide_context=True,
    )

    print_conf >> print_table >> print_storage_type
