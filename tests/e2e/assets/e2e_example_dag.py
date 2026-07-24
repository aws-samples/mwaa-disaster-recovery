"""Example workload DAG used by the MWAA DR e2e test framework.

Uploaded to s3://<dags-bucket>/dags/ at infrastructure provisioning time so
the environment has a user DAG alongside the DR framework DAGs (which are
deployed later by the CDK stacks). Compatible with Airflow 2.10+ and 3.x.
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="e2e_example_dag",
    description="E2E test example workload",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["e2e"],
) as dag:

    @task
    def hello():
        print("hello from the e2e example DAG")

    hello()
