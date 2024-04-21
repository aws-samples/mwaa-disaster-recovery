"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from airflow import version
from airflow.exceptions import AirflowFailException
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class DefaultDagFactory(BaseDRFactory):
    def __init__(
        self, dag_id: str, path_prefix: str, storage_type: str = None, batch_size=5000
    ) -> None:
        super().__init__(dag_id, path_prefix, storage_type, batch_size)

    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        return []

    def fail_task(self):
        raise AirflowFailException(
            f"The DR factory does not currently support your Airflow version {version.version}"
        )

    def create_backup_dag(self) -> DAG:
        return self.create_dag()

    def create_restore_dag(self) -> DAG:
        return self.create_dag()

    def create_dag(self) -> DAG:
        default_args = {
            "owner": "airflow",
            "start_date": days_ago(1),
        }

        with DAG(
            dag_id=self.dag_id,
            schedule_interval=None,
            catchup=False,
            default_args=default_args,
        ) as dag:
            default_task = PythonOperator(
                task_id=f"Airflow-Version--{version.version}--Not-Supported",
                python_callable=self.fail_task,
            )

            default_task

        return dag
