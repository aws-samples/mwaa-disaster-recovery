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
from datetime import datetime
from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class DefaultDagFactory(BaseDRFactory):
    """
    A factory class for creating default DAGs for unsupported versions of Airflow.

    This class extends the `BaseDRFactory` class and provides a default implementation
    for creating backup and restore DAGs when no specific implementation is available.

    Args:
        dag_id (str): The ID of the DAG to be created.
        path_prefix (str, optional): The prefix for the storage path. Defaults to None.
        storage_type (str, optional): The type of storage to use. Defaults to None.
        batch_size (int, optional): The batch size for data transfer operations. Defaults to 5000.

    Attributes:
        dag_id (str): The ID of the DAG to be created.
        path_prefix (str): The prefix for the storage path.
        storage_type (str): The type of storage to use.
        batch_size (int): The batch size for data transfer operations.
    """

    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        """
        Set up tables for the DAG.

        This method is a placeholder and returns an empty list.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the tables.

        Returns:
            list[BaseTable]: An empty list.
        """
        return []

    def fail_task(self):
        """
        Raise an AirflowFailException with a message indicating that the current Airflow version is not supported.
        """
        raise AirflowFailException(
            f"The DR factory does not currently support your Airflow version {version.version}"
        )

    def create_backup_dag(self) -> DAG:
        """
        Create a backup DAG.

        This method calls the `create_dag` method to create a default backup DAG.

        Returns:
            DAG: The created backup DAG.
        """
        return self.create_dag()

    def create_restore_dag(self) -> DAG:
        """
        Create a restore DAG.

        This method calls the `create_dag` method to create a default restore DAG.

        Returns:
            DAG: The created restore DAG.
        """
        return self.create_dag()

    def create_dag(self) -> DAG:
        """
        Create a default DAG.

        This method creates a DAG with a single PythonOperator task that raises an AirflowFailException
        indicating that the current Airflow version is not supported.

        Returns:
            DAG: The created DAG.
        """
        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
        }

        with DAG(
            dag_id=self.dag_id,
            schedule=None,
            catchup=False,
            default_args=default_args,
        ) as dag:
            default_task = PythonOperator(
                task_id=f"Airflow-Version--{version.version}--Not-Supported",
                python_callable=self.fail_task,
            )

            default_task

        return dag
