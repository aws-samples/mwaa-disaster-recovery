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

from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from mwaa_dr.v_2_4.dr_factory import DRFactory_2_4


class DRFactory_2_5(DRFactory_2_4):
    """
    A factory class for creating and managing database tables and their dependencies
    for the Airflow 2.5.1 version.

    This class inherits from the `BaseDRFactory` class and is responsible for setting up
    the necessary tables and their relationships based on the Airflow 2.5.1 database
    schema: https://airflow.apache.org/docs/apache-airflow/2.5.1/database-erd-ref.html.

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.

    Attributes:
        dag_id (str): The ID of the DAG for which the tables are being set up.
        path_prefix (str): The prefix for the storage path where the table data will be stored.
        storage_type (str): The type of storage to use for the tables (e.g., 'local', 's3').
        batch_size (int): The batch size for exporting table data.
    """

    def task_instance(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'task_instance' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'task_instance' table.
        """
        return BaseTable(
            name="task_instance",
            model=model,
            columns=[
                "dag_id",
                "map_index",
                "run_id",
                "task_id",
                "duration",
                "end_date",
                "executor_config",
                "external_executor_id",
                "hostname",
                "job_id",
                "max_tries",
                "next_kwargs",
                "next_method",
                "operator",
                "pid",
                "pool",
                "pool_slots",
                "priority_weight",
                "queue",
                "queued_by_job_id",
                "queued_dttm",
                "start_date",
                "state",
                "trigger_id",
                "trigger_timeout",
                "try_number",
                "unixname",
                "updated_at",
            ],
            export_mappings={
                "executor_config": "'\\x' || encode(executor_config,'hex') as executor_config"
            },
            export_filter="state NOT IN ('running','restarting','queued','scheduled', 'up_for_retry','up_for_reschedule')",
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
