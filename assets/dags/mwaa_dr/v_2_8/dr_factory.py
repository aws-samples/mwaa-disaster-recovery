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
from mwaa_dr.v_2_7.dr_factory import DRFactory_2_7


class DRFactory_2_8(DRFactory_2_7):
    """
    Factory class for creating database models for Apache Airflow 2.8.0.

    This class inherits from DRFactory_2_7 and extends it to support the new
    features and schema changes introduced in Apache Airflow 2.8.0: https://airflow.apache.org/docs/apache-airflow/2.8.1/database-erd-ref.html

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.
    """

    def dag_run(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Create a BaseTable model for the dag_run table in Apache Airflow 2.8.1.
        In particular, adds the `clear_number` field to the 2.7.3 dag_run table.
        Args:
            model (DependencyModel[BaseTable]): The dependency model for the dag_run table.

        Returns:
            BaseTable: The BaseTable model for the dag_run table.
        """
        return BaseTable(
            name="dag_run",
            model=model,
            columns=[
                "clear_number",  # New Field
                "conf",
                "creating_job_id",
                "dag_hash",
                "dag_id",
                "data_interval_end",
                "data_interval_start",
                "end_date",
                "execution_date",
                "external_trigger",
                "last_scheduling_decision",
                "log_template_id",
                "queued_at",
                "run_id",
                "run_type",
                "start_date",
                "state",
                "updated_at",
            ],
            export_mappings={"conf": "'\\x' || encode(conf,'hex') as conf"},
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def log(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Create a BaseTable model for the log table in Apache Airflow 2.8.1.
        In particular, adds the `owner_display_name` field to the 2.7.3 log table.
        Args:
            model (DependencyModel[BaseTable]): The dependency model for the log table.

        Returns:
            BaseTable: The BaseTable model for the log table.
        """
        return BaseTable(
            name="log",
            model=model,
            columns=[
                "dag_id",
                "dttm",
                "event",
                "execution_date",
                "extra",
                "map_index",
                "owner",
                "owner_display_name",  # New Field
                "task_id",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
