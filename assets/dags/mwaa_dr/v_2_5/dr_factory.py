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

from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.framework.model.active_dag_table import ActiveDagTable
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.connection_table import ConnectionTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from mwaa_dr.framework.model.variable_table import VariableTable


class DRFactory_2_5(BaseDRFactory):
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

    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        """
        Sets up the necessary tables and their dependencies based on the Airflow 2.5.1 database schema.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the tables.

        Returns:
            list[BaseTable]: A list of BaseTable instances representing the tables and their dependencies.
        """
        active_dag = self.active_dag(model)

        variable = self.variable(model)
        connection = self.connection(model)
        slot_pool = self.slot_pool(model)

        log = self.log(model)
        job = self.job(model)
        dag_run = self.dag_run(model)
        trigger = self.trigger(model)

        task_instance = self.task_instance(model)
        task_fail = self.task_fail(model)

        task_instance << [job, trigger, dag_run]
        task_fail << [task_instance, dag_run]
        active_dag << [
            variable,
            connection,
            slot_pool,
            log,
            job,
            dag_run,
            trigger,
            task_instance,
            task_fail,
        ]

        return [
            variable,
            connection,
            slot_pool,
            log,
            job,
            dag_run,
            trigger,
            task_instance,
            task_fail,
            active_dag,
        ]

    def active_dag(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the ActiveDagTable.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the ActiveDagTable.
        """
        return ActiveDagTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def variable(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the VariableTable.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the VariableTable.
        """
        return VariableTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def connection(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the ConnectionTable.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the ConnectionTable.
        """
        return ConnectionTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def dag_run(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'dag_run' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'dag_run' table.
        """
        return BaseTable(
            name="dag_run",
            model=model,
            columns=[
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

    def slot_pool(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'slot_pool' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'slot_pool' table.
        """
        return BaseTable(
            name="slot_pool",
            model=model,
            columns=["description", "pool", "slots"],
            export_filter="pool != 'default_pool'",
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def log(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'log' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'log' table.
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
                "task_id",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def task_fail(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'task_fail' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'task_fail' table.
        """
        return BaseTable(
            name="task_fail",
            model=model,
            columns=[
                "dag_id",
                "duration",
                "end_date",
                "map_index",
                "run_id",
                "start_date",
                "task_id",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def job(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'job' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'job' table.
        """
        return BaseTable(
            name="job",
            model=model,
            columns=[
                "dag_id",
                "end_date",
                "executor_class",
                "hostname",
                "job_type",
                "latest_heartbeat",
                "start_date",
                "state",
                "unixname",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def trigger(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """
        Creates an instance of the BaseTable for the 'trigger' table.

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the table.

        Returns:
            BaseTable: An instance of the BaseTable representing the 'job' table.
        """
        return BaseTable(
            name="trigger",
            model=model,
            columns=["classpath", "created_date", "kwargs", "triggerer_id"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
