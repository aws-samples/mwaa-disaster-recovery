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

from mwaa_dr.framework.factory.glue_dr_factory import GlueDRFactory
from mwaa_dr.framework.model.active_dag_table import ActiveDagTable
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.connection_table import ConnectionTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from mwaa_dr.framework.model.variable_table import VariableTable


class DRFactory_3_0(GlueDRFactory):
    """
    Factory class for creating database models for Apache Airflow 3.0.

    This class extends GlueDRFactory to define the Airflow 3.0 metadata table
    schema. Airflow 3.0 introduces new tables (dag_version, dag_code, asset,
    asset_event, backfill, backfill_dag_run, dag_run_note, task_instance_note,
    task_instance_history) and removes tables (serialized_dag, sla_miss,
    rendered_task_instance_fields).

    Variable and connection tables are included for schema reference but are
    excluded from Glue export/import — they are handled via the MWAA REST API.

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.
    """

    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        """
        Sets up the Airflow 3.0 tables and their dependencies.

        Defines the complete Airflow 3.0 metadata table schema including:
        - Level 0 (no deps): variable, connection, slot_pool, log
        - Level 1: job, dag_run, trigger, dag_version, dag_code, asset, backfill
        - Level 2: task_instance, dag_run_note, asset_event, backfill_dag_run
        - Level 3: task_fail, xcom, task_instance_history, task_instance_note
        - Level 99 (sink): active_dag

        Args:
            model (DependencyModel[BaseTable]): The dependency model for the tables.

        Returns:
            list[BaseTable]: A list of BaseTable instances representing the tables.
        """
        active_dag = self.active_dag(model)

        # Level 0: no dependencies
        variable = self.variable(model)
        connection = self.connection(model)
        slot_pool = self.slot_pool(model)
        log = self.log(model)

        # Level 1: no inter-dependencies at this level
        job = self.job(model)
        dag_run = self.dag_run(model)
        trigger = self.trigger(model)
        dag_version = self.dag_version(model)
        asset = self.asset(model)
        backfill = self.backfill(model)

        # Level 1 with dependency on level 1
        dag_code = self.dag_code(model)

        # Level 2
        task_instance = self.task_instance(model)
        dag_run_note = self.dag_run_note(model)
        asset_event = self.asset_event(model)
        backfill_dag_run = self.backfill_dag_run(model)

        # Level 3
        task_fail = self.task_fail(model)
        xcom = self.xcom(model)
        task_instance_history = self.task_instance_history(model)
        task_instance_note = self.task_instance_note(model)

        # Wire dependencies
        dag_code << [dag_version]
        task_instance << [job, trigger, dag_run]
        dag_run_note << [dag_run]
        asset_event << [asset]
        backfill_dag_run << [backfill, dag_run]
        task_fail << [task_instance, dag_run]
        xcom << [task_instance, dag_run]
        task_instance_history << [task_instance]
        task_instance_note << [task_instance]

        # active_dag is the sink node — depends on everything
        active_dag << [
            variable,
            connection,
            slot_pool,
            log,
            job,
            dag_run,
            trigger,
            dag_version,
            dag_code,
            asset,
            backfill,
            task_instance,
            dag_run_note,
            asset_event,
            backfill_dag_run,
            task_fail,
            xcom,
            task_instance_history,
            task_instance_note,
        ]

        return [
            variable,
            connection,
            slot_pool,
            log,
            job,
            dag_run,
            trigger,
            dag_version,
            dag_code,
            asset,
            backfill,
            task_instance,
            dag_run_note,
            asset_event,
            backfill_dag_run,
            task_fail,
            xcom,
            task_instance_history,
            task_instance_note,
            active_dag,
        ]

    def active_dag(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the active_dag table model."""
        return ActiveDagTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def variable(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the variable table model (handled via REST API, not Glue)."""
        return VariableTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def connection(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the connection table model (handled via REST API, not Glue)."""
        return ConnectionTable(
            model=model,
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def slot_pool(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the slot_pool table model for Airflow 3.0."""
        return BaseTable(
            name="slot_pool",
            model=model,
            columns=["description", "include_deferred", "pool", "slots"],
            export_filter="pool != 'default_pool'",
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def log(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the log table model for Airflow 3.0."""
        return BaseTable(
            name="log",
            model=model,
            columns=[
                "dag_id",
                "dttm",
                "event",
                "extra",
                "map_index",
                "owner",
                "owner_display_name",
                "run_id",
                "task_id",
                "try_number",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def job(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the job table model for Airflow 3.0."""
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

    def dag_run(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the dag_run table model for Airflow 3.0."""
        return BaseTable(
            name="dag_run",
            model=model,
            columns=[
                "backfill_id",
                "conf",
                "dag_id",
                "dag_version_id",
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
            ],
            export_filter="dag_id != 'backup_metadata'",
            export_mappings={"conf": "'\\x' || encode(conf,'hex') as conf"},
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def trigger(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the trigger table model for Airflow 3.0."""
        return BaseTable(
            name="trigger",
            model=model,
            columns=["classpath", "created_date", "kwargs", "triggerer_id"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def dag_version(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the dag_version table model (new in Airflow 3.0)."""
        return BaseTable(
            name="dag_version",
            model=model,
            columns=["dag_id", "created_at", "version_number"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def dag_code(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the dag_code table model (new in Airflow 3.0)."""
        return BaseTable(
            name="dag_code",
            model=model,
            columns=["dag_version_id", "fileloc", "source_code"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def asset(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the asset table model (new in Airflow 3.0)."""
        return BaseTable(
            name="asset",
            model=model,
            columns=["created_at", "extra", "group", "name", "updated_at", "uri"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def asset_event(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the asset_event table model (new in Airflow 3.0)."""
        return BaseTable(
            name="asset_event",
            model=model,
            columns=["asset_id", "extra", "source_dag_id", "source_map_index",
                      "source_run_id", "source_task_id", "timestamp"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def backfill(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the backfill table model (new in Airflow 3.0)."""
        return BaseTable(
            name="backfill",
            model=model,
            columns=["completed_at", "created_at", "dag_id", "dag_run_conf",
                      "from_date", "is_reversed", "max_active_runs",
                      "reprocess_behavior", "to_date", "updated_at"],
            export_mappings={
                "dag_run_conf": "'\\x' || encode(dag_run_conf,'hex') as dag_run_conf"
            },
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def backfill_dag_run(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the backfill_dag_run table model (new in Airflow 3.0)."""
        return BaseTable(
            name="backfill_dag_run",
            model=model,
            columns=["backfill_id", "dag_run_id", "sort_ordinal"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def dag_run_note(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the dag_run_note table model (new in Airflow 3.0)."""
        return BaseTable(
            name="dag_run_note",
            model=model,
            columns=["content", "created_at", "dag_run_id", "updated_at", "user_id"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def task_instance(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the task_instance table model for Airflow 3.0."""
        return BaseTable(
            name="task_instance",
            model=model,
            columns=[
                "dag_id",
                "dag_version_id",
                "duration",
                "end_date",
                "executor",
                "executor_config",
                "external_executor_id",
                "hostname",
                "job_id",
                "map_index",
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
                "rendered_map_index",
                "run_id",
                "start_date",
                "state",
                "task_display_name",
                "task_id",
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

    def task_fail(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the task_fail table model for Airflow 3.0."""
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

    def xcom(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the xcom table model for Airflow 3.0."""
        return BaseTable(
            name="xcom",
            model=model,
            columns=[
                "dag_run_id",
                "key",
                "map_index",
                "task_id",
                "dag_id",
                "run_id",
                "timestamp",
                "value",
            ],
            export_mappings={"value": "'\\x' || encode(value,'hex') as value"},
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def task_instance_history(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the task_instance_history table model (new in Airflow 3.0)."""
        return BaseTable(
            name="task_instance_history",
            model=model,
            columns=[
                "dag_id",
                "dag_version_id",
                "duration",
                "end_date",
                "executor",
                "executor_config",
                "hostname",
                "map_index",
                "max_tries",
                "operator",
                "pid",
                "pool",
                "pool_slots",
                "priority_weight",
                "queue",
                "queued_dttm",
                "run_id",
                "start_date",
                "state",
                "task_display_name",
                "task_id",
                "try_number",
                "unixname",
            ],
            export_mappings={
                "executor_config": "'\\x' || encode(executor_config,'hex') as executor_config"
            },
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def task_instance_note(self, model: DependencyModel[BaseTable]) -> BaseTable:
        """Create the task_instance_note table model (new in Airflow 3.0)."""
        return BaseTable(
            name="task_instance_note",
            model=model,
            columns=["content", "created_at", "dag_id", "map_index",
                      "run_id", "task_id", "try_number", "updated_at", "user_id"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
