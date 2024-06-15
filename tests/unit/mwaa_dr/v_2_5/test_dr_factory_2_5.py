# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

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

from unittest.mock import patch

from airflow.models import DAG
from sure import expect

from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.connection_table import ConnectionTable
from mwaa_dr.framework.model.variable_table import VariableTable
from mwaa_dr.framework.model.active_dag_table import ActiveDagTable
from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.v_2_5.dr_factory import DRFactory_2_5


def check_base_table(
    factory: BaseDRFactory,
    actual_table: BaseTable,
    expected_name: str,
    expected_columns: list[str] = None,
    expected_mappings: dict[str, str] = None,
    expected_export_filter: str = None,
) -> None:
    expected_table = BaseTable(
        name=expected_name,
        model=factory.model,
        columns=expected_columns,
        export_mappings=expected_mappings,
        export_filter=expected_export_filter,
        storage_type=factory.storage_type,
        path_prefix=factory.path_prefix,
        batch_size=factory.batch_size,
    )
    expect(actual_table).to.equal(expected_table)


class TestDRFactory_2_5:
    def test_active_dag_creation(self):
        factory = DRFactory_2_5("dag")
        expected = ActiveDagTable(
            model=factory.model,
            storage_type=factory.storage_type,
            path_prefix=factory.path_prefix,
            batch_size=factory.batch_size,
        )
        actual = factory.active_dag(factory.model)

        expect(actual).to.equal(expected)

    def test_variable_creation(self):
        factory = DRFactory_2_5("dag")
        expected = VariableTable(
            model=factory.model,
            storage_type=factory.storage_type,
            path_prefix=factory.path_prefix,
            batch_size=factory.batch_size,
        )
        actual = factory.variable(factory.model)

        expect(actual).to.equal(expected)

    def test_connection_creation(self):
        factory = DRFactory_2_5("dag")
        expected = ConnectionTable(
            model=factory.model,
            storage_type=factory.storage_type,
            path_prefix=factory.path_prefix,
            batch_size=factory.batch_size,
        )
        actual = factory.connection(factory.model)

        expect(actual).to.equal(expected)

    def test_dag_run_creation(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.dag_run(factory.model),
            expected_name="dag_run",
            expected_columns=[
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
            expected_mappings={"conf": "'\\x' || encode(conf,'hex') as conf"},
        )

    def test_task_instance_creation(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.task_instance(factory.model),
            expected_name="task_instance",
            expected_columns=[
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
            expected_mappings={
                "executor_config": "'\\x' || encode(executor_config,'hex') as executor_config"
            },
            expected_export_filter="state NOT IN ('running','restarting','queued','scheduled', 'up_for_retry','up_for_reschedule')",
        )

    def test_slot_pool(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.slot_pool(factory.model),
            expected_name="slot_pool",
            expected_columns=[
                "description",
                "pool",
                "slots",
            ],
            expected_export_filter="pool != 'default_pool'",
        )

    def test_log(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.log(factory.model),
            expected_name="log",
            expected_columns=[
                "dag_id",
                "dttm",
                "event",
                "execution_date",
                "extra",
                "map_index",
                "owner",
                "task_id",
            ],
        )

    def test_task_fail(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.task_fail(factory.model),
            expected_name="task_fail",
            expected_columns=[
                "dag_id",
                "duration",
                "end_date",
                "map_index",
                "run_id",
                "start_date",
                "task_id",
            ],
        )

    def test_job(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.job(factory.model),
            expected_name="job",
            expected_columns=[
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
        )

    def test_trigger(self):
        factory = DRFactory_2_5("dag")
        check_base_table(
            factory=factory,
            actual_table=factory.trigger(factory.model),
            expected_name="trigger",
            expected_columns=["classpath", "created_date", "kwargs", "triggerer_id"],
        )

    def test_setup_tables(self):
        factory = DRFactory_2_5("dag")
        model = factory.model
        tables = factory.setup_tables(factory.model)

        active_dag = model.search("name", "active_dag")
        variable = model.search("name", "variable")
        connection = model.search("name", "connection")
        slot_pool = model.search("name", "slot_pool")
        log = model.search("name", "log")
        job = model.search("name", "job")
        dag_run = model.search("name", "dag_run")
        trigger = model.search("name", "trigger")
        task_instance = model.search("name", "task_instance")
        task_fail = model.search("name", "task_fail")

        expect(model.dependents(task_instance)).to.equal({job, trigger, dag_run})
        expect(model.dependents(active_dag)).to.equal(
            {
                variable,
                connection,
                slot_pool,
                log,
                job,
                dag_run,
                trigger,
                task_instance,
                task_fail,
            }
        )
        expect(tables).to.equal(
            [
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
        )

    # Testing BaseDRFactory.create_backup_dag and BaseDRFactory.create_restore_dag
    # in the context of DRFactory_2_5
    def test_base_dr_factory_create_backup_dag(self):
        factory = DRFactory_2_5("metadata_backup")
        model = factory.model

        with patch(
            "airflow.models.Variable.get", return_value="@hourly"
        ) as variable_get:
            dag: DAG = factory.create_backup_dag()

            expect(dag.dag_id).to.equal("metadata_backup")
            expect(dag.default_args["on_failure_callback"].__qualname__).to.equal(
                factory.notify_failure_to_sns.__qualname__
            )

            tasks = dag.tasks
            expect(len(tasks)).to.equal(12)

            setup = dag.get_task("setup")
            teardown = dag.get_task("teardown")

            variable = dag.get_task("export_tables.export_variables")
            connection = dag.get_task("export_tables.export_connections")
            slot_pool = dag.get_task("export_tables.export_slot_pools")
            log = dag.get_task("export_tables.export_logs")
            job = dag.get_task("export_tables.export_jobs")
            dag_run = dag.get_task("export_tables.export_dag_runs")
            trigger = dag.get_task("export_tables.export_triggers")
            task_instance = dag.get_task("export_tables.export_task_instances")
            task_fail = dag.get_task("export_tables.export_task_fails")
            active_dag = dag.get_task("export_tables.export_active_dags")

            expect(setup.python_callable.__qualname__).to.be(
                factory.setup_backup.__qualname__
            )
            expect(teardown.python_callable.__qualname__).to.equal(
                factory.teardown_backup.__qualname__
            )

            expect(variable.python_callable.__qualname__).to.equal(
                model.search("name", "variable").backup.__qualname__
            )
            expect(connection.python_callable.__qualname__).to.equal(
                model.search("name", "connection").backup.__qualname__
            )
            expect(slot_pool.python_callable.__qualname__).to.equal(
                model.search("name", "slot_pool").backup.__qualname__
            )
            expect(log.python_callable.__qualname__).to.equal(
                model.search("name", "log").backup.__qualname__
            )
            expect(job.python_callable.__qualname__).to.equal(
                model.search("name", "job").backup.__qualname__
            )
            expect(dag_run.python_callable.__qualname__).to.equal(
                model.search("name", "dag_run").backup.__qualname__
            )
            expect(trigger.python_callable.__qualname__).to.equal(
                model.search("name", "trigger").backup.__qualname__
            )
            expect(task_instance.python_callable.__qualname__).to.equal(
                model.search("name", "task_instance").backup.__qualname__
            )
            expect(task_fail.python_callable.__qualname__).to.equal(
                model.search("name", "task_fail").backup.__qualname__
            )
            expect(active_dag.python_callable.__qualname__).to.equal(
                model.search("name", "active_dag").backup.__qualname__
            )

            table_tasks = {
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
            }

            expect(setup.downstream_task_ids).to.equal({t.task_id for t in table_tasks})
            expect(teardown.upstream_task_ids).to.equal(
                {t.task_id for t in table_tasks}
            )

    def test_base_dr_factory_create_restore_dag(self):
        factory = DRFactory_2_5("metadata_restore")
        factory.model

        dag: DAG = factory.create_restore_dag()

        expect(dag.dag_id).to.equal("metadata_restore")
        expect(dag.default_args["on_failure_callback"].__qualname__).to.equal(
            factory.notify_failure_to_sfn.__qualname__
        )

        tasks = dag.tasks
        expect(len(tasks)).to.equal(15)

        setup = dag.get_task("setup")
        teardown = dag.get_task("teardown")
        restore_start = dag.get_task("restore_start")
        restore_end = dag.get_task("restore_end")
        notify_success = dag.get_task("notify_success")

        variable = dag.get_task("restore_variable")
        connection = dag.get_task("restore_connection")
        slot_pool = dag.get_task("restore_slot_pool")
        log = dag.get_task("restore_log")
        job = dag.get_task("restore_job")
        dag_run = dag.get_task("restore_dag_run")
        trigger = dag.get_task("restore_trigger")
        task_instance = dag.get_task("restore_task_instance")
        task_fail = dag.get_task("restore_task_fail")
        active_dag = dag.get_task("restore_active_dag")

        expect(setup.downstream_task_ids).to.equal({restore_start.task_id})
        expect(restore_start.downstream_task_ids).to.equal(
            {
                variable.task_id,
                connection.task_id,
                slot_pool.task_id,
                log.task_id,
                job.task_id,
                dag_run.task_id,
                trigger.task_id,
            }
        )
        expect(active_dag.upstream_task_ids).to.equal(
            {
                variable.task_id,
                connection.task_id,
                slot_pool.task_id,
                log.task_id,
                job.task_id,
                dag_run.task_id,
                trigger.task_id,
                task_instance.task_id,
                task_fail.task_id,
            }
        )
        expect(task_instance.upstream_task_ids).to.equal(
            {job.task_id, dag_run.task_id, trigger.task_id}
        )
        expect(task_fail.upstream_task_ids).to.equal(
            {
                dag_run.task_id,
                task_instance.task_id,
            }
        )
        expect(active_dag.downstream_task_ids).to.equal({restore_end.task_id})
        expect(restore_end.downstream_task_ids).to.equal({teardown.task_id})
        expect(teardown.downstream_task_ids).to.equal({notify_success.task_id})
