# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Unit tests for DRFactory_3_0.

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

from sure import expect

from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.active_dag_table import ActiveDagTable
from mwaa_dr.framework.model.connection_table import ConnectionTable
from mwaa_dr.framework.model.variable_table import VariableTable
from mwaa_dr.v_3_0.dr_factory import DRFactory_3_0


# Tables expected in Airflow 3.0
EXPECTED_TABLE_NAMES = {
    "variable",
    "connection",
    "slot_pool",
    "log",
    "job",
    "dag_run",
    "trigger",
    "dag_version",
    "dag_code",
    "asset",
    "backfill",
    "task_instance",
    "dag_run_note",
    "asset_event",
    "backfill_dag_run",
    "task_fail",
    "xcom",
    "task_instance_history",
    "task_instance_note",
    "active_dag",
}

# Tables removed in Airflow 3.0 (should NOT be present)
REMOVED_TABLE_NAMES = {
    "serialized_dag",
    "sla_miss",
    "rendered_task_instance_fields",
}

# New tables added in Airflow 3.0
NEW_TABLE_NAMES = {
    "dag_version",
    "dag_code",
    "asset",
    "asset_event",
    "backfill",
    "backfill_dag_run",
    "dag_run_note",
    "task_instance_note",
    "task_instance_history",
}


class TestDRFactory_3_0:
    def test_setup_tables_returns_correct_table_set(self):
        """Test that setup_tables returns all expected Airflow 3.0 tables."""
        factory = DRFactory_3_0("dag")
        tables = factory.setup_tables(factory.model)
        table_names = {t.name for t in tables}

        expect(table_names).to.equal(EXPECTED_TABLE_NAMES)

    def test_setup_tables_includes_new_airflow_3_tables(self):
        """Test that all new Airflow 3.0 tables are present."""
        factory = DRFactory_3_0("dag")
        tables = factory.setup_tables(factory.model)
        table_names = {t.name for t in tables}

        for name in NEW_TABLE_NAMES:
            expect(table_names).to.contain(name)

    def test_excluded_tables_are_absent(self):
        """Test that tables removed in Airflow 3.0 are not present."""
        factory = DRFactory_3_0("dag")
        tables = factory.setup_tables(factory.model)
        table_names = {t.name for t in tables}

        for name in REMOVED_TABLE_NAMES:
            expect(table_names).should_not.contain(name)

    def test_task_instance_depends_on_job_trigger_dag_run(self):
        """Test task_instance depends on job, trigger, and dag_run."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        task_instance = model.search("name", "task_instance")
        job = model.search("name", "job")
        trigger = model.search("name", "trigger")
        dag_run = model.search("name", "dag_run")

        expect(model.dependents(task_instance)).to.equal({job, trigger, dag_run})

    def test_task_fail_depends_on_task_instance_and_dag_run(self):
        """Test task_fail depends on task_instance and dag_run."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        task_fail = model.search("name", "task_fail")
        task_instance = model.search("name", "task_instance")
        dag_run = model.search("name", "dag_run")

        expect(model.dependents(task_fail)).to.equal({task_instance, dag_run})

    def test_xcom_depends_on_task_instance_and_dag_run(self):
        """Test xcom depends on task_instance and dag_run."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        xcom = model.search("name", "xcom")
        task_instance = model.search("name", "task_instance")
        dag_run = model.search("name", "dag_run")

        expect(model.dependents(xcom)).to.equal({task_instance, dag_run})

    def test_dag_code_depends_on_dag_version(self):
        """Test dag_code depends on dag_version."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        dag_code = model.search("name", "dag_code")
        dag_version = model.search("name", "dag_version")

        expect(model.dependents(dag_code)).to.equal({dag_version})

    def test_dag_run_note_depends_on_dag_run(self):
        """Test dag_run_note depends on dag_run."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        dag_run_note = model.search("name", "dag_run_note")
        dag_run = model.search("name", "dag_run")

        expect(model.dependents(dag_run_note)).to.equal({dag_run})

    def test_asset_event_depends_on_asset(self):
        """Test asset_event depends on asset."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        asset_event = model.search("name", "asset_event")
        asset = model.search("name", "asset")

        expect(model.dependents(asset_event)).to.equal({asset})

    def test_backfill_dag_run_depends_on_backfill_and_dag_run(self):
        """Test backfill_dag_run depends on backfill and dag_run."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        backfill_dag_run = model.search("name", "backfill_dag_run")
        backfill = model.search("name", "backfill")
        dag_run = model.search("name", "dag_run")

        expect(model.dependents(backfill_dag_run)).to.equal({backfill, dag_run})

    def test_task_instance_history_depends_on_task_instance(self):
        """Test task_instance_history depends on task_instance."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        ti_history = model.search("name", "task_instance_history")
        task_instance = model.search("name", "task_instance")

        expect(model.dependents(ti_history)).to.equal({task_instance})

    def test_task_instance_note_depends_on_task_instance(self):
        """Test task_instance_note depends on task_instance."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        ti_note = model.search("name", "task_instance_note")
        task_instance = model.search("name", "task_instance")

        expect(model.dependents(ti_note)).to.equal({task_instance})

    def test_active_dag_is_sink_node(self):
        """Test active_dag depends on all other tables (sink node)."""
        factory = DRFactory_3_0("dag")
        tables = factory.setup_tables(factory.model)
        model = factory.model

        active_dag = model.search("name", "active_dag")
        all_other_tables = {t for t in tables if t.name != "active_dag"}

        expect(model.dependents(active_dag)).to.equal(all_other_tables)

    def test_sources_are_tables_with_no_prerequisites(self):
        """Test that source nodes have no prerequisites in the dependency model."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        sources = model.sources()
        source_names = {s.name for s in sources}

        # Sources are tables with no entries in reverse_graph (no prerequisites).
        # This includes level 0 tables and level 1 tables that don't depend on
        # any other table (only active_dag depends on them, but they don't
        # depend on anything themselves).
        expected_sources = {
            "variable", "connection", "slot_pool", "log",
            "job", "dag_run", "trigger", "dag_version", "asset", "backfill",
        }
        expect(source_names).to.equal(expected_sources)

    def test_sinks_is_active_dag(self):
        """Test that the only sink node is active_dag."""
        factory = DRFactory_3_0("dag")
        factory.setup_tables(factory.model)
        model = factory.model

        sinks = model.sinks()
        sink_names = {s.name for s in sinks}

        expect(sink_names).to.equal({"active_dag"})
