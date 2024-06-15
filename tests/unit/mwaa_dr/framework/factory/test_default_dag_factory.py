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

from airflow.version import version

from airflow.exceptions import AirflowFailException
from sure import expect

from mwaa_dr.framework.factory.default_dag_factory import DefaultDagFactory


class TestDefaultDagFactory:
    def test_construction(self):
        factory = DefaultDagFactory(
            dag_id="dag", path_prefix="data", storage_type="LOCAL_FS", batch_size=1000
        )

        expect(factory.dag_id).to.equal("dag")
        expect(factory.path_prefix).to.equal("data")
        expect(factory.storage_type).to.equal("LOCAL_FS")
        expect(factory.batch_size).to.equal(1000)

    def test_setup_tables(self):
        factory = DefaultDagFactory(dag_id="dag")
        expect(factory.setup_tables(factory.model)).to.equal([])

    def test_fail_task(self):
        factory = DefaultDagFactory(dag_id="dag")
        factory.fail_task.when.called_with().should.throw(
            AirflowFailException,
            f"The DR factory does not currently support your Airflow version {version}",
        )

    def test_create_backup_dag(self):
        factory = DefaultDagFactory(dag_id="dag")

        with patch.object(factory, "create_dag") as create_dag:
            expect(factory.create_backup_dag()).to.equal(create_dag())

    def test_create_restore_dag(self):
        factory = DefaultDagFactory(dag_id="dag")

        with patch.object(factory, "create_dag") as create_dag:
            expect(factory.create_restore_dag()).to.equal(create_dag())

    def test_create_dag(self):
        factory = DefaultDagFactory(dag_id="dag")
        dag = factory.create_dag()

        expect(dag.dag_id).to.equal("dag")
        expect(len(dag.tasks)).to.equal(1)
        expect(dag.tasks[0].task_id).to.equal(
            f"Airflow-Version--{version}--Not-Supported"
        )
        expect(dag.tasks[0].python_callable).to.equal(factory.fail_task)
