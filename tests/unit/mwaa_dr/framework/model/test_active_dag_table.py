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

import io

from sure import expect
from unittest.mock import patch, call
import pytest
from types import SimpleNamespace

from mwaa_dr.framework.model.active_dag_table import ActiveDagTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class TestActiveDagTable:
    def test_construction(self):
        model = DependencyModel()
        table = ActiveDagTable(
            model=model, storage_type="LOCAL_FS", path_prefix="data", batch_size=1000
        )

        expect(table.name).to.equal("active_dag")
        expect(table.model).to.equal(model)
        expect(table.storage_type).to.equal("LOCAL_FS")
        expect(table.path_prefix).to.equal("data")
        expect(table.batch_size).to.equal(1000)

    @pytest.fixture(scope="function")
    def mock_session_execution_with_contents(self):
        with patch("sqlalchemy.orm.Session") as Session:
            session = Session.return_value
            all = session.execute.return_value.all
            all.return_value = (("dag_id_1"), ("dag_id_2"))
            yield session

    @pytest.fixture(scope="function")
    def mock_session_execution_without_contents(self):
        with patch("sqlalchemy.orm.Session.execute") as execution:
            all = execution.return_value.all
            all.return_value = ()
            yield Session

    def test_backup_with_active_dags(self):
        model = DependencyModel()
        table = ActiveDagTable(
            model=model, storage_type="LOCAL_FS", path_prefix="data", batch_size=1000
        )
        context = dict()

        with (
            patch.object(table, "write") as write,
            patch("sqlalchemy.orm.Session.execute") as execute,
            patch("sqlalchemy.orm.Session.commit") as commit,
            patch("sqlalchemy.orm.Session.close") as close,
        ):
            execute.return_value.all.return_value = [
                SimpleNamespace(dag_id="dag_id_1"),
                SimpleNamespace(dag_id="dag_id_2"),
            ]
            table.backup(**context)

            expect(execute.call_count).to.be(3)
            calls = [
                call("DROP TABLE IF EXISTS active_dags"),
                call(
                    "CREATE TABLE active_dags AS SELECT dag_id FROM dag WHERE NOT is_paused AND is_active AND dag_id NOT IN ('backup_metadata', 'restore_metadata')"
                ),
                call("SELECT * FROM active_dags"),
            ]
            execute.assert_has_calls(calls)
            write.assert_called_once_with("dag_id_1\ndag_id_2\n", context)

    def test_backup_without_active_dags(self):
        model = DependencyModel()
        table = ActiveDagTable(
            model=model, storage_type="LOCAL_FS", path_prefix="data", batch_size=1000
        )
        context = dict()

        with (
            patch.object(table, "write") as write,
            patch("sqlalchemy.orm.Session.execute") as execute,
            patch("sqlalchemy.orm.Session.commit") as commit,
            patch("sqlalchemy.orm.Session.close") as close,
        ):
            execute.return_value.all.return_value = []
            table.backup(**context)

            expect(execute.call_count).to.be(3)
            calls = [
                call("DROP TABLE IF EXISTS active_dags"),
                call(
                    "CREATE TABLE active_dags AS SELECT dag_id FROM dag WHERE NOT is_paused AND is_active AND dag_id NOT IN ('backup_metadata', 'restore_metadata')"
                ),
                call("SELECT * FROM active_dags"),
            ]
            execute.assert_has_calls(calls)
            write.assert_called_once_with("", context)

    def test_restore(self):
        model = DependencyModel()
        table = ActiveDagTable(
            model=model, storage_type="LOCAL_FS", path_prefix="data", batch_size=1000
        )
        context = dict()

        with (
            io.BytesIO() as store,
            patch.object(table, "read", return_value="active_dag.csv") as read,
            patch("sqlalchemy.orm.Session.execute") as execute,
            patch("sqlalchemy.orm.Session.commit") as commit,
            patch("sqlalchemy.orm.Session.close") as close,
            patch("sqlalchemy.engine.Engine.raw_connection") as connection,
            patch("builtins.open", return_value=store),
        ):
            table.restore(**context)

            expect(read.call_count).to.be(1)

            expect(execute.call_count).to.be(2)
            expect(commit.call_count).to.be(2)
            expect(close.call_count).to.be(1)
            calls = [
                call("CREATE TABLE IF NOT EXISTS active_dags(dag_id VARCHAR(250))"),
                call(
                    "UPDATE dag d SET is_paused=false FROM active_dags ad WHERE d.dag_id = ad.dag_id"
                ),
            ]
            execute.assert_has_calls(calls)

            connection.return_value.cursor.return_value.copy_expert.assert_called_once_with(
                "COPY active_dags FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", store
            )
            expect(connection.return_value.commit.call_count).to.be(1)
            expect(connection.return_value.close.call_count).to.be(1)
