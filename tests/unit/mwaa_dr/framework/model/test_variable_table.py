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
from io import StringIO

from airflow.models import Variable
from sure import expect

from mwaa_dr.framework.model.variable_table import VariableTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class TestVariableTable:
    def test_construction(self):
        model = DependencyModel()
        table = VariableTable(
            model=model, storage_type="LOCAL_FS", path_prefix="data", batch_size=1000
        )

        expect(table.name).to.equal("variable")
        expect(table.model).to.equal(model)
        expect(table.storage_type).to.equal("LOCAL_FS")
        expect(table.path_prefix).to.equal("data")
        expect(table.batch_size).to.equal(1000)

    def test_backup_with_variables(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}

        with (
            patch("sqlalchemy.orm.Session.query") as query,
            patch.object(table, "write") as write,
        ):
            query.return_value.all.return_value = [
                Variable(key="key1", val="val1", description="description1")
            ]

            table.backup(**context)

            write.assert_called_once_with("key1,val1,description1\r\n", context)

    def test_backup_without_variables(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}

        with (
            patch("sqlalchemy.orm.Session.query") as query,
            patch.object(table, "write") as write,
        ):
            query.return_value.all.return_value = []

            table.backup(**context)

            write.assert_called_once_with("", context)

    def test_restore_non_existent_variables_append(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}

        buffer = StringIO("key1,val1,description1\r\n")

        def mock_variable_get_call(key, default_var):
            if default_var:
                return default_var
            raise KeyError(f"Key [{key}] not found!")

        with (
            patch.object(table, "read", return_value=buffer),
            patch("sqlalchemy.orm.Session.__enter__") as session,
            patch("airflow.models.Variable.get", new=mock_variable_get_call),
            patch("airflow.models.Variable.set") as var_set,
        ):
            table.restore(**context)

            var_set.assert_called_once_with(
                key="key1",
                value="val1",
                description="description1",
                session=session.return_value,
            )
            session.return_value.commit.assert_called_once()

    def test_restore_existing_variables_append(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}
        buffer = StringIO("key1,val1,description1\r\n")

        def mock_variable_get_call(key, default_var):
            if key == "key1":
                return "val1"
            return default_var

        with (
            patch.object(table, "read", return_value=buffer),
            patch("sqlalchemy.orm.Session.__enter__") as session,
            patch("airflow.models.Variable.get", new=mock_variable_get_call),
            patch("airflow.models.Variable.set") as var_set,
        ):
            table.restore(**context)

            var_set.assert_not_called()
            session.return_value.commit.assert_called_once()

    def test_restore_existing_variables_replace(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}
        buffer = StringIO("key1,val1,description1\r\n")

        def mock_variable_get_call(key, default_var):
            if key == "DR_VARIABLE_RESTORE_STRATEGY":
                return "REPLACE"
            if key == "key1":
                return "val1"
            return default_var

        with (
            patch.object(table, "read", return_value=buffer),
            patch("sqlalchemy.orm.Session.__enter__") as session,
            patch("airflow.models.Variable.get", new=mock_variable_get_call),
            patch("airflow.models.Variable.set") as var_set,
        ):
            table.restore(**context)

            var_set.assert_called_once_with(
                key="key1",
                value="val1",
                description="description1",
                session=session.return_value,
            )
            session.return_value.commit.assert_called_once()

    def test_restore_existing_variables_replace_special_vars(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}
        buffer = StringIO(
            "key1,val1,description1\r\n"
            + "DR_VARIABLE_RESTORE_STRATEGY,APPEND,description2\r\n"
            "DR_CONNECTION_RESTORE_STRATEGY,APPEND,description2\r\n"
        )

        def mock_variable_get_call(key, default_var):
            if key == "DR_VARIABLE_RESTORE_STRATEGY":
                return "REPLACE"
            if key == "key1":
                return "val1"
            return default_var

        with (
            patch.object(table, "read", return_value=buffer),
            patch("sqlalchemy.orm.Session.__enter__") as session,
            patch("airflow.models.Variable.get", new=mock_variable_get_call),
            patch("airflow.models.Variable.set") as var_set,
        ):
            table.restore(**context)

            var_set.assert_called_once_with(
                key="key1",
                value="val1",
                description="description1",
                session=session.return_value,
            )
            session.return_value.commit.assert_called_once()

    def test_restore_variables_do_nothing(self):
        model = DependencyModel()
        table = VariableTable(
            model=model,
            storage_type="LOCAL_FS",
            path_prefix="data",
        )
        context = {}
        buffer = StringIO("key1,val1,description1\r\n")

        def mock_variable_get_call(key, default_var):
            if key == "DR_VARIABLE_RESTORE_STRATEGY":
                return "DO_NOTHING"
            if key == "key1":
                return "val1"
            return default_var

        with (
            patch.object(table, "read", return_value=buffer),
            patch("sqlalchemy.orm.Session.__enter__") as session,
            patch("airflow.models.Variable.get", new=mock_variable_get_call),
            patch("airflow.models.Variable.set") as var_set,
        ):
            table.restore(**context)

            var_set.assert_not_called()
            session.return_value.commit.assert_not_called()
