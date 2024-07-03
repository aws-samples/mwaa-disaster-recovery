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

import os
import types
from copy import copy, deepcopy

from sure import expect
from unittest.mock import patch, DEFAULT
import pytest

import io
import boto3
from botocore.exceptions import ClientError

from moto import mock_aws

from mwaa_dr.framework.model.base_table import BaseTable, S3
from mwaa_dr.framework.model.dependency_model import DependencyModel


class TestBaseTable:
    def test_construction_defaults(self):
        model = DependencyModel()
        table = BaseTable(
            name="test_table",
            model=model,
        )

        expect(table.name).to.equal("test_table")
        expect(table.model).to.equal(model)
        expect(table.columns).to.equal([])
        expect(table.export_mappings).to.equal({})
        expect(table.export_filter).to.equal("")
        expect(table.storage_type).to.equal("S3")
        expect(table.path_prefix).to.be.falsy
        expect(table.batch_size).to.equal(5000)
        expect(model.nodes).to.contain(table)

    def test_construction_with_params(self):
        model = DependencyModel()
        table = BaseTable(
            name="test_table",
            model=model,
            columns=["col1", "col2"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )
        expect(table.name).to.equal("test_table")
        expect(table.model).to.equal(model)
        expect(table.columns).to.equal(["col1", "col2"])
        expect(table.export_mappings).to.equal({"col1": "col1"})
        expect(table.export_filter).to.equal("col1 = 'test'")
        expect(table.storage_type).to.equal("S3")
        expect(table.path_prefix).to.equal("test")
        expect(table.batch_size).to.equal(1000)
        expect(model.nodes).to.contain(table)

    @pytest.fixture(scope="function")
    def mock_context(self):
        conf = dict()
        conf["bucket"] = "backup-bucket"
        dag_run = types.SimpleNamespace()
        dag_run.conf = conf
        context = dict()
        context["dag_run"] = dag_run
        yield context

    def test_bucket_with_context(self, mock_context):
        expect(BaseTable.bucket(mock_context)).to.equal("backup-bucket")

    @patch("airflow.models.Variable.get", return_value="a_bucket")
    def test_bucket_without_context(self, mock):
        expect(BaseTable.bucket()).to.equal("a_bucket")

    @patch("airflow.models.Variable.get", return_value="--dummy-bucket--")
    def test_bucket_with_context_without_bucket(self, mock):
        conf = dict()

        dag_run = types.SimpleNamespace()
        dag_run.conf = conf

        context = dict()
        context["dag_run"] = dag_run

        expect(BaseTable.bucket(context)).to.equal("--dummy-bucket--")

    @patch("airflow.models.Variable.get", return_value="default")
    def test_get_config_no_context_no_var_key(self, mock):
        expect(BaseTable.config(conf_key="key", default_val="default")).to.equal(
            "default"
        )

    def test_base_table__str__and__repr__(self):
        model = DependencyModel()
        table = BaseTable(
            name="test_table",
            model=model,
            columns=["col1", "col2"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )

        expect(str(table)).to.equal("Table(test_table)")
        expect(repr(table)).to.equal("Table(test_table)")

    def test_copy(self):
        model = DependencyModel()
        table1 = BaseTable(
            name="table1",
            model=model,
            columns=["col1", "col2"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )
        table2 = copy(table1)
        expect(table1).to.equal(table2)
        expect(table1).should_not.be(table2)
        expect(table1.model).to.be(table2.model)
        expect(table1.columns).to.be(table2.columns)

    def test_deep_copy(self):
        model = DependencyModel()
        table1 = BaseTable(
            name="table1",
            model=model,
            columns=["col1", "col2"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )
        table2 = deepcopy(table1)
        expect(table1).to.equal(table2)
        expect(table1).should_not.be(table2)
        expect(table1.model).should_not.be(table2.model)
        expect(table1.columns).should_not.be(table2.columns)

    def test__eq__(self):
        model = DependencyModel()
        table1 = BaseTable(
            name="table1",
            model=model,
        )
        table2 = BaseTable(
            name="table2",
            model=model,
        )
        expect(table1 == table2).to.be.false

        table3 = BaseTable(
            name="table1",
            model=model,
        )
        expect(table1 == table3).to.be.true

        expect(table1 == "table1").to.be.false

    def test__hash__(self):
        model = DependencyModel()
        table1 = BaseTable(
            name="table1",
            model=model,
        )
        table2 = BaseTable(
            name="table2",
            model=model,
        )
        table3 = BaseTable(
            name="table1",
            model=model,
        )
        expect(hash(table1) == hash(table2)).to.be.false
        expect(hash(table1) == hash(table3)).to.be.true

    def test_rshift(self):
        model = DependencyModel()
        table1 = BaseTable(
            name="table1",
            model=model,
        )

        table2 = BaseTable(
            name="table2",
            model=model,
        )
        expect(table1 >> table2).to.equal(table2)
        expect(model.forward_graph[table1]).to.contain(table2)
        expect(model.reverse_graph[table2]).to.contain(table1)

    def test_lshift(self):
        model = DependencyModel()
        table1 = BaseTable(name="table1", model=model)

        table2 = BaseTable(name="table2", model=model)
        expect(table1 << table2).to.equal(table2)
        expect(model.forward_graph[table2]).to.contain(table1)
        expect(model.reverse_graph[table1]).to.contain(table2)

    def test_lshift_list(self):
        model = DependencyModel()
        table1 = BaseTable(name="table1", model=model)

        table2 = BaseTable(name="table2", model=model)

        table3 = BaseTable(name="table3", model=model)

        table_list = [table2, table3]
        expect(table1 << table_list).to.equal(table_list)
        expect(model.forward_graph[table2]).to.contain(table1)
        expect(model.forward_graph[table3]).to.contain(table1)
        expect(model.reverse_graph[table1]).to.contain(table2)
        expect(model.reverse_graph[table1]).to.contain(table3)

    def test_get_name(self):
        model = DependencyModel()
        table = BaseTable(
            name="table1",
            model=model,
        )
        expect(table.get_name()).to.equal("table1")

    def test_all_columns_many(self):
        model = DependencyModel()
        table = BaseTable(
            name="table1",
            model=model,
            columns=["col1", "col2"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )
        expect(table.all_columns()).to.equal("col1, col2")

    def test_all_columns_one(self):
        model = DependencyModel()
        table = BaseTable(
            name="table1",
            model=model,
            columns=["col1"],
            export_mappings={"col1": "col1"},
            export_filter="col1 = 'test'",
            storage_type="S3",
            path_prefix="test",
            batch_size=1000,
        )
        expect(table.all_columns()).to.equal("col1")

    def test_all_columns_none(self):
        model = DependencyModel()
        table = BaseTable(
            name="table",
            model=model,
            columns=[],
        )
        expect(table.all_columns()).to.equal("*")

    store: io.BytesIO
    backup_data: str

    def mock_open(self, *args):
        self.store = io.BytesIO()
        self.backup_data = ""
        return DEFAULT

    def mock_close(self, *args):
        self.backup_data = self.store.getvalue().decode("utf-8")
        self.store.close()
        return DEFAULT

    def mock_write(self, data):
        self.store.write(data)
        return DEFAULT

    @pytest.fixture(scope="function")
    def mock_table_for_s3(self):
        model = DependencyModel()
        task_instance = BaseTable(
            name="task_instance",
            model=model,
            columns=[
                "dag_id",
                "executor_config",
                "state",
            ],
            export_mappings=dict(
                executor_config="'\\x' || encode(executor_config,'hex') as executor_config"
            ),
            export_filter="state NOT IN ('running', 'restarting')",
            storage_type=S3,
            path_prefix="data",
            batch_size=1000,
        )
        return task_instance

    @pytest.fixture(scope="function")
    def mock_table_for_local_fs(self):
        model = DependencyModel()
        task_instance = BaseTable(
            name="task_instance",
            model=model,
            columns=[
                "dag_id",
                "executor_config",
                "state",
            ],
            export_mappings=dict(
                executor_config="'\\x' || encode(executor_config,'hex') as executor_config"
            ),
            export_filter="state NOT IN ('running', 'restarting')",
            storage_type="LOCAL_FS",
            path_prefix="data",
            batch_size=1000,
        )
        return task_instance

    @pytest.fixture(scope="function")
    def mock_session_execution(self):
        with patch("sqlalchemy.orm.Session.execute") as execute:
            fetchmany = execute.return_value.fetchmany
            fetchmany.side_effect = [
                (
                    ("dag_id_1", "executor_config_1", "state_1"),
                    ("dag_id_2", "executor_config_2", "state_2"),
                ),
                (),
            ]
            yield execute

    @pytest.fixture(scope="function")
    def mock_smart_open(self):
        with patch("smart_open.open", side_effect=self.mock_open) as smart_file:
            smart_file.return_value.write.side_effect = self.mock_write
            smart_file.return_value.close.side_effect = self.mock_close
            yield smart_file

    @pytest.fixture(scope="function")
    def mock_builtins_open(self):
        with patch("builtins.open", side_effect=self.mock_open) as builtins_file:
            builtins_file.return_value.write.side_effect = self.mock_write
            builtins_file.return_value.close.side_effect = self.mock_close
            yield builtins_file

    def test_backup_s3(
        self, mock_table_for_s3, mock_session_execution, mock_smart_open, mock_context
    ):
        mock_table_for_s3.backup(**mock_context)

        expect(mock_session_execution.call_count).to.equal(1)
        expect(mock_session_execution.call_args[0][0]).to.equal(
            "SELECT dag_id, '\\x' || encode(executor_config,'hex') as executor_config, state FROM task_instance WHERE state NOT IN ('running', 'restarting')"
        )

        expect(mock_smart_open.call_count).to.equal(1)

        data = (
            "dag_id_1,executor_config_1,state_1\r\n"
            + "dag_id_2,executor_config_2,state_2\r\n"
        )
        expect(self.backup_data).to.equal(data)

    def test_backup_local_fs(
        self, mock_table_for_local_fs, mock_session_execution, mock_builtins_open
    ):
        context = dict()
        mock_table_for_local_fs.backup(**context)

        expect(mock_session_execution.call_count).to.equal(1)
        expect(mock_session_execution.call_args[0][0]).to.equal(
            "SELECT dag_id, '\\x' || encode(executor_config,'hex') as executor_config, state FROM task_instance WHERE state NOT IN ('running', 'restarting')"
        )

        expect(mock_builtins_open.call_count).to.equal(1)

        data = (
            "dag_id_1,executor_config_1,state_1\r\n"
            + "dag_id_2,executor_config_2,state_2\r\n"
        )
        expect(self.backup_data).to.equal(data)

    @pytest.fixture(scope="function")
    def mock_sql_raw_connection(self):
        with patch("sqlalchemy.engine.Engine.raw_connection") as connection:
            yield connection

    @pytest.fixture(scope="function")
    def mock_s3_bucket(self):
        with mock_aws():
            bucket_name = "backup-bucket"
            data = "data"

            conn = boto3.resource("s3")
            conn.create_bucket(Bucket=bucket_name)

            s3 = boto3.client("s3")
            s3.put_object(Bucket=bucket_name, Key="data/task_instance.csv", Body=data)
            yield s3

    def test_read_from_s3(self, mock_table_for_s3, mock_s3_bucket, mock_context):
        buffer = io.StringIO("")
        with patch("smart_open.open", return_value=buffer):
            stream = mock_table_for_s3.read_from_s3(mock_context)
            expect(stream).to.be(buffer)

    def test_read_from_s3_error(self, mock_table_for_s3, mock_context):
        with mock_aws():
            mock_table_for_s3.read_from_s3.when.called_with(
                mock_context
            ).should.have.raised(OSError)

    def test_read_from_local(self, mock_table_for_local_fs):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        buffer = io.StringIO("test, running\r\n")
        with patch("builtins.open", return_value=buffer):
            expect(mock_table_for_local_fs.read_from_local()).to.be(buffer)
        del os.environ["AIRFLOW_HOME"]

    def test_read(mock_context):
        table_for_s3 = BaseTable(
            name="task_instance", model=DependencyModel(), storage_type=S3
        )
        buffer = io.StringIO("test, running\r\n")

        with patch.object(table_for_s3, "read_from_s3", return_value=buffer):
            expect(table_for_s3.read(mock_context)).to.be(buffer)

        table_for_local_fs = BaseTable(
            name="task_instance", model=DependencyModel(), storage_type="LOCAL_FS"
        )
        with patch.object(table_for_local_fs, "read_from_local", return_value=buffer):
            expect(table_for_local_fs.read(dict())).to.be(buffer)

    def test_restore_multi_columns(self, mock_context, mock_sql_raw_connection):
        task_instance = BaseTable(
            name="task_instance", model=DependencyModel(), columns=["dag_id", "state"]
        )

        with (
            io.BytesIO(b"test,running\r\n") as store,
            patch.object(task_instance, "read", return_value=store),
        ):
            task_instance.restore(**mock_context)
            expect(task_instance.read.call_count).to.equal(1)
            expect(task_instance.read.call_args[0][0]).to.equal(mock_context)
            mock_sql_raw_connection.return_value.cursor.return_value.copy_expert.assert_called_with(
                "COPY task_instance (dag_id, state) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)",
                store,
            )
            mock_sql_raw_connection.return_value.commit.assert_called_once()
            mock_sql_raw_connection.return_value.close.assert_called_once()

    def test_restore_no_columns(self, mock_context, mock_sql_raw_connection):
        task_instance = BaseTable(name="task_instance", model=DependencyModel())

        with (
            io.BytesIO(b"test,running\r\n") as store,
            patch.object(task_instance, "read", return_value=store),
        ):
            task_instance.restore(**mock_context)
            expect(task_instance.read.call_count).to.equal(1)
            expect(task_instance.read.call_args[0][0]).to.equal(mock_context)
            mock_sql_raw_connection.return_value.cursor.return_value.copy_expert.assert_called_with(
                "COPY task_instance FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", store
            )
            mock_sql_raw_connection.return_value.commit.assert_called_once()
            mock_sql_raw_connection.return_value.close.assert_called_once()

    @pytest.fixture(scope="function")
    def mock_s3_client(self):
        with mock_aws():
            bucket_name = "backup-bucket"
            conn = boto3.resource("s3")
            conn.create_bucket(Bucket=bucket_name)

            s3 = boto3.client("s3")
            yield s3

    def test_write_to_s3(self, mock_table_for_s3, mock_context, mock_s3_client):
        with mock_aws():
            mock_table_for_s3.write_to_s3("data", "task_instance", mock_context)

            s3 = boto3.resource("s3")
            task_instance_object = s3.Object("backup-bucket", "data/task_instance.csv")
            data = task_instance_object.get()["Body"].read().decode("utf-8")

            expect(data).to.equal("data")

    def test_write_to_local(self, mock_table_for_local_fs):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        try:
            os.makedirs("/tmp/dags/data")
        except OSError:
            pass

        mock_table_for_local_fs.write_to_local("data", "task_instance")
        with open("/tmp/dags/data/task_instance.csv") as file:
            expect(file.read()).to.equal("data")

        del os.environ["AIRFLOW_HOME"]

    def test_write(self, mock_context):
        table_for_s3 = BaseTable(
            name="task_instance", model=DependencyModel(), storage_type=S3
        )

        with patch.object(table_for_s3, "write_to_s3") as s3_write:
            table_for_s3.write("data", mock_context)
            s3_write.assert_called_once_with("data", "task_instance", mock_context)

        table_for_local_fs = BaseTable(
            name="task_instance", model=DependencyModel(), storage_type="LOCAL_FS"
        )
        with patch.object(table_for_local_fs, "write_to_local") as local_write:
            table_for_local_fs.write("data", dict())
            local_write.assert_called_once_with("data", "task_instance")
