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
import pytest
import types
import json
from unittest.mock import patch

from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from sure import expect

from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.framework.model.base_table import BaseTable


@patch.multiple(BaseDRFactory, __abstractmethods__=set())
class TestBaseDRFactory:
    def test_construction_defaults(self):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.dag_id).to.equal("dag")
        expect(factory.path_prefix).to.equal("data")
        expect(factory.storage_type).to.equal("S3")
        expect(factory.batch_size).to.equal(5000)
        expect(factory.tables_cache).to.be(None)
        expect(factory.model).to.be.truthy

    def test_construction(self):
        factory = BaseDRFactory(
            dag_id="dag", path_prefix="path", storage_type="LOCAL_FS", batch_size=1000
        )
        expect(factory.dag_id).to.equal("dag")
        expect(factory.path_prefix).to.equal("path")
        expect(factory.storage_type).to.equal("LOCAL_FS")
        expect(factory.batch_size).to.equal(1000)
        expect(factory.tables_cache).to.be(None)
        expect(factory.model).to.be.truthy

    def test_tables_uncached(self):
        factory = BaseDRFactory(dag_id="dag")

        table = BaseTable(name="test", model=factory.model)
        with patch.object(factory, "setup_tables", return_value=[table]):
            expect(factory.tables()).to.equal([table])

    def test_tables_cached(self):
        factory = BaseDRFactory(dag_id="dag")
        factory.tables_cache = [BaseTable(name="test", model=factory.model)]

        table = BaseTable(name="test", model=factory.model)
        with patch.object(factory, "setup_tables", return_value=[table]):
            expect(factory.tables()).to.equal(factory.tables_cache)

    @pytest.fixture(scope="function")
    def mock_context(self):
        conf = dict()
        conf["bucket"] = "backup-bucket"
        conf["task_token"] = "token"
        dag_run = types.SimpleNamespace(dag_id="dag1", run_id="run1", conf=conf)
        dag_run.get_task_instances = lambda: [
            types.SimpleNamespace(task_id="t1", state="running"),
            types.SimpleNamespace(task_id="t2", state="success"),
        ]
        context = dict()
        context["dag_run"] = dag_run
        yield context

    @pytest.fixture(scope="function")
    def mock_result(self):
        return {
            "dag": "dag1",
            "dag_run": "run1",
            "tasks": ["t1 => running", "t2 => success"],
        }

    def test_bucket(self, mock_context):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.bucket(mock_context)).to.equal("backup-bucket")

    def test_schedule(self):
        factory = BaseDRFactory(dag_id="dag")
        with patch.object(Variable, "get", return_value="@hourly"):
            expect(factory.schedule()).to.equal("@hourly")

    def test_token(self, mock_context):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.task_token(mock_context)).to.equal("token")

    def test_token_none(self):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.task_token({}, "default")).to.equal("default")

    def test_dag_run_result(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.dag_run_result(mock_context)).to.equal(mock_result)

    def test_notify_success_to_sfn_for_mwaa(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        mock_result["status"] = "Success"
        mock_result["location"] = "s3://backup-bucket/data"

        with patch("boto3.client") as boto3_client:
            factory.notify_success_to_sfn(**mock_context)

            boto3_client().send_task_success.assert_called_once_with(
                taskToken="token", output=json.dumps(mock_result)
            )

    def test_notify_success_to_sfn_for_local(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        os.environ["AIRFLOW_HOME"] = "/tmp"

        with patch("boto3.client") as boto3_client:
            factory.notify_success_to_sfn(**mock_context)

            boto3_client().send_task_success.assert_not_called()

    def test_notify_success_to_sfn_for_mwaa_no_token(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")
        mock_context["dag_run"].conf.pop("task_token")

        mock_result["status"] = "Success"
        mock_result["location"] = "s3://backup-bucket/data"

        with patch("boto3.client") as boto3_client:
            factory.notify_success_to_sfn(**mock_context)

            boto3_client().send_task_success.assert_not_called()

    def test_notify_failure_to_sfn_for_mwaa(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        mock_result["status"] = "Fail"

        with patch("boto3.client") as boto3_client:
            factory.notify_failure_to_sfn(mock_context)

            boto3_client().send_task_failure.assert_called_once_with(
                taskToken="token",
                error="Restore Failure",
                cause=json.dumps(mock_result),
            )

    def test_notify_failure_to_sfn_for_local(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        mock_result["status"] = "Fail"

        with patch("boto3.client") as boto3_client:
            factory.notify_failure_to_sfn(mock_context)

            boto3_client().send_task_failure.assert_not_called()

    def test_notify_failure_to_sfn_for_mwaa_no_token(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")
        mock_context["dag_run"].conf.pop("task_token")

        mock_result["status"] = "Fail"

        with patch("boto3.client") as boto3_client:
            factory.notify_failure_to_sfn(mock_context)

            boto3_client().send_task_failure.assert_not_called()

    def test_notify_failure_to_sns_mwaa(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        mock_result["status"] = "Fail"

        with (
            patch("boto3.client") as boto3_client,
            patch("airflow.models.Variable.get", return_value="topic-arn"),
        ):
            factory.notify_failure_to_sns(mock_context)

            boto3_client().publish.assert_called_once_with(
                TopicArn="topic-arn",
                Subject="MWAA DR DAG Failure",
                Message=json.dumps(mock_result, indent=2),
            )

    def test_notify_failure_to_sns_mwaa_topic_missing(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        mock_result["status"] = "Fail"

        with (
            patch("boto3.client") as boto3_client,
            patch("airflow.models.Variable.get", return_value="--missing--"),
        ):
            factory.notify_failure_to_sns(mock_context)

            boto3_client().publish.assert_not_called()

    def test_notify_failure_to_sns_local(self, mock_context, mock_result):
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        mock_result["status"] = "Fail"

        with (
            patch("boto3.client") as boto3_client,
            patch("airflow.models.Variable.get", return_value="topic-arn"),
        ):
            factory.notify_failure_to_sns(mock_context)

            boto3_client().publish.assert_not_called()

    def test_dr_type_no_context(self):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.dr_type(None)).to.equal("WARM_STANDBY")

    def test_dr_type_with_context_no_dr_type(self, mock_context):
        factory = BaseDRFactory(dag_id="dag")
        expect(factory.dr_type(mock_context)).to.equal("WARM_STANDBY")

    def test_dr_type_with_context(self, mock_context):
        factory = BaseDRFactory(dag_id="dag")
        mock_context.get("dag_run").conf["dr_type"] = "BACKUP_RESTORE"

        expect(factory.dr_type(mock_context)).to.equal("BACKUP_RESTORE")

    def test_setup_backup_local_folder_missing(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        with patch("os.mkdir") as mkdir, patch("os.path.exists", return_value=False):
            factory.setup_backup(**mock_context)
            mkdir.assert_called_once_with("/tmp/dags/data")

        del os.environ["AIRFLOW_HOME"]

    def test_setup_backup_local_folder_present(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        with patch("os.mkdir") as mkdir, patch("os.path.exists", return_value=True):
            factory.setup_backup(**mock_context)
            mkdir.assert_not_called()

        del os.environ["AIRFLOW_HOME"]

    def test_setup_backup_mwaa(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        with patch("os.mkdir") as mkdir, patch("os.path.exists", return_value=False):
            factory.setup_backup(**mock_context)
            mkdir.assert_not_called()

        del os.environ["AIRFLOW_HOME"]

    def test_teardown_backup(self, mock_context):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        with patch("builtins.print") as mock_print:
            factory.teardown_backup(**mock_context)
            mock_print.assert_called_once_with(
                "Executing the backup workflow teardown ..."
            )

    def test_setup_restore_local_with_data(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        with patch("os.path.exists", return_value=True):
            factory.setup_restore.when.called_with(**mock_context).should_not.throw(
                AirflowFailException
            )
        del os.environ["AIRFLOW_HOME"]

    def test_setup_restore_local_no_data(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="LOCAL_FS")

        with patch("os.path.exists", return_value=False):
            factory.setup_restore.when.called_with(**mock_context).should.throw(
                AirflowFailException
            )

        del os.environ["AIRFLOW_HOME"]

    def test_setup_restore_s3_with_data(self, mock_context):
        os.environ["AIRFLOW_HOME"] = "/tmp"
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        with patch("os.path.exists", return_value=False):
            factory.setup_restore.when.called_with(**mock_context).should_not.throw(
                AirflowFailException
            )

        del os.environ["AIRFLOW_HOME"]

    def test_teardown_restore(self, mock_context):
        factory = BaseDRFactory(dag_id="dag", storage_type="S3")

        with patch("builtins.print") as mock_print:
            factory.teardown_restore(**mock_context)
            mock_print.assert_called_once_with(
                "Executing the restore workflow teardown ..."
            )

    # NOTE: create_backup_dag and create_restore_dag are tested in v_2_5.dr_factory
