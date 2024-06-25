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

import json
from unittest.mock import patch
from sure import expect
from moto import mock_aws
from tests.unit.mocks.mock_setup import aws_credentials, aws_mwaa
from airflow_cli_client import (
    AirflowCliClient,
    AirflowCliResult,
)
from airflow_dag_trigger_function import handler


@mock_aws
def test_handler(aws_mwaa):
    event = {
        "mwaa_env_name": "test-env",
        "mwaa_env_version": "2.8.1",
        "dag": "test-dag",
        "bucket": "test-bucket",
        "task_token": "test-sfn-token",
        "dr_type": "BACKUP_RESTORE",
    }

    unpause_result = AirflowCliResult(stdout="Dag: test-dag, paused: False", stderr="")
    trigger_result_obj = [
        {
            "conf": {
                "bucket": "backup-bucket",
                "task_token": "sfn-task-token",
                "dr_type": "WARM_STANDBY",
            },
            "dag_id": "test-dag",
            "dag_run_id": "manual__2024-04-29T19:17:11+00:00",
            "data_interval_start": "2024-04-29 19:17:11+00:00",
            "data_interval_end": "2024-04-29 19:17:11+00:00",
            "end_date": None,
            "external_trigger": "True",
            "last_scheduling_decision": None,
            "logical_date": "2024-04-29 19:17:11+00:00",
            "run_type": "manual",
            "start_date": None,
            "state": "queued",
        }
    ]
    trigger_result = AirflowCliResult(
        stdout=json.dumps(trigger_result_obj),
        stderr="UserWarning: Could not import graphviz.",
    )

    with patch.object(AirflowCliClient, "unpause_dag", return_value=unpause_result):
        with patch.object(AirflowCliClient, "trigger_dag", return_value=trigger_result):
            result = handler(event, None)

    expect(result).to.equal(trigger_result.to_json())
