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
from sure import expect
from moto import mock_aws
from tests.unit.mocks.mock_setup import aws_credentials, aws_mwaa, warm_standby_env_vars
from lib.functions.airflow_cli_client import (
    AirflowCliCommand,
    AirflowCliInput,
    AirflowCliResult,
    AirflowCliException,
)
from lib.functions.airflow_cli_function import on_event, on_create, on_update, on_delete


def test_on_event_create():
    with patch("lib.functions.airflow_cli_function.on_create") as on_create_mock:
        event = {"RequestType": "Create"}
        on_event(event, None)
    on_create_mock.assert_called_once_with(event)


def test_on_event_update():
    with patch("lib.functions.airflow_cli_function.on_update") as on_update_mock:
        event = {"RequestType": "Update"}
        on_event(event, None)
    on_update_mock.assert_called_once_with(event)


def test_on_event_delete():
    with patch("lib.functions.airflow_cli_function.on_delete") as on_delete_mock:
        event = {"RequestType": "Delete"}
        on_event(event, None)
    on_delete_mock.assert_called_once_with(event)


def test_on_event_others():
    event = {"RequestType": "SomethingElse"}
    on_event.when.called_with(event, None).should.have.raised(
        AirflowCliException, "Invalid request type: SomethingElse"
    )


cli_input = AirflowCliInput(
    create=[
        AirflowCliCommand(command="variables set KEY-A VALUE-A"),
        AirflowCliCommand(command="variables set KEY-B VALUE-B"),
    ],
    update=[
        AirflowCliCommand(command="variables set KEY-A VALUE-A"),
        AirflowCliCommand(command="variables set KEY-B VALUE-B"),
    ],
    delete=[
        AirflowCliCommand(command="variables delete KEY-A"),
        AirflowCliCommand(command="variables delete KEY-B"),
    ],
)


@mock_aws
def test_on_create(aws_mwaa, warm_standby_env_vars):
    event = {
        "RequestId": "request-id",
        "RequestType": "Create",
        "ResourceProperties": {"airflow_cli_input": cli_input.to_json()},
    }
    results = [
        AirflowCliResult(stdout="Variable KEY-A created", stderr="").to_json(),
        AirflowCliResult(stdout="Variable KEY-B created", stderr="").to_json(),
    ]
    expected_result = {
        "PhysicalResourceId": "airflow-cli-request-id",
        "Data": {"results": results},
        "Reason": "Successfully executed Airflow CLI commands",
    }

    def mock_execute_command(self, command):
        command_terms = command.command.split()
        if command_terms[0] != "variables" and command_terms[0] != "set":
            raise NotImplementedError(
                f"Mock for the command not implemented: {command.command}"
            )

        return AirflowCliResult(
            stdout=f"Variable {command_terms[2]} created", stderr=""
        )

    with patch(
        "lib.functions.airflow_cli_client.AirflowCliClient.execute",
        new=mock_execute_command,
    ):
        expect(on_create(event)).to.equal(expected_result)


@mock_aws
def test_on_update(aws_mwaa, warm_standby_env_vars):
    event = {
        "RequestId": "id-1",
        "PhysicalResourceId": "airflow-cli-id-1",
        "RequestType": "Update",
        "ResourceProperties": {"airflow_cli_input": cli_input.to_json()},
    }
    results = [
        AirflowCliResult(stdout="Variable KEY-A created", stderr="").to_json(),
        AirflowCliResult(stdout="Variable KEY-B created", stderr="").to_json(),
    ]
    expected_result = {
        "PhysicalResourceId": "airflow-cli-id-1",
        "Data": {"results": results},
        "Reason": "Successfully executed Airflow CLI commands",
    }

    def mock_execute_command(self, command):
        command_terms = command.command.split()
        if command_terms[0] != "variables" and command_terms[0] != "set":
            raise NotImplementedError(
                f"Mock for the command not implemented: {command.command}"
            )

        return AirflowCliResult(
            stdout=f"Variable {command_terms[2]} created", stderr=""
        )

    with patch(
        "lib.functions.airflow_cli_client.AirflowCliClient.execute",
        new=mock_execute_command,
    ):
        expect(on_update(event)).to.equal(expected_result)


@mock_aws
def test_on_delete(aws_mwaa, warm_standby_env_vars):
    event = {
        "RequestId": "id-1",
        "PhysicalResourceId": "airflow-cli-id-1",
        "RequestType": "Update",
        "ResourceProperties": {"airflow_cli_input": cli_input.to_json()},
    }
    results = [
        AirflowCliResult(stdout="Variable KEY-A deleted", stderr="").to_json(),
        AirflowCliResult(stdout="Variable KEY-B deleted", stderr="").to_json(),
    ]
    expected_result = {
        "PhysicalResourceId": "airflow-cli-id-1",
        "Data": {"results": results},
        "Reason": "Successfully executed Airflow CLI commands",
    }

    def mock_execute_command(self, command):
        command_terms = command.command.split()
        if command_terms[0] != "variables" and command_terms[0] != "delete":
            raise NotImplementedError(
                f"Mock for the command not implemented: {command.command}"
            )

        return AirflowCliResult(
            stdout=f"Variable {command_terms[2]} deleted", stderr=""
        )

    with patch(
        "lib.functions.airflow_cli_client.AirflowCliClient.execute",
        new=mock_execute_command,
    ):
        expect(on_delete(event)).to.equal(expected_result)
