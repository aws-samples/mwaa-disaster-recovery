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

import ast
import base64
import http.client
import json
from dataclasses import dataclass

import boto3
from dataclasses_json import dataclass_json
from typing import Optional


@dataclass_json
@dataclass
class AirflowCliCommand:
    """An object for issuing Airflow CLI commands."""

    command: str
    """ Command string such as `dags pause dag-name`. """

    configuration: Optional[dict] = None
    """ A dictionary to be passed as configuration. """

    result_check: Optional[str] = None
    """ A string to be used to check the result of the command. """


@dataclass_json
@dataclass
class AirflowCliInput:
    """
    An object for issuing Airflow CLI commands during a cloudformation
    stack deployment as a custom resource.
    """

    create: list[AirflowCliCommand]
    """ List of Airflow CLI commands to be executed when the custom resource is created. """

    update: list[AirflowCliCommand]
    """ List of Airflow CLI commands to be executed when the custom resource is updated. """

    delete: list[AirflowCliCommand]
    """ List of Airflow CLI commands to be executed when the custom resource is deleted. """


@dataclass_json
@dataclass
class AirflowCliResult:
    """An object representing the result of an Airflow CLI command."""

    stdout: str
    """ Standard output of the command. """

    stderr: str
    """ Standard error of the command. """


class AirflowCliException(Exception):
    """
    An Airflow CLI exception.
    """

    def __init__(self, message: str, result: AirflowCliResult = None):
        """
        Initializes an Airflow CLI exception.

        :param message: The exception message.
        :param result: The Airflow CLI result.
        """
        super().__init__(message)
        self.result = result


class AirflowCliClient:
    """
    An Airflow CLI client to issue CLI commands to an MWAA environment.
    """

    def __init__(self, environment_name: str, environment_version: str, config=None):
        """
        Initializes an Airflow CLI client.

        :param environment_name: The name of the MWAA environment.
        :param environment_version: The version of the MWAA environment.
        :param config: A boto3 config object.
        """
        self.environment_name = environment_name
        self.environment_version = environment_version
        self.client = boto3.client("mwaa", config=config)
        self.token = None

    def setup(self):
        """
        Sets up the Airflow CLI client by creating a CLI token.
        """
        if not self.environment_name:
            raise AirflowCliException("Environment name not provided!")

        if not self.environment_version:
            raise AirflowCliException("Environment version not provided!")

        self.token = self.token or self.client.create_cli_token(
            Name=self.environment_name
        )
        return self.token

    def execute(self, command: AirflowCliCommand):
        """
        Executes an Airflow CLI command.

        :param command: An Airflow CLI command.
        """
        conf = ""
        if command.configuration:
            conf = f" --conf '{json.dumps(command.configuration)}'"
        payload = f"{command.command}{conf}"

        print(f"Executing CLI command: {payload} ...")
        token = self.setup()

        url = f'https://{token["WebServerHostname"]}/aws_mwaa/cli'
        headers = {
            "Authorization": f'Bearer {token["CliToken"]}',
            "Content-Type": "text/plain",
        }
        conn = http.client.HTTPSConnection(token["WebServerHostname"])
        conn.request("POST", url, payload, headers)
        data = conn.getresponse().read().decode("UTF-8")

        try:
            result = ast.literal_eval(data)

            stdout = base64.b64decode(result["stdout"] or "").decode("utf-8")
            stderr = base64.b64decode(result["stderr"] or "").decode("utf-8")

            print(stdout if stdout else "")
            print(stderr if stderr else "")

            if command.result_check and command.result_check not in stdout:
                raise AirflowCliException(
                    f"The command, {command.command}, failed with the following error: {result}",
                    result=result,
                )

            return AirflowCliResult(stdout, stderr)
        except Exception:
            raise AirflowCliException(data)

    def execute_all(self, commands: list[AirflowCliCommand]) -> list[AirflowCliResult]:
        """
        Executes a list of Airflow CLI commands.

        :param commands: A list of Airflow CLI commands.
        """
        results = []
        for command in commands:
            results.append(self.execute(command))
        return results

    def unpause_dag(self, dag_name: str):
        """
        Unpauses a DAG.

        :param dag_name: The name of the DAG.
        """
        result = self.execute(AirflowCliCommand(command=f"dags unpause {dag_name}"))
        if "paused: False" not in result.stdout:
            raise AirflowCliException(
                f"The dag, {dag_name}, failed to unpause with the following error: {result}",
                result=result,
            )
        return result

    def trigger_dag(self, dag_name: str, configuration: dict):
        """
        Triggers a DAG.

        :param dag_name: The name of the DAG.
        :param configuration: A dictionary of configuration.
        """
        sem_ver = self.environment_version.split(".")

        command = ""
        expected_result = ""

        if int(sem_ver[0]) <= 2 and int(sem_ver[1]) <= 5:
            command = f"dags trigger {dag_name}"
            expected_result = "triggered: True"
        else:
            command = f"dags trigger -o json {dag_name}"
            expected_result = '"external_trigger": "True"'

        result = self.execute(
            AirflowCliCommand(command=command, configuration=configuration)
        )
        if expected_result not in result.stdout:
            raise AirflowCliException(
                f"The dag, {dag_name}, failed to trigger with the following error: {result}",
                result=result,
            )
        return result

    def set_variable(self, key: str, value: str):
        """
        Sets a variable.

        :param key: The key of the variable.
        :param value: The value of the variable.
        """
        result = self.execute(AirflowCliCommand(command=f"variables set {key} {value}"))
        if f"Variable {key} created" not in result.stdout:
            raise AirflowCliException(
                f"The variable, {key}, failed to set with the following error: {result}",
                result=result,
            )
        return result
