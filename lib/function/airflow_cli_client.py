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

import boto3
import http.client
import ast
import json
import base64
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class AirflowCliCommand:
    command: str
    configuration: dict = None
    result_check: str = None

@dataclass_json
@dataclass
class AirflowCliInput:
    create: list[AirflowCliCommand]
    update: list[AirflowCliCommand]
    delete: list[AirflowCliCommand]

@dataclass_json
@dataclass
class AirflowCliResult:
    stdout: str
    stderr: str


class AirflowCliClient:
    def __init__(self, environment_name: str, config=None):
        self.environment_name = environment_name
        self.client = boto3.client('mwaa', config=config)
        self.token = None

    def setup(self):
        if not self.environment_name:
            raise Exception('Environment name not provided')
        
        self.token = self.token or self.client.create_cli_token(Name=self.environment_name)        
        return self.token


    def execute(self, command: AirflowCliCommand):
        conf = ''
        if command.configuration:
            conf = f" --conf '{json.dumps(command.configuration)}'"
        payload = f'{command.command}{conf}'

        print(f'Executing CLI command: {payload} ...')        
        token = self.setup()

        url = f'https://{token["WebServerHostname"]}/aws_mwaa/cli'
        headers = {
            'Authorization': f'Bearer {token["CliToken"]}',
            'Content-Type': 'text/plain'
        }
        conn = http.client.HTTPSConnection(token['WebServerHostname'])
        conn.request("POST", url, payload, headers)
        data = conn.getresponse().read().decode('UTF-8')

        try:
            result = ast.literal_eval(data)

            stdout = base64.b64decode(result['stdout'] or '').decode('utf-8')
            stderr = base64.b64decode(result['stderr'] or '').decode('utf-8')

            print(stdout if stdout else '')
            print(stderr if stderr else '')

            if command.result_check and command.result_check not in stdout:
                raise Exception(f'The command, {command.command}, failed with the following error: {result}')

            return AirflowCliResult(stdout, stderr)
        except:
            raise RuntimeError(data)

    def execute_all(self, commands: list) -> list[AirflowCliResult]:
        results = []
        for command in commands:
            results.append(self.execute(command))
        return results

    def unpause_dag(self, dag_name: str):
        result = self.execute(AirflowCliCommand(command=f'dags unpause {dag_name}'))
        if 'paused: False' not in result.stdout:
            raise Exception(f'The dag, {dag_name}, failed to unpause with the following error: {result}')
        return result

    def trigger_dag(self, dag_name: str, configuration: dict):
        result = self.execute(AirflowCliCommand(command=f'dags trigger {dag_name}', configuration=configuration))
        if 'triggered: True' not in result.stdout:
            raise Exception(f'The dag, {dag_name}, failed to trigger with the following error: {result}')
        return result

    def set_variable(self, key: str, value: str):
        result = self.execute(AirflowCliCommand(command=f'variables set {key} {value}'))
        if 'created' not in result.stdout:
            raise Exception(f'The variable, {key}, failed to set with the following error: {result}')
        return result
