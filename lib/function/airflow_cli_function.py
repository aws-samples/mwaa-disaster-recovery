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
import json
from airflow_cli_client import AirflowCliClient, AirflowCliInput

def on_event(event, context):
    print(f'OnEvent: {json.dumps(event)}')

    request_type = event['RequestType']

    if request_type == 'Create':
        return on_create(event)

    if request_type == 'Update':
        return on_update(event)

    if request_type == 'Delete':
        return on_delete(event)

    raise Exception(f'Invalid request type: {request_type}')

def on_create(event):    
    request_id = event['RequestId']
    props = event['ResourceProperties']
    cli_input = AirflowCliInput.from_json(props['airflow_cli_input'])
    
    results = execute_commands(cli_input.create)

    return {
        'PhysicalResourceId': f'airflow-cli-{request_id}',
        'Data': results,
        'Reason': 'Successfully executed Airflow CLI commands'
    }

def on_update(event):    
    resource_id = event['PhysicalResourceId']
    props = event['ResourceProperties']
    cli_input = AirflowCliInput.from_json(props['airflow_cli_input'])
    
    results = execute_commands(cli_input.update)

    return {
        'PhysicalResourceId': resource_id,
        'Data': results,
        'Reason': 'Successfully executed Airflow CLI commands'
    }

def on_delete(event):    
    resource_id = event['PhysicalResourceId']
    props = event['ResourceProperties']
    cli_input = AirflowCliInput.from_json(props['airflow_cli_input'])
    
    results = execute_commands(cli_input.delete)

    return {
        'PhysicalResourceId': resource_id,
        'Data': results,
        'Reason': 'Successfully executed Airflow CLI commands'
    }

def execute_commands(commands):
    mwaa_env_name = os.getenv('MWAA_ENV_NAME')
    print(f'Executing commands: {commands}')

    airflow_cli = AirflowCliClient(mwaa_env_name)
    results = airflow_cli.execute_all(commands)
    json_results = list(map(lambda result: result.to_json(), results))

    result = { 'results': json_results }
    print(f'Command results: {result}')
    return result
