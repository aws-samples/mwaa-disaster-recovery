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
import boto3


def handler(event, context):
    print(f'Event: {json.dumps(event)}')

    execution_role_arn = event['execution_role_arn']
    name = event['name']
    subnet_ids = event['network']['subnet_ids']
    security_groups_ids = event['network']['security_group_ids']
    source_bucket_arn = event['source_bucket_arn']

    env_backup = json.loads(event['environment'])['Environment']
    
    propogated_fields = [
        'AirflowConfigurationOptions', 'AirflowVersion', 'DagS3Path', 'EndpointManagement', 
        'EnvironmentClass', 'KmsKey', 'MaxWorkers', 'MinWorkers', 'Name', 'PluginsS3ObjectVersion', 
        'PluginsS3Path', 'RequirementsS3ObjectVersion', 'RequirementsS3Path', 'Schedulers', 
        'StartupScriptS3ObjectVersion', 'StartupScriptS3Path', 'WebserverAccessMode', 'WeeklyMaintenanceWindowStart'
    ]
    environment = {}
    for field in propogated_fields:
        if field in env_backup:
            environment[field] = env_backup[field]

    DagProcessingLogs = env_backup['LoggingConfiguration'].get('DagProcessingLogs') or {}
    SchedulerLogs = env_backup['LoggingConfiguration'].get('SchedulerLogs') or {}
    TaskLogs = env_backup['LoggingConfiguration'].get('TaskLogs') or {}
    WebserverLogs = env_backup['LoggingConfiguration'].get('WebserverLogs') or {}
    WorkerLogs = env_backup['LoggingConfiguration'].get('WorkerLogs') or {}

    environment['LoggingConfiguration'] = {
        'DagProcessingLogs': {
            'Enabled': DagProcessingLogs.get('Enabled') or False,
            'LogLevel': DagProcessingLogs.get('LogLevel')
        },
        'SchedulerLogs': {
            'Enabled': SchedulerLogs.get('Enabled') or False,
            'LogLevel': SchedulerLogs.get('LogLevel')
        },
        'TaskLogs': {
            'Enabled': TaskLogs.get('Enabled') or False,
            'LogLevel': TaskLogs.get('LogLevel')
        },
        'WebserverLogs': {
            'Enabled': WebserverLogs.get('Enabled') or False,
            'LogLevel': WebserverLogs.get('LogLevel')
        },
        'WorkerLogs': {
            'Enabled': WorkerLogs.get('Enabled') or False,
            'LogLevel': WorkerLogs.get('LogLevel')
        }
    }
    environment['ExecutionRoleArn'] = execution_role_arn
    environment['Name'] = name
    environment['NetworkConfiguration'] = {
        'SecurityGroupIds': security_groups_ids,
        'SubnetIds': subnet_ids
    }
    environment['SourceBucketArn'] = source_bucket_arn

    if len(env_backup['Tags']) > 0:
        environment['Tags'] = env_backup['Tags']

    print(f'Creating new envrionment with settings : {json.dumps(env_backup)}')

    client = boto3.client('mwaa')
    result = client.create_environment(**environment)

    print(f'Result: {json.dumps(result)}')    
    return result
