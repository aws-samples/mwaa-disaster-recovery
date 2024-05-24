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
from .airflow_cli_client import AirflowCliClient


def handler(event, context):
    print(f"Event: {json.dumps(event)}")

    mwaa_env_name = event["mwaa_env_name"]
    mwaa_env_version = event["mwaa_env_version"]
    dag = event["dag"]
    bucket = event["bucket"]
    task_token = event["task_token"]
    dr_type = event["dr_type"]
    config = {"bucket": bucket, "task_token": task_token, "dr_type": dr_type}

    airflow_cli = AirflowCliClient(mwaa_env_name, mwaa_env_version)

    print(f"Unpausing DAG {dag} ...")
    result = airflow_cli.unpause_dag(dag)
    print(f"Unpausing result: {result}")

    print(f"Triggering DAG {dag} with config {config} ...")
    result = airflow_cli.trigger_dag(dag, config)
    print(f"DAG trigger result: {result}")

    return result.to_json()
