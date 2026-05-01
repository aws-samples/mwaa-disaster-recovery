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
from datetime import datetime, timezone

import boto3
from airflow_cli_client import AirflowCliClient


def handler(event, context):
    print(f"Event: {json.dumps(event)}")

    mwaa_env_name = event["mwaa_env_name"]
    mwaa_env_version = event["mwaa_env_version"]
    dag = event["dag"]
    bucket = event["bucket"]
    task_token = event["task_token"]
    dr_type = event["dr_type"]
    connection_restore_strategy = event["connection_restore_strategy"]
    variable_restore_strategy = event["variable_restore_strategy"]
    conf = {
        "bucket": bucket,
        "dr_type": dr_type,
        "connection_restore_strategy": connection_restore_strategy,
        "variable_restore_strategy": variable_restore_strategy,
        "task_token": task_token,
    }

    sem_ver = mwaa_env_version.split(".")
    is_v3 = int(sem_ver[0]) >= 3

    # Unpause the DAG first
    airflow_cli = AirflowCliClient(mwaa_env_name, mwaa_env_version)
    print(f"Unpausing DAG {dag} ...")
    result = airflow_cli.unpause_dag(dag)
    print(f"Unpausing result: {result}")

    if is_v3:
        # AF 3.x: Use InvokeRestApi to trigger DAGs (CLI triggers don't persist)
        print(f"Triggering DAG {dag} via InvokeRestApi ...")
        result = _trigger_via_rest_api(mwaa_env_name, dag, conf)
        print(f"DAG trigger result: {json.dumps(result)}")
        return json.dumps(result)
    else:
        # AF 2.x: Use CLI to trigger DAGs
        print(f"Triggering DAG {dag} via CLI ...")
        result = airflow_cli.trigger_dag(dag, conf)
        print(f"DAG trigger result: {result}")
        return result.to_json()


def _trigger_via_rest_api(env_name, dag_id, conf):
    """Trigger a DAG via MWAA InvokeRestApi."""
    client = boto3.client("mwaa")
    logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    response = client.invoke_rest_api(
        Name=env_name,
        Method="POST",
        Path=f"/dags/{dag_id}/dagRuns",
        Body={"conf": conf, "logical_date": logical_date},
    )
    status = response.get("RestApiStatusCode", 0)
    data = response.get("RestApiResponse", {})

    if status >= 400:
        raise Exception(f"Failed to trigger DAG {dag_id}: {status} {data}")

    print(
        f"DAG {dag_id} triggered: run_id={data.get('dag_run_id')}, state={data.get('state')}"
    )
    return data
