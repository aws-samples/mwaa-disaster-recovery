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
from time import time

import boto3
from botocore.config import Config

DATA_POINTS = 1
PERIOD_SECS = 300


def handler(event, context):
    print(f"Event: {json.dumps(event)}")

    region = event["region"]
    env_name = event["env_name"]
    simulate_dr = event["simulate_dr"]

    if simulate_dr == "YES":
        print(f"Simulating DR for {env_name}!")
        env_name = "DummyEnvForSimulatingDR"

    data_points = DATA_POINTS
    period = PERIOD_SECS
    end_time = int(time())
    start_time = end_time - data_points * period

    config = Config(region_name=region, retries={"max_attempts": 3, "mode": "standard"})
    cw_client = boto3.client("cloudwatch", config=config)
    queries = [
        {
            "Id": "scheduler_heartbeat",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AmazonMWAA",
                    "MetricName": "SchedulerHeartbeat",
                    "Dimensions": [
                        {"Name": "Environment", "Value": env_name},
                        {"Name": "Function", "Value": "Scheduler"},
                    ],
                },
                "Period": period,
                "Stat": "Average",
            },
        }
    ]
    print(f"Query: {json.dumps(queries)}")
    print(f"Start time: {start_time} | End time: {end_time}")

    response = None
    try:
        response = cw_client.get_metric_data(
            StartTime=start_time,
            EndTime=end_time,
            MaxDatapoints=data_points,
            MetricDataQueries=queries,
        )
        print(f"Response: {json.dumps(response, default=str)}")

        values = response["MetricDataResults"][0]["Values"]
        if len(values) > 0 and values[0] > 0:
            return "HEALTHY"
    except Exception as e:
        print(f"Error: {e}")

    return "UNHEALTHY"
