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
import pytest
import boto3
from unittest.mock import patch
from sure import expect
from tests.unit.mocks.mock_setup import boto_make_api_call, aws_credentials
from lib.functions.cloudwatch_health_check_function import handler

heartbeat_missing_response = {
    "MetricDataResults": [
        {
            "Id": "scheduler_heartbeat",
            "Label": "SchedulerHeartbeat",
            "Timestamps": [],
            "Values": [],
            "StatusCode": "Complete",
        }
    ],
    "Messages": [],
    "ResponseMetadata": {
        "RequestId": "3cbf2ff0-47f3-4788-a9e8-309d10b61fe8",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amzn-requestid": "3cbf2ff0-47f3-4788-a9e8-309d10b61fe8",
            "content-type": "text/xml",
            "content-length": "518",
            "date": "Mon, 29 Apr 2024 18:38:18 GMT",
        },
        "RetryAttempts": 0,
    },
}

heartbeat_active_response = {
    "MetricDataResults": [
        {
            "Id": "scheduler_heartbeat",
            "Label": "SchedulerHeartbeat",
            "Timestamps": ["2024-04-29 18:28:00+00:00"],
            "Values": [1.9],
            "StatusCode": "Complete",
        }
    ],
    "Messages": [],
    "ResponseMetadata": {
        "RequestId": "cbda0fd9-4671-4481-a635-cd75b4d6fc20",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amzn-requestid": "cbda0fd9-4671-4481-a635-cd75b4d6fc20",
            "content-type": "text/xml",
            "content-length": "635",
            "date": "Mon, 29 Apr 2024 18:33:58 GMT",
        },
        "RetryAttempts": 0,
    },
}


@pytest.fixture(scope="function")
def aws_cloudwatch(aws_credentials):
    """Mocked AWS cloudwatch client."""
    with patch(boto_make_api_call, new=mock_api_call):
        yield boto3.client("cloudwatch", region_name="us-east-1")


def mock_api_call(self, operation, kwarg):
    if operation == "GetMetricData":
        kwarg_text = json.dumps(kwarg)
        if "DummyEnvForSimulatingDR" in kwarg_text:
            return heartbeat_missing_response
        else:
            return heartbeat_active_response

    raise NotImplementedError(f"The mock operation {operation} is not implemented")


@pytest.fixture(scope="function")
def aws_cloudwatch_impaired(aws_credentials):
    """Mocked AWS cloudwatch client that always fails API call."""

    def cloudwath_failure(self, operation, kwarg):
        raise Exception("503 Service Unavailable")

    with patch(boto_make_api_call, new=cloudwath_failure):
        yield boto3.client("cloudwatch", region_name="us-east-1")


def test_health_check_function_normal_operation(aws_cloudwatch):
    event = {"region": "us-east-1", "env_name": "test-env", "simulate_dr": "NO"}

    expect(handler(event, None)).to.equal("HEALTHY")


def test_health_check_function_mwaa_impaired(aws_cloudwatch):
    event = {"region": "us-east-1", "env_name": "test-env", "simulate_dr": "YES"}

    expect(handler(event, None)).to.equal("UNHEALTHY")


def test_health_check_function_cloud_watch_impaired(aws_cloudwatch_impaired):
    event = {"region": "us-east-1", "env_name": "test-env", "simulate_dr": "YES"}

    expect(handler(event, None)).to.equal("UNHEALTHY")
