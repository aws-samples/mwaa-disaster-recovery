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

import pytest
import boto3
from unittest.mock import patch
from sure import expect
from tests.unit.mocks.mock_setup import boto_make_api_call, aws_credentials
from lib.functions.disable_scheduler_function import handler

schedule = {
    "ResponseMetadata": {
        "RequestId": "d7a7c8b1-6b4e-43bd-be3e-0fa0495db22d",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amzn-requestid": "d7a7c8b1-6b4e-43bd-be3e-0fa0495db22d",
            "content-type": "application/json",
            "content-length": "975",
            "date": "Mon, 29 Apr 2024 18:38:21 GMT",
        },
        "RetryAttempts": 0,
    },
    "ActionAfterCompletion": "NONE",
    "Arn": "arn:aws:scheduler:us-east-2:123456789999:schedule/default/mwaa-2-8-1-public-scheduler",
    "CreationDate": "2024-04-29 13:47:52.516000+00:00",
    "FlexibleTimeWindow": {"Mode": "OFF"},
    "GroupName": "default",
    "LastModificationDate": "2024-04-29 18:35:20.020000+00:00",
    "Name": "mwaa-2-8-1-public-scheduler",
    "ScheduleExpression": "rate(5 minutes)",
    "ScheduleExpressionTimezone": "UTC",
    "State": "ENABLED",
    "Target": {
        "Arn": "arn:aws:states:us-east-2:123456789999:stateMachine:mwaa281publicstatemachine9B69F6BA-ofukfWgQcD8C",
        "Input": '{"simulate_dr": "YES"}',
        "RetryPolicy": {"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 185},
        "RoleArn": "arn:aws:iam::123456789999:role/mwaa-2-8-1-public-seconda-mwaa281publicschedulerrol-mmt5AABnhyis",
    },
}

schedule_update_request = {
    "ActionAfterCompletion": "NONE",
    "FlexibleTimeWindow": {"Mode": "OFF"},
    "GroupName": "default",
    "Name": "mwaa-2-8-1-public-scheduler",
    "ScheduleExpression": "rate(5 minutes)",
    "ScheduleExpressionTimezone": "UTC",
    "State": "DISABLED",
    "Target": {
        "Arn": "arn:aws:states:us-east-2:123456789999:stateMachine:mwaa281publicstatemachine9B69F6BA-ofukfWgQcD8C",
        "Input": '{"simulate_dr": "YES"}',
        "RetryPolicy": {"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 185},
        "RoleArn": "arn:aws:iam::123456789999:role/mwaa-2-8-1-public-seconda-mwaa281publicschedulerrol-mmt5AABnhyis",
    },
}


schedule_update_response = {
    "ResponseMetadata": {
        "RequestId": "820f0775-7de0-4705-ac77-3267b85acb43",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amzn-requestid": "820f0775-7de0-4705-ac77-3267b85acb43",
            "content-type": "application/json",
            "content-length": "103",
            "date": "Mon, 29 Apr 2024 18:38:21 GMT",
        },
        "RetryAttempts": 0,
    },
    "ScheduleArn": "arn:aws:scheduler:us-east-2:988740490609:schedule/default/mwaa-2-8-1-public-scheduler",
}


@pytest.fixture(scope="function")
def aws_scheduler(aws_credentials):
    """Mocked AWS scheduler client."""
    with patch(boto_make_api_call, new=mock_api_call):
        yield boto3.client("scheduler", region_name="us-east-1")


def mock_api_call(self, operation, kwarg):
    if operation == "GetSchedule" and kwarg == {"Name": "dr-schedule"}:
        return schedule
    elif operation == "UpdateSchedule" and kwarg == schedule_update_request:
        return schedule_update_response

    raise NotImplementedError(f"The mock operation {operation} is not implemented")


def test_handler(aws_scheduler):
    event = {"schedule_name": "dr-schedule"}
    response = handler(event, {})
    expect(response).to.equal(schedule_update_response)
