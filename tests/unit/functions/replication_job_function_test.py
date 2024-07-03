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
import boto3
from sure import expect

from unittest.mock import patch

from tests.unit.mocks.mock_setup import aws_credentials, boto_make_api_call

create_job_result = {
    "JobId": "XXX",
}

describe_job_result = {
    "Job": {
        "JobId": "XXX",
        "Status": "COMPLETE",
    }
}


def test_handler_no_result_input():
    event = {
        "account": "XXX",
        "source_bucket": "XXX",
        "report_bucket": "XXX",
        "replication_job_role": "XXX",
        "result": "",
    }
    with patch("boto3.client") as boto3_client:
        boto3_client().create_job.return_value = create_job_result
        boto3_client().describe_job.return_value = describe_job_result

        import replication_job_function

        result = replication_job_function.handler(event, {})

        expect(result).to.equal(
            json.dumps(
                {
                    "JobId": "XXX",
                    "Status": "COMPLETE",
                }
            )
        )


def test_handler_with_result_input():
    event = {
        "account": "XXX",
        "source_bucket": "XXX",
        "report_bucket": "XXX",
        "replication_job_role": "XXX",
        "result": {
            "JobId": "XXX",
            "Status": "CREATING",
        },
    }

    with patch("boto3.client") as boto3_client:
        boto3_client().describe_job.return_value = describe_job_result

        import replication_job_function

        result = replication_job_function.handler(event, {})

        expect(result).to.equal(
            json.dumps(
                {
                    "JobId": "XXX",
                    "Status": "COMPLETE",
                }
            )
        )
