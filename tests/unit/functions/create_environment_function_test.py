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
from sure import expect
from tests.unit.mocks.mock_setup import aws_credentials, aws_mwaa
from lib.functions.create_environment_function import handler
from tests.unit.mocks.environment_configs import (
    mwaa_environment_with_tags,
    mwaa_environment_without_tags,
    create_environment_response,
)


def test_handler_without_env_tags(aws_mwaa):
    event = {
        "execution_role_arn": "arn:aws:iam::123456789999:role/mwaa-exec-role",
        "name": "mwaa-2-8-1-public-secondary",
        "network": {
            "subnet_ids": ["subnet-12345678", "subnet-87654321"],
            "security_group_ids": ["sg-12345678"],
        },
        "source_bucket_arn": "arn:aws:s3:::secondary-bucket",
        "environment": json.dumps({"Environment": mwaa_environment_without_tags}),
    }

    expect(handler(event, None)).to.equal(create_environment_response)


def test_handler_with_env_tags(aws_mwaa):
    event = {
        "execution_role_arn": "arn:aws:iam::123456789999:role/mwaa-exec-role",
        "name": "mwaa-2-8-1-public-secondary",
        "network": {
            "subnet_ids": ["subnet-12345678", "subnet-87654321"],
            "security_group_ids": ["sg-12345678"],
        },
        "source_bucket_arn": "arn:aws:s3:::secondary-bucket",
        "environment": json.dumps({"Environment": mwaa_environment_with_tags}),
    }

    expect(handler(event, None)).to.equal(create_environment_response)
