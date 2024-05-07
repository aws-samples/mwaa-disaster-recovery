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

import os
import pytest
from moto import mock_aws
import boto3
from unittest.mock import patch
from tests.unit.mocks.environment_configs import create_environment_response, get_environment_response

@pytest.fixture(scope="function")
def aws_credentials():
    """ Mocked AWS Credentials for moto. """
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def env_vars():
    os.environ["MWAA_ENV_NAME"] = "my-mwaa-env"
    os.environ["MWAA_ENV_VERSION"] = "2.8.1"


boto_make_api_call = 'botocore.client.BaseClient._make_api_call'
mwaa_cli_token_text = 'token'
mwaa_web_server_hostname = 'a-host-name.aws'

mwaa_cli_token = {
    'CliToken': mwaa_cli_token_text,
    'WebServerHostname': mwaa_web_server_hostname
}

def mock_api_call(self, operation, kwarg):
    if operation == 'CreateCliToken':
        return mwaa_cli_token
    elif operation == 'CreateEnvironment':
        return create_environment_response
    elif operation == 'GetEnvironment':
        return get_environment_response

    raise NotImplementedError(f'The mock operation {operation} is not implemented')

@pytest.fixture(scope="function")
def aws_mwaa(aws_credentials):
    """ Mocked AWS MWAA client. """
    with patch(boto_make_api_call, new=mock_api_call):
        yield boto3.client("mwaa", region_name="us-east-1")

