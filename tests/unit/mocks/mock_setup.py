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
from tests.unit.mocks.environment_configs import (
    create_environment_response,
    get_environment_response,
)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def warm_standby_env_vars():
    os.environ["STACK_NAME_PREFIX"] = "mwaa-2-10-1-public"

    os.environ["AWS_ACCOUNT_ID"] = "123456789999"
    os.environ["DR_TYPE"] = "WARM_STANDBY"

    os.environ["MWAA_VERSION"] = "2.10.1"
    os.environ["MWAA_UPDATE_EXECUTION_ROLE"] = "YES"
    os.environ["MWAA_NOTIFICATION_EMAILS"] = '["abc@example.com"]'
    os.environ["MWAA_SIMULATE_DR"] = "YES"

    os.environ["HEALTH_CHECK_ENABLED"] = "YES"

    os.environ["PRIMARY_REGION"] = "us-east-1"
    os.environ["PRIMARY_MWAA_ENVIRONMENT_NAME"] = "mwaa-2-10-1-public-primary"
    os.environ["PRIMARY_MWAA_ROLE_ARN"] = (
        "arn:aws:iam::123456789999:role/mwaa-dr-primary-role"
    )
    os.environ["PRIMARY_DAGS_BUCKET_NAME"] = "mwaa-dags-primary"
    os.environ["PRIMARY_VPC_ID"] = "vpc-12345678999988ffa"
    os.environ["PRIMARY_SUBNET_IDS"] = (
        '["subnet-00001111aaaabbbb2", "subnet-00001111aaaabbbb3"]'
    )
    os.environ["PRIMARY_SECURITY_GROUP_IDS"] = '["sg-00001111aaaabbcc22"]'
    os.environ["PRIMARY_BACKUP_SCHEDULE"] = "0 * * * *"

    os.environ["SECONDARY_REGION"] = "us-east-2"
    os.environ["SECONDARY_MWAA_ENVIRONMENT_NAME"] = "mwaa-2-10-1-public-secondary"
    os.environ["SECONDARY_MWAA_ROLE_ARN"] = (
        "arn:aws:iam::123456789999:role/mwaa-dr-secondary-role"
    )
    os.environ["SECONDARY_DAGS_BUCKET_NAME"] = "mwaa-dags-secondary"
    os.environ["SECONDARY_VPC_ID"] = "vpc-12345678999988ffb"
    os.environ["SECONDARY_SUBNET_IDS"] = (
        '["subnet-00001111aaaabbbb4", "subnet-00001111aaaabbbb5"]'
    )
    os.environ["SECONDARY_SECURITY_GROUP_IDS"] = '["sg-00001111aaaabbcc22"]'
    os.environ["SECONDARY_CREATE_SFN_VPCE"] = "YES"

    os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "APPEND"
    os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "APPEND"


@pytest.fixture(scope="function")
def backup_restore_env_vars():
    os.environ["STACK_NAME_PREFIX"] = "mwaa-2-10-1-public"

    os.environ["AWS_ACCOUNT_ID"] = "123456789999"
    os.environ["DR_TYPE"] = "BACKUP_RESTORE"

    os.environ["MWAA_VERSION"] = "2.10.1"
    os.environ["MWAA_UPDATE_EXECUTION_ROLE"] = "YES"
    os.environ["MWAA_NOTIFICATION_EMAILS"] = '["abc@example.com"]'
    os.environ["MWAA_SIMULATE_DR"] = "YES"

    os.environ["HEALTH_CHECK_ENABLED"] = "YES"

    os.environ["PRIMARY_REGION"] = "us-east-1"
    os.environ["PRIMARY_MWAA_ENVIRONMENT_NAME"] = "mwaa-2-10-1-public-primary"
    os.environ["PRIMARY_MWAA_ROLE_ARN"] = (
        "arn:aws:iam::123456789999:role/mwaa-dr-primary-role"
    )
    os.environ["PRIMARY_DAGS_BUCKET_NAME"] = "mwaa-dags-primary"
    os.environ["PRIMARY_VPC_ID"] = "vpc-12345678999988ffa"
    os.environ["PRIMARY_SUBNET_IDS"] = (
        '["subnet-00001111aaaabbbb2", "subnet-00001111aaaabbbb3"]'
    )
    os.environ["PRIMARY_SECURITY_GROUP_IDS"] = '["sg-00001111aaaabbcc22"]'
    os.environ["PRIMARY_BACKUP_SCHEDULE"] = "0 * * * *"

    os.environ["SECONDARY_REGION"] = "us-east-2"
    os.environ["SECONDARY_MWAA_ENVIRONMENT_NAME"] = "mwaa-2-10-1-public-secondary"
    os.environ["SECONDARY_MWAA_ROLE_ARN"] = (
        "arn:aws:iam::123456789999:role/mwaa-dr-secondary-role"
    )
    os.environ["SECONDARY_DAGS_BUCKET_NAME"] = "mwaa-dags-secondary"
    os.environ["SECONDARY_VPC_ID"] = "vpc-12345678999988ffb"
    os.environ["SECONDARY_SUBNET_IDS"] = (
        '["subnet-00001111aaaabbbb4", "subnet-00001111aaaabbbb5"]'
    )
    os.environ["SECONDARY_SECURITY_GROUP_IDS"] = '["sg-00001111aaaabbcc22"]'
    os.environ["SECONDARY_CREATE_SFN_VPCE"] = "YES"

    os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "REPLACE"
    os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "REPLACE"


boto_make_api_call = "botocore.client.BaseClient._make_api_call"
mwaa_cli_token_text = "token"
mwaa_web_server_hostname = "a-host-name.aws"

mwaa_cli_token = {
    "CliToken": mwaa_cli_token_text,
    "WebServerHostname": mwaa_web_server_hostname,
}


def mock_api_call(self, operation, kwarg):
    if operation == "CreateCliToken":
        return mwaa_cli_token
    elif operation == "CreateEnvironment":
        return create_environment_response
    elif operation == "GetEnvironment":
        return get_environment_response

    raise NotImplementedError(f"The mock operation {operation} is not implemented")


@pytest.fixture(scope="function")
def aws_mwaa(aws_credentials):
    """Mocked AWS MWAA client."""
    with patch(boto_make_api_call, new=mock_api_call):
        yield boto3.client("mwaa", region_name="us-east-1")
