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

import aws_cdk as cdk
from tests.unit.mocks.mock_setup import (
    aws_credentials,
    backup_restore_env_vars,
    warm_standby_env_vars,
)

import config
from lib.stacks.mwaa_primary_stack import MwaaPrimaryStack
from lib.stacks.mwaa_secondary_stack import MwaaSecondaryStack
import pytest


@pytest.fixture(scope="function")
def backup_restore_stacks(aws_credentials, backup_restore_env_vars):
    conf = config.Config()
    app = cdk.App()
    return create_stacks(conf, app)


@pytest.fixture(scope="function")
def warm_standby_stacks(aws_credentials, warm_standby_env_vars):
    conf = config.Config()
    app = cdk.App()
    return create_stacks(conf, app)


def create_stacks(conf, app):
    secondary_stack = MwaaSecondaryStack(
        app,
        conf.get_name("secondary-stack"),
        conf=conf,
        env=cdk.Environment(account=conf.aws_account_id, region=conf.secondary_region),
    )

    primary_stack = MwaaPrimaryStack(
        app,
        conf.get_name("primary-stack"),
        conf=conf,
        secondary_backup_bucket=secondary_stack.backup_bucket,
        secondary_source_bucket=secondary_stack.source_bucket,
        env=cdk.Environment(account=conf.aws_account_id, region=conf.primary_region),
    )

    return {
        "primary": primary_stack,
        "secondary": secondary_stack,
    }
