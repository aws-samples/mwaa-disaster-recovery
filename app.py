#!/usr/bin/env python3

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

import config
from lib.stacks.mwaa_primary_stack import MwaaPrimaryStack
from lib.stacks.mwaa_secondary_stack import MwaaSecondaryStack

conf = config.Config()
app = cdk.App()

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

primary_stack.add_dependency(secondary_stack)

app.synth()
