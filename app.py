#!/usr/bin/env python3

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
