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
import aws_cdk as cdk
from aws_cdk.assertions import Template
from sure import expect

import config
from lib.stacks.mwaa_secondary_stack import MwaaSecondaryStack

from tests.unit.mocks.mock_setup import aws_credentials, backup_restore_env_vars, warm_standby_env_vars
from tests.unit.mocks.stacks import backup_restore_stacks, warm_standby_stacks

class TestMwaaSecondaryStack:
    def test_backup_restore_secondary_stack(self, backup_restore_stacks):
        secondary_stack = backup_restore_stacks['secondary']

        expect(secondary_stack.state_machine).to.be.truthy
        
        secondary_template = Template.from_stack(secondary_stack)

        secondary_template.resource_count_is('AWS::EC2::VPCEndpoint', 1)
        secondary_template.resource_count_is('AWS::EC2::SecurityGroupIngress', 1)
        secondary_template.resource_count_is('AWS::S3::Bucket', 1)
        secondary_template.resource_count_is('AWS::S3::BucketPolicy', 1)
        secondary_template.resource_count_is('Custom::S3AutoDeleteObjects', 1)
        secondary_template.resource_count_is('AWS::IAM::Role', 8)
        secondary_template.resource_count_is('AWS::Lambda::Function', 6)
        secondary_template.resource_count_is('AWS::IAM::Policy', 7)
        secondary_template.resource_count_is('AWS::StepFunctions::StateMachine', 1)
        secondary_template.resource_count_is('AWS::Scheduler::Schedule', 1)
        secondary_template.resource_count_is('AWS::SNS::Topic', 1)
        secondary_template.resource_count_is('AWS::SNS::Subscription', 1)
        secondary_template.resource_count_is('AWS::SNS::TopicPolicy', 1)
        secondary_template.resource_count_is('AWS::Events::Rule', 1)


    def test_warm_standby_secondary_stack(self, warm_standby_stacks):
        secondary_stack = warm_standby_stacks['secondary']

        expect(secondary_stack.state_machine).to.be.truthy
        
        secondary_template = Template.from_stack(secondary_stack)

        secondary_template.resource_count_is('AWS::EC2::VPCEndpoint', 1)
        secondary_template.resource_count_is('AWS::EC2::SecurityGroupIngress', 1)
        secondary_template.resource_count_is('AWS::S3::Bucket', 1)
        secondary_template.resource_count_is('AWS::S3::BucketPolicy', 1)
        secondary_template.resource_count_is('Custom::S3AutoDeleteObjects', 1)
        secondary_template.resource_count_is('AWS::IAM::Role', 6)
        secondary_template.resource_count_is('AWS::Lambda::Function', 4)
        secondary_template.resource_count_is('AWS::IAM::Policy', 5)
        secondary_template.resource_count_is('AWS::StepFunctions::StateMachine', 1)
        secondary_template.resource_count_is('AWS::Scheduler::Schedule', 1)
        secondary_template.resource_count_is('AWS::SNS::Topic', 1)
        secondary_template.resource_count_is('AWS::SNS::Subscription', 1)
        secondary_template.resource_count_is('AWS::SNS::TopicPolicy', 1)
        secondary_template.resource_count_is('AWS::Events::Rule', 1)

    def test_warm_standby_secondary_stack_no_execution_role_update(self, aws_credentials, warm_standby_env_vars):
        os.environ['MWAA_UPDATE_EXECUTION_ROLE'] = 'NO'

        conf = config.Config()
        app = cdk.App()

        secondary_stack = MwaaSecondaryStack(
            app,
            conf.get_name("secondary-stack"),
            conf=conf,
            env=cdk.Environment(account=conf.aws_account_id, region=conf.secondary_region),
        )

        secondary_template = Template.from_stack(secondary_stack)

        # One less then when MWAA_UPDATE_EXECUTION_ROLE is set to YES
        secondary_template.resource_count_is('AWS::IAM::Policy', 4)


    def test_warm_standby_secondary_stack_no_sfn_vpce(self, aws_credentials, warm_standby_env_vars):
        os.environ['SECONDARY_CREATE_SFN_VPCE'] = 'NO'

        conf = config.Config()
        app = cdk.App()

        secondary_stack = MwaaSecondaryStack(
            app,
            conf.get_name("secondary-stack"),
            conf=conf,
            env=cdk.Environment(account=conf.aws_account_id, region=conf.secondary_region),
        )

        secondary_template = Template.from_stack(secondary_stack)

        # These resources are not created when SECONDARY_CREATE_SFN_VPCE is set to NO
        secondary_template.resource_count_is('AWS::EC2::VPCEndpoint', 0)
        secondary_template.resource_count_is('AWS::EC2::SecurityGroupIngress', 0)
