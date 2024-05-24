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

from aws_cdk.assertions import Template
from sure import expect

from tests.unit.mocks.mock_setup import aws_credentials, warm_standby_env_vars
from tests.unit.mocks.stacks import warm_standby_stacks

class TestMwaaPrimaryStack:
    def test_primary_stack(self, warm_standby_stacks):
        primary_stack = warm_standby_stacks['primary']

        expect(primary_stack.source_bucket).to.be.truthy
        expect(primary_stack.cross_region_replication_role).to.be.truthy
        
        primary_template = Template.from_stack(primary_stack)

        primary_template.resource_count_is('AWS::S3::Bucket', 1)
        primary_template.resource_count_is('AWS::Lambda::Function', 7)
        primary_template.resource_count_is('AWS::Lambda::LayerVersion', 1)
        primary_template.resource_count_is('AWS::IAM::Role', 8)
        primary_template.resource_count_is('AWS::IAM::Policy', 7)
        primary_template.resource_count_is('AWS::S3::BucketPolicy', 1)
        primary_template.resource_count_is('Custom::S3AutoDeleteObjects', 1)
        primary_template.resource_count_is('Custom::CDKBucketDeployment', 1)
        primary_template.resource_count_is('AWS::SNS::Topic', 1)
        primary_template.resource_count_is('AWS::SNS::Subscription', 1)
        primary_template.resource_count_is('AWS::SNS::TopicPolicy', 1)
        primary_template.resource_count_is('Custom::AirflowCli', 2)

