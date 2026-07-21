"""
CDK assertion tests for Glue resources provisioned conditionally
when MWAA version starts with "3.".

Requirements: 9.3, 9.4, 10.4
"""

from aws_cdk.assertions import Template, Match

from tests.unit.mocks.mock_setup import (
    aws_credentials,
    warm_standby_env_vars,
    warm_standby_v3_env_vars,
)
from tests.unit.mocks.stacks import warm_standby_stacks, warm_standby_v3_stacks


class TestGlueResourcesV3:
    """Test that version 3.x config produces Glue IAM role, MWAA role policies, and script deployment."""

    def test_primary_stack_creates_glue_iam_role(self, warm_standby_v3_stacks):
        primary_stack = warm_standby_v3_stacks["primary"]
        template = Template.from_stack(primary_stack)

        # Glue IAM role should exist with glue.amazonaws.com trust policy
        template.has_resource_properties(
            "AWS::IAM::Role",
            Match.object_like(
                {
                    "AssumeRolePolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": "sts:AssumeRole",
                                            "Effect": "Allow",
                                            "Principal": {
                                                "Service": "glue.amazonaws.com"
                                            },
                                        }
                                    )
                                ]
                            )
                        }
                    )
                }
            ),
        )

    def test_primary_stack_glue_role_has_vpc_permissions(self, warm_standby_v3_stacks):
        primary_stack = warm_standby_v3_stacks["primary"]
        template = Template.from_stack(primary_stack)

        # Glue role policy should include VPC networking permissions
        template.has_resource_properties(
            "AWS::IAM::Policy",
            Match.object_like(
                {
                    "PolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": Match.array_with(
                                                [
                                                    "ec2:CreateNetworkInterface",
                                                    "ec2:DeleteNetworkInterface",
                                                    "ec2:DescribeNetworkInterfaces",
                                                ]
                                            ),
                                            "Effect": "Allow",
                                        }
                                    )
                                ]
                            )
                        }
                    )
                }
            ),
        )

    def test_primary_stack_glue_role_has_cloudwatch_permissions(
        self, warm_standby_v3_stacks
    ):
        primary_stack = warm_standby_v3_stacks["primary"]
        template = Template.from_stack(primary_stack)

        template.has_resource_properties(
            "AWS::IAM::Policy",
            Match.object_like(
                {
                    "PolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": [
                                                "logs:CreateLogGroup",
                                                "logs:CreateLogStream",
                                                "logs:PutLogEvents",
                                            ],
                                            "Effect": "Allow",
                                        }
                                    )
                                ]
                            )
                        }
                    )
                }
            ),
        )

    def test_primary_stack_deploys_glue_scripts(self, warm_standby_v3_stacks):
        primary_stack = warm_standby_v3_stacks["primary"]
        template = Template.from_stack(primary_stack)

        # Should have 2 BucketDeployments: one for DAGs, one for Glue scripts
        template.resource_count_is("Custom::CDKBucketDeployment", 2)

    def test_secondary_stack_creates_glue_iam_role(self, warm_standby_v3_stacks):
        secondary_stack = warm_standby_v3_stacks["secondary"]
        template = Template.from_stack(secondary_stack)

        # Glue IAM role should exist with glue.amazonaws.com trust policy
        template.has_resource_properties(
            "AWS::IAM::Role",
            Match.object_like(
                {
                    "AssumeRolePolicyDocument": Match.object_like(
                        {
                            "Statement": Match.array_with(
                                [
                                    Match.object_like(
                                        {
                                            "Action": "sts:AssumeRole",
                                            "Effect": "Allow",
                                            "Principal": {
                                                "Service": "glue.amazonaws.com"
                                            },
                                        }
                                    )
                                ]
                            )
                        }
                    )
                }
            ),
        )

    def test_secondary_stack_no_glue_script_deployment(self, warm_standby_v3_stacks):
        secondary_stack = warm_standby_v3_stacks["secondary"]
        template = Template.from_stack(secondary_stack)

        # Secondary stack should NOT have BucketDeployment (scripts replicated from primary)
        template.resource_count_is("Custom::CDKBucketDeployment", 0)


class TestGlueResourcesV2BackwardCompatibility:
    """Test that version 2.x config does NOT produce Glue resources."""

    def test_primary_stack_no_glue_role_for_v2(self, warm_standby_stacks):
        primary_stack = warm_standby_stacks["primary"]
        template = Template.from_stack(primary_stack)

        # No IAM role with glue.amazonaws.com trust policy should exist
        roles = template.find_resources(
            "AWS::IAM::Role",
            {
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Action": "sts:AssumeRole",
                                "Effect": "Allow",
                                "Principal": {"Service": "glue.amazonaws.com"},
                            }
                        ]
                    }
                }
            },
        )
        assert len(roles) == 0, "Glue IAM role should not exist for v2.x"

    def test_primary_stack_single_bucket_deployment_for_v2(self, warm_standby_stacks):
        primary_stack = warm_standby_stacks["primary"]
        template = Template.from_stack(primary_stack)

        # Only 1 BucketDeployment (DAGs only, no Glue scripts)
        template.resource_count_is("Custom::CDKBucketDeployment", 1)

    def test_secondary_stack_no_glue_role_for_v2(self, warm_standby_stacks):
        secondary_stack = warm_standby_stacks["secondary"]
        template = Template.from_stack(secondary_stack)

        roles = template.find_resources(
            "AWS::IAM::Role",
            {
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Action": "sts:AssumeRole",
                                "Effect": "Allow",
                                "Principal": {"Service": "glue.amazonaws.com"},
                            }
                        ]
                    }
                }
            },
        )
        assert len(roles) == 0, "Glue IAM role should not exist for v2.x"

    def test_v2_primary_stack_resource_counts_unchanged(self, warm_standby_stacks):
        """Verify existing v2.x resource counts are not affected."""
        primary_stack = warm_standby_stacks["primary"]
        template = Template.from_stack(primary_stack)

        template.resource_count_is("AWS::S3::Bucket", 2)
        template.resource_count_is("Custom::CDKBucketDeployment", 1)
        template.resource_count_is("AWS::SNS::Topic", 1)
        template.resource_count_is("Custom::AirflowCli", 2)
