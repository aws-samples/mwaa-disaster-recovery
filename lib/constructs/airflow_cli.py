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

from dataclasses import dataclass

from aws_cdk import BundlingOptions, CustomResource, Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import custom_resources as cr
from constructs import Construct
from dataclasses_json import dataclass_json

from lib.functions.airflow_cli_client import AirflowCliInput


@dataclass_json
@dataclass
class VpcInfo:
    vpc: ec2.IVpc
    vpc_subnets: ec2.SubnetSelection
    security_groups: list[ec2.ISecurityGroup]


class AirflowCli(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        env_version: str,
        vpc_info: VpcInfo,
        cli_input: AirflowCliInput,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)

        _cli_role = self.create_airflow_cli_function_role(f"{id}-role", env_name)
        _cli_function = self.create_airflow_cli_function(
            f"{id}-function", env_name, env_version, _cli_role, vpc_info
        )

        _provider = cr.Provider(
            self,
            f"{id}-provider",
            on_event_handler=_cli_function,
        )

        _custom_resource = CustomResource(
            self,
            f"{id}-custom-resource",
            resource_type="Custom::AirflowCli",
            service_token=_provider.service_token,
            removal_policy=RemovalPolicy.DESTROY,
            properties={"airflow_cli_input": cli_input.to_json()},
        )

        self._custom_resource = _custom_resource
        self._provider = _provider
        self._cli_function = _cli_function

    def create_airflow_cli_function_role(self, id: str, env_name: str) -> iam.Role:
        stack = Stack.of(self)
        mwaa_arn = (
            f"arn:aws:airflow:{stack.region}:{stack.account}:environment/{env_name}"
        )

        role = iam.Role(
            self,
            id,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        role.add_to_policy(
            iam.PolicyStatement(
                resources=[mwaa_arn],
                actions=["airflow:CreateCliToken"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                resources=["*"],
                actions=[
                    "ec2:DescribeInstances",
                    "ec2:CreateNetworkInterface",
                    "ec2:AttachNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "autoscaling:CompleteLifecycleAction",
                    "ec2:DeleteNetworkInterface",
                ],
            )
        )

        return role

    def create_airflow_cli_function(
        self,
        id: str,
        env_name: str,
        env_version: str,
        cli_role: iam.Role,
        vpc_info: VpcInfo,
    ) -> _lambda.Function:
        cli_fn = _lambda.Function(
            self,
            id,
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="airflow_cli_function.on_event",
            role=cli_role,
            vpc=vpc_info.vpc,
            vpc_subnets=vpc_info.vpc_subnets,
            security_groups=vpc_info.security_groups,
            timeout=Duration.minutes(10),
            environment={
                "MWAA_ENV_NAME": env_name,
                "MWAA_ENV_VERSION": env_version,
            },
        )

        return cli_fn
