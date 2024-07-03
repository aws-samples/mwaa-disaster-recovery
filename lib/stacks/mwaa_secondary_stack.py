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
from dataclasses import dataclass

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

import config
from lib.stacks.mwaa_base_stack import MwaaBaseStack, VpcInfo


@dataclass
class DagTriggerStateInput:
    state_name: str
    mwaa_env_name: str
    dag_name: str
    bucket: str
    dr_type: str
    function: _lambda.Function


class MwaaSecondaryStack(MwaaBaseStack):
    @property
    def backup_bucket(self) -> s3.Bucket:
        return self._backup_bucket

    @property
    def source_bucket(self) -> s3.Bucket:
        return self._source_bucket

    @property
    def state_machine(self) -> sfn.StateMachine:
        return self._state_machine

    def __init__(
        self, scope: Construct, construct_id: str, conf: config.Config = None, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, conf, **kwargs)
        conf = self.conf

        vpc_id = conf.secondary_vpc_id
        subnet_ids = conf.secondary_subnet_ids
        security_group_ids = conf.secondary_security_group_ids
        self._vpc = self.get_vpc_info(conf, vpc_id, subnet_ids, security_group_ids)

        mwaa_role = iam.Role.from_role_arn(
            self, conf.get_name("mwaa-role"), conf.secondary_mwaa_role_arn
        )

        self.setup_buckets(conf, mwaa_role)
        self.update_execution_role(conf, mwaa_role)

        schedule_name = conf.get_name("scheduler")

        if conf.dr_type == config.DR_BACKUP_RESTORE:
            self._state_machine = self.create_backup_restore_state_machine(
                conf=conf, vpc_info=self._vpc, schedule_name=schedule_name
            )
        else:
            self._state_machine = self.create_warm_standby_state_machine(
                conf=conf, vpc_info=self._vpc, schedule_name=schedule_name
            )

        self.create_scheduler(
            conf=conf, schedule_name=schedule_name, state_machine=self._state_machine
        )
        self.sns_topic = self.create_sns_topic_with_email_subscriptions(conf)
        self.setup_notification(
            conf,
            self.sns_topic,
            self._state_machine,
            ["FAILED", "TIMED_OUT", "ABORTED"],
        )

        if conf.secondary_create_step_functions_vpce:
            self.setup_sfn_vpce(conf, self._vpc)

    def setup_buckets(self, conf: config.Config, mwaa_role: iam.IRole) -> s3.Bucket:
        _source_bucket = s3.Bucket.from_bucket_name(
            self, conf.get_name("source-bucket"), conf.secondary_dags_bucket_name
        )
        self._source_bucket = _source_bucket

        _backup_bucket = s3.Bucket(
            self,
            conf.get_name("backup-bucket"),
            bucket_name=cdk.PhysicalName.GENERATE_IF_NEEDED,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        _backup_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[mwaa_role],
                actions=[
                    "s3:Abort*",
                    "s3:DeleteObject*",
                    "s3:GetBucket*",
                    "s3:GetObject*",
                    "s3:List*",
                    "s3:PutObject",
                    "s3:PutObjectLegalHold",
                    "s3:PutObjectRetention",
                    "s3:PutObjectTagging",
                    "s3:PutObjectVersionTagging",
                ],
                resources=[
                    _backup_bucket.bucket_arn,
                    _backup_bucket.arn_for_objects("*"),
                ],
            )
        )

        self._backup_bucket = _backup_bucket
        cdk.CfnOutput(self, "MWAA-Backup-Bucket-Name", value=_backup_bucket.bucket_name)

    def create_warm_standby_state_machine(
        self, conf: config.Config, vpc_info: VpcInfo, schedule_name: str
    ) -> sfn.StateMachine:
        success = sfn.Succeed(self, "Workflow Complete")

        scheduler_heartbeat_fn = self.get_scheduler_heartbeat_function(conf)
        get_scheduler_heartbeat_state = self.create_get_scheduler_heartbeat_state(
            state_name="Get Scheduler Heartbeat",
            scheduler_heartbeat_fn=scheduler_heartbeat_fn,
            env_name=conf.primary_mwaa_environment_name,
            env_region=conf.primary_region,
            simulate_dr="$$.Execution.Input.simulate_dr",
        )

        dag_trigger_function = self.create_dag_trigger_function(
            logical_id=conf.get_name("dag-trigger-function"),
            env_name=conf.secondary_mwaa_environment_name,
            vpc_info=vpc_info,
        )

        cleanup_metadata_state = self.create_dag_trigger_state(
            state_name="Cleanup Metadata",
            mwaa_env_name=conf.secondary_mwaa_environment_name,
            mwaa_env_version=conf.mwaa_version,
            dag_name=conf.metadata_cleanup_dag_name,
            bucket=self.backup_bucket.bucket_name,
            dr_type=conf.dr_type,
            connection_restore_strategy=conf.dr_connection_restore_strategy,
            variable_restore_strategy=conf.dr_variable_restore_strategy,
            function=dag_trigger_function,
        )

        cool_off_state = sfn.Wait(
            self,
            f"Cool Off {conf.secondary_cleanup_cool_off_secs}s",
            time=sfn.WaitTime.duration(
                cdk.Duration.seconds(conf.secondary_cleanup_cool_off_secs)
            ),
        )

        restore_metadata_state = self.create_dag_trigger_state(
            state_name="Restore Metadata",
            mwaa_env_name=conf.secondary_mwaa_environment_name,
            mwaa_env_version=conf.mwaa_version,
            dag_name=conf.metadata_import_dag_name,
            bucket=self.backup_bucket.bucket_name,
            dr_type=conf.dr_type,
            connection_restore_strategy=conf.dr_connection_restore_strategy,
            variable_restore_strategy=conf.dr_variable_restore_strategy,
            function=dag_trigger_function,
        )

        disable_schedule_state = self.create_disable_scheduler_state(schedule_name)

        unhealthy_flow = (
            disable_schedule_state.next(cleanup_metadata_state)
            .next(cool_off_state)
            .next(restore_metadata_state)
            .next(success)
        )

        check_heartbeat_flow = (
            sfn.Choice(self, "Check Heartbeat")
            .when(sfn.Condition.string_equals("$.Payload", "UNHEALTHY"), unhealthy_flow)
            .otherwise(success)
        )

        overall_flow = get_scheduler_heartbeat_state.next(check_heartbeat_flow)

        state_machine = sfn.StateMachine(
            self,
            conf.get_name("state-machine"),
            definition_body=sfn.DefinitionBody.from_chainable(overall_flow),
            timeout=cdk.Duration.minutes(conf.state_machine_timeout_mins),
        )
        return state_machine

    def create_backup_restore_state_machine(
        self, conf: config.Config, vpc_info: VpcInfo, schedule_name: str
    ) -> sfn.StateMachine:
        scheduler_heartbeat_fn = self.get_scheduler_heartbeat_function(conf)
        get_scheduler_heartbeat_state = self.create_get_scheduler_heartbeat_state(
            state_name="Get Scheduler Heartbeat",
            scheduler_heartbeat_fn=scheduler_heartbeat_fn,
            env_name=conf.primary_mwaa_environment_name,
            env_region=conf.primary_region,
            simulate_dr="$$.Execution.Input.simulate_dr",
        )

        get_env_function = self.create_get_environment_function(conf)
        get_env_state = self.create_get_environment_details_state(
            state_name="Get Environment Details",
            get_env_fn=get_env_function,
            env_name=conf.primary_mwaa_environment_name,
            env_region=conf.primary_region,
        )

        store_env_state = self.create_store_environment_details_state(
            conf, self.backup_bucket
        )

        healthy_flow = get_env_state.next(store_env_state)

        disable_schedule_state = self.create_disable_scheduler_state(schedule_name)

        read_env_state = self.create_read_environment_details_state(
            conf, self.backup_bucket
        )
        create_env_state = self.create_new_environment_state(conf)
        wait_state = sfn.Wait(
            self,
            f"Wait {conf.mwaa_create_env_polling_interval_secs}s",
            time=sfn.WaitTime.duration(
                cdk.Duration.seconds(conf.mwaa_create_env_polling_interval_secs)
            ),
        )

        get_status_state = self.create_get_environment_details_state(
            state_name="Get Environment Status",
            get_env_fn=get_env_function,
            env_name=conf.secondary_mwaa_environment_name,
            env_region=conf.secondary_region,
            result_selector={"result.$": "States.StringToJson($.Payload)"},
        )

        dag_trigger_function = self.create_dag_trigger_function(
            logical_id=conf.get_name("dag-trigger-function"),
            env_name=conf.secondary_mwaa_environment_name,
            vpc_info=vpc_info,
        )
        restore_metadata_state = self.create_dag_trigger_state(
            state_name="Restore Metadata",
            mwaa_env_name=conf.secondary_mwaa_environment_name,
            mwaa_env_version=conf.mwaa_version,
            dag_name=conf.metadata_import_dag_name,
            bucket=self.backup_bucket.bucket_name,
            dr_type=conf.dr_type,
            connection_restore_strategy=conf.dr_connection_restore_strategy,
            variable_restore_strategy=conf.dr_variable_restore_strategy,
            function=dag_trigger_function,
        )

        check_environment_flow = (
            sfn.Choice(self, "Check Environment Status")
            .when(
                sfn.Condition.string_equals("$.result.Environment.Status", "AVAILABLE"),
                restore_metadata_state,
            )
            .otherwise(wait_state)
        )

        unhealthy_flow = (
            disable_schedule_state.next(read_env_state)
            .next(create_env_state)
            .next(wait_state)
            .next(get_status_state)
            .next(check_environment_flow)
        )

        check_heartbeat_flow = (
            sfn.Choice(self, "Check Heartbeat")
            .when(sfn.Condition.string_equals("$.Payload", "HEALTHY"), healthy_flow)
            .otherwise(unhealthy_flow)
        )

        overall_flow = get_scheduler_heartbeat_state.next(check_heartbeat_flow)

        state_machine = sfn.StateMachine(
            self,
            conf.get_name("state-machine"),
            definition_body=sfn.DefinitionBody.from_chainable(overall_flow),
            timeout=cdk.Duration.minutes(conf.state_machine_timeout_mins),
        )
        return state_machine

    def create_get_scheduler_heartbeat_state(
        self,
        state_name: str,
        scheduler_heartbeat_fn: _lambda.Function,
        env_name: str,
        env_region: str,
        simulate_dr: str = None,
        result_selector: dict = None,
    ) -> tasks.CallAwsService:
        state = tasks.LambdaInvoke(
            self,
            state_name,
            lambda_function=scheduler_heartbeat_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "env_name": env_name,
                    "region": env_region,
                    "simulate_dr": (
                        sfn.JsonPath.string_at(simulate_dr) if simulate_dr else "NO"
                    ),
                }
            ),
            retry_on_service_exceptions=False,
            result_selector=result_selector,
        )
        return state

    def create_get_environment_details_state(
        self,
        state_name: str,
        get_env_fn: _lambda.Function,
        env_name: str,
        env_region: str,
        result_selector: dict = None,
    ) -> tasks.CallAwsService:
        state = tasks.LambdaInvoke(
            self,
            state_name,
            lambda_function=get_env_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "env_name": env_name,
                    "env_region": env_region,
                }
            ),
            retry_on_service_exceptions=False,
            result_selector=result_selector,
        )
        return state

    def create_get_environment_function(self, conf: config.Config) -> _lambda.Function:
        account_id = conf.aws_account_id

        primary_region = conf.primary_region
        secondary_region = conf.secondary_region

        primary_env_name = conf.primary_mwaa_environment_name
        secondary_env_name = conf.secondary_mwaa_environment_name

        primary_mwaa_arn = f"arn:aws:airflow:{primary_region}:{account_id}:environment/{primary_env_name}"
        primary_dummy_dr_arn = (
            f"arn:aws:airflow:{primary_region}:{account_id}:environment/MWAA-Dummy-DR"
        )
        secondary_mwaa_arn = f"arn:aws:airflow:{secondary_region}:{account_id}:environment/{secondary_env_name}"

        mwaa_arns = [primary_mwaa_arn, primary_dummy_dr_arn, secondary_mwaa_arn]

        get_env_fn = _lambda.Function(
            self,
            conf.get_name("get-environment-function"),
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="get_environment_function.handler",
            timeout=cdk.Duration.seconds(10),
        )

        get_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=mwaa_arns,
                actions=["airflow:GetEnvironment"],
            )
        )
        return get_env_fn

    def create_store_environment_details_state(
        self, conf: config.Config, bucket: s3.Bucket
    ) -> tasks.CallAwsService:
        key = conf.mwaa_backup_file_name

        state = tasks.CallAwsService(
            self,
            "Store Environment Details",
            service="s3",
            action="putObject",
            parameters={
                "Bucket": bucket.bucket_name,
                "Key": key,
                "Body": sfn.JsonPath.object_at("$"),
            },
            iam_resources=[bucket.arn_for_objects(key)],
        )
        return state

    def create_dag_trigger_function(
        self, logical_id: str, env_name: str, vpc_info: VpcInfo
    ) -> _lambda.Function:
        mwaa_arn = (
            f"arn:aws:airflow:{self.region}:{self.account}:environment/{env_name}"
        )

        trigger_fn = _lambda.Function(
            self,
            logical_id,
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="airflow_dag_trigger_function.handler",
            vpc=vpc_info.vpc,
            vpc_subnets=vpc_info.vpc_subnets,
            security_groups=vpc_info.security_groups,
            timeout=cdk.Duration.minutes(10),
        )
        trigger_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=[mwaa_arn],
                actions=["airflow:CreateCliToken"],
            )
        )
        return trigger_fn

    def create_dag_trigger_state(
        self,
        state_name: str,
        mwaa_env_name: str,
        mwaa_env_version: str,
        dag_name: str,
        bucket: str,
        dr_type: str,
        variable_restore_strategy: str,
        connection_restore_strategy: str,
        function: _lambda.Function,
    ) -> tasks.LambdaInvoke:
        state = tasks.LambdaInvoke(
            self,
            state_name,
            lambda_function=function,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object(
                {
                    "task_token": sfn.JsonPath.task_token,
                    "mwaa_env_name": mwaa_env_name,
                    "mwaa_env_version": mwaa_env_version,
                    "dag": dag_name,
                    "bucket": bucket,
                    "dr_type": dr_type,
                    "variable_restore_strategy": variable_restore_strategy,
                    "connection_restore_strategy": connection_restore_strategy,
                }
            ),
            retry_on_service_exceptions=True,
        )
        return state

    def update_execution_role(self, conf: config.Config, role: iam.IRole) -> None:
        if conf.mwaa_update_execution_role.lower() == "yes":
            iam.Policy(
                self,
                conf.get_name("mwaa-execution-role-policy"),
                roles=[role],
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "states:SendTaskSuccess",
                            "states:SendTaskFailure",
                            "states:SendTaskHeartbeat",
                        ],
                        resources=[
                            f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"
                        ],
                    )
                ],
            )

    def create_read_environment_details_state(
        self, conf: config.Config, bucket: s3.Bucket
    ) -> tasks.CallAwsService:
        key = conf.mwaa_backup_file_name

        state = tasks.CallAwsService(
            self,
            "Read Environment Backup",
            service="s3",
            action="getObject",
            parameters={
                "Bucket": bucket.bucket_name,
                "Key": key,
            },
            iam_resources=[bucket.arn_for_objects(key)],
            result_selector={
                "result.$": "States.StringToJson($.Body)",
            },
        )
        return state

    def create_new_environment_state(self, conf: config.Config) -> tasks.LambdaInvoke:
        state = tasks.LambdaInvoke(
            self,
            "Create New Environment",
            lambda_function=self.create_new_environment_function(conf),
            payload=sfn.TaskInput.from_object(
                {
                    "execution_role_arn": conf.secondary_mwaa_role_arn,
                    "name": conf.secondary_mwaa_environment_name,
                    "network": {
                        "subnet_ids": conf.secondary_subnet_ids,
                        "security_group_ids": conf.secondary_security_group_ids,
                    },
                    "source_bucket_arn": self.source_bucket.bucket_arn,
                    "environment": sfn.JsonPath.object_at("$.result.Payload"),
                }
            ),
            retry_on_service_exceptions=True,
        )
        return state

    def create_new_environment_function(self, conf: config.Config) -> _lambda.Function:
        create_env_fn = _lambda.Function(
            self,
            conf.get_name("create-environment-function"),
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="create_environment_function.handler",
            timeout=cdk.Duration.seconds(10),
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:airflow:{conf.secondary_region}:{conf.aws_account_id}:environment/{conf.secondary_mwaa_environment_name}"
                ],
                actions=["airflow:CreateEnvironment"],
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=[self.source_bucket.bucket_arn],
                actions=["s3:GetEncryptionConfiguration"],
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=["arn:aws:logs:*:*:log-group:airflow-*:*"],
                actions=[
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:DescribeLogGroups",
                ],
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=["*"],
                actions=[
                    "ec2:AttachNetworkInterface",
                    "ec2:CreateNetworkInterface",
                    "ec2:CreateNetworkInterfacePermission",
                    "ec2:DescribeDhcpOptions",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcEndpoints",
                    "ec2:DescribeVpcs",
                ],
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=[
                    "arn:aws:ec2:*:*:vpc/*",
                    "arn:aws:ec2:*:*:vpc-endpoint/*",
                    "arn:aws:ec2:*:*:security-group/*",
                    "arn:aws:ec2:*:*:subnet/*",
                ],
                actions=["ec2:CreateVpcEndpoint", "ec2:ModifyVpcEndpoint"],
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=["arn:aws:ec2:*:*:vpc-endpoint/*"], actions=["ec2:CreateTags"]
            )
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(resources=["*"], actions=["cloudwatch:PutMetricData"])
        )
        create_env_fn.add_to_role_policy(
            iam.PolicyStatement(resources=["*"], actions=["iam:PassRole"])
        )

        return create_env_fn

    def create_scheduler(
        self, schedule_name: str, conf: config.Config, state_machine: sfn.StateMachine
    ) -> scheduler.CfnSchedule:
        role = iam.Role(
            self,
            conf.get_name("scheduler-role"),
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            inline_policies={
                "sfn-start-execution": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            resources=[state_machine.state_machine_arn],
                            actions=["states:StartExecution"],
                        )
                    ]
                )
            },
        )

        state = "ENABLED" if conf.health_check_enabled else "DISABLED"
        schedule = scheduler.CfnSchedule(
            self,
            conf.get_name("scheduler"),
            name=schedule_name,
            flexible_time_window={"mode": "OFF"},
            schedule_expression=f"rate({conf.health_check_interval_mins} minutes)",
            target={
                "arn": state_machine.state_machine_arn,
                "roleArn": role.role_arn,
                "input": json.dumps(
                    {
                        "simulate_dr": conf.mwaa_simulate_dr,
                    }
                ),
            },
            state=state,
        )
        return schedule

    def create_disable_scheduler_state(self, schedule_name: str) -> tasks.LambdaInvoke:
        state = tasks.LambdaInvoke(
            self,
            "Disable Schedule",
            lambda_function=self.disable_scheduler_function(self.conf),
            payload=sfn.TaskInput.from_object({"schedule_name": schedule_name}),
            retry_on_service_exceptions=True,
        )
        return state

    def disable_scheduler_function(self, conf: config.Config) -> _lambda.Function:
        disable_scheduler_fn = _lambda.Function(
            self,
            conf.get_name("disable-scheduler-function"),
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="disable_scheduler_function.handler",
            timeout=cdk.Duration.seconds(10),
        )
        disable_scheduler_fn.add_to_role_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:scheduler:{conf.secondary_region}:{conf.aws_account_id}:schedule/*"
                ],
                actions=["scheduler:GetSchedule", "scheduler:UpdateSchedule"],
            )
        )
        disable_scheduler_fn.add_to_role_policy(
            iam.PolicyStatement(resources=["*"], actions=["iam:PassRole"])
        )

        return disable_scheduler_fn

    def get_scheduler_heartbeat_function(self, conf: config.Config) -> _lambda.Function:
        cloudwatch_health_check_fn = _lambda.Function(
            self,
            conf.get_name("cloudwatch-health-check-function"),
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                path="lib/functions",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="cloudwatch_health_check_function.handler",
            timeout=cdk.Duration.seconds(10),
        )
        cloudwatch_health_check_fn.add_to_role_policy(
            iam.PolicyStatement(resources=["*"], actions=["cloudwatch:GetMetricData"])
        )

        return cloudwatch_health_check_fn

    def setup_sfn_vpce(
        self, conf: config.Config, vpc_info: VpcInfo
    ) -> ec2.InterfaceVpcEndpoint:
        vpce = vpc_info.vpc.add_interface_endpoint(
            conf.get_name("sfn-vpce"),
            service=ec2.InterfaceVpcEndpointAwsService.STEP_FUNCTIONS,
            security_groups=vpc_info.security_groups,
            subnets=vpc_info.vpc_subnets,
            private_dns_enabled=True,
        )
        return vpce
