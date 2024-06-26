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

import time

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import custom_resources as cr
from constructs import Construct

import config
from lib.dr_constructs.airflow_cli import AirflowCli, VpcInfo
from lib.functions.airflow_cli_client import AirflowCliCommand, AirflowCliInput
from lib.stacks.mwaa_base_stack import MwaaBaseStack


class MwaaPrimaryStack(MwaaBaseStack):
    @property
    def backup_bucket(self) -> s3.Bucket:
        return self._backup_bucket

    @property
    def source_bucket(self) -> s3.Bucket:
        return self._source_bucket

    @property
    def cross_region_replication_role(self) -> iam.IRole:
        return self._crr_role

    @property
    def dags_deployment(self) -> s3_deployment.BucketDeployment:
        return self._dags_deployment

    @property
    def variables_airflow_cli(self) -> AirflowCli:
        return self._variables_airflow_cli

    @property
    def unpause_airflow_cli(self) -> AirflowCli:
        return self._unpause_airflow_cli

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        secondary_backup_bucket: s3.Bucket,
        secondary_source_bucket: s3.Bucket,
        conf: config.Config = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, conf, **kwargs)
        conf = self.conf

        vpc_id = conf.primary_vpc_id
        subnet_ids = conf.primary_subnet_ids
        security_group_ids = conf.primary_security_group_ids
        self._vpc = self.get_vpc_info(conf, vpc_id, subnet_ids, security_group_ids)
        mwaa_role = iam.Role.from_role_arn(
            self, conf.get_name("mwaa-primary-role"), conf.primary_mwaa_role_arn
        )

        self.setup_cross_region_bucket_replications(
            secondary_backup_bucket, secondary_source_bucket, mwaa_role, conf
        )

        failure_notification_topic = self.create_sns_topic_with_email_subscriptions(
            conf
        )
        failure_notification_topic.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[mwaa_role],
                actions=["sns:Publish"],
                resources=[failure_notification_topic.topic_arn],
            )
        )
        self.failure_notification_topic = failure_notification_topic

        self.setup_variables_airflow_cli(conf)
        self.setup_dags_unpause_cli(conf)

        self.variables_airflow_cli.node.add_dependency(failure_notification_topic)
        self.dags_deployment.node.add_dependency(self.variables_airflow_cli)
        self.unpause_airflow_cli.node.add_dependency(self.dags_deployment)

    def setup_variables_airflow_cli(self, conf: config.Config) -> AirflowCli:
        set_backup_schedule_cmd = AirflowCliCommand(
            command=f'variables set DR_BACKUP_SCHEDULE "{conf.primary_backup_schedule}"'
        )
        set_backup_bucket_cmd = AirflowCliCommand(
            command=f"variables set DR_BACKUP_BUCKET {self.backup_bucket.bucket_name}"
        )
        set_notification_emails_cmd = AirflowCliCommand(
            command=f"variables set DR_SNS_TOPIC_ARN {self.failure_notification_topic.topic_arn}"
        )

        unset_backup_schedule_cmd = AirflowCliCommand(
            command=f"variables delete DR_BACKUP_SCHEDULE"
        )
        unset_backup_bucket_cmd = AirflowCliCommand(
            command=f"variables delete DR_BACKUP_BUCKET"
        )
        unset_notification_emails_cmd = AirflowCliCommand(
            command=f"variables delete DR_SNS_TOPIC_ARN"
        )

        env_name = conf.primary_mwaa_environment_name
        env_version = conf.mwaa_version

        cli_input = AirflowCliInput(
            create=[
                set_backup_schedule_cmd,
                set_backup_bucket_cmd,
                set_notification_emails_cmd,
            ],
            update=[
                set_backup_schedule_cmd,
                set_backup_bucket_cmd,
                set_notification_emails_cmd,
            ],
            delete=[
                unset_backup_schedule_cmd,
                unset_backup_bucket_cmd,
                unset_notification_emails_cmd,
            ],
        )

        airflow_cli = AirflowCli(
            self,
            id=conf.get_name("airflow-cli"),
            env_name=env_name,
            env_version=env_version,
            vpc_info=self.vpc,
            cli_input=cli_input,
        )

        self._variables_airflow_cli = airflow_cli
        return airflow_cli

    def setup_dags_unpause_cli(self, conf: config.Config) -> AirflowCli:
        unpause_dag_cmd = AirflowCliCommand(
            command=f"dags unpause {conf.metadata_export_dag_name}"
        )
        pause_dag_cmd = AirflowCliCommand(
            command=f"dags pause {conf.metadata_export_dag_name}"
        )

        env_name = conf.primary_mwaa_environment_name
        env_version = conf.mwaa_version

        cli_input = AirflowCliInput(
            create=[unpause_dag_cmd], update=[unpause_dag_cmd], delete=[pause_dag_cmd]
        )

        airflow_cli = AirflowCli(
            self,
            id=conf.get_name("airflow-cli-unpause"),
            env_name=env_name,
            env_version=env_version,
            vpc_info=self.vpc,
            cli_input=cli_input,
        )

        self._unpause_airflow_cli = airflow_cli
        return airflow_cli

    def setup_cross_region_bucket_replications(
        self,
        secondary_backup_bucket: s3.Bucket,
        secondary_source_bucket: s3.Bucket,
        mwaa_role: iam.IRole,
        conf: config.Config,
    ) -> None:
        _backup_bucket = self.create_backup_bucket(
            conf.get_name("backup-bucket"), mwaa_role
        )
        _source_bucket = s3.Bucket.from_bucket_name(
            self, conf.get_name("source-bucket"), conf.primary_dags_bucket_name
        )

        _crr_role = self.create_cross_region_replication_role(
            conf.get_name("crr-role"),
            _source_bucket,
            _backup_bucket,
            secondary_source_bucket,
            secondary_backup_bucket,
        )

        self.configure_internal_bucket_replication(
            _backup_bucket, secondary_backup_bucket, _crr_role
        )
        _source_bucket_new_object_cr = self.configure_external_bucket_replication(
            conf.get_name("source-bucket-custom-resource"),
            _source_bucket,
            secondary_source_bucket,
            _crr_role,
        )

        dags_deployment = s3_deployment.BucketDeployment(
            self,
            conf.get_name("dags-deployment"),
            sources=[s3_deployment.Source.asset(f"assets/dags")],
            destination_bucket=_source_bucket,
            destination_key_prefix=conf.mwaa_dags_s3_path,
            prune=False,
        )

        dags_deployment.node.add_dependency(_source_bucket_new_object_cr)

        self._source_bucket = _source_bucket
        self._backup_bucket = _backup_bucket
        self._crr_role = _crr_role
        self._dags_deployment = dags_deployment

    def create_backup_bucket(
        self, backup_bucket_id: str, mwaa_role: iam.IRole
    ) -> s3.Bucket:
        _backup_bucket = s3.Bucket(
            self,
            backup_bucket_id,
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

        cdk.CfnOutput(self, "MWAA-Backup-Bucket-Name", value=_backup_bucket.bucket_name)
        return _backup_bucket

    def create_cross_region_replication_role(
        self,
        role_id: str,
        _source_bucket: s3.IBucket,
        _backup_bucket: s3.IBucket,
        secondary_source_bucket: s3.IBucket,
        secondary_backup_bucket: s3.IBucket,
    ) -> iam.Role:
        _crr_role = iam.Role(
            self,
            role_id,
            assumed_by=iam.ServicePrincipal("s3.amazonaws.com"),
            path="/service-role/",
        )

        _crr_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetReplicationConfiguration", "s3:ListBucket"],
                resources=[_source_bucket.bucket_arn, _backup_bucket.bucket_arn],
            )
        )
        _crr_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging",
                ],
                resources=[
                    _source_bucket.arn_for_objects("*"),
                    _backup_bucket.arn_for_objects("*"),
                ],
            )
        )
        _crr_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                ],
                resources=[
                    secondary_source_bucket.arn_for_objects("*"),
                    secondary_backup_bucket.arn_for_objects("*"),
                ],
            )
        )
        return _crr_role

    def configure_internal_bucket_replication(
        self,
        primary_bucket: s3.IBucket,
        secondary_bucket: s3.IBucket,
        crr_role: iam.Role,
    ):
        # Setting up cross-region bucket replication for a stack internal bucket
        _cfn_bucket = primary_bucket.node.default_child

        _cfn_bucket.replication_configuration = (
            s3.CfnBucket.ReplicationConfigurationProperty(
                role=crr_role.role_arn,
                rules=[
                    s3.CfnBucket.ReplicationRuleProperty(
                        destination=s3.CfnBucket.ReplicationDestinationProperty(
                            bucket=secondary_bucket.bucket_arn
                        ),
                        status="Enabled",
                    )
                ],
            )
        )

    def configure_external_bucket_replication(
        self,
        id: str,
        primary_bucket: s3.IBucket,
        secondary_bucket: s3.IBucket,
        crr_role: iam.Role,
    ) -> cr.AwsCustomResource:
        # Setting up cross-region bucket replication for a stack external bucket
        # Have to use custom resource because the bucket is created external to this stack
        custom_resource = cr.AwsCustomResource(
            self,
            id,
            on_update=cr.AwsSdkCall(
                service="S3",
                action="putBucketReplication",
                parameters={
                    "Bucket": primary_bucket.bucket_name,
                    "ReplicationConfiguration": {
                        "Role": crr_role.role_arn,
                        "Rules": [
                            {
                                "Destination": {"Bucket": secondary_bucket.bucket_arn},
                                "Prefix": "",
                                "Status": "Enabled",
                            }
                        ],
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of(str(int(time.time()))),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [
                    iam.PolicyStatement(
                        actions=["s3:PutReplicationConfiguration"],
                        resources=[primary_bucket.bucket_arn],
                    ),
                    iam.PolicyStatement(actions=["iam:PassRole"], resources=["*"]),
                ]
            ),
        )
        return custom_resource
