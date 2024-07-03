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

import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_sub
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets

from constructs import Construct

import config


@dataclass
class VpcInfo:
    vpc: ec2.IVpc
    vpc_subnets: ec2.SubnetSelection
    security_groups: list[ec2.ISecurityGroup]


class MwaaBaseStack(Stack):

    @property
    def vpc(self) -> VpcInfo:
        return self._vpc

    def __init__(
        self, scope: Construct, construct_id: str, conf: config.Config = None, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.conf = conf or config.Config()
        self._vpc = None

    def get_vpc_info(
        self,
        conf: config.Config,
        vpc_id: str,
        subnet_ids: list[str],
        security_group_ids: list[str],
    ) -> VpcInfo:
        vpc = ec2.Vpc.from_lookup(self, conf.get_name("vpc"), vpc_id=vpc_id)

        subnets = list(
            map(
                lambda subnet_id: ec2.Subnet.from_subnet_id(
                    self, f'{conf.get_name("subnet")}-{subnet_id}', subnet_id
                ),
                subnet_ids,
            )
        )
        subnet_selection = ec2.SubnetSelection(subnets=subnets)

        security_groups = list(
            map(
                lambda sg_id: ec2.SecurityGroup.from_security_group_id(
                    self, f'{conf.get_name("sg")}-{sg_id}', sg_id
                ),
                security_group_ids,
            )
        )

        return VpcInfo(vpc, subnet_selection, security_groups)

    def create_sns_topic_with_email_subscriptions(self, conf: config.Config):
        emails = conf.mwaa_notification_emails
        topic = sns.Topic(self, conf.get_name("email-topic"))
        for email in emails:
            topic.add_subscription(sns_sub.EmailSubscription(email))
        return topic

    def setup_notification(
        self,
        conf: config.Config,
        sns_topic: sns.Topic,
        state_machine: sfn.StateMachine,
        statuses: list[str]
    ) -> tuple[sns.Topic, events.Rule]:
        rule = events.Rule(
            self,
            conf.get_name("sfn-failure-rule"),
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": statuses,
                    "stateMachineArn": [state_machine.state_machine_arn],
                },
            ),
            targets=[targets.SnsTopic(sns_topic)],
        )
        return sns_topic, rule
