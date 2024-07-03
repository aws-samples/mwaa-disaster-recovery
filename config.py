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

import json
import os

from dotenv import load_dotenv

load_dotenv()

# DR Option
DR_BACKUP_RESTORE = "BACKUP_RESTORE"
DR_WARM_STANDBY = "WARM_STANDBY"
APPEND = "APPEND"
REPLACE = "REPLACE"
DO_NOTHING = "DO_NOTHING"

# Environment variables for stack and the mwaa_dr framework configuration
STACK_NAME_PREFIX = "STACK_NAME_PREFIX"
AWS_ACCOUNT_ID = "AWS_ACCOUNT_ID"
DR_TYPE = "DR_TYPE"
DR_VARIABLE_RESTORE_STRATEGY = "DR_VARIABLE_RESTORE_STRATEGY"
DR_CONNECTION_RESTORE_STRATEGY = "DR_CONNECTION_RESTORE_STRATEGY"
MWAA_VERSION = "MWAA_VERSION"
MWAA_DAGS_S3_PATH = "MWAA_DAGS_S3_PATH"
MWAA_NOTIFICATION_EMAILS = "MWAA_NOTIFICATION_EMAILS"
MWAA_BACKUP_FILE_NAME = "MWAA_BACKUP_FILE_NAME"
MWAA_UPDATE_EXECUTION_ROLE = "MWAA_UPDATE_EXECUTION_ROLE"
MWAA_CREATE_ENV_POLLING_INTERVAL_SECS = "MWAA_CREATE_ENV_POLLING_INTERVAL_SECS"
METADATA_EXPORT_DAG_NAME = "METADATA_EXPORT_DAG_NAME"
METADATA_IMPORT_DAG_NAME = "METADATA_IMPORT_DAG_NAME"
METADATA_CLEANUP_DAG_NAME = "METADATA_CLEANUP_DAG_NAME"
STATE_MACHINE_TIMEOUT_MINS = "STATE_MACHINE_TIMEOUT_MINS"
MWAA_SIMULATE_DR = "MWAA_SIMULATE_DR"

HEALTH_CHECK_ENABLED = "HEALTH_CHECK_ENABLED"
HEALTH_CHECK_INTERVAL_MINS = "HEALTH_CHECK_INTERVAL_MINS"
HEALTH_CHECK_MAX_RETRY = "HEALTH_CHECK_MAX_RETRY"
HEALTH_CHECK_RETRY_INTERVAL_SECS = "HEALTH_CHECK_RETRY_INTERVAL_SECS"
HEALTH_CHECK_RETRY_BACKOFF_RATE = "HEALTH_CHECK_RETRY_BACKOFF_RATE"


# Environment variables for primary stack configuration
PRIMARY_REGION = "PRIMARY_REGION"
PRIMARY_MWAA_ENVIRONMENT_NAME = "PRIMARY_MWAA_ENVIRONMENT_NAME"
PRIMARY_MWAA_ROLE_ARN = "PRIMARY_MWAA_ROLE_ARN"
PRIMARY_DAGS_BUCKET_NAME = "PRIMARY_DAGS_BUCKET_NAME"
PRIMARY_SCHEDULE_INTERVAL = "PRIMARY_SCHEDULE_INTERVAL"
PRIMARY_VPC_ID = "PRIMARY_VPC_ID"
PRIMARY_SUBNET_IDS = "PRIMARY_SUBNET_IDS"
PRIMARY_SECURITY_GROUP_IDS = "PRIMARY_SECURITY_GROUP_IDS"
PRIMARY_BACKUP_SCHEDULE = "PRIMARY_BACKUP_SCHEDULE"
PRIMARY_REPLICATION_POLLING_INTERVAL_SECS = "PRIMARY_REPLICATION_POLLING_INTERVAL_SECS"

# Environment variables for secondary stack configuration
SECONDARY_REGION = "SECONDARY_REGION"
SECONDARY_MWAA_ENVIRONMENT_NAME = "SECONDARY_MWAA_ENVIRONMENT_NAME"
SECONDARY_MWAA_ROLE_ARN = "SECONDARY_MWAA_ROLE_ARN"
SECONDARY_DAGS_BUCKET_NAME = "SECONDARY_DAGS_BUCKET_NAME"
SECONDARY_VPC_ID = "SECONDARY_VPC_ID"
SECONDARY_SUBNET_IDS = "SECONDARY_SUBNET_IDS"
SECONDARY_SECURITY_GROUP_IDS = "SECONDARY_SECURITY_GROUP_IDS"
SECONDARY_CREATE_SFN_VPCE = "SECONDARY_CREATE_SFN_VPCE"
SECONDARY_CLEANUP_COOL_OFF_SECS = "SECONDARY_CLEANUP_COOL_OFF_SECS"

REQUIRED_CONFIGS = [
    STACK_NAME_PREFIX,
    AWS_ACCOUNT_ID,
    DR_TYPE,
    MWAA_VERSION,
    MWAA_UPDATE_EXECUTION_ROLE,
    PRIMARY_REGION,
    PRIMARY_MWAA_ENVIRONMENT_NAME,
    PRIMARY_MWAA_ROLE_ARN,
    PRIMARY_DAGS_BUCKET_NAME,
    PRIMARY_VPC_ID,
    PRIMARY_SUBNET_IDS,
    PRIMARY_SECURITY_GROUP_IDS,
    PRIMARY_BACKUP_SCHEDULE,
    SECONDARY_REGION,
    SECONDARY_MWAA_ENVIRONMENT_NAME,
    SECONDARY_MWAA_ROLE_ARN,
    SECONDARY_DAGS_BUCKET_NAME,
    SECONDARY_VPC_ID,
    SECONDARY_SUBNET_IDS,
    SECONDARY_SECURITY_GROUP_IDS,
    SECONDARY_CREATE_SFN_VPCE,
]

DEFAULT_CONFIGS = {
    DR_VARIABLE_RESTORE_STRATEGY: "APPEND",
    DR_CONNECTION_RESTORE_STRATEGY: "APPEND",
    MWAA_DAGS_S3_PATH: "dags",
    MWAA_NOTIFICATION_EMAILS: "[]",
    MWAA_BACKUP_FILE_NAME: "environment.json",
    MWAA_CREATE_ENV_POLLING_INTERVAL_SECS: "60",
    METADATA_EXPORT_DAG_NAME: "backup_metadata",
    METADATA_IMPORT_DAG_NAME: "restore_metadata",
    METADATA_CLEANUP_DAG_NAME: "cleanup_metadata",
    STATE_MACHINE_TIMEOUT_MINS: "60",
    MWAA_SIMULATE_DR: "NO",
    HEALTH_CHECK_ENABLED: "YES",
    HEALTH_CHECK_INTERVAL_MINS: "5",
    HEALTH_CHECK_MAX_RETRY: "2",
    HEALTH_CHECK_RETRY_INTERVAL_SECS: "10",
    HEALTH_CHECK_RETRY_BACKOFF_RATE: "2",
    PRIMARY_SCHEDULE_INTERVAL: "0 * * * *",
    PRIMARY_REPLICATION_POLLING_INTERVAL_SECS: "30",
    SECONDARY_CLEANUP_COOL_OFF_SECS: "30",
}


class Config:
    def __init__(self, required_configs=None, default_configs=None):
        self.required_configs = required_configs or REQUIRED_CONFIGS
        self.default_configs = default_configs or DEFAULT_CONFIGS

    def get(self, key) -> str:
        value = os.getenv(key)

        if key in self.required_configs and not value:
            raise ValueError(f"The required environment variable, {key}, is not set!")

        if not value:
            value = self.default_configs[key]
        return value

    def get_name(self, name) -> str:
        return f"{self.stack_name_prefix}-{name}"

    @property
    def stack_name_prefix(self) -> str:
        return self.get(STACK_NAME_PREFIX)

    @property
    def aws_account_id(self) -> str:
        return self.get(AWS_ACCOUNT_ID)

    @property
    def dr_type(self) -> str:
        type = self.get(DR_TYPE)
        if type not in [DR_BACKUP_RESTORE, DR_WARM_STANDBY]:
            raise ValueError(f"The DR type, {type}, is not supported!")
        return type

    @property
    def dr_variable_restore_strategy(self) -> str:
        return self.get_restore_strategy(DR_VARIABLE_RESTORE_STRATEGY)

    @property
    def dr_connection_restore_strategy(self) -> str:
        return self.get_restore_strategy(DR_CONNECTION_RESTORE_STRATEGY)

    def get_restore_strategy(self, variable) -> str:
        strategy = self.get(variable)
        if strategy == DO_NOTHING:
            return strategy

        if self.dr_type == DR_BACKUP_RESTORE:
            return REPLACE

        if strategy not in ["APPEND", "REPLACE"]:
            raise ValueError(f"The restore strategy, {strategy}, is not supported!")

        return strategy

    @property
    def mwaa_version(self) -> str:
        version = self.get(MWAA_VERSION)
        if version not in ["2.5.1", "2.6.3", "2.7.2", "2.8.1"]:
            raise ValueError(f"The MWAA version, {version}, is not supported!")
        return version

    @property
    def mwaa_dags_s3_path(self) -> str:
        return self.get(MWAA_DAGS_S3_PATH)

    @property
    def mwaa_notification_emails(self) -> list[str]:
        emails = self.get(MWAA_NOTIFICATION_EMAILS)
        return json.loads(emails)

    @property
    def mwaa_backup_file_name(self) -> str:
        return self.get(MWAA_BACKUP_FILE_NAME)

    @property
    def mwaa_update_execution_role(self) -> str:
        return self.get(MWAA_UPDATE_EXECUTION_ROLE)

    @property
    def mwaa_create_env_polling_interval_secs(self) -> int:
        return int(self.get(MWAA_CREATE_ENV_POLLING_INTERVAL_SECS))

    @property
    def mwaa_simulate_dr(self) -> str:
        return self.get(MWAA_SIMULATE_DR)

    @property
    def metadata_export_dag_name(self) -> str:
        return self.get(METADATA_EXPORT_DAG_NAME)

    @property
    def metadata_import_dag_name(self) -> str:
        return self.get(METADATA_IMPORT_DAG_NAME)

    @property
    def metadata_cleanup_dag_name(self) -> str:
        return self.get(METADATA_CLEANUP_DAG_NAME)

    @property
    def state_machine_timeout_mins(self) -> int:
        return int(self.get(STATE_MACHINE_TIMEOUT_MINS))

    @property
    def health_check_enabled(self) -> bool:
        return self.get(HEALTH_CHECK_ENABLED) == "YES"

    @property
    def health_check_interval_mins(self) -> int:
        return int(self.get(HEALTH_CHECK_INTERVAL_MINS))

    @property
    def health_check_max_retry(self) -> int:
        return int(self.get(HEALTH_CHECK_MAX_RETRY))

    @property
    def health_check_retry_interval_secs(self) -> int:
        return int(self.get(HEALTH_CHECK_RETRY_INTERVAL_SECS))

    @property
    def health_check_retry_backoff_rate(self) -> int:
        return int(self.get(HEALTH_CHECK_RETRY_BACKOFF_RATE))

    @property
    def primary_region(self) -> str:
        return self.get(PRIMARY_REGION)

    @property
    def primary_mwaa_environment_name(self) -> str:
        return self.get(PRIMARY_MWAA_ENVIRONMENT_NAME)

    @property
    def primary_mwaa_role_arn(self) -> str:
        return self.get(PRIMARY_MWAA_ROLE_ARN)

    @property
    def primary_dags_bucket_name(self) -> str:
        return self.get(PRIMARY_DAGS_BUCKET_NAME)

    @property
    def primary_schedule_interval(self) -> str:
        return self.get(PRIMARY_SCHEDULE_INTERVAL)

    @property
    def primary_vpc_id(self) -> str:
        return self.get(PRIMARY_VPC_ID)

    @property
    def primary_subnet_ids(self) -> list[str]:
        subnet_ids = self.get(PRIMARY_SUBNET_IDS)
        return json.loads(subnet_ids)

    @property
    def primary_security_group_ids(self) -> list[str]:
        sg_ids = self.get(PRIMARY_SECURITY_GROUP_IDS)
        return json.loads(sg_ids)

    @property
    def primary_backup_schedule(self) -> str:
        return self.get(PRIMARY_BACKUP_SCHEDULE)

    @property
    def primary_replication_polling_interval_secs(self) -> int:
        return int(self.get(PRIMARY_REPLICATION_POLLING_INTERVAL_SECS))

    @property
    def secondary_region(self) -> str:
        return self.get(SECONDARY_REGION)

    @property
    def secondary_mwaa_environment_name(self) -> str:
        return self.get(SECONDARY_MWAA_ENVIRONMENT_NAME)

    @property
    def secondary_mwaa_role_arn(self) -> str:
        return self.get(SECONDARY_MWAA_ROLE_ARN)

    @property
    def secondary_dags_bucket_name(self) -> str:
        return self.get(SECONDARY_DAGS_BUCKET_NAME)

    @property
    def secondary_vpc_id(self) -> str:
        return self.get(SECONDARY_VPC_ID)

    @property
    def secondary_subnet_ids(self) -> list[str]:
        subnet_ids = self.get(SECONDARY_SUBNET_IDS)
        return json.loads(subnet_ids)

    @property
    def secondary_security_group_ids(self) -> list[str]:
        sg_ids = self.get(SECONDARY_SECURITY_GROUP_IDS)
        return json.loads(sg_ids)

    @property
    def secondary_create_step_functions_vpce(self) -> bool:
        return self.get("SECONDARY_CREATE_SFN_VPCE") == "YES"

    @property
    def secondary_cleanup_cool_off_secs(self) -> int:
        return int(self.get("SECONDARY_CLEANUP_COOL_OFF_SECS"))
