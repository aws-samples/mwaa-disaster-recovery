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
import json
from sure import expect

from config import Config
from tests.unit.mocks.mock_setup import backup_restore_env_vars
import pytest


class TestConfig:

    def test_config_construction(backup_restore_env_vars):
        config = Config()

        expect(config.required_configs).to.be.truthy
        expect(config.default_configs).to.be.truthy

    def test_get_required_supplied(backup_restore_env_vars):
        os.environ["PRIMARY_MWAA_ENVIRONMENT_NAME"] = "env"
        config = Config()

        expect(config.get("PRIMARY_MWAA_ENVIRONMENT_NAME")).to.equals("env")

    def test_get_required_missing(backup_restore_env_vars):
        os.environ["PRIMARY_MWAA_ENVIRONMENT_NAME"] = ""
        config = Config()

        config.get.when.called_with("PRIMARY_MWAA_ENVIRONMENT_NAME").should.have.raised(
            ValueError,
            "The required environment variable, PRIMARY_MWAA_ENVIRONMENT_NAME, is not set!",
        )

    def test_get_optional_supplied(backup_restore_env_vars):
        os.environ["MWAA_SIMULATE_DR"] = "YES"
        config = Config()

        expect(config.get("MWAA_SIMULATE_DR")).to.equal("YES")

    def test_get_optional_missing(backup_restore_env_vars):
        os.environ["MWAA_DAGS_S3_PATH"] = ""
        config = Config()

        expect(config.get("MWAA_DAGS_S3_PATH")).to.equal("dags")

    def test_get_name(backup_restore_env_vars):
        os.environ["STACK_NAME_PREFIX"] = "mwaa"
        config = Config()

        expect(config.get_name("env")).to.equal("mwaa-env")

    def test_stack_name_prefix(backup_restore_env_vars):
        os.environ["STACK_NAME_PREFIX"] = "mwaa"
        config = Config()

        expect(config.stack_name_prefix).to.equal("mwaa")

    def test_aws_account_id(backup_restore_env_vars):
        os.environ["AWS_ACCOUNT_ID"] = "123456789999"
        config = Config()

        expect(config.aws_account_id).to.equal("123456789999")

    def test_dr_type(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "BACKUP_RESTORE"
        config = Config()

        expect(config.dr_type).to.equal("BACKUP_RESTORE")

    def test_dr_type_error(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "ACTIVE_ACTIVE"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.dr_type

    def test_get_restore_strategy_do_nothing(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "BACKUP_RESTORE"
        os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "DO_NOTHING"
        os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "DO_NOTHING"

        config = Config()

        expect(config.dr_variable_restore_strategy).to.equal("DO_NOTHING")
        expect(config.dr_connection_restore_strategy).to.equal("DO_NOTHING")

    def test_get_restore_strategy_backup_restore(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "BACKUP_RESTORE"
        os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "APPEND"
        os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "APPEND"

        config = Config()

        expect(config.dr_variable_restore_strategy).to.equal("REPLACE")
        expect(config.dr_connection_restore_strategy).to.equal("REPLACE")

    def test_get_restore_strategy_warm_standby(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "WARM_STANDBY"
        os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "APPEND"
        os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "APPEND"

        config = Config()

        expect(config.dr_variable_restore_strategy).to.equal("APPEND")
        expect(config.dr_connection_restore_strategy).to.equal("APPEND")

    def test_get_restore_strategy_warm_standby(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "WARM_STANDBY"
        os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "REPLACE"
        os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "REPLACE"

        config = Config()

        expect(config.dr_variable_restore_strategy).to.equal("REPLACE")
        expect(config.dr_connection_restore_strategy).to.equal("REPLACE")

    def test_get_restore_strategy_invalid(backup_restore_env_vars):
        os.environ["DR_TYPE"] = "WARM_STANDBY"
        os.environ["DR_VARIABLE_RESTORE_STRATEGY"] = "OVERWRITE"
        os.environ["DR_CONNECTION_RESTORE_STRATEGY"] = "OVERWRITE"

        config = Config()

        with pytest.raises(ValueError):
            config.dr_variable_restore_strategy

        with pytest.raises(ValueError):
            config.dr_connection_restore_strategy

    def test_mwaa_version_supported(backup_restore_env_vars):
        for version in ["2.5.1", "2.6.3", "2.7.2", "2.8.1", "2.10.1"]:
            os.environ["MWAA_VERSION"] = version
            config = Config()

            expect(config.mwaa_version).to.equal(version)

    def test_mwaa_version_unsupported(backup_restore_env_vars):
        os.environ["MWAA_VERSION"] = "2.2.2"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.mwaa_version

    def test_mwaa_dags_s3_path(backup_restore_env_vars):
        os.environ["MWAA_DAGS_S3_PATH"] = "dags"
        config = Config()

        expect(config.mwaa_dags_s3_path).to.equal("dags")

    def test_mwaa_notification_emails_valid(backup_restore_env_vars):
        list_of_emails = ["[]", '["abc@def.com"]', '["abc@def.com", "xyz@abc.com"]']

        for emails in list_of_emails:
            os.environ["MWAA_NOTIFICATION_EMAILS"] = emails
            config = Config()

            expect(config.mwaa_notification_emails).to.equal(json.loads(emails))

    def test_mwaa_notification_emails_invalid(backup_restore_env_vars):
        os.environ["MWAA_NOTIFICATION_EMAILS"] = "[abc@def.com]"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.mwaa_notification_emails

    def test_mwaa_backup_file_name(backup_restore_env_vars):
        os.environ["MWAA_BACKUP_FILE_NAME"] = "env.json"
        config = Config()

        expect(config.mwaa_backup_file_name).to.equal("env.json")

    def test_mwaa_update_execution_role(backup_restore_env_vars):
        os.environ["MWAA_UPDATE_EXECUTION_ROLE"] = "NO"
        config = Config()

        expect(config.mwaa_update_execution_role).to.equal("NO")

    def test_mwaa_create_env_polling_interval_secs(backup_restore_env_vars):
        os.environ["MWAA_CREATE_ENV_POLLING_INTERVAL_SECS"] = "30"
        config = Config()

        expect(config.mwaa_create_env_polling_interval_secs).to.equal(30)

    def test_mwaa_simulate_dr(backup_restore_env_vars):
        os.environ["MWAA_SIMULATE_DR"] = "YES"
        config = Config()

        expect(config.mwaa_simulate_dr).to.equal("YES")

    def test_metadata_export_dag_name(backup_restore_env_vars):
        os.environ["METADATA_EXPORT_DAG_NAME"] = "export"
        config = Config()

        expect(config.metadata_export_dag_name).to.equal("export")

    def test_metadata_import_dag_name(backup_restore_env_vars):
        os.environ["METADATA_IMPORT_DAG_NAME"] = "import"
        config = Config()

        expect(config.metadata_import_dag_name).to.equal("import")

    def test_metadata_cleanup_dag_name(backup_restore_env_vars):
        os.environ["METADATA_CLEANUP_DAG_NAME"] = "cleanup"
        config = Config()

        expect(config.metadata_cleanup_dag_name).to.equal("cleanup")

    def test_state_machine_timeout_mins(backup_restore_env_vars):
        os.environ["STATE_MACHINE_TIMEOUT_MINS"] = "30"
        config = Config()

        expect(config.state_machine_timeout_mins).to.equal(30)

    def test_health_check_enabled_falsy(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_ENABLED"] = "NO"
        config = Config()

        expect(config.health_check_enabled).to.be.falsy

    def test_health_check_enabled_truthy(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_ENABLED"] = "YES"
        config = Config()

        expect(config.health_check_enabled).to.be.truthy

    def test_health_check_interval_mins(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_INTERVAL_MINS"] = "30"
        config = Config()

        expect(config.health_check_interval_mins).to.equal(30)

    def test_health_check_max_retry(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_MAX_RETRY"] = "3"
        config = Config()

        expect(config.health_check_max_retry).to.equal(3)

    def test_health_check_retry_interval_secs(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_RETRY_INTERVAL_SECS"] = "30"
        config = Config()

        expect(config.health_check_retry_interval_secs).to.equal(30)

    def test_health_check_retry_backoff_rate(backup_restore_env_vars):
        os.environ["HEALTH_CHECK_RETRY_BACKOFF_RATE"] = "5"
        config = Config()

        expect(config.health_check_retry_backoff_rate).to.equal(5)

    def test_primary_region(backup_restore_env_vars):
        os.environ["PRIMARY_REGION"] = "us-west-1"
        config = Config()

        expect(config.primary_region).to.equal("us-west-1")

    def test_primary_mwaa_environment_name(backup_restore_env_vars):
        os.environ["PRIMARY_MWAA_ENVIRONMENT_NAME"] = "env"
        config = Config()

        expect(config.primary_mwaa_environment_name).to.equal("env")

    def test_primary_mwaa_role_arn(backup_restore_env_vars):
        os.environ["PRIMARY_MWAA_ROLE_ARN"] = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        config = Config()

        expect(config.primary_mwaa_role_arn).to.equal(
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        )

    def test_primary_dags_bucket_name(backup_restore_env_vars):
        os.environ["PRIMARY_DAGS_BUCKET_NAME"] = "bucket"
        config = Config()

        expect(config.primary_dags_bucket_name).to.equal("bucket")

    def test_primary_schedule_interval(backup_restore_env_vars):
        os.environ["PRIMARY_SCHEDULE_INTERVAL"] = "1 * * * *"
        config = Config()

        expect(config.primary_schedule_interval).to.equal("1 * * * *")

    def test_primary_vpc_id(backup_restore_env_vars):
        os.environ["PRIMARY_VPC_ID"] = "vpc-123"
        config = Config()

        expect(config.primary_vpc_id).to.equal("vpc-123")

    def test_primary_subnet_ids_valid(backup_restore_env_vars):
        list_of_subnets = ["[]", '["sb-123"]', '["sb-123", "sb-456"]']

        for subnets in list_of_subnets:
            os.environ["PRIMARY_SUBNET_IDS"] = subnets
            config = Config()

            expect(config.primary_subnet_ids).to.equal(json.loads(subnets))

    def test_primary_subnet_ids_invalid(backup_restore_env_vars):
        os.environ["PRIMARY_SUBNET_IDS"] = "[sb-123]"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.primary_subnet_ids

    def test_primary_security_group_ids(backup_restore_env_vars):
        list_of_sgs = ["[]", '["sg-123"]', '["sg-123", "sg-456"]']

        for sgs in list_of_sgs:
            os.environ["PRIMARY_SECURITY_GROUP_IDS"] = sgs
            config = Config()

            expect(config.primary_security_group_ids).to.equal(json.loads(sgs))

    def test_primary_security_group_ids_invalid(backup_restore_env_vars):
        os.environ["PRIMARY_SECURITY_GROUP_IDS"] = "[sg-123]"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.primary_security_group_ids

    def test_primary_backup_schedule(backup_restore_env_vars):
        os.environ["PRIMARY_BACKUP_SCHEDULE"] = "1 * * * *"
        config = Config()

        expect(config.primary_backup_schedule).to.equal("1 * * * *")

    def test_primary_replication_polling_internal_secs(backup_restore_env_vars):
        os.environ["PRIMARY_REPLICATION_POLLING_INTERVAL_SECS"] = "15"
        config = Config()

        expect(config.primary_replication_polling_interval_secs).to.equal(15)

    def test_secondary_region(backup_restore_env_vars):
        os.environ["SECONDARY_REGION"] = "us-west-2"
        config = Config()

        expect(config.secondary_region).to.equal("us-west-2")

    def test_secondary_mwaa_environment_name(backup_restore_env_vars):
        os.environ["SECONDARY_MWAA_ENVIRONMENT_NAME"] = "env"
        config = Config()

        expect(config.secondary_mwaa_environment_name).to.equal("env")

    def test_secondary_mwaa_role_arn(backup_restore_env_vars):
        os.environ["SECONDARY_MWAA_ROLE_ARN"] = (
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        )
        config = Config()

        expect(config.secondary_mwaa_role_arn).to.equal(
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        )

    def test_secondary_dags_bucket_name(backup_restore_env_vars):
        os.environ["SECONDARY_DAGS_BUCKET_NAME"] = "bucket"
        config = Config()

        expect(config.secondary_dags_bucket_name).to.equal("bucket")

    def test_secondary_vpc_id(backup_restore_env_vars):
        os.environ["SECONDARY_VPC_ID"] = "vpc-123"
        config = Config()

        expect(config.secondary_vpc_id).to.equal("vpc-123")

    def test_secondary_subnet_ids_valid(backup_restore_env_vars):
        list_of_subnets = ["[]", '["sb-123"]', '["sb-123", "sb-456"]']

        for subnets in list_of_subnets:
            os.environ["SECONDARY_SUBNET_IDS"] = subnets
            config = Config()

            expect(config.secondary_subnet_ids).to.equal(json.loads(subnets))

    def test_secondary_subnet_ids_invalid(backup_restore_env_vars):
        os.environ["SECONDARY_SUBNET_IDS"] = "[sb-123]"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.secondary_subnet_ids

    def test_secondary_security_group_ids(backup_restore_env_vars):
        list_of_sgs = ["[]", '["sg-123"]', '["sg-123", "sg-456"]']

        for sgs in list_of_sgs:
            os.environ["SECONDARY_SECURITY_GROUP_IDS"] = sgs
            config = Config()

            expect(config.secondary_security_group_ids).to.equal(json.loads(sgs))

    def test_secondary_security_group_ids_invalid(backup_restore_env_vars):
        os.environ["SECONDARY_SECURITY_GROUP_IDS"] = "[sg-123]"
        config = Config()

        with pytest.raises(ValueError) as exception:
            config.secondary_security_group_ids

    def test_secondary_create_step_functions_vpce_truty(backup_restore_env_vars):
        os.environ["SECONDARY_CREATE_SFN_VPCE"] = "YES"
        config = Config()

        expect(config.secondary_create_step_functions_vpce).to.be.truthy

    def test_secondary_create_step_functions_vpce_falsy(backup_restore_env_vars):
        os.environ["SECONDARY_CREATE_SFN_VPCE"] = "NO"
        config = Config()

        expect(config.secondary_create_step_functions_vpce).to.be.falsy

    def test_secondary_cleanup_cool_off_secs(warm_standby_env_vars):
        os.environ["SECONDARY_CLEANUP_COOL_OFF_SECS"] = "30"
        config = Config()

        expect(config.secondary_cleanup_cool_off_secs).to.equal(30)
