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

from moto import mock_aws
from tests.unit.mocks.mock_setup import aws_credentials, aws_mwaa
import pytest
from botocore.config import Config as AwsConfig
from lib.function.airflow_cli_client import AirflowCliClient, AirflowCliException, AirflowCliCommand, AirflowCliResult
from sure import expect
import httpretty
from unittest.mock import Mock
import json

class TestAirflowCliClient:
    """ Test AirflowCliClient """
    @mock_aws
    def test_init_default(self, aws_mwaa):
        """ Test constructor"""

        env_name = "test-env"
        env_version = "2.8.1"
        client = AirflowCliClient(environment_name=env_name, environment_version=env_version)

        client.environment_name.should.equal(env_name)
        client.environment_version.should.equal(env_version)
        client.token.should.be.none


    @mock_aws
    def test_init_with_conf(self, aws_mwaa):
        """ Test constructor with config """

        env_name = "test-env"
        env_version = "2.8.1"
        config = AwsConfig(region_name='us-east-1')
        client = AirflowCliClient(environment_name=env_name, environment_version=env_version, config=config)

        client.environment_name.should.equal(env_name)
        client.environment_version.should.equal(env_version)
        client.token.should.be.none


    @mock_aws
    def test_setup_no_env_name(self, aws_mwaa):
        client = AirflowCliClient(environment_name=None, environment_version=None)
        client.setup.when.called_with().should.have.raised(AirflowCliException, "Environment name not provided!")


    @mock_aws
    def test_setup_no_env_version(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version=None)
        client.setup.when.called_with().should.have.raised(AirflowCliException, "Environment version not provided!")


    @mock_aws
    def test_setup_cli_token(self, aws_mwaa):
        from tests.unit.mocks.mock_setup import mwaa_cli_token

        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')
        expect(client.setup()).to.equal(mwaa_cli_token)


    @mock_aws
    @httpretty.activate(allow_net_connect=False)
    def test_execute_command_with_config(self, aws_mwaa):
        from tests.unit.mocks.mock_setup import mwaa_cli_token, mwaa_web_server_hostname
        import json

        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')
        config = {
            'bucket': 'my-bucket',
            'task_token': 'sfn-token',
            'dr_type': 'BACKUP_RESTORE'
        }
        command = AirflowCliCommand(
            command='dags trigger -o json restore_metadata',
            configuration=config
        )
        stdout_obj = [
            {
                "conf": {
                    "bucket": "my-bucket",
                    "task_token": "sfn-token",
                    "dr_type": "BACKUP_RESTORE"
                },
                "dag_id": "restore_metadata",
                "dag_run_id": "manual__2024-04-29T19:17:11+00:00",
                "data_interval_start": "2024-04-29 19:17:11+00:00",
                "data_interval_end": "2024-04-29 19:17:11+00:00",
                "end_date": None,
                "external_trigger": "True",
                "last_scheduling_decision": None,
                "logical_date": "2024-04-29 19:17:11+00:00",
                "run_type": "manual",
                "start_date": None,
                "state": "queued"
            }
        ]
        stdout = json.dumps(stdout_obj)
        stderr = 'UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.'

        import base64
        result = {
            'stdout': base64.b64encode(stdout.encode('utf-8')).decode('utf-8'),
            'stderr': base64.b64encode(stderr.encode('utf-8')).decode('utf-8')
        }

        httpretty.register_uri(
            httpretty.POST,
            f'https://{mwaa_web_server_hostname}/aws_mwaa/cli',
            body=json.dumps(result),
            status=200
        )

        result = client.execute(command)
        expect(result.stdout).to.equal(stdout)
        expect(result.stderr).to.equal(stderr)


    @mock_aws
    @httpretty.activate(allow_net_connect=False)
    def test_execute_command_with_result_check_success(self, aws_mwaa):
        from tests.unit.mocks.mock_setup import mwaa_cli_token, mwaa_web_server_hostname
        import json

        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')
        config = {
            'bucket': 'my-bucket',
            'task_token': 'sfn-token',
            'dr_type': 'BACKUP_RESTORE'
        }
        command = AirflowCliCommand(
            command='dags trigger -o json restore_metadata',
            configuration=config,
            result_check='"external_trigger": "True"'
        )
        stdout_obj = [
            {
                "conf": {
                    "bucket": "my-bucket",
                    "task_token": "sfn-token",
                    "dr_type": "BACKUP_RESTORE"
                },
                "dag_id": "restore_metadata",
                "dag_run_id": "manual__2024-04-29T19:17:11+00:00",
                "data_interval_start": "2024-04-29 19:17:11+00:00",
                "data_interval_end": "2024-04-29 19:17:11+00:00",
                "end_date": None,
                "external_trigger": "True",
                "last_scheduling_decision": None,
                "logical_date": "2024-04-29 19:17:11+00:00",
                "run_type": "manual",
                "start_date": None,
                "state": "queued"
            }
        ]
        stdout = json.dumps(stdout_obj)
        stderr = 'UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.'

        import base64
        result = {
            'stdout': base64.b64encode(stdout.encode('utf-8')).decode('utf-8'),
            'stderr': base64.b64encode(stderr.encode('utf-8')).decode('utf-8')
        }

        httpretty.register_uri(
            httpretty.POST,
            f'https://{mwaa_web_server_hostname}/aws_mwaa/cli',
            body=json.dumps(result),
            status=200
        )

        result = client.execute(command)
        expect(result.stdout).to.equal(stdout)
        expect(result.stderr).to.equal(stderr)


    @mock_aws
    @httpretty.activate(allow_net_connect=False)
    def test_execute_command_with_result_check_failure(self, aws_mwaa):
        from tests.unit.mocks.mock_setup import mwaa_cli_token, mwaa_web_server_hostname
        import json

        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')
        config = {
            'bucket': 'my-bucket',
            'task_token': 'sfn-token',
            'dr_type': 'BACKUP_RESTORE'
        }
        command = AirflowCliCommand(
            command='dags trigger -o json restore_metadata',
            configuration=config,
            result_check='triggered: True'
        )
        stdout_obj = [
            {
                "conf": {
                    "bucket": "my-bucket",
                    "task_token": "sfn-token",
                    "dr_type": "BACKUP_RESTORE"
                },
                "dag_id": "restore_metadata",
                "dag_run_id": "manual__2024-04-29T19:17:11+00:00",
                "data_interval_start": "2024-04-29 19:17:11+00:00",
                "data_interval_end": "2024-04-29 19:17:11+00:00",
                "end_date": None,
                "external_trigger": "True",
                "last_scheduling_decision": None,
                "logical_date": "2024-04-29 19:17:11+00:00",
                "run_type": "manual",
                "start_date": None,
                "state": "queued"
            }
        ]
        stdout = json.dumps(stdout_obj)
        stderr = 'UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.'

        import base64
        result = {
            'stdout': base64.b64encode(stdout.encode('utf-8')).decode('utf-8'),
            'stderr': base64.b64encode(stderr.encode('utf-8')).decode('utf-8')
        }

        httpretty.register_uri(
            httpretty.POST,
            f'https://{mwaa_web_server_hostname}/aws_mwaa/cli',
            body=json.dumps(result),
            status=200
        )

        client.execute.when.called_with(command).should.have.raised(AirflowCliException)


    @mock_aws
    @httpretty.activate(allow_net_connect=False)
    def test_execute_command_with_network_error(self, aws_mwaa):
        from tests.unit.mocks.mock_setup import mwaa_cli_token, mwaa_web_server_hostname
        import json

        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')
        command = AirflowCliCommand(
            command='dags unpause restore_metadata',
            result_check='paused: False'
        )        

        httpretty.register_uri(
            httpretty.POST,
            f'https://{mwaa_web_server_hostname}/aws_mwaa/cli',
            body='Gateway Timeout',
            status=504
        )

        client.execute.when.called_with(command).should.have.raised(AirflowCliException)


    @mock_aws
    def test_execute_all_empty(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')

        command1 = AirflowCliCommand('dags unpause a-dag')
        result1 = AirflowCliResult(stdout='Dag: a-dag, paused: False', stderr='a warning')
        command2 = AirflowCliCommand('dags unpause b-dag')
        result2 = AirflowCliResult(stdout='Dag: b-dag, paused: False', stderr='b warning')

        def execute_mock(command):
            if command == command1:
                return result1
            elif command == command2:
                return result2
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        expect(client.execute_all([])).to.equal([])
        expect(client.execute_all([command1])).to.equal([result1])
        expect(client.execute_all([command1, command2])).to.equal([result1, result2])


    @mock_aws
    def test_unpause_dag(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')

        command = AirflowCliCommand('dags unpause a-dag')
        result = AirflowCliResult(stdout='Dag: a-dag, paused: False', stderr='a warning')

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        expect(client.unpause_dag('a-dag')).to.equal(result)


    @mock_aws
    def test_unpause_dag_error(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')

        command = AirflowCliCommand('dags unpause a-dag')
        result = AirflowCliResult(stdout='Unknown Dag a-dag', stderr='Unknown Dag a-dag')

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        client.unpause_dag.when.called_with('a-dag').should.have.raised(AirflowCliException)


    @mock_aws
    def test_trigger_dag_v_2_5_1(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.5.1')

        conf = {
            "bucket": "backup-bucket",
            "task_token": "sfn-task-token",
            "dr_type": "WARM_STANDBY"
        }
        command = AirflowCliCommand(command='dags trigger restore_metadata', configuration=conf)
        result = AirflowCliResult(
            stdout='Created <DagRun restore_metadata @ 2024-03-06T22:29:45+00:00: manual__2024-03-06T22:29:45+00:00, state:queued, queued_at: 2024-03-06 22:29:45.252723+00:00. externally triggered: True>\n',
            stderr=''
        )

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        expect(client.trigger_dag('restore_metadata', conf)).to.equal(result)


    @mock_aws
    def test_trigger_dag_v_greater_than_2_5_1(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.8.1')

        conf = {
            "bucket": "backup-bucket",
            "task_token": "sfn-task-token",
            "dr_type": "WARM_STANDBY"
        }
        command = AirflowCliCommand(command='dags trigger -o json restore_metadata', configuration=conf)
        
        result_obj = [
            {
                "conf": {
                    "bucket": "backup-bucket",
                    "task_token": "sfn-task-token",
                    "dr_type": "WARM_STANDBY"
                },
                "dag_id": "restore_metadata",
                "dag_run_id": "manual__2024-04-29T19:17:11+00:00",
                "data_interval_start": "2024-04-29 19:17:11+00:00",
                "data_interval_end": "2024-04-29 19:17:11+00:00",
                "end_date": None,
                "external_trigger": "True",
                "last_scheduling_decision": None,
                "logical_date": "2024-04-29 19:17:11+00:00",
                "run_type": "manual",
                "start_date": None,
                "state": "queued"
            }
        ]
        result = AirflowCliResult(
            stdout=json.dumps(result_obj),
            stderr='UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.'
        )

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        expect(client.trigger_dag('restore_metadata', conf)).to.equal(result)


    @mock_aws
    def test_trigger_dag_error(self, aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.5.1')

        conf = {
            "bucket": "backup-bucket",
            "task_token": "sfn-task-token",
            "dr_type": "WARM_STANDBY"
        }
        command = AirflowCliCommand(command='dags trigger restore_metadata', configuration=conf)
        result = AirflowCliResult(
            stdout='Unknown Dag restore_metadata',
            stderr='Unknown Dag restore_metadata'
        )

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        client.trigger_dag.when.called_with('restore_metadata', conf).should.have.raised(AirflowCliException)


    @mock_aws
    def test_set_variable(aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.5.1')

        command = AirflowCliCommand(command='variables set A-KEY A-VALUE')
        result = AirflowCliResult(
            stdout='Variable A-KEY created',
            stderr=''
        )

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        expect(client.set_variable('A-KEY', 'A-VALUE')).to.equal(result)


    @mock_aws
    def test_set_variable_error(aws_mwaa):
        client = AirflowCliClient(environment_name='an-env', environment_version='2.5.1')

        command = AirflowCliCommand(command='variables set A-KEY A-VALUE')
        result = AirflowCliResult(
            stdout='Variable A-KEY not created',
            stderr=''
        )

        def execute_mock(cmd):
            if cmd == command:
                return result
            raise NotImplementedError('Mock not implemented for the supplied command')

        client.execute = Mock(side_effect=execute_mock)

        client.set_variable.when.called_with('A-KEY', 'A-VALUE').should.have.raised(AirflowCliException)
