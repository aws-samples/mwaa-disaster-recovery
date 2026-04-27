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

from unittest.mock import MagicMock, patch

import pytest
import requests

from mwaa_dr.framework.mwaa_rest_api_client import MwaaRestApiClient, MAX_RETRIES


class TestMwaaRestApiClientAuthentication:
    """Tests for authentication token retrieval and session creation.
    Requirements: 11.9
    """

    @patch("mwaa_dr.framework.mwaa_rest_api_client.boto3")
    def test_get_web_login_token_calls_mwaa_api(self, mock_boto3):
        mock_mwaa = MagicMock()
        mock_boto3.client.return_value = mock_mwaa
        mock_mwaa.create_web_login_token.return_value = {
            "WebServerHostname": "abc123.us-east-1.airflow.amazonaws.com",
            "WebToken": "test-token-xyz",
        }

        client = MwaaRestApiClient("my-env", "us-east-1")
        hostname, token = client._get_web_login_token()

        mock_boto3.client.assert_called_once_with("mwaa", region_name="us-east-1")
        mock_mwaa.create_web_login_token.assert_called_once_with(Name="my-env")
        assert hostname == "abc123.us-east-1.airflow.amazonaws.com"
        assert token == "test-token-xyz"

    @patch("mwaa_dr.framework.mwaa_rest_api_client.requests.Session")
    @patch("mwaa_dr.framework.mwaa_rest_api_client.boto3")
    def test_get_session_authenticates_with_token(self, mock_boto3, mock_session_cls):
        mock_mwaa = MagicMock()
        mock_boto3.client.return_value = mock_mwaa
        mock_mwaa.create_web_login_token.return_value = {
            "WebServerHostname": "host.us-east-1.airflow.amazonaws.com",
            "WebToken": "my-web-token",
        }

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_login_response = MagicMock()
        mock_session.get.return_value = mock_login_response

        client = MwaaRestApiClient("my-env", "us-east-1")
        session = client._get_session()

        mock_session.get.assert_called_once_with(
            "https://host.us-east-1.airflow.amazonaws.com/aws_mwaa/login",
            params={"token": "my-web-token"},
            allow_redirects=True,
        )
        mock_login_response.raise_for_status.assert_called_once()
        assert session.base_url == "https://host.us-east-1.airflow.amazonaws.com/api/v2"


class TestMwaaRestApiClientVariables:
    """Tests for variable CRUD operations.
    Requirements: 11.1, 11.2
    """

    def _make_client_with_mock_session(self):
        """Helper to create a client with a mocked session."""
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()
        mock_session.base_url = "https://host.airflow.amazonaws.com/api/v2"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        return client, mock_session, mock_response

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_list_variables_returns_all_variables(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        variables = [
            {"key": "var1", "value": "val1", "description": "desc1"},
            {"key": "var2", "value": "val2", "description": "desc2"},
            {"key": "var3", "value": "val3", "description": ""},
        ]
        mock_response.json.return_value = {"variables": variables}

        result = client.list_variables()

        mock_session.request.assert_called_once_with(
            "GET",
            "https://host.airflow.amazonaws.com/api/v2/variables",
        )
        assert result == variables
        assert len(result) == 3

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_list_variables_returns_empty_list_when_none(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session
        mock_response.json.return_value = {"variables": []}

        result = client.list_variables()
        assert result == []

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_create_variable_sends_post_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.create_variable("my_key", "my_value", description="my desc")

        mock_session.request.assert_called_once_with(
            "POST",
            "https://host.airflow.amazonaws.com/api/v2/variables",
            json={"key": "my_key", "value": "my_value", "description": "my desc"},
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_create_variable_without_description(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.create_variable("my_key", "my_value")

        mock_session.request.assert_called_once_with(
            "POST",
            "https://host.airflow.amazonaws.com/api/v2/variables",
            json={"key": "my_key", "value": "my_value"},
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_update_variable_sends_patch_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.update_variable("my_key", "new_value", description="updated desc")

        mock_session.request.assert_called_once_with(
            "PATCH",
            "https://host.airflow.amazonaws.com/api/v2/variables/my_key",
            json={"key": "my_key", "value": "new_value", "description": "updated desc"},
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_update_variable_without_description(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.update_variable("my_key", "new_value")

        mock_session.request.assert_called_once_with(
            "PATCH",
            "https://host.airflow.amazonaws.com/api/v2/variables/my_key",
            json={"key": "my_key", "value": "new_value"},
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_delete_variable_sends_delete_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.delete_variable("my_key")

        mock_session.request.assert_called_once_with(
            "DELETE",
            "https://host.airflow.amazonaws.com/api/v2/variables/my_key",
        )


class TestMwaaRestApiClientConnections:
    """Tests for connection CRUD operations.
    Requirements: 11.3, 11.4
    """

    def _make_client_with_mock_session(self):
        """Helper to create a client with a mocked session."""
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()
        mock_session.base_url = "https://host.airflow.amazonaws.com/api/v2"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        return client, mock_session, mock_response

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_list_connections_returns_all_connections(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        connections = [
            {"conn_id": "conn1", "conn_type": "postgres", "host": "db1.example.com"},
            {"conn_id": "conn2", "conn_type": "s3", "host": ""},
        ]
        mock_response.json.return_value = {"connections": connections}

        result = client.list_connections()

        mock_session.request.assert_called_once_with(
            "GET",
            "https://host.airflow.amazonaws.com/api/v2/connections",
        )
        assert result == connections
        assert len(result) == 2

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_list_connections_returns_empty_list_when_none(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session
        mock_response.json.return_value = {"connections": []}

        result = client.list_connections()
        assert result == []

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_create_connection_sends_post_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        conn_data = {
            "conn_id": "my_conn",
            "conn_type": "postgres",
            "host": "db.example.com",
            "login": "admin",
            "password": "secret",
            "port": 5432,
            "schema": "airflow",
        }
        client.create_connection(conn_data)

        mock_session.request.assert_called_once_with(
            "POST",
            "https://host.airflow.amazonaws.com/api/v2/connections",
            json=conn_data,
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_update_connection_sends_patch_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        conn_data = {"host": "new-db.example.com", "password": "new-secret"}
        client.update_connection("my_conn", conn_data)

        mock_session.request.assert_called_once_with(
            "PATCH",
            "https://host.airflow.amazonaws.com/api/v2/connections/my_conn",
            json=conn_data,
        )

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_delete_connection_sends_delete_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session

        client.delete_connection("my_conn")

        mock_session.request.assert_called_once_with(
            "DELETE",
            "https://host.airflow.amazonaws.com/api/v2/connections/my_conn",
        )


class TestMwaaRestApiClientRetry:
    """Tests for retry behavior with exponential backoff on transient errors.
    Requirements: 11.9
    """

    @patch("mwaa_dr.framework.mwaa_rest_api_client.time.sleep")
    def test_retry_on_server_error_succeeds_on_second_attempt(self, mock_sleep):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()

        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = "Internal Server Error"

        success_response = MagicMock()
        success_response.status_code = 200

        mock_session.request.side_effect = [fail_response, success_response]

        result = client._request_with_retry(
            "GET", "https://host/api/v2/variables", mock_session
        )

        assert result == success_response
        assert mock_session.request.call_count == 2
        mock_sleep.assert_called_once_with(1)  # BACKOFF_BASE * 2^0

    @patch("mwaa_dr.framework.mwaa_rest_api_client.time.sleep")
    def test_retry_exhausts_all_attempts_then_raises(self, mock_sleep):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()

        fail_response = MagicMock()
        fail_response.status_code = 503
        fail_response.text = "Service Unavailable"

        mock_session.request.return_value = fail_response

        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            client._request_with_retry(
                "GET", "https://host/api/v2/variables", mock_session
            )

        assert "503" in str(exc_info.value)
        assert mock_session.request.call_count == MAX_RETRIES
        # Should sleep between attempts: 1s, 2s (not after last attempt)
        assert mock_sleep.call_count == MAX_RETRIES - 1

    @patch("mwaa_dr.framework.mwaa_rest_api_client.time.sleep")
    def test_retry_exponential_backoff_timing(self, mock_sleep):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()

        fail_response = MagicMock()
        fail_response.status_code = 429
        fail_response.text = "Too Many Requests"

        mock_session.request.return_value = fail_response

        with pytest.raises(requests.exceptions.HTTPError):
            client._request_with_retry(
                "GET", "https://host/api/v2/variables", mock_session
            )

        # Verify exponential backoff: 1*2^0=1, 1*2^1=2
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_calls == [1, 2]

    @patch("mwaa_dr.framework.mwaa_rest_api_client.time.sleep")
    def test_no_retry_on_success(self, mock_sleep):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()

        success_response = MagicMock()
        success_response.status_code = 200

        mock_session.request.return_value = success_response

        result = client._request_with_retry(
            "GET", "https://host/api/v2/variables", mock_session
        )

        assert result == success_response
        assert mock_session.request.call_count == 1
        mock_sleep.assert_not_called()

    @patch("mwaa_dr.framework.mwaa_rest_api_client.time.sleep")
    def test_retry_on_client_error(self, mock_sleep):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()

        fail_response = MagicMock()
        fail_response.status_code = 400
        fail_response.text = "Bad Request"

        success_response = MagicMock()
        success_response.status_code = 200

        mock_session.request.side_effect = [fail_response, success_response]

        result = client._request_with_retry(
            "POST", "https://host/api/v2/variables", mock_session, json={"key": "k"}
        )

        assert result == success_response
        assert mock_session.request.call_count == 2


class TestMwaaRestApiClientGetMethods:
    """Tests for get_variable and get_connection methods."""

    def _make_client_with_mock_session(self):
        client = MwaaRestApiClient("my-env", "us-east-1")
        mock_session = MagicMock()
        mock_session.base_url = "https://host.airflow.amazonaws.com/api/v2"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        return client, mock_session, mock_response

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_get_variable_sends_get_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session
        mock_response.json.return_value = {
            "key": "my_var",
            "value": "my_val",
            "description": "desc",
        }

        result = client.get_variable("my_var")

        mock_session.request.assert_called_once_with(
            "GET",
            "https://host.airflow.amazonaws.com/api/v2/variables/my_var",
        )
        assert result["key"] == "my_var"
        assert result["value"] == "my_val"

    @patch.object(MwaaRestApiClient, "_get_session")
    def test_get_connection_sends_get_request(self, mock_get_session):
        client, mock_session, mock_response = self._make_client_with_mock_session()
        mock_get_session.return_value = mock_session
        mock_response.json.return_value = {
            "connection_id": "my_conn",
            "conn_type": "postgres",
            "host": "db.example.com",
        }

        result = client.get_connection("my_conn")

        mock_session.request.assert_called_once_with(
            "GET",
            "https://host.airflow.amazonaws.com/api/v2/connections/my_conn",
        )
        assert result["connection_id"] == "my_conn"
        assert result["conn_type"] == "postgres"
