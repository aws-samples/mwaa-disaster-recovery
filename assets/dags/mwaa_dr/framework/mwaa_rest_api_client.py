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

import logging
import time

import boto3
import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1  # seconds


class MwaaRestApiClient:
    """Client for MWAA Airflow REST API using web login token authentication.

    Authenticates via the MWAA CreateWebLoginToken API and uses the resulting
    session cookie to interact with the Airflow REST API (v2) for managing
    variables and connections.
    """

    def __init__(self, env_name: str, region: str):
        """Initialize the MWAA REST API client.

        Args:
            env_name: The name of the MWAA environment.
            region: The AWS region where the MWAA environment is deployed.
        """
        self.env_name = env_name
        self.region = region

    def _get_web_login_token(self) -> tuple:
        """Obtain a web login token from the MWAA CreateWebLoginToken API.

        Returns:
            A tuple of (web_server_hostname, web_token).

        Raises:
            Exception: If the CreateWebLoginToken API call fails.
        """
        client = boto3.client("mwaa", region_name=self.region)
        response = client.create_web_login_token(Name=self.env_name)
        web_server_hostname = response["WebServerHostname"]
        web_token = response["WebToken"]
        return web_server_hostname, web_token

    def _get_session(self) -> requests.Session:
        """Create an authenticated requests.Session using the web login token.

        Logs in to the MWAA webserver with the web token to obtain a session
        cookie, then returns a Session object configured for subsequent API calls.

        Returns:
            An authenticated requests.Session.

        Raises:
            Exception: If authentication fails.
        """
        web_server_hostname, web_token = self._get_web_login_token()
        base_url = f"https://{web_server_hostname}"

        session = requests.Session()
        # Log in using the web token to obtain session cookies
        login_url = f"{base_url}/aws_mwaa/login"
        login_response = session.get(
            login_url,
            params={"token": web_token},
            allow_redirects=True,
        )
        login_response.raise_for_status()

        # Store the base URL on the session for convenience
        session.base_url = f"{base_url}/api/v2"
        return session

    def _request_with_retry(self, method, url, session, **kwargs):
        """Execute an HTTP request with retry and exponential backoff.

        Retries up to MAX_RETRIES times on 4xx/5xx responses with exponential
        backoff.

        Args:
            method: HTTP method (e.g., "GET", "POST", "PATCH", "DELETE").
            url: The full URL to request.
            session: The authenticated requests.Session.
            **kwargs: Additional keyword arguments passed to session.request.

        Returns:
            The requests.Response object.

        Raises:
            requests.exceptions.HTTPError: If all retry attempts are exhausted.
        """
        last_exception = None
        for attempt in range(MAX_RETRIES):
            response = session.request(method, url, **kwargs)
            if response.status_code < 400:
                return response

            last_exception = requests.exceptions.HTTPError(
                f"{response.status_code}: {response.text}",
                response=response,
            )

            if attempt < MAX_RETRIES - 1:
                wait_time = BACKOFF_BASE * (2**attempt)
                logger.warning(
                    "Request to %s returned %s. Retrying in %s seconds (attempt %d/%d).",
                    url,
                    response.status_code,
                    wait_time,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(wait_time)

        raise last_exception

    # -------------------------------------------------------------------------
    # Variable methods
    # -------------------------------------------------------------------------

    def list_variables(self) -> list:
        """List all Airflow variables.

        Returns:
            A list of variable dicts from the Airflow REST API.
        """
        session = self._get_session()
        url = f"{session.base_url}/variables"
        response = self._request_with_retry("GET", url, session)
        response.raise_for_status()
        data = response.json()
        return data.get("variables", [])

    def get_variable(self, key: str) -> dict:
        """Get a single Airflow variable by key.

        Args:
            key: The variable key.

        Returns:
            A dict representing the variable.
        """
        session = self._get_session()
        url = f"{session.base_url}/variables/{key}"
        response = self._request_with_retry("GET", url, session)
        response.raise_for_status()
        return response.json()

    def create_variable(self, key: str, value: str, description: str = None):
        """Create a new Airflow variable.

        Args:
            key: The variable key.
            value: The variable value.
            description: Optional description for the variable.
        """
        session = self._get_session()
        url = f"{session.base_url}/variables"
        payload = {"key": key, "value": value}
        if description is not None:
            payload["description"] = description
        response = self._request_with_retry("POST", url, session, json=payload)
        response.raise_for_status()

    def update_variable(self, key: str, value: str, description: str = None):
        """Update an existing Airflow variable.

        Args:
            key: The variable key.
            value: The new variable value.
            description: Optional new description for the variable.
        """
        session = self._get_session()
        url = f"{session.base_url}/variables/{key}"
        payload = {"key": key, "value": value}
        if description is not None:
            payload["description"] = description
        response = self._request_with_retry("PATCH", url, session, json=payload)
        response.raise_for_status()

    def delete_variable(self, key: str):
        """Delete an Airflow variable.

        Args:
            key: The variable key to delete.
        """
        session = self._get_session()
        url = f"{session.base_url}/variables/{key}"
        response = self._request_with_retry("DELETE", url, session)
        response.raise_for_status()

    # -------------------------------------------------------------------------
    # Connection methods
    # -------------------------------------------------------------------------

    def list_connections(self) -> list:
        """List all Airflow connections.

        Returns:
            A list of connection dicts from the Airflow REST API.
        """
        session = self._get_session()
        url = f"{session.base_url}/connections"
        response = self._request_with_retry("GET", url, session)
        response.raise_for_status()
        data = response.json()
        return data.get("connections", [])

    def get_connection(self, conn_id: str) -> dict:
        """Get a single Airflow connection by ID.

        Args:
            conn_id: The connection ID.

        Returns:
            A dict representing the connection.
        """
        session = self._get_session()
        url = f"{session.base_url}/connections/{conn_id}"
        response = self._request_with_retry("GET", url, session)
        response.raise_for_status()
        return response.json()

    def create_connection(self, conn_data: dict):
        """Create a new Airflow connection.

        Args:
            conn_data: A dict containing connection fields (conn_id, conn_type,
                host, login, password, port, schema, extra, description, etc.).
        """
        session = self._get_session()
        url = f"{session.base_url}/connections"
        response = self._request_with_retry("POST", url, session, json=conn_data)
        response.raise_for_status()

    def update_connection(self, conn_id: str, conn_data: dict):
        """Update an existing Airflow connection.

        Args:
            conn_id: The connection ID to update.
            conn_data: A dict containing the updated connection fields.
        """
        session = self._get_session()
        url = f"{session.base_url}/connections/{conn_id}"
        response = self._request_with_retry("PATCH", url, session, json=conn_data)
        response.raise_for_status()

    def delete_connection(self, conn_id: str):
        """Delete an Airflow connection.

        Args:
            conn_id: The connection ID to delete.
        """
        session = self._get_session()
        url = f"{session.base_url}/connections/{conn_id}"
        response = self._request_with_retry("DELETE", url, session)
        response.raise_for_status()
