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

import boto3

logger = logging.getLogger(__name__)


class MwaaRestApiClient:
    """Client for MWAA Airflow REST API using the InvokeRestApi AWS API.

    Uses the MWAA ``InvokeRestApi`` operation which authenticates via IAM
    credentials and works on both Airflow 2.x and 3.x.
    """

    def __init__(self, env_name: str, region: str):
        self.env_name = env_name
        self.region = region
        self._client = boto3.client("mwaa", region_name=region)

    def _invoke(self, method: str, path: str, body: dict = None) -> dict:
        """Invoke the MWAA REST API.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE).
            path: API path (e.g., "/variables").
            body: Optional request body dict.

        Returns:
            The parsed response body dict.

        Raises:
            Exception: If the API returns a non-2xx status code.
        """
        kwargs = {
            "Name": self.env_name,
            "Method": method,
            "Path": path,
        }
        if body is not None:
            kwargs["Body"] = body

        response = self._client.invoke_rest_api(**kwargs)
        status = response.get("RestApiStatusCode", 0)
        data = response.get("RestApiResponse", {})

        if status >= 400:
            raise Exception(f"MWAA REST API error {status} on {method} {path}: {data}")
        return data

    def _list_all(self, path: str, key: str) -> list:
        """Paginate through all results for a list endpoint.

        Args:
            path: API path (e.g., "/variables").
            key: Response key containing the list (e.g., "variables").

        Returns:
            Complete list of all items across all pages.
        """
        all_items = []
        offset = 0
        limit = 100
        while True:
            data = self._invoke("GET", f"{path}?limit={limit}&offset={offset}")
            items = data.get(key, [])
            all_items.extend(items)
            total = data.get("total_entries", 0)
            offset += limit
            if offset >= total or not items:
                break
        return all_items

    # --- Variable methods ---

    def list_variables(self) -> list:
        return self._list_all("/variables", "variables")

    def get_variable(self, key: str) -> dict:
        return self._invoke("GET", f"/variables/{key}")

    def create_variable(self, key: str, value: str, description: str = None):
        payload = {"key": key, "value": value}
        if description is not None:
            payload["description"] = description
        self._invoke("POST", "/variables", body=payload)

    def update_variable(self, key: str, value: str, description: str = None):
        payload = {"key": key, "value": value}
        if description is not None:
            payload["description"] = description
        self._invoke("PATCH", f"/variables/{key}", body=payload)

    def delete_variable(self, key: str):
        self._invoke("DELETE", f"/variables/{key}")

    # --- Connection methods ---

    def list_connections(self) -> list:
        return self._list_all("/connections", "connections")

    def get_connection(self, conn_id: str) -> dict:
        return self._invoke("GET", f"/connections/{conn_id}")

    def create_connection(self, conn_data: dict):
        self._invoke("POST", "/connections", body=conn_data)

    def update_connection(self, conn_id: str, conn_data: dict):
        self._invoke("PATCH", f"/connections/{conn_id}", body=conn_data)

    def delete_connection(self, conn_id: str):
        self._invoke("DELETE", f"/connections/{conn_id}")
