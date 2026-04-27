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
from dataclasses import dataclass
from urllib.parse import urlparse, unquote


@dataclass
class DatabaseCredentials:
    """Database credentials extracted from MWAA worker environment variables."""

    jdbc_url: str
    username: str
    password: str
    host: str
    port: str
    database: str


class CredentialExtractor:
    """Extracts MWAA metadata database credentials from environment variables."""

    @staticmethod
    def extract() -> DatabaseCredentials:
        """
        Extract database credentials from MWAA worker environment variables.

        Strategy:
        1. Try DB_SECRETS (Airflow 3.x) + POSTGRES_HOST/PORT/DB
        2. Fall back to AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (Airflow 2.x)
        3. Fall back to AIRFLOW__CORE__SQL_ALCHEMY_CONN (legacy)
        4. Raise ValueError if none available

        Returns:
            DatabaseCredentials: The extracted database credentials.

        Raises:
            ValueError: If no credentials are found or JSON is malformed.
        """
        # Strategy 1: DB_SECRETS (Airflow 3.x)
        db_secrets = os.environ.get("DB_SECRETS")
        if db_secrets:
            return CredentialExtractor._extract_from_db_secrets(db_secrets)

        # Strategy 2: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (Airflow 2.x)
        sql_alchemy_conn = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        if sql_alchemy_conn:
            return CredentialExtractor._extract_from_sql_alchemy_conn(sql_alchemy_conn)

        # Strategy 3: AIRFLOW__CORE__SQL_ALCHEMY_CONN (legacy fallback)
        sql_alchemy_conn_legacy = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        if sql_alchemy_conn_legacy:
            return CredentialExtractor._extract_from_sql_alchemy_conn(
                sql_alchemy_conn_legacy
            )

        raise ValueError(
            "Unable to extract database credentials. None of the expected environment "
            "variables are available: DB_SECRETS (Airflow 3.x), "
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (Airflow 2.x), or "
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN (legacy)."
        )

    @staticmethod
    def _extract_from_db_secrets(db_secrets: str) -> DatabaseCredentials:
        """
        Parse DB_SECRETS JSON and combine with POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB.

        Args:
            db_secrets: JSON string containing 'username' and 'password' fields.

        Returns:
            DatabaseCredentials: The extracted credentials.

        Raises:
            ValueError: If JSON is malformed or required keys are missing.
        """
        try:
            secrets = json.loads(db_secrets)
        except json.JSONDecodeError as e:
            raise ValueError(f"DB_SECRETS contains malformed JSON: {e}")

        if "username" not in secrets:
            raise ValueError("DB_SECRETS JSON is missing the required 'username' key.")
        if "password" not in secrets:
            raise ValueError("DB_SECRETS JSON is missing the required 'password' key.")

        username = secrets["username"]
        password = secrets["password"]
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        database = os.environ.get("POSTGRES_DB", "AirflowMetadata")
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

        return DatabaseCredentials(
            jdbc_url=jdbc_url,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

    @staticmethod
    def _extract_from_sql_alchemy_conn(conn_string: str) -> DatabaseCredentials:
        """
        Parse a SQLAlchemy connection string to extract credentials.

        Supports formats like:
            postgresql+psycopg2://user:pass@host:port/db?params
            postgresql://user:pass@host:port/db

        Args:
            conn_string: SQLAlchemy connection string.

        Returns:
            DatabaseCredentials: The extracted credentials.

        Raises:
            ValueError: If the connection string cannot be parsed.
        """
        try:
            parsed = urlparse(conn_string)
            username = unquote(parsed.username) if parsed.username else ""
            password = unquote(parsed.password) if parsed.password else ""
            host = parsed.hostname or "localhost"
            port = str(parsed.port) if parsed.port else "5432"
            database = parsed.path.lstrip("/") if parsed.path else ""

            jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

            return DatabaseCredentials(
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                host=host,
                port=port,
                database=database,
            )
        except Exception as e:
            raise ValueError(f"Failed to parse SQLAlchemy connection string: {e}")
