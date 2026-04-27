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
from unittest.mock import patch

import pytest

from mwaa_dr.framework.credential_extractor import (
    CredentialExtractor,
    DatabaseCredentials,
)


class TestCredentialExtractor:

    def test_extract_from_db_secrets_valid(self):
        """Test valid DB_SECRETS parsing (Airflow 3.x path) - Requirement 1.1"""
        env = {
            "DB_SECRETS": json.dumps(
                {"username": "airflow_user", "password": "s3cr3t"}
            ),
            "POSTGRES_HOST": "my-db-host.rds.amazonaws.com",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "AirflowMetadata",
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert isinstance(creds, DatabaseCredentials)
        assert creds.username == "airflow_user"
        assert creds.password == "s3cr3t"
        assert creds.host == "my-db-host.rds.amazonaws.com"
        assert creds.port == "5432"
        assert creds.database == "AirflowMetadata"
        assert (
            creds.jdbc_url
            == "jdbc:postgresql://my-db-host.rds.amazonaws.com:5432/AirflowMetadata"
        )

    def test_extract_from_db_secrets_defaults(self):
        """Test DB_SECRETS with default host/port/db when env vars are missing."""
        env = {
            "DB_SECRETS": json.dumps({"username": "admin", "password": "pass123"}),
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert creds.username == "admin"
        assert creds.password == "pass123"
        assert creds.host == "localhost"
        assert creds.port == "5432"
        assert creds.database == "AirflowMetadata"
        assert creds.jdbc_url == "jdbc:postgresql://localhost:5432/AirflowMetadata"

    def test_extract_from_sql_alchemy_conn(self):
        """Test valid SQLAlchemy connection string parsing (Airflow 2.x path) - Requirement 1.2"""
        conn_str = "postgresql+psycopg2://myuser:mypass@db-host.example.com:5433/airflow_db?sslmode=require"
        env = {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_str,
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert isinstance(creds, DatabaseCredentials)
        assert creds.username == "myuser"
        assert creds.password == "mypass"
        assert creds.host == "db-host.example.com"
        assert creds.port == "5433"
        assert creds.database == "airflow_db"
        assert creds.jdbc_url == "jdbc:postgresql://db-host.example.com:5433/airflow_db"

    def test_extract_from_legacy_sql_alchemy_conn(self):
        """Test legacy AIRFLOW__CORE__SQL_ALCHEMY_CONN fallback."""
        conn_str = "postgresql://legacyuser:legacypass@legacy-host:5432/legacy_db"
        env = {
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": conn_str,
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert creds.username == "legacyuser"
        assert creds.password == "legacypass"
        assert creds.host == "legacy-host"
        assert creds.port == "5432"
        assert creds.database == "legacy_db"

    def test_extract_priority_db_secrets_over_sql_alchemy(self):
        """Test that DB_SECRETS takes priority over SQLAlchemy conn string."""
        env = {
            "DB_SECRETS": json.dumps(
                {"username": "primary_user", "password": "primary_pass"}
            ),
            "POSTGRES_HOST": "primary-host",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "PrimaryDB",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://other:other@other-host:5432/other_db",
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert creds.username == "primary_user"
        assert creds.host == "primary-host"

    def test_extract_missing_credentials_raises_value_error(self):
        """Test missing credentials raises ValueError - Requirement 1.3"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                CredentialExtractor.extract()

        assert "Unable to extract database credentials" in str(exc_info.value)
        assert "DB_SECRETS" in str(exc_info.value)
        assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" in str(exc_info.value)

    def test_extract_malformed_db_secrets_json_raises_value_error(self):
        """Test malformed DB_SECRETS JSON raises ValueError - Requirement 1.4"""
        env = {
            "DB_SECRETS": "not-valid-json{{{",
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                CredentialExtractor.extract()

        assert "malformed JSON" in str(exc_info.value)

    def test_extract_db_secrets_missing_username_key(self):
        """Test DB_SECRETS JSON missing 'username' key raises ValueError - Requirement 1.4"""
        env = {
            "DB_SECRETS": json.dumps({"password": "pass123"}),
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                CredentialExtractor.extract()

        assert "username" in str(exc_info.value)

    def test_extract_db_secrets_missing_password_key(self):
        """Test DB_SECRETS JSON missing 'password' key raises ValueError - Requirement 1.4"""
        env = {
            "DB_SECRETS": json.dumps({"username": "user123"}),
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                CredentialExtractor.extract()

        assert "password" in str(exc_info.value)

    def test_extract_sql_alchemy_conn_with_url_encoded_chars(self):
        """Test SQLAlchemy connection string with URL-encoded special characters."""
        conn_str = "postgresql+psycopg2://user%40domain:p%40ss%23word@host:5432/db"
        env = {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_str,
        }
        with patch.dict(os.environ, env, clear=True):
            creds = CredentialExtractor.extract()

        assert creds.username == "user@domain"
        assert creds.password == "p@ss#word"
