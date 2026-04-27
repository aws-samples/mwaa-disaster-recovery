# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Property-based tests for CredentialExtractor using Hypothesis.

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

from hypothesis import given, settings, assume
from hypothesis.strategies import text, integers, composite, sampled_from

from mwaa_dr.framework.credential_extractor import (
    CredentialExtractor,
    DatabaseCredentials,
)


# --- Strategies ---


@composite
def valid_hostnames(draw):
    """Generate valid hostname-like strings (non-empty, no whitespace)."""
    hostname = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789-.",
            min_size=1,
            max_size=63,
        )
    )
    assume(not hostname.startswith("-"))
    assume(not hostname.endswith("-"))
    assume(".." not in hostname)
    return hostname


@composite
def valid_ports(draw):
    """Generate valid port number strings."""
    port = draw(integers(min_value=1, max_value=65535))
    return str(port)


@composite
def valid_db_names(draw):
    """Generate valid PostgreSQL database names."""
    name = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
            min_size=1,
            max_size=63,
        )
    )
    assume(name[0].isalpha() or name[0] == "_")
    return name


@composite
def valid_credentials(draw):
    """Generate valid username/password pairs (non-empty strings)."""
    username = draw(text(min_size=1, max_size=100))
    password = draw(text(min_size=1, max_size=100))
    return username, password


# --- Property Tests ---


class TestCredentialExtractorProperties:
    """
    **Validates: Requirements 1.1**

    Property 1: DB_SECRETS credential extraction preserves components

    For any valid JSON string containing username and password fields,
    and any valid POSTGRES_HOST, POSTGRES_PORT, and POSTGRES_DB values,
    calling CredentialExtractor.extract() with these environment variables
    set SHALL return a DatabaseCredentials where jdbc_url equals
    jdbc:postgresql://{host}:{port}/{db}, username matches the JSON
    username field, and password matches the JSON password field.
    """

    @given(
        creds=valid_credentials(),
        host=valid_hostnames(),
        port=valid_ports(),
        db=valid_db_names(),
    )
    @settings(max_examples=100)
    def test_db_secrets_extraction_preserves_components(self, creds, host, port, db):
        """
        **Validates: Requirements 1.1**

        Property 1: DB_SECRETS credential extraction preserves components
        """
        username, password = creds

        db_secrets_json = json.dumps({"username": username, "password": password})

        env = {
            "DB_SECRETS": db_secrets_json,
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": port,
            "POSTGRES_DB": db,
        }

        with patch.dict(os.environ, env, clear=True):
            result = CredentialExtractor.extract()

        assert isinstance(result, DatabaseCredentials)
        assert result.username == username
        assert result.password == password
        assert result.host == host
        assert result.port == port
        assert result.database == db
        assert result.jdbc_url == f"jdbc:postgresql://{host}:{port}/{db}"


# --- Strategies for SQLAlchemy connection strings ---

SQLALCHEMY_SCHEMES = [
    "postgresql+psycopg2",
    "postgresql",
]


@composite
def alphanumeric_credentials(draw):
    """Generate alphanumeric username/password pairs safe for URL embedding."""
    username = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            min_size=1,
            max_size=50,
        )
    )
    password = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            min_size=1,
            max_size=50,
        )
    )
    return username, password


@composite
def valid_sqlalchemy_conn_strings(draw):
    """Generate valid SQLAlchemy PostgreSQL connection strings with components."""
    scheme = draw(sampled_from(SQLALCHEMY_SCHEMES))
    username, password = draw(alphanumeric_credentials())
    host = draw(valid_hostnames())
    port = draw(valid_ports())
    db = draw(valid_db_names())

    conn_string = f"{scheme}://{username}:{password}@{host}:{port}/{db}"

    return conn_string, username, password, host, port, db


# --- Property 2 Tests ---


class TestSQLAlchemyCredentialExtractorProperties:
    """
    **Validates: Requirements 1.2**

    Property 2: SQLAlchemy connection string credential extraction preserves components

    For any valid SQLAlchemy PostgreSQL connection string of the form
    postgresql+psycopg2://{user}:{pass}@{host}:{port}/{db}?...,
    calling CredentialExtractor.extract() with AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    set to that string SHALL return a DatabaseCredentials where the username,
    password, host, port, and database fields match the components embedded
    in the connection string.
    """

    @given(data=valid_sqlalchemy_conn_strings())
    @settings(max_examples=100)
    def test_sqlalchemy_conn_extraction_preserves_components(self, data):
        """
        **Validates: Requirements 1.2**

        Property 2: SQLAlchemy connection string credential extraction preserves components
        """
        conn_string, username, password, host, port, db = data

        env = {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_string,
        }

        with patch.dict(os.environ, env, clear=True):
            result = CredentialExtractor.extract()

        assert isinstance(result, DatabaseCredentials)
        assert result.username == username
        assert result.password == password
        assert result.host == host
        assert result.port == port
        assert result.database == db
        assert result.jdbc_url == f"jdbc:postgresql://{host}:{port}/{db}"
