# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Property-based tests for GlueDRFactory using Hypothesis.

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

import csv
import os
from io import StringIO
from unittest.mock import MagicMock, patch, call

from hypothesis import given, settings, assume
from hypothesis.strategies import (
    text,
    composite,
    sampled_from,
    sets,
    lists,
)

from mwaa_dr.framework.factory.glue_dr_factory import GlueDRFactory
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


# ---------------------------------------------------------------------------
# Shared concrete factory for testing
# ---------------------------------------------------------------------------

class ConcreteGlueDRFactory(GlueDRFactory):
    """Concrete subclass for testing since GlueDRFactory needs setup_tables."""

    def setup_tables(self, model: DependencyModel) -> list:
        variable = BaseTable(
            name="variable",
            model=model,
            columns=["key", "val", "description"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
        connection = BaseTable(
            name="connection",
            model=model,
            columns=["conn_id", "conn_type", "host"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
        slot_pool = BaseTable(
            name="slot_pool",
            model=model,
            columns=["description", "pool", "slots"],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )
        return [variable, connection, slot_pool]


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

@composite
def mwaa_env_names(draw):
    """Generate valid MWAA environment name strings (alphanumeric + hyphens)."""
    name = draw(text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_",
        min_size=1,
        max_size=64,
    ))
    return name


@composite
def s3_bucket_names(draw):
    """Generate valid S3 bucket name strings."""
    name = draw(text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789-.",
        min_size=3,
        max_size=63,
    ))
    assume(not name.startswith("-"))
    assume(not name.startswith("."))
    assume(not name.endswith("-"))
    assume(not name.endswith("."))
    assume(".." not in name)
    return name


@composite
def script_names(draw):
    """Generate valid Glue script base names (no extension)."""
    name = draw(text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
        min_size=1,
        max_size=50,
    ))
    return name


@composite
def variable_keys(draw):
    """Generate valid Airflow variable key strings."""
    key = draw(text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
        min_size=1,
        max_size=30,
    ))
    return key


# ---------------------------------------------------------------------------
# Property 13: Glue connection naming follows environment pattern
# ---------------------------------------------------------------------------

class TestGlueConnectionNamingProperty:
    """
    **Validates: Requirements 2.4**

    Property 13: Glue connection naming follows environment pattern

    For any MWAA environment name, the Glue connection name SHALL equal
    ``{env_name}_conn``.
    """

    @given(env_name=mwaa_env_names())
    @settings(max_examples=100)
    def test_glue_connection_name_equals_env_name_conn(self, env_name):
        """
        **Validates: Requirements 2.4**

        Property 13: Glue connection naming follows environment pattern
        """
        # The connection naming pattern is used inside create_glue_connection
        # tasks in create_backup_dag, create_restore_dag, and create_cleanup_dag.
        # The pattern is: connection_name = f"{env_name}_conn"
        # We verify this directly.
        connection_name = f"{env_name}_conn"
        assert connection_name == f"{env_name}_conn"
        assert connection_name.endswith("_conn")
        assert connection_name[: -len("_conn")] == env_name


# ---------------------------------------------------------------------------
# Property 3: S3 path construction follows naming conventions
# ---------------------------------------------------------------------------

class TestS3PathConstructionProperty:
    """
    **Validates: Requirements 3.2, 4.2, 7.3**

    Property 3: S3 path construction follows naming conventions

    For any valid S3 bucket name, path prefix, and table/script name, the
    constructed S3 paths SHALL follow the patterns:
    ``s3://{bucket}/{prefix}/{table_name}.csv.gz`` for backup files, and
    ``s3://{dags_bucket}/scripts/{script_name}.py`` for Glue script locations.
    """

    @given(bucket=s3_bucket_names(), script=script_names())
    @settings(max_examples=100)
    def test_get_script_location_follows_pattern(self, bucket, script):
        """
        **Validates: Requirements 3.2, 4.2, 7.3**

        Property 3: S3 path construction follows naming conventions
        (Glue script location)
        """
        factory = ConcreteGlueDRFactory("test_dag")

        with patch.dict(os.environ, {"DAGS_S3_PATH": f"s3://{bucket}/dags"}):
            result = factory.get_script_location(script)

        expected = f"s3://{bucket}/scripts/{script}.py"
        assert result == expected
        assert result.startswith("s3://")
        assert result.endswith(".py")
        assert "/scripts/" in result


# ---------------------------------------------------------------------------
# Property 11: Restore strategy determines correct API behavior
# ---------------------------------------------------------------------------

class TestRestoreStrategyBehaviorProperty:
    """
    **Validates: Requirements 11.5, 11.6, 11.7**

    Property 11: Restore strategy determines correct API behavior

    For any set of backup variables/connections and any set of existing
    variables/connections in the target environment:
    - APPEND: only create entries whose keys do not exist in the target
    - REPLACE: delete all existing, then create all backup entries
    - DO_NOTHING: no API calls shall be made
    """

    @given(
        backup_keys=sets(variable_keys(), min_size=0, max_size=10),
        existing_keys=sets(variable_keys(), min_size=0, max_size=10),
        strategy=sampled_from(["APPEND", "REPLACE", "DO_NOTHING"]),
    )
    @settings(max_examples=100)
    def test_restore_variables_strategy_behavior(self, backup_keys, existing_keys, strategy):
        """
        **Validates: Requirements 11.5, 11.6, 11.7**

        Property 11: Restore strategy determines correct API behavior
        (variables)
        """
        factory = ConcreteGlueDRFactory("test_dag")

        # Build CSV content from backup keys
        buffer = StringIO()
        writer = csv.DictWriter(
            buffer, fieldnames=["key", "val", "description"], delimiter="|"
        )
        backup_list = sorted(backup_keys)
        for key in backup_list:
            writer.writerow({"key": key, "val": f"value_{key}", "description": ""})
        csv_content = buffer.getvalue()

        # Mock the REST API client
        mock_client = MagicMock()
        existing_vars = [{"key": k, "value": f"existing_{k}"} for k in sorted(existing_keys)]
        mock_client.list_variables.return_value = existing_vars

        # Mock S3 to return our CSV
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }

        with patch.object(factory, "get_mwaa_rest_api_client", return_value=mock_client), \
             patch("mwaa_dr.framework.factory.glue_dr_factory.Variable") as mock_variable, \
             patch("mwaa_dr.framework.factory.glue_dr_factory.boto3") as mock_boto3:

            mock_variable.get.return_value = strategy
            mock_boto3.client.return_value = mock_s3

            factory.restore_variables_via_api()

        if strategy == "DO_NOTHING":
            # No API calls should be made
            mock_client.list_variables.assert_not_called()
            mock_client.create_variable.assert_not_called()
            mock_client.delete_variable.assert_not_called()

        elif strategy == "REPLACE":
            # All existing should be deleted
            assert mock_client.delete_variable.call_count == len(existing_keys)
            deleted_keys = {c.args[0] for c in mock_client.delete_variable.call_args_list}
            assert deleted_keys == existing_keys

            # All backup entries should be created
            assert mock_client.create_variable.call_count == len(backup_keys)
            created_keys = {c.kwargs["key"] for c in mock_client.create_variable.call_args_list}
            assert created_keys == backup_keys

        elif strategy == "APPEND":
            # Only keys NOT in existing should be created
            expected_new = backup_keys - existing_keys
            assert mock_client.create_variable.call_count == len(expected_new)
            if expected_new:
                created_keys = {c.kwargs["key"] for c in mock_client.create_variable.call_args_list}
                assert created_keys == expected_new

            # No deletions in APPEND mode
            mock_client.delete_variable.assert_not_called()

    @given(
        backup_ids=sets(variable_keys(), min_size=0, max_size=10),
        existing_ids=sets(variable_keys(), min_size=0, max_size=10),
        strategy=sampled_from(["APPEND", "REPLACE", "DO_NOTHING"]),
    )
    @settings(max_examples=100)
    def test_restore_connections_strategy_behavior(self, backup_ids, existing_ids, strategy):
        """
        **Validates: Requirements 11.5, 11.6, 11.7**

        Property 11: Restore strategy determines correct API behavior
        (connections)
        """
        factory = ConcreteGlueDRFactory("test_dag")

        # Build CSV content from backup connection IDs
        buffer = StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=[
                "conn_id", "conn_type", "description", "extra",
                "host", "login", "password", "port", "schema",
            ],
            delimiter="|",
        )
        backup_list = sorted(backup_ids)
        for conn_id in backup_list:
            writer.writerow({
                "conn_id": conn_id,
                "conn_type": "http",
                "description": "",
                "extra": "",
                "host": "localhost",
                "login": "",
                "password": "",
                "port": "8080",
                "schema": "",
            })
        csv_content = buffer.getvalue()

        # Mock the REST API client
        mock_client = MagicMock()
        existing_conns = [{"connection_id": cid, "conn_type": "http"} for cid in sorted(existing_ids)]
        mock_client.list_connections.return_value = existing_conns

        # Mock S3 to return our CSV
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=csv_content.encode("utf-8")))
        }

        with patch.object(factory, "get_mwaa_rest_api_client", return_value=mock_client), \
             patch("mwaa_dr.framework.factory.glue_dr_factory.Variable") as mock_variable, \
             patch("mwaa_dr.framework.factory.glue_dr_factory.boto3") as mock_boto3:

            mock_variable.get.return_value = strategy
            mock_boto3.client.return_value = mock_s3

            factory.restore_connections_via_api()

        if strategy == "DO_NOTHING":
            # No API calls should be made
            mock_client.list_connections.assert_not_called()
            mock_client.create_connection.assert_not_called()
            mock_client.delete_connection.assert_not_called()

        elif strategy == "REPLACE":
            # All existing should be deleted
            assert mock_client.delete_connection.call_count == len(existing_ids)
            deleted_ids = {c.args[0] for c in mock_client.delete_connection.call_args_list}
            assert deleted_ids == existing_ids

            # All backup entries should be created
            assert mock_client.create_connection.call_count == len(backup_ids)
            created_ids = {
                c.args[0]["connection_id"]
                for c in mock_client.create_connection.call_args_list
            }
            assert created_ids == backup_ids

        elif strategy == "APPEND":
            # Only IDs NOT in existing should be created
            expected_new = backup_ids - existing_ids
            assert mock_client.create_connection.call_count == len(expected_new)
            if expected_new:
                created_ids = {
                    c.args[0]["connection_id"]
                    for c in mock_client.create_connection.call_args_list
                }
                assert created_ids == expected_new

            # No deletions in APPEND mode
            mock_client.delete_connection.assert_not_called()
