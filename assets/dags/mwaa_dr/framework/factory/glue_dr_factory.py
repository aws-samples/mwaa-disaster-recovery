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

import csv
import json
import logging
import os
from datetime import datetime, timedelta
from io import StringIO

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from mwaa_dr.framework.credential_extractor import CredentialExtractor
from mwaa_dr.framework.factory.base_dr_factory import BaseDRFactory
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.mwaa_rest_api_client import MwaaRestApiClient

try:
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
except ImportError:
    GlueJobOperator = None  # Not available in test/CI environments

logger = logging.getLogger(__name__)


def _get_vpc_requirements(env_name, region):
    """Get VPC networking requirements for a Glue connection.

    Uses DR_SUBNET_ID and DR_SECURITY_GROUP_IDS Airflow variables if set (avoids
    mwaa:GetEnvironment which is restricted in AF 3.2.1+). Falls back to the
    MWAA API for environments that allow it.
    """
    from airflow.models import Variable

    subnet_id = Variable.get("DR_SUBNET_ID", default_var="")
    security_groups = Variable.get("DR_SECURITY_GROUP_IDS", default_var="")

    if subnet_id and security_groups:
        # Use pre-configured variables (set by CDK)
        sg_list = [sg.strip() for sg in security_groups.split(",")]
        ec2_client = boto3.client("ec2", region_name=region)
        subnet_response = ec2_client.describe_subnets(SubnetIds=[subnet_id])
        availability_zone = subnet_response["Subnets"][0]["AvailabilityZone"]
        return {
            "SubnetId": subnet_id,
            "SecurityGroupIdList": sg_list,
            "AvailabilityZone": availability_zone,
        }

    # Fallback: get from MWAA environment (requires mwaa:GetEnvironment)
    mwaa_client = boto3.client("mwaa", region_name=region)
    env_response = mwaa_client.get_environment(Name=env_name)
    network_config = env_response["Environment"]["NetworkConfiguration"]
    subnet_ids = network_config["SubnetIds"]
    security_group_ids = network_config["SecurityGroupIds"]

    ec2_client = boto3.client("ec2", region_name=region)
    subnet_response = ec2_client.describe_subnets(SubnetIds=[subnet_ids[0]])
    availability_zone = subnet_response["Subnets"][0]["AvailabilityZone"]

    return {
        "SubnetId": subnet_ids[0],
        "SecurityGroupIdList": security_group_ids,
        "AvailabilityZone": availability_zone,
    }


class GlueDRFactory(BaseDRFactory):
    """Factory that uses AWS Glue jobs for database operations.

    Extends BaseDRFactory to replace PythonOperator-based database tasks with
    Glue job orchestration. Used by Airflow 3.0+ where direct ORM access from
    DAGs is prohibited.

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.
    """

    def __init__(self, dag_id, path_prefix=None, storage_type=None, batch_size=5000):
        super().__init__(dag_id, path_prefix, storage_type, batch_size)

    # --- Glue job helpers ---

    def get_glue_role_name(self) -> str:
        """Get the Glue IAM role name from the GLUE_ROLE_ARN Airflow variable.

        The variable may contain a full ARN or just the role name.
        GlueJobOperator expects the role name (not ARN) in newer provider versions.
        """
        role_value = Variable.get("GLUE_ROLE_ARN", default_var="")
        # Extract role name from ARN if full ARN is provided
        if role_value.startswith("arn:"):
            return role_value.split("/")[-1]
        return role_value

    def get_glue_connection_name(self) -> str:
        """Get the Glue JDBC connection name (deterministic: {env_name}_conn)."""
        env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
            "DR_MWAA_ENV_NAME", default_var=""
        )
        return f"{env_name}_conn"

    def get_glue_job_name(self, suffix: str) -> str:
        """Construct a Glue job name namespaced by the MWAA environment.

        Format: {env_name}_{dag_id}_{suffix}
        This ensures multiple deployments in the same account/region don't
        share or overwrite each other's Glue jobs.
        """
        env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
            "DR_MWAA_ENV_NAME", default_var=""
        )
        return f"{env_name}_{self.dag_id}_{suffix}"

    def get_script_location(self, script_name: str) -> str:
        """Construct the S3 location for a Glue script.

        Uses the DR_DAGS_BUCKET Airflow variable (set by CDK).
        No API calls at parse time to avoid silent failures.
        """
        bucket = Variable.get("DR_DAGS_BUCKET", default_var="")
        return f"s3://{bucket}/scripts/{script_name}.py"

    def get_table_definitions(self) -> list:
        """Build a JSON-serializable list of table definitions from the model.

        Each table definition includes the table name, date field (if any),
        column list, binary columns, and dependency level computed from the
        topological sort of the dependency model.

        Returns:
            list[dict]: A list of dicts with keys: table, date_field, columns,
                binary_columns, dependency_level.
        """
        dependency_order = self.get_table_dependency_order()

        # Build a lookup from table name to its dependency level
        level_lookup = {}
        for level_index, level_tables in enumerate(dependency_order):
            for table_name in level_tables:
                level_lookup[table_name] = level_index

        table_defs = []
        for table in self.tables():
            # Determine binary columns from export_mappings
            # Binary columns are those that have hex-encoding in their export mapping
            binary_columns = []
            for col, mapping in table.export_mappings.items():
                if "encode(" in mapping and "hex" in mapping:
                    binary_columns.append(col)

            # Determine date field from export_filter or known patterns
            date_field = self._detect_date_field(table)

            table_defs.append(
                {
                    "table": table.name,
                    "date_field": date_field,
                    "columns": table.columns if table.columns else [],
                    "binary_columns": binary_columns,
                    "dependency_level": level_lookup.get(table.name, 0),
                }
            )

        return table_defs

    def get_table_dependency_order(self) -> list:
        """Compute a topological sort of tables from the dependency model.

        Returns a list of lists, where each inner list contains table names
        at the same dependency level. Level 0 contains tables with no
        dependencies (sources), level 1 contains tables that depend only on
        level 0 tables, and so on.

        The dependency model uses:
        - ``reverse_graph[node]`` = set of nodes that must come BEFORE node
          (i.e., node's prerequisites/parents)
        - ``forward_graph[node]`` = set of nodes that come AFTER node
          (i.e., node's dependents/children)

        Returns:
            list[list[str]]: Ordered levels of table names.
        """
        model = self.model
        # Ensure tables are initialized
        self.tables()

        # reverse_graph[node] = prerequisites that must be processed first
        # Sources (level 0) have no entries in reverse_graph

        # BFS-based topological sort by levels
        processed = set()
        levels = []

        while len(processed) < len(model.nodes):
            # Find nodes whose prerequisites are all already processed
            current_level = []
            for node in model.nodes:
                if node in processed:
                    continue
                prerequisites = model.reverse_graph[node]
                if prerequisites.issubset(processed):
                    current_level.append(node)

            if not current_level:
                # Cycle detected — add remaining nodes to break the cycle
                remaining = [n for n in model.nodes if n not in processed]
                levels.append([t.name for t in remaining])
                break

            levels.append([t.name for t in current_level])
            processed.update(current_level)

        return levels

    # --- REST API helpers ---

    def get_mwaa_rest_api_client(self) -> MwaaRestApiClient:
        """Create and return a configured MwaaRestApiClient.

        Reads the MWAA environment name from the ``MWAA_ENV_NAME`` environment
        variable (or ``DR_MWAA_ENV_NAME`` Airflow variable as fallback) and the
        AWS region from ``AWS_REGION`` (falling back to ``AWS_DEFAULT_REGION``).

        Returns:
            MwaaRestApiClient: A client configured for the current MWAA environment.
        """
        env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
            "DR_MWAA_ENV_NAME", default_var=""
        )
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", ""))
        return MwaaRestApiClient(env_name, region)

    def backup_variables_via_api(self):
        """Backup all Airflow variables via the MWAA REST API to S3 as CSV.

        Retrieves all variables using ``GET /api/v2/variables``, writes them
        to a pipe-delimited CSV file at
        ``s3://{backup_bucket}/{path_prefix}/variable.csv``.
        """
        client = self.get_mwaa_rest_api_client()
        variables = client.list_variables()

        buffer = StringIO()
        keys = ["key", "val", "description"]
        writer = csv.DictWriter(buffer, fieldnames=keys, delimiter="|")
        for var in variables:
            writer.writerow(
                {
                    "key": var.get("key", ""),
                    "val": var.get("value", ""),
                    "description": var.get("description", ""),
                }
            )

        backup_bucket = self.bucket()
        s3_key = f"{self.path_prefix}/variable.csv"
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=backup_bucket,
            Key=s3_key,
            Body=buffer.getvalue().encode("utf-8"),
        )
        logger.info(
            "Backed up %d variables to s3://%s/%s",
            len(variables),
            backup_bucket,
            s3_key,
        )

    def backup_connections_via_api(self):
        """Backup all Airflow connections via the MWAA REST API to S3 as CSV.

        Retrieves all connections using ``GET /api/v2/connections``, writes them
        to a pipe-delimited CSV file at
        ``s3://{backup_bucket}/{path_prefix}/connection.csv``.
        """
        client = self.get_mwaa_rest_api_client()
        connections = client.list_connections()

        buffer = StringIO()
        keys = [
            "conn_id",
            "conn_type",
            "description",
            "extra",
            "host",
            "login",
            "password",
            "port",
            "schema",
        ]
        writer = csv.DictWriter(buffer, fieldnames=keys, delimiter="|")
        for conn in connections:
            writer.writerow(
                {
                    "conn_id": conn.get("connection_id", ""),
                    "conn_type": conn.get("conn_type", ""),
                    "description": conn.get("description", ""),
                    "extra": conn.get("extra", ""),
                    "host": conn.get("host", ""),
                    "login": conn.get("login", ""),
                    "password": conn.get("password", ""),
                    "port": conn.get("port", ""),
                    "schema": conn.get("schema", ""),
                }
            )

        backup_bucket = self.bucket()
        s3_key = f"{self.path_prefix}/connection.csv"
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=backup_bucket,
            Key=s3_key,
            Body=buffer.getvalue().encode("utf-8"),
        )
        logger.info(
            "Backed up %d connections to s3://%s/%s",
            len(connections),
            backup_bucket,
            s3_key,
        )

    # --- REST API restore helpers ---

    def restore_variables_via_api(self, context=None):
        """Restore Airflow variables from a CSV backup in S3 via the MWAA REST API.

        Reads the pipe-delimited CSV file at
        ``s3://{backup_bucket}/{path_prefix}/variable.csv`` and creates or
        updates variables in the target environment based on the configured
        restore strategy (``DR_VARIABLE_RESTORE_STRATEGY`` Airflow Variable):

        - ``APPEND``: Only create variables whose keys do not already exist.
        - ``REPLACE``: Delete all existing variables, then create from backup.
        - ``DO_NOTHING``: Skip variable restore entirely.
        """
        strategy = Variable.get(
            "DR_VARIABLE_RESTORE_STRATEGY", default_var="APPEND"
        ).upper()
        logger.info("Variable restore strategy: %s", strategy)

        if strategy == "DO_NOTHING":
            logger.info("Skipping variable restore (DO_NOTHING strategy).")
            return

        client = self.get_mwaa_rest_api_client()

        # Read backup CSV from S3 — use dag_run conf bucket when available
        # (Step Functions passes the correct backup bucket in conf)
        backup_bucket = self.bucket(context)
        s3_key = f"{self.path_prefix}/variable.csv"
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=backup_bucket, Key=s3_key)
            csv_content = response["Body"].read().decode("utf-8")
        except Exception as e:
            logger.warning(
                "Could not read variable backup from s3://%s/%s: %s",
                backup_bucket,
                s3_key,
                e,
            )
            return

        reader = csv.DictReader(
            StringIO(csv_content),
            fieldnames=["key", "val", "description"],
            delimiter="|",
        )
        backup_vars = list(reader)
        logger.info("Read %d variables from backup.", len(backup_vars))

        if strategy == "REPLACE":
            # Delete all existing variables first
            existing = client.list_variables()
            for var in existing:
                key = var.get("key", "")
                try:
                    client.delete_variable(key)
                    logger.info("Deleted existing variable '%s'.", key)
                except Exception as e:
                    logger.warning("Failed to delete variable '%s': %s", key, e)

            # Create all from backup
            for var in backup_vars:
                try:
                    client.create_variable(
                        key=var["key"],
                        value=var.get("val", ""),
                        description=var.get("description") or None,
                    )
                    logger.info("Created variable '%s'.", var["key"])
                except Exception as e:
                    logger.warning("Failed to create variable '%s': %s", var["key"], e)

        elif strategy == "APPEND":
            # Only create variables whose keys don't already exist
            existing = client.list_variables()
            existing_keys = {v.get("key", "") for v in existing}

            for var in backup_vars:
                if var["key"] not in existing_keys:
                    try:
                        client.create_variable(
                            key=var["key"],
                            value=var.get("val", ""),
                            description=var.get("description") or None,
                        )
                        logger.info("Created variable '%s' (APPEND).", var["key"])
                    except Exception as e:
                        logger.warning(
                            "Failed to create variable '%s': %s", var["key"], e
                        )
                else:
                    logger.info(
                        "Skipping variable '%s' — already exists (APPEND).", var["key"]
                    )

        logger.info("Variable restore complete (strategy=%s).", strategy)

    def restore_connections_via_api(self, context=None):
        """Restore Airflow connections from a CSV backup in S3 via the MWAA REST API.

        Reads the pipe-delimited CSV file at
        ``s3://{backup_bucket}/{path_prefix}/connection.csv`` and creates or
        updates connections in the target environment based on the configured
        restore strategy (``DR_CONNECTION_RESTORE_STRATEGY`` Airflow Variable):

        - ``APPEND``: Only create connections whose IDs do not already exist.
        - ``REPLACE``: Delete all existing connections, then create from backup.
        - ``DO_NOTHING``: Skip connection restore entirely.
        """
        strategy = Variable.get(
            "DR_CONNECTION_RESTORE_STRATEGY", default_var="APPEND"
        ).upper()
        logger.info("Connection restore strategy: %s", strategy)

        if strategy == "DO_NOTHING":
            logger.info("Skipping connection restore (DO_NOTHING strategy).")
            return

        client = self.get_mwaa_rest_api_client()

        # Read backup CSV from S3 — use dag_run conf bucket when available
        backup_bucket = self.bucket(context)
        s3_key = f"{self.path_prefix}/connection.csv"
        s3_client = boto3.client("s3")

        try:
            response = s3_client.get_object(Bucket=backup_bucket, Key=s3_key)
            csv_content = response["Body"].read().decode("utf-8")
        except Exception as e:
            logger.warning(
                "Could not read connection backup from s3://%s/%s: %s",
                backup_bucket,
                s3_key,
                e,
            )
            return

        reader = csv.DictReader(
            StringIO(csv_content),
            fieldnames=[
                "conn_id",
                "conn_type",
                "description",
                "extra",
                "host",
                "login",
                "password",
                "port",
                "schema",
            ],
            delimiter="|",
        )
        backup_conns = list(reader)
        logger.info("Read %d connections from backup.", len(backup_conns))

        if strategy == "REPLACE":
            # Delete all existing connections first
            existing = client.list_connections()
            for conn in existing:
                conn_id = conn.get("connection_id", "")
                try:
                    client.delete_connection(conn_id)
                    logger.info("Deleted existing connection '%s'.", conn_id)
                except Exception as e:
                    logger.warning("Failed to delete connection '%s': %s", conn_id, e)

            # Create all from backup
            for conn in backup_conns:
                conn_data = self._build_connection_payload(conn)
                try:
                    client.create_connection(conn_data)
                    logger.info("Created connection '%s'.", conn["conn_id"])
                except Exception as e:
                    logger.warning(
                        "Failed to create connection '%s': %s", conn["conn_id"], e
                    )

        elif strategy == "APPEND":
            # Only create connections whose IDs don't already exist
            existing = client.list_connections()
            existing_ids = {c.get("connection_id", "") for c in existing}

            for conn in backup_conns:
                if conn["conn_id"] not in existing_ids:
                    conn_data = self._build_connection_payload(conn)
                    try:
                        client.create_connection(conn_data)
                        logger.info(
                            "Created connection '%s' (APPEND).", conn["conn_id"]
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to create connection '%s': %s",
                            conn["conn_id"],
                            e,
                        )
                else:
                    logger.info(
                        "Skipping connection '%s' — already exists (APPEND).",
                        conn["conn_id"],
                    )

        logger.info("Connection restore complete (strategy=%s).", strategy)

    @staticmethod
    def _build_connection_payload(conn: dict) -> dict:
        """Build a connection payload dict for the REST API from a CSV row.

        Args:
            conn: A dict with keys from the CSV fieldnames.

        Returns:
            dict: A payload suitable for ``MwaaRestApiClient.create_connection()``.
        """
        payload = {
            "connection_id": conn.get("conn_id", ""),
            "conn_type": conn.get("conn_type", ""),
        }
        # Only include optional fields if they have a value
        for csv_key, api_key in [
            ("description", "description"),
            ("extra", "extra"),
            ("host", "host"),
            ("login", "login"),
            ("password", "password"),
            ("schema", "schema"),
        ]:
            val = conn.get(csv_key, "")
            if val:
                payload[api_key] = val

        port_val = conn.get("port", "")
        if port_val:
            try:
                payload["port"] = int(port_val)
            except (ValueError, TypeError):
                payload["port"] = port_val

        return payload

    # --- Overridden DAG creation methods ---

    def create_backup_dag(self) -> DAG:
        """Create the backup DAG using Glue jobs and the MWAA REST API.

        Builds a TaskFlow DAG with the following structure:
        - ``extract_credentials`` → ``create_glue_connection`` → ``GlueJobOperator(export)``
        - In parallel: ``backup_variables_via_api`` and ``backup_connections_via_api``

        The Glue export job handles all metadata tables except ``variable`` and
        ``connection``, which are backed up via the MWAA REST API to preserve
        Fernet-encrypted values.

        Returns:
            DAG: The backup DAG.
        """
        factory = self

        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
            "on_failure_callback": self.notify_failure_to_sns,
        }

        dag = DAG(
            dag_id=self.dag_id,
            schedule=self.schedule(),
            catchup=False,
            default_args=default_args,
        )

        with dag:

            @task
            def setup_glue_connection():
                """Extract credentials and create/reuse a Glue JDBC connection.

                Credentials stay within this task — never exposed via XCom.
                """
                creds = CredentialExtractor.extract()
                env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
                    "DR_MWAA_ENV_NAME", default_var=""
                )
                region = os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                )
                connection_name = f"{env_name}_conn"
                glue_client = boto3.client("glue", region_name=region)

                conn_input = {
                    "Name": connection_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": creds.jdbc_url,
                        "USERNAME": creds.username,
                        "PASSWORD": creds.password,
                    },
                    "PhysicalConnectionRequirements": _get_vpc_requirements(
                        env_name, region
                    ),
                }
                try:
                    glue_client.get_connection(Name=connection_name)
                    glue_client.update_connection(
                        Name=connection_name, ConnectionInput=conn_input
                    )
                except glue_client.exceptions.EntityNotFoundException:
                    glue_client.create_connection(ConnectionInput=conn_input)
                logger.info("Glue connection '%s' ready.", connection_name)

            @task
            def backup_variables_via_api():
                """Backup Airflow variables via the MWAA REST API to S3."""
                factory.backup_variables_via_api()

            @task
            def backup_connections_via_api():
                """Backup Airflow connections via the MWAA REST API to S3."""
                factory.backup_connections_via_api()

            # Build the DAG structure
            setup_task = setup_glue_connection()

            # Filter out variable and connection from table definitions
            table_defs = [
                td
                for td in factory.get_table_definitions()
                if td["table"] not in ("variable", "connection")
            ]
            dependency_order = factory.get_table_dependency_order()

            backup_bucket = factory.bucket()
            max_age = Variable.get("DR_MAX_AGE_IN_DAYS", default_var="0")

            export_job = GlueJobOperator(
                task_id="glue_export",
                # A lingering Glue run (job max concurrency is 1) fails
                # StartJobRun with ConcurrentRunsExceeded — retry instead
                # of failing the whole workflow.
                retries=4,
                retry_delay=timedelta(minutes=2),
                job_name=factory.get_glue_job_name("export"),
                script_location=factory.get_script_location("mwaa_metadb_export"),
                iam_role_name=factory.get_glue_role_name(),
                update_config=True,
                replace_script_file=True,
                deferrable=False,
                create_job_kwargs={
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "Connections": {
                        "Connections": [factory.get_glue_connection_name()]
                    },
                },
                script_args={
                    "--S3_OUTPUT_PATH": f"s3://{backup_bucket}/{factory.path_prefix}",
                    "--EXPORT_TABLES": json.dumps(table_defs),
                    "--GLUE_CONNECTION_NAME": factory.get_glue_connection_name(),
                    "--MAX_AGE_IN_DAYS": str(max_age),
                    "--TABLE_DEPENDENCY_ORDER": json.dumps(dependency_order),
                },
                region_name=os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                ),
            )

            setup_task >> export_job

            # Variables and connections backup run in parallel with the Glue job
            backup_variables_via_api()
            backup_connections_via_api()

        return dag

    def create_restore_dag(self) -> DAG:
        """Create the restore DAG using Glue jobs and the MWAA REST API.

        Builds a TaskFlow DAG with the following structure:
        - ``extract_credentials`` → ``create_glue_connection`` →
          ``GlueJobOperator(import)`` in parallel with
          ``restore_variables_via_api`` and ``restore_connections_via_api``
          → ``notify_success_to_sfn``
        - On failure, ``notify_failure_to_sfn`` is called with error details.

        The Glue import job handles all metadata tables except ``variable`` and
        ``connection``, which are restored via the MWAA REST API to preserve
        Fernet-encrypted values.

        Returns:
            DAG: The restore DAG.
        """
        factory = self

        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
        }

        # NOTE: the failure callback must be DAG-level, not in default_args.
        # As a per-task callback it fires on task attempt failures even when
        # the task will retry (observed on MWAA Airflow 3), sending a
        # premature send_task_failure to Step Functions while the run is
        # still recovering. DAG-level fires once, on terminal run failure.
        dag = DAG(
            dag_id=self.dag_id,
            schedule=None,
            catchup=False,
            default_args=default_args,
            on_failure_callback=self.notify_failure_to_sfn,
        )

        with dag:

            @task
            def setup_glue_connection():
                """Extract credentials and create/reuse a Glue JDBC connection.

                Credentials stay within this task — never exposed via XCom.
                """
                creds = CredentialExtractor.extract()
                env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
                    "DR_MWAA_ENV_NAME", default_var=""
                )
                region = os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                )
                connection_name = f"{env_name}_conn"
                glue_client = boto3.client("glue", region_name=region)

                conn_input = {
                    "Name": connection_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": creds.jdbc_url,
                        "USERNAME": creds.username,
                        "PASSWORD": creds.password,
                    },
                    "PhysicalConnectionRequirements": _get_vpc_requirements(
                        env_name, region
                    ),
                }
                try:
                    glue_client.get_connection(Name=connection_name)
                    glue_client.update_connection(
                        Name=connection_name, ConnectionInput=conn_input
                    )
                except glue_client.exceptions.EntityNotFoundException:
                    glue_client.create_connection(ConnectionInput=conn_input)
                logger.info("Glue connection '%s' ready.", connection_name)

            @task
            def restore_variables_via_api_task(**context):
                """Restore Airflow variables via the MWAA REST API from S3."""
                factory.restore_variables_via_api(context=context)

            @task
            def restore_connections_via_api_task(**context):
                """Restore Airflow connections via the MWAA REST API from S3."""
                factory.restore_connections_via_api(context=context)

            @task
            def notify_success_to_sfn(**context):
                """Send success callback to StepFunctions if all upstreams passed."""
                dag_run = context.get("dag_run")
                task_token = (
                    dag_run.conf.get("task_token") if dag_run and dag_run.conf else None
                )

                if not task_token:
                    logger.warning(
                        "No task_token found in dag_run.conf, skipping SFN callback."
                    )
                    return

                # Check if any upstream task failed. Use dag_run.get_task_instances
                # where available (AF 2.x), otherwise fall through to send success
                # (with all_done trigger_rule, if we're running then everything
                # finished — the failure callback handles the unhappy path).
                try:
                    all_tis = dag_run.get_task_instances()
                    our_id = context.get("task_instance").task_id if context.get("task_instance") else "notify_success_to_sfn"
                    failed = [
                        ti.task_id for ti in all_tis
                        if ti.task_id not in (our_id, "notify_failure_to_sfn")
                        and ti.state not in ("success", "skipped", None)
                    ]
                    if failed:
                        logger.info(
                            "Skipping success callback — tasks not all "
                            "successful: %s", failed
                        )
                        return
                except Exception as e:
                    # AF 3.x RuntimeTaskInstance may not support this — fall
                    # through and send success (failure task handles the inverse)
                    logger.info("Could not inspect upstream states (%s), "
                                "proceeding with success callback.", e)

                result = {
                    "dag": dag_run.dag_id,
                    "dag_run": dag_run.run_id,
                    "status": "Success",
                    "location": f"s3://{factory.bucket()}/{factory.path_prefix}",
                }

                sfn = boto3.client("stepfunctions")
                sfn.send_task_success(taskToken=task_token, output=json.dumps(result))
                logger.info("Sent task success to StepFunctions.")

            @task
            def notify_failure_to_sfn(**context):
                """Send failure callback to StepFunctions if any upstream failed."""
                dag_run = context.get("dag_run")
                task_token = (
                    dag_run.conf.get("task_token") if dag_run and dag_run.conf else None
                )

                if not task_token:
                    logger.warning(
                        "No task_token found in dag_run.conf, skipping SFN failure callback."
                    )
                    return

                # Check if any upstream task actually failed
                try:
                    all_tis = dag_run.get_task_instances()
                    our_id = context.get("task_instance").task_id if context.get("task_instance") else "notify_failure_to_sfn"
                    failed = [
                        ti.task_id for ti in all_tis
                        if ti.task_id not in (our_id, "notify_success_to_sfn")
                        and ti.state not in ("success", "skipped", None)
                    ]
                except Exception:
                    # AF 3.x — can't inspect states; assume failure since this
                    # task's trigger_rule is all_done and the dag-level
                    # on_failure_callback also fires on terminal failure.
                    failed = ["unknown (state inspection unavailable)"]

                if not failed:
                    logger.info(
                        "Skipping failure callback — all upstream tasks succeeded."
                    )
                    return

                result = {
                    "dag": dag_run.dag_id,
                    "dag_run": dag_run.run_id,
                    "tasks": failed,
                    "status": "Fail",
                }

                sfn = boto3.client("stepfunctions")
                sfn.send_task_failure(
                    taskToken=task_token,
                    error="Restore Failure",
                    cause=json.dumps(result),
                )
                logger.info("Sent task failure to StepFunctions.")

            # Build the DAG structure
            setup_task = setup_glue_connection()

            # Filter out variable and connection from table definitions
            table_defs = [
                td
                for td in factory.get_table_definitions()
                if td["table"] not in ("variable", "connection")
            ]
            dependency_order = factory.get_table_dependency_order()

            backup_bucket = factory.bucket()

            import_job = GlueJobOperator(
                task_id="glue_import",
                # A lingering Glue run (job max concurrency is 1) fails
                # StartJobRun with ConcurrentRunsExceeded — retry instead
                # of failing the whole workflow.
                retries=4,
                retry_delay=timedelta(minutes=2),
                job_name=factory.get_glue_job_name("import"),
                script_location=factory.get_script_location("mwaa_metadb_import"),
                iam_role_name=factory.get_glue_role_name(),
                update_config=True,
                replace_script_file=True,
                deferrable=False,
                create_job_kwargs={
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "Connections": {
                        "Connections": [factory.get_glue_connection_name()]
                    },
                },
                script_args={
                    "--S3_INPUT_PATH": f"s3://{backup_bucket}/{factory.path_prefix}",
                    "--IMPORT_TABLES": json.dumps(table_defs),
                    "--GLUE_CONNECTION_NAME": factory.get_glue_connection_name(),
                    "--TABLE_DEPENDENCY_ORDER": json.dumps(dependency_order),
                },
                region_name=os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                ),
            )

            setup_task >> import_job

            # Variables and connections restore run in parallel with the Glue job
            restore_vars = restore_variables_via_api_task()
            restore_conns = restore_connections_via_api_task()

            # All restore tasks must complete before success notification.
            # Use an EmptyOperator with all_done as a join point — @task
            # with trigger_rule doesn't fire reliably on Airflow 3.x.
            join = EmptyOperator(
                task_id="join_all_done",
                trigger_rule="all_done",
            )
            success = notify_success_to_sfn()
            failure = notify_failure_to_sfn()

            [import_job, restore_vars, restore_conns] >> join
            join >> success
            join >> failure

        return dag

    def create_cleanup_dag(self) -> DAG:
        """Create the cleanup DAG using a Glue job for metadata table cleanup.

        Builds a TaskFlow DAG with the following structure:
        - ``extract_credentials`` → ``create_glue_connection`` →
          ``GlueJobOperator(cleanup)`` → ``notify_success_to_sfn``
        - On failure, ``notify_failure_to_sfn`` is called with error details.

        The Glue cleanup job handles all metadata tables, deleting records
        in reverse dependency order to avoid foreign key constraint violations.
        It preserves ``default_pool`` in ``slot_pool`` and ``SchedulerJob``
        entries in the ``job`` table.

        Returns:
            DAG: The cleanup DAG.
        """
        factory = self

        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
        }

        # NOTE: the failure callback must be DAG-level, not in default_args.
        # As a per-task callback it fires on task attempt failures even when
        # the task will retry (observed on MWAA Airflow 3), sending a
        # premature send_task_failure to Step Functions while the run is
        # still recovering. DAG-level fires once, on terminal run failure.
        dag = DAG(
            dag_id=self.dag_id,
            schedule=None,
            catchup=False,
            default_args=default_args,
            on_failure_callback=self.notify_failure_to_sfn,
        )

        with dag:

            @task
            def setup_glue_connection():
                """Extract credentials and create/reuse a Glue JDBC connection.

                Credentials stay within this task — never exposed via XCom.
                """
                creds = CredentialExtractor.extract()
                env_name = os.environ.get("MWAA_ENV_NAME", "") or Variable.get(
                    "DR_MWAA_ENV_NAME", default_var=""
                )
                region = os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                )
                connection_name = f"{env_name}_conn"
                glue_client = boto3.client("glue", region_name=region)

                conn_input = {
                    "Name": connection_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": creds.jdbc_url,
                        "USERNAME": creds.username,
                        "PASSWORD": creds.password,
                    },
                    "PhysicalConnectionRequirements": _get_vpc_requirements(
                        env_name, region
                    ),
                }
                try:
                    glue_client.get_connection(Name=connection_name)
                    glue_client.update_connection(
                        Name=connection_name, ConnectionInput=conn_input
                    )
                except glue_client.exceptions.EntityNotFoundException:
                    glue_client.create_connection(ConnectionInput=conn_input)
                logger.info("Glue connection '%s' ready.", connection_name)

            @task
            def notify_success_to_sfn(**context):
                """Send success callback to StepFunctions if all upstreams passed."""
                dag_run = context.get("dag_run")
                task_token = (
                    dag_run.conf.get("task_token") if dag_run and dag_run.conf else None
                )

                if not task_token:
                    logger.warning(
                        "No task_token found in dag_run.conf, skipping SFN callback."
                    )
                    return

                try:
                    all_tis = dag_run.get_task_instances()
                    our_id = context.get("task_instance").task_id if context.get("task_instance") else "notify_success_to_sfn"
                    failed = [
                        ti.task_id for ti in all_tis
                        if ti.task_id not in (our_id, "notify_failure_to_sfn")
                        and ti.state not in ("success", "skipped", None)
                    ]
                    if failed:
                        logger.info(
                            "Skipping success callback — tasks not all "
                            "successful: %s", failed
                        )
                        return
                except Exception as e:
                    logger.info("Could not inspect upstream states (%s), "
                                "proceeding with success callback.", e)

                result = {
                    "dag": dag_run.dag_id,
                    "dag_run": dag_run.run_id,
                    "status": "Success",
                }

                sfn = boto3.client("stepfunctions")
                sfn.send_task_success(taskToken=task_token, output=json.dumps(result))
                logger.info("Sent task success to StepFunctions.")

            @task
            def notify_failure_to_sfn(**context):
                """Send failure callback to StepFunctions if any upstream failed."""
                dag_run = context.get("dag_run")
                task_token = (
                    dag_run.conf.get("task_token") if dag_run and dag_run.conf else None
                )

                if not task_token:
                    logger.warning(
                        "No task_token found in dag_run.conf, skipping SFN failure callback."
                    )
                    return

                try:
                    all_tis = dag_run.get_task_instances()
                    our_id = context.get("task_instance").task_id if context.get("task_instance") else "notify_failure_to_sfn"
                    failed = [
                        ti.task_id for ti in all_tis
                        if ti.task_id not in (our_id, "notify_success_to_sfn")
                        and ti.state not in ("success", "skipped", None)
                    ]
                except Exception:
                    failed = ["unknown (state inspection unavailable)"]

                if not failed:
                    logger.info(
                        "Skipping failure callback — all upstream tasks succeeded."
                    )
                    return

                result = {
                    "dag": dag_run.dag_id,
                    "dag_run": dag_run.run_id,
                    "tasks": failed,
                    "status": "Fail",
                }

                sfn = boto3.client("stepfunctions")
                sfn.send_task_failure(
                    taskToken=task_token,
                    error="Cleanup Failure",
                    cause=json.dumps(result),
                )
                logger.info("Sent task failure to StepFunctions.")

            # Build the DAG structure
            setup_task = setup_glue_connection()

            table_defs = factory.get_table_definitions()
            dependency_order = factory.get_table_dependency_order()

            cleanup_job = GlueJobOperator(
                task_id="glue_cleanup",
                # A lingering Glue run (job max concurrency is 1) fails
                # StartJobRun with ConcurrentRunsExceeded — retry instead
                # of failing the whole workflow.
                retries=4,
                retry_delay=timedelta(minutes=2),
                job_name=factory.get_glue_job_name("cleanup"),
                script_location=factory.get_script_location("mwaa_metadb_cleanup"),
                iam_role_name=factory.get_glue_role_name(),
                update_config=True,
                replace_script_file=True,
                deferrable=False,
                create_job_kwargs={
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "Connections": {
                        "Connections": [factory.get_glue_connection_name()]
                    },
                },
                script_args={
                    "--CLEANUP_TABLES": json.dumps(table_defs),
                    "--GLUE_CONNECTION_NAME": factory.get_glue_connection_name(),
                    "--TABLE_DEPENDENCY_ORDER": json.dumps(dependency_order),
                },
                region_name=os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")
                ),
            )

            setup_task >> cleanup_job

            # Notify StepFunctions on success or failure.
            # Use an EmptyOperator with all_done as a join point — @task
            # with trigger_rule doesn't fire reliably on Airflow 3.x.
            join = EmptyOperator(
                task_id="join_all_done",
                trigger_rule="all_done",
            )
            success = notify_success_to_sfn()
            failure = notify_failure_to_sfn()

            cleanup_job >> join
            join >> success
            join >> failure

        return dag

    # --- Private helpers ---

    @staticmethod
    def _detect_date_field(table: BaseTable) -> str:
        """Detect the date field for a table based on known patterns.

        Checks common date column names that exist in the table's column list.

        Args:
            table: The table to inspect.

        Returns:
            str or None: The date field name, or None if no date field is detected.
        """
        known_date_fields = [
            "execution_date",
            "start_date",
            "timestamp",
            "created_date",
            "dttm",
        ]
        for field in known_date_fields:
            if field in table.columns:
                return field
        return None
