# Requirements Document

## Introduction

This feature adds Apache Airflow 3.0 support to the MWAA Disaster Recovery framework. Airflow 3.0 prohibits direct database access via the ORM from within DAGs (`RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0`), which breaks the current backup, restore, and cleanup mechanisms that rely on `airflow.settings.Session` and `settings.engine.raw_connection()`. The solution replaces direct database operations with AWS Glue jobs orchestrated from the DAG. The DAG extracts database credentials from MWAA worker environment variables, creates Glue connections and jobs dynamically, and delegates all database read/write operations to Glue scripts running in the same VPC via JDBC. This approach preserves the existing DR framework's factory pattern, StepFunctions integration, and support for both Backup/Restore and Warm Standby strategies.

## Glossary

- **MWAA**: Amazon Managed Workflows for Apache Airflow — the managed service hosting Airflow environments.
- **DR_Framework**: The existing disaster recovery framework in `assets/dags/mwaa_dr/` that provides backup, restore, and cleanup DAGs via version-specific factory classes.
- **BaseDRFactory**: The abstract base class (`base_dr_factory.py`) that defines the interface for creating backup, restore, and cleanup DAGs using PythonOperator tasks.
- **GlueDRFactory**: The new factory class for Airflow 3.0 that replaces PythonOperator-based database tasks with Glue job orchestration tasks.
- **Glue_Connection**: An AWS Glue JDBC connection resource that stores database credentials and VPC networking configuration for connecting to the MWAA metadata database.
- **Glue_Export_Job**: An AWS Glue job that reads metadata tables from the MWAA PostgreSQL database via JDBC and writes compressed CSV files to S3.
- **Glue_Import_Job**: An AWS Glue job that reads compressed CSV backup files from S3 and writes them into the MWAA PostgreSQL metadata database via JDBC.
- **Glue_Cleanup_Job**: An AWS Glue job that truncates or deletes records from metadata tables in the MWAA PostgreSQL database via JDBC.
- **DB_SECRETS**: An environment variable available in Airflow 3.x MWAA workers containing a JSON string with `username` and `password` fields for the metadata database.
- **Credential_Extractor**: The DAG-side logic that reads database connection credentials from environment variables (`DB_SECRETS`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` for Airflow 3.x, or `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` for Airflow 2.x).
- **CDK_Stack**: The AWS CDK infrastructure stacks (`mwaa_primary_stack.py`, `mwaa_secondary_stack.py`) that provision AWS resources for the DR solution.
- **Glue_IAM_Role**: An IAM role assumed by AWS Glue jobs, granting permissions for VPC networking, S3 read/write, CloudWatch logging, and PostgreSQL database access.
- **Task_Token**: The AWS StepFunctions task token passed to DAGs via `dag_run.conf` for callback integration in the DR workflow.
- **Backup_Bucket**: The S3 bucket where metadata backup CSV files are stored, replicated cross-region.
- **MWAA_REST_API**: The Airflow REST API exposed by the MWAA webserver, accessed via a web login token obtained from the `CreateWebLoginToken` MWAA API. Used for variable and connection backup/restore because these tables contain Fernet-encrypted values that can only be decrypted/re-encrypted by Airflow itself.
- **Fernet_Key**: The encryption key used by Airflow to encrypt sensitive fields in the `variable` and `connection` tables. Each MWAA environment has its own Fernet key, so values must be decrypted via the source environment's API and re-encrypted via the target environment's API.

## Requirements

### Requirement 1: Credential Extraction from MWAA Worker Environment

**User Story:** As a DR framework operator, I want the DAG to extract database credentials from the MWAA worker environment, so that Glue connections can be created without direct ORM access.

#### Acceptance Criteria

1. WHEN running on Airflow 3.x where `DB_SECRETS` environment variable is available, THE Credential_Extractor SHALL parse the JSON value to obtain `username` and `password` fields and read `POSTGRES_HOST`, `POSTGRES_PORT`, and `POSTGRES_DB` environment variables to construct a JDBC connection URL.
2. WHEN running on Airflow 2.x where `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` environment variable is available, THE Credential_Extractor SHALL parse the SQLAlchemy connection string to extract the JDBC URL, username, and password.
3. IF neither `DB_SECRETS` nor `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` environment variables are available, THEN THE Credential_Extractor SHALL raise a descriptive error indicating that database credentials are not accessible.
4. IF the `DB_SECRETS` environment variable contains malformed JSON, THEN THE Credential_Extractor SHALL raise a descriptive error including the parse failure reason.

### Requirement 2: Glue Connection Management

**User Story:** As a DR framework operator, I want the DAG to create and manage AWS Glue JDBC connections to the MWAA metadata database, so that Glue jobs can connect to the database within the MWAA VPC.

#### Acceptance Criteria

1. WHEN a backup, restore, or cleanup DAG executes, THE GlueDRFactory SHALL create a Glue_Connection using the extracted credentials, the MWAA environment's subnet, security groups, and availability zone.
2. WHEN a Glue_Connection with the same name already exists, THE GlueDRFactory SHALL reuse the existing connection without creating a duplicate.
3. THE GlueDRFactory SHALL retrieve the MWAA environment's VPC networking configuration (subnet ID, security group IDs, availability zone) by calling the `mwaa:GetEnvironment` and `ec2:DescribeSubnets` APIs.
4. THE GlueDRFactory SHALL name the Glue_Connection using the pattern `{MWAA_ENV_NAME}_conn` to ensure uniqueness per environment.

### Requirement 3: Glue-Based Metadata Export (Backup)

**User Story:** As a DR framework operator, I want metadata backup to run as a Glue job instead of direct SQL queries, so that backup works on Airflow 3.0 where ORM access is prohibited.

#### Acceptance Criteria

1. WHEN the backup DAG executes, THE GlueDRFactory SHALL create and run a Glue_Export_Job that connects to the metadata database via JDBC and exports configured tables (excluding `variable` and `connection`, which are handled via the MWAA_REST_API) to compressed CSV files in the Backup_Bucket.
2. THE Glue_Export_Job SHALL export each table as a gzip-compressed CSV file to the S3 path `s3://{backup_bucket}/{path_prefix}/{table_name}.csv.gz`.
3. THE Glue_Export_Job SHALL apply date-based filtering on tables that have a date field, exporting only records within a configurable age window.
4. IF a configured table does not exist in the database, THEN THE Glue_Export_Job SHALL log a warning and skip that table without failing the job.
5. THE Glue_Export_Job SHALL produce an export summary JSON file containing the timestamp, table names, and row counts for each exported table.
6. THE GlueDRFactory SHALL pass the Glue_Connection name, S3 output path, table definitions, and maximum age parameter to the Glue_Export_Job as job arguments.
7. THE GlueDRFactory SHALL pass the table dependency ordering to the Glue_Export_Job, and THE Glue_Export_Job SHALL export tables in reverse dependency order (child tables before parent tables) to ensure snapshot consistency of the backup set.
8. THE Glue_Export_Job SHALL export tables that have no dependency relationship between them in parallel (e.g., `slot_pool`, `log`, `job`, `dag_run`, and `trigger` can be exported concurrently) to optimize backup performance.

### Requirement 4: Glue-Based Metadata Import (Restore)

**User Story:** As a DR framework operator, I want metadata restore to run as a Glue job instead of direct COPY commands, so that restore works on Airflow 3.0 where ORM access is prohibited.

#### Acceptance Criteria

1. WHEN the restore DAG executes, THE GlueDRFactory SHALL create and run a Glue_Import_Job that reads compressed CSV backup files from the Backup_Bucket and writes them into the metadata database via JDBC (excluding `variable` and `connection`, which are handled via the MWAA_REST_API).
2. THE Glue_Import_Job SHALL read backup files from the S3 path `s3://{backup_bucket}/{path_prefix}/{table_name}.csv.gz` for each configured table.
3. IF a backup file for a configured table does not exist in S3, THEN THE Glue_Import_Job SHALL log a warning and skip that table without failing the job.
4. WHEN duplicate key conflicts occur during import, THE Glue_Import_Job SHALL skip conflicting records and continue importing remaining records.
5. THE GlueDRFactory SHALL send a success Task_Token callback to StepFunctions after the Glue_Import_Job completes successfully.
6. IF the Glue_Import_Job fails, THEN THE GlueDRFactory SHALL send a failure Task_Token callback to StepFunctions with the error details.
7. THE GlueDRFactory SHALL pass the table dependency ordering to the Glue_Import_Job, and THE Glue_Import_Job SHALL import tables in dependency order (parent tables before child tables) to avoid foreign key constraint violations.
8. THE Glue_Import_Job SHALL import tables that have no dependency relationship between them in parallel to optimize restore performance.

### Requirement 5: Glue-Based Metadata Cleanup

**User Story:** As a DR framework operator, I want metadata cleanup to run as a Glue job instead of ORM session queries, so that cleanup works on Airflow 3.0 for the Warm Standby DR strategy.

#### Acceptance Criteria

1. WHEN the cleanup DAG executes in Warm Standby mode, THE GlueDRFactory SHALL create and run a Glue_Cleanup_Job that connects to the metadata database via JDBC and deletes records from configured metadata tables.
2. THE Glue_Cleanup_Job SHALL delete tables in reverse dependency order (child tables before parent tables) to avoid foreign key constraint violations during cleanup.
3. THE Glue_Cleanup_Job SHALL delete tables that have no dependency relationship between them in parallel to optimize cleanup performance.
4. THE Glue_Cleanup_Job SHALL preserve the `default_pool` entry in the `slot_pool` table and `SchedulerJob` entries in the `job` table during cleanup.
5. THE GlueDRFactory SHALL send a success Task_Token callback to StepFunctions after the Glue_Cleanup_Job completes successfully.
6. IF the Glue_Cleanup_Job fails, THEN THE GlueDRFactory SHALL send a failure Task_Token callback to StepFunctions with the error details.

### Requirement 6: Airflow 3.0 Version-Specific Factory

**User Story:** As a DR framework developer, I want a version-specific factory for Airflow 3.0 that follows the existing factory pattern, so that the framework can be extended consistently.

#### Acceptance Criteria

1. THE DR_Framework SHALL include a `v_3_0` package with a `DRFactory_3_0` class that extends GlueDRFactory.
2. THE DRFactory_3_0 SHALL define the Airflow 3.0 metadata table schema including new tables: `dag_version`, `dag_code`, `asset`, `asset_event`, `backfill`, `backfill_dag_run`, `dag_run_note`, `task_instance_note`, and `task_instance_history`.
3. THE DRFactory_3_0 SHALL exclude tables removed in Airflow 3.0: `serialized_dag`, `sla_miss`, and `rendered_task_instance_fields`.
4. WHEN the Airflow version starts with "3.", THE entry point DAGs (`backup_metadata.py`, `restore_metadata.py`, `cleanup_metadata.py`) SHALL select the DRFactory_3_0 factory.
5. THE DRFactory_3_0 SHALL define table dependencies using the DependencyModel (e.g., `task_instance` depends on `dag_run`, `job`, and `trigger`; `xcom` depends on `task_instance` and `dag_run`) so that: export uses reverse dependency order (child tables before parent tables) for snapshot consistency, import uses forward dependency order (parent tables before child tables) to respect foreign key constraints, and cleanup uses reverse dependency order (child tables before parent tables) to avoid foreign key violations.

### Requirement 7: Glue Script Deployment via CDK

**User Story:** As a DR framework operator, I want the Glue scripts (export, import, cleanup) to be deployed to S3 by the CDK stack, so that Glue jobs can reference them at runtime without the DAG needing to upload them.

#### Acceptance Criteria

1. WHEN the MWAA version is 3.x, THE CDK_Stack SHALL deploy the Glue export, import, and cleanup Python scripts to the primary DAGs S3 bucket under a `scripts/` prefix (e.g., `s3://{dags_bucket}/scripts/mwaa_metadb_export.py`).
2. THE CDK_Stack SHALL include the Glue scripts in the existing `BucketDeployment` that deploys DAGs, so that scripts are replicated to the secondary region via the existing cross-region replication.
3. WHEN creating Glue jobs, THE GlueDRFactory SHALL reference the script location in the DAGs S3 bucket using the pattern `s3://{dags_bucket}/scripts/{script_name}.py`.
4. THE Glue_IAM_Role SHALL have read access to the scripts prefix in the DAGs S3 bucket.

### Requirement 8: IAM Permissions for Glue Operations

**User Story:** As a DR framework operator, I want the necessary IAM permissions provisioned automatically, so that the MWAA execution role can create and manage Glue resources and the Glue role can access the database and S3.

#### Acceptance Criteria

1. WHEN the MWAA version is 3.x, THE CDK_Stack SHALL grant the MWAA execution role permissions to create, get, and start Glue jobs and connections (`glue:CreateJob`, `glue:GetJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:CreateConnection`, `glue:GetConnection`).
2. WHEN the MWAA version is 3.x, THE CDK_Stack SHALL grant the MWAA execution role permissions to call `mwaa:GetEnvironment`, `mwaa:CreateWebLoginToken`, `ec2:DescribeSubnets`, and `ec2:DescribeSecurityGroups` for VPC configuration retrieval and REST API authentication.
3. WHEN the MWAA version is 3.x, THE CDK_Stack SHALL grant the MWAA execution role `iam:PassRole` permission scoped to the Glue_IAM_Role with a condition restricting the passed-to service to `glue.amazonaws.com`.
4. WHEN the MWAA version is 3.x, THE CDK_Stack SHALL create a Glue_IAM_Role with permissions for VPC networking (`ec2:CreateNetworkInterface`, `ec2:DeleteNetworkInterface`, `ec2:DescribeNetworkInterfaces`), S3 access to the Backup_Bucket, and CloudWatch logging.
5. THE CDK_Stack SHALL configure the Glue_IAM_Role trust policy to allow the `glue.amazonaws.com` service principal to assume the role.

### Requirement 9: Configuration and Version Support Updates

**User Story:** As a DR framework operator, I want the configuration to support Airflow 3.x versions, so that I can deploy the DR solution for Airflow 3.0 environments.

#### Acceptance Criteria

1. THE Config class SHALL include Airflow 3.0 version strings (starting with `"3.0"`) in the `SUPPORTED_MWAA_VERSIONS` list.
2. THE Config class SHALL expose a new `GLUE_ROLE_ARN` configuration property for specifying the Glue execution role ARN.
3. WHEN the configured MWAA version starts with "3.", THE CDK_Stack SHALL provision Glue-related IAM resources (Glue_IAM_Role, MWAA role policy additions).
4. WHEN the configured MWAA version starts with "2.", THE CDK_Stack SHALL not provision Glue-related IAM resources, preserving backward compatibility.

### Requirement 10: Backward Compatibility with Airflow 2.x

**User Story:** As a DR framework operator running Airflow 2.x, I want the existing direct-database backup and restore mechanism to continue working unchanged, so that upgrading the framework does not break my current setup.

#### Acceptance Criteria

1. WHEN the Airflow version starts with "2.", THE entry point DAGs SHALL continue to select the existing version-specific factory classes (DRFactory_2_4 through DRFactory_2_11) with no behavioral changes.
2. THE BaseTable class and its `backup()` and `restore()` methods using `settings.Session` and `settings.engine.raw_connection()` SHALL remain unchanged for Airflow 2.x usage.
3. THE BaseDRFactory class and its `cleanup_tables()` method using `settings.Session` SHALL remain unchanged for Airflow 2.x usage.
4. THE existing unit tests for Airflow 2.x factory classes SHALL continue to pass without modification.

### Requirement 11: Variable and Connection Table Handling via MWAA Airflow REST API

**User Story:** As a DR framework operator, I want the Variable and Connection tables to be backed up and restored via the MWAA Airflow REST API instead of Glue JDBC, so that Fernet-encrypted values are properly decrypted during export and re-encrypted during import on Airflow 3.0.

#### Acceptance Criteria

1. WHEN backing up variables on Airflow 3.x, THE GlueDRFactory SHALL use the MWAA Airflow REST API (`/api/v2/variables`) to retrieve all variables with decrypted values and write them to a CSV file in the Backup_Bucket.
2. WHEN backing up connections on Airflow 3.x, THE GlueDRFactory SHALL use the MWAA Airflow REST API (`/api/v2/connections`) to retrieve all connections with decrypted passwords and extras and write them to a CSV file in the Backup_Bucket.
3. WHEN restoring variables on Airflow 3.x, THE GlueDRFactory SHALL use the MWAA Airflow REST API (`POST /api/v2/variables` or `PATCH /api/v2/variables/{variable_key}`) to create or update variables so that values are re-encrypted with the target environment's Fernet key.
4. WHEN restoring connections on Airflow 3.x, THE GlueDRFactory SHALL use the MWAA Airflow REST API (`POST /api/v2/connections` or `PATCH /api/v2/connections/{connection_id}`) to create or update connections so that passwords and extras are re-encrypted with the target environment's Fernet key.
5. THE GlueDRFactory SHALL support the `DR_VARIABLE_RESTORE_STRATEGY` and `DR_CONNECTION_RESTORE_STRATEGY` settings with values `APPEND`, `REPLACE`, and `DO_NOTHING` when using the REST API.
6. WHEN the restore strategy is `APPEND`, THE GlueDRFactory SHALL only create variables or connections whose keys do not already exist in the target environment.
7. WHEN the restore strategy is `REPLACE`, THE GlueDRFactory SHALL delete existing variables or connections and recreate them from the backup.
8. WHEN the restore strategy is `DO_NOTHING`, THE GlueDRFactory SHALL skip the variable or connection table entirely.
9. THE GlueDRFactory SHALL obtain a web login token via the MWAA `CreateWebLoginToken` API and use it to authenticate REST API calls to the Airflow webserver endpoint.

### Requirement 12: Glue Import Script for Database Restore

**User Story:** As a DR framework developer, I want a Glue import script that performs the reverse of the export script, so that metadata can be restored into a target MWAA environment's database.

#### Acceptance Criteria

1. THE Glue_Import_Job script SHALL accept parameters for S3 input path, Glue connection name, table definitions, and restore strategies.
2. THE Glue_Import_Job script SHALL read gzip-compressed CSV files from S3 and write records to the corresponding PostgreSQL tables via JDBC.
3. THE Glue_Import_Job script SHALL handle tables with binary columns (e.g., `executor_config`, `conf`, `value` in xcom) by decoding hex-encoded values back to binary.
4. THE Glue_Import_Job script SHALL produce an import summary JSON file containing the timestamp, table names, rows imported, and rows skipped for each table.
5. FOR ALL valid backup CSV files, exporting via Glue_Export_Job then importing via Glue_Import_Job SHALL produce equivalent records in the target database (round-trip property).
