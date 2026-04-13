# Implementation Plan: Airflow 3.0 Glue DR Support

## Overview

This plan implements Airflow 3.0 support for the MWAA Disaster Recovery framework by replacing direct ORM database access with AWS Glue jobs and the MWAA REST API. Implementation proceeds bottom-up: utility classes first, then the factory layer, Glue scripts, CDK infrastructure, config updates, and finally DAG entry point wiring.

## Tasks

- [ ] 1. Implement CredentialExtractor utility
  - [ ] 1.1 Create `assets/dags/mwaa_dr/framework/credential_extractor.py` with the `CredentialExtractor` class
    - Implement `extract()` static method that returns a `DatabaseCredentials` with `jdbc_url`, `username`, `password`, `host`, `port`, `database`
    - Strategy 1: Parse `DB_SECRETS` JSON env var + `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` (Airflow 3.x)
    - Strategy 2: Parse `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` SQLAlchemy connection string (Airflow 2.x)
    - Strategy 3: Parse `AIRFLOW__CORE__SQL_ALCHEMY_CONN` (legacy fallback)
    - Raise `ValueError` with descriptive message if no credentials found or JSON is malformed
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ]* 1.2 Write unit tests for CredentialExtractor
    - Test valid `DB_SECRETS` parsing (Airflow 3.x path)
    - Test valid SQLAlchemy connection string parsing (Airflow 2.x path)
    - Test missing credentials raises `ValueError`
    - Test malformed `DB_SECRETS` JSON raises `ValueError`
    - Test missing JSON keys raises `ValueError`
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ]* 1.3 Write property test for DB_SECRETS credential extraction
    - **Property 1: DB_SECRETS credential extraction preserves components**
    - **Validates: Requirements 1.1**

  - [ ]* 1.4 Write property test for SQLAlchemy connection string credential extraction
    - **Property 2: SQLAlchemy connection string credential extraction preserves components**
    - **Validates: Requirements 1.2**

- [ ] 2. Implement MwaaRestApiClient utility
  - [ ] 2.1 Create `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py` with the `MwaaRestApiClient` class
    - Implement `__init__(self, env_name, region)` storing environment name and region
    - Implement `_get_web_login_token()` using `CreateWebLoginToken` MWAA API
    - Implement `_get_session()` returning an authenticated `requests.Session`
    - Implement variable methods: `list_variables()`, `get_variable()`, `create_variable()`, `update_variable()`, `delete_variable()`
    - Implement connection methods: `list_connections()`, `get_connection()`, `create_connection()`, `update_connection()`, `delete_connection()`
    - Add retry with exponential backoff (3 attempts) for 4xx/5xx responses
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.9_

  - [ ]* 2.2 Write unit tests for MwaaRestApiClient (mocked HTTP)
    - Test `list_variables` returns all variables
    - Test `create_variable`, `update_variable`, `delete_variable` send correct requests
    - Test `list_connections`, `create_connection`, `update_connection`, `delete_connection` send correct requests
    - Test authentication token is obtained and used correctly
    - Test retry behavior on transient errors
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.9_

- [ ] 3. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement GlueDRFactory base class
  - [ ] 4.1 Create `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py` with the `GlueDRFactory` class extending `BaseDRFactory`
    - Implement `__init__` calling `super().__init__()` with same parameters
    - Implement `get_glue_role_name()` reading from `GLUE_ROLE_ARN` Airflow variable
    - Implement `get_script_location(script_name)` constructing `s3://{dags_bucket}/scripts/{script_name}.py`
    - Implement `get_table_definitions()` returning JSON-serializable list of table defs from `self.tables()`
    - Implement `get_table_dependency_order()` returning topological sort from `self.model`
    - Implement `get_mwaa_rest_api_client()` returning a configured `MwaaRestApiClient`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.6, 7.3_

  - [ ] 4.2 Implement `create_backup_dag()` in GlueDRFactory
    - Override to create a TaskFlow DAG with: `extract_credentials` → `create_glue_connection` → `GlueJobOperator(export)` in parallel with `backup_variables_via_api` and `backup_connections_via_api`
    - `extract_credentials` task calls `CredentialExtractor.extract()`
    - `create_glue_connection` task creates/reuses Glue JDBC connection using MWAA VPC networking (`mwaa:GetEnvironment`, `ec2:DescribeSubnets`)
    - Name connection as `{MWAA_ENV_NAME}_conn`
    - Pass table definitions, dependency ordering, S3 path, max age, and connection name to Glue job
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.6, 3.7, 11.1, 11.2_

  - [ ] 4.3 Implement `backup_variables_via_api()` and `backup_connections_via_api()` in GlueDRFactory
    - Use `MwaaRestApiClient` to GET `/api/v2/variables` and `/api/v2/connections`
    - Write decrypted values to CSV files in S3 backup bucket
    - _Requirements: 11.1, 11.2_

  - [ ] 4.4 Implement `create_restore_dag()` in GlueDRFactory
    - Override to create a TaskFlow DAG with: `extract_credentials` → `create_glue_connection` → `GlueJobOperator(import)` in parallel with `restore_variables_via_api` and `restore_connections_via_api` → `notify_success_to_sfn`
    - On failure, call `notify_failure_to_sfn` with error details
    - _Requirements: 4.1, 4.5, 4.6, 4.7, 11.3, 11.4_

  - [ ] 4.5 Implement `restore_variables_via_api()` and `restore_connections_via_api()` in GlueDRFactory
    - Read CSV backup files from S3
    - Support `DR_VARIABLE_RESTORE_STRATEGY` / `DR_CONNECTION_RESTORE_STRATEGY` with `APPEND`, `REPLACE`, `DO_NOTHING`
    - Use `MwaaRestApiClient` to POST/PATCH/DELETE variables and connections
    - _Requirements: 11.3, 11.4, 11.5, 11.6, 11.7, 11.8_

  - [ ] 4.6 Implement `create_cleanup_dag()` in GlueDRFactory
    - Override to create a TaskFlow DAG with: `extract_credentials` → `create_glue_connection` → `GlueJobOperator(cleanup)` → `notify_success_to_sfn`
    - On failure, call `notify_failure_to_sfn` with error details
    - _Requirements: 5.1, 5.4, 5.5_

  - [ ]* 4.7 Write unit tests for GlueDRFactory (mocked AWS)
    - Test `create_glue_connection` creates connection with correct VPC config
    - Test `create_glue_connection` reuses existing connection
    - Test `create_backup_dag` produces DAG with correct task structure
    - Test `create_restore_dag` produces DAG with correct task structure and SFN callbacks
    - Test `create_cleanup_dag` produces DAG with correct task structure and SFN callbacks
    - Test Glue job arguments contain all required parameters
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.6, 4.1, 4.5, 4.6, 5.1, 5.4, 5.5_

  - [ ]* 4.8 Write property test for Glue connection naming
    - **Property 13: Glue connection naming follows environment pattern**
    - **Validates: Requirements 2.4**

  - [ ]* 4.9 Write property test for S3 path construction
    - **Property 3: S3 path construction follows naming conventions**
    - **Validates: Requirements 3.2, 4.2, 7.3**

  - [ ]* 4.10 Write property test for restore strategy behavior
    - **Property 11: Restore strategy determines correct API behavior**
    - **Validates: Requirements 11.5, 11.6, 11.7**

- [ ] 5. Implement DRFactory_3_0 version-specific factory
  - [ ] 5.1 Create `assets/dags/mwaa_dr/v_3_0/__init__.py` and `assets/dags/mwaa_dr/v_3_0/dr_factory.py` with `DRFactory_3_0` extending `GlueDRFactory`
    - Implement `setup_tables(model)` defining Airflow 3.0 table schema
    - Include new tables: `dag_version`, `dag_code`, `asset`, `asset_event`, `backfill`, `backfill_dag_run`, `dag_run_note`, `task_instance_note`, `task_instance_history`
    - Exclude removed tables: `serialized_dag`, `sla_miss`, `rendered_task_instance_fields`
    - Define dependency model: `task_instance` depends on `dag_run`, `job`, `trigger`; `xcom` depends on `task_instance`, `dag_run`; etc.
    - Include `variable` and `connection` in table definitions for schema reference (excluded from Glue export/import)
    - _Requirements: 6.1, 6.2, 6.3, 6.5_

  - [ ]* 5.2 Write unit tests for DRFactory_3_0
    - Test `setup_tables` returns correct Airflow 3.0 table set
    - Test excluded tables are absent
    - Test dependency model is wired correctly
    - _Requirements: 6.1, 6.2, 6.3, 6.5_

  - [ ]* 5.3 Write property test for dependency ordering
    - **Property 5: Dependency ordering produces valid topological sorts**
    - **Validates: Requirements 3.7, 4.7, 5.2, 6.5**

- [ ] 6. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement Glue scripts
  - [ ] 7.1 Create `assets/glue_scripts/mwaa_metadb_export.py`
    - Accept job parameters: `S3_OUTPUT_PATH`, `EXPORT_TABLES` (JSON), `GLUE_CONNECTION_NAME`, `MAX_AGE_IN_DAYS`, `TABLE_DEPENDENCY_ORDER` (JSON)
    - Connect to PostgreSQL via JDBC using the Glue connection
    - Export each table (excluding `variable` and `connection`) to gzip-compressed CSV at `s3://{bucket}/{prefix}/{table_name}.csv.gz`
    - Apply date-based filtering on tables with a date field
    - Export tables in reverse dependency order; tables at the same level can run in parallel
    - Skip missing tables with a warning
    - Handle binary columns by hex-encoding
    - Produce export summary JSON with timestamp, table names, and row counts
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.7, 3.8_

  - [ ] 7.2 Create `assets/glue_scripts/mwaa_metadb_import.py`
    - Accept job parameters: `S3_INPUT_PATH`, `IMPORT_TABLES` (JSON), `GLUE_CONNECTION_NAME`, `TABLE_DEPENDENCY_ORDER` (JSON)
    - Read gzip-compressed CSV files from S3 for each table
    - Write records to PostgreSQL via JDBC in dependency order (parent tables first); tables at the same level can run in parallel
    - Handle binary columns by decoding hex-encoded values back to binary
    - Skip duplicate key conflicts, continue importing remaining records
    - Skip missing backup files with a warning
    - Produce import summary JSON with timestamp, table names, rows imported, rows skipped
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.7, 4.8, 12.1, 12.2, 12.3, 12.4_

  - [ ] 7.3 Create `assets/glue_scripts/mwaa_metadb_cleanup.py`
    - Accept job parameters: `GLUE_CONNECTION_NAME`, `CLEANUP_TABLES` (JSON), `TABLE_DEPENDENCY_ORDER` (JSON)
    - Delete records from metadata tables via JDBC in reverse dependency order; tables at the same level can run in parallel
    - Preserve `default_pool` in `slot_pool` and `SchedulerJob` entries in `job`
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ]* 7.4 Write property test for job summary correctness
    - **Property 6: Job summary contains all processed tables**
    - **Validates: Requirements 3.5, 12.4**

  - [ ]* 7.5 Write property test for cleanup preserving protected records
    - **Property 8: Cleanup preserves protected records**
    - **Validates: Requirements 5.4**

- [ ] 8. Update config.py for Airflow 3.x support
  - [ ] 8.1 Update `config.py`
    - Add `"3.0.2"` to `SUPPORTED_MWAA_VERSIONS` list
    - Add `GLUE_ROLE_ARN = "GLUE_ROLE_ARN"` constant
    - Add `GLUE_ROLE_ARN` to `DEFAULT_CONFIGS` with a sensible default (empty string)
    - Add `glue_role_arn` property to the `Config` class
    - _Requirements: 9.1, 9.2_

  - [ ]* 8.2 Write unit tests for config changes
    - Test `SUPPORTED_MWAA_VERSIONS` includes `"3.0.2"`
    - Test `glue_role_arn` property reads from environment
    - Test version validation accepts `"3.0.2"`
    - _Requirements: 9.1, 9.2_

- [ ] 9. Update CDK stacks for Glue IAM and script deployment
  - [ ] 9.1 Update `lib/stacks/mwaa_primary_stack.py`
    - Add conditional block: when `conf.mwaa_version.startswith("3.")`, create Glue IAM role with VPC networking, S3, and CloudWatch permissions
    - Grant MWAA execution role Glue permissions (`glue:CreateJob`, `glue:GetJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:CreateConnection`, `glue:GetConnection`)
    - Grant MWAA execution role `mwaa:GetEnvironment`, `mwaa:CreateWebLoginToken`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`
    - Grant MWAA execution role `iam:PassRole` scoped to Glue role with condition for `glue.amazonaws.com`
    - Deploy Glue scripts from `assets/glue_scripts/` to `s3://{dags_bucket}/scripts/` via the existing `BucketDeployment` or a new one
    - Configure Glue IAM role trust policy for `glue.amazonaws.com`
    - _Requirements: 7.1, 7.2, 7.4, 8.1, 8.2, 8.3, 8.4, 8.5, 9.3_

  - [ ] 9.2 Update `lib/stacks/mwaa_secondary_stack.py`
    - Add same conditional Glue IAM role and MWAA role policy additions when `conf.mwaa_version.startswith("3.")`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 9.3_

  - [ ]* 9.3 Write CDK assertion tests for Glue resources
    - Test that version 3.x config produces Glue IAM role, MWAA role policies, and script deployment
    - Test that version 2.x config does NOT produce Glue resources (backward compatibility)
    - _Requirements: 9.3, 9.4, 10.4_

- [ ] 10. Update entry point DAGs for version 3.x routing
  - [ ] 10.1 Update `assets/dags/mwaa_dr/backup_metadata.py`
    - Add `elif airflow_version.startswith("3."):` block before the `else` fallback
    - Import and instantiate `DRFactory_3_0` from `mwaa_dr.v_3_0.dr_factory`
    - Preserve all existing 2.x version branches unchanged
    - _Requirements: 6.4, 10.1_

  - [ ] 10.2 Update `assets/dags/mwaa_dr/restore_metadata.py`
    - Add `elif airflow_version.startswith("3."):` block before the `else` fallback
    - Import and instantiate `DRFactory_3_0` from `mwaa_dr.v_3_0.dr_factory`
    - Preserve all existing 2.x version branches unchanged
    - _Requirements: 6.4, 10.1_

  - [ ] 10.3 Update `assets/dags/mwaa_dr/cleanup_metadata.py`
    - Add `elif airflow_version.startswith("3."):` block before the `else` fallback
    - Import and instantiate `DRFactory_3_0` from `mwaa_dr.v_3_0.dr_factory`
    - Preserve all existing 2.x version branches unchanged
    - _Requirements: 6.4, 10.1_

  - [ ]* 10.4 Write property test for version routing
    - **Property 9: Version routing selects correct factory**
    - **Validates: Requirements 6.4, 10.1**

- [ ] 11. Backward compatibility verification
  - [ ]* 11.1 Run existing unit tests to verify no regressions
    - All existing tests for Airflow 2.x factory classes must pass without modification
    - `BaseTable`, `BaseDRFactory`, and `cleanup_tables()` remain unchanged
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

- [ ] 12. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- All code is Python; Glue scripts use PySpark/awsglue libraries
- The `variable` and `connection` tables are handled via the MWAA REST API, not Glue JDBC, due to Fernet encryption
