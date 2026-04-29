# PR #52 Code Review Findings — Airflow 3.0 Support via AWS Glue

**Reviewer:** awskamen
**Date:** 2026-04-28
**PR:** https://github.com/aws-samples/mwaa-disaster-recovery/pull/52
**Branch:** `pr-52` (7 commits, +8,761 / -8 lines, 39 files)

---

## Executive Summary

PR #52 adds Apache Airflow 3.0 support to the MWAA Disaster Recovery solution. This is a significant architectural change driven by Airflow 3.0's prohibition of direct ORM access from DAGs (`RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0`).

**Approach:** The PR replaces the ORM-based backup/restore/cleanup with:
1. **AWS Glue jobs** for metadata table operations via JDBC (export, import, cleanup)
2. **MWAA REST API** for `variable` and `connection` tables (to preserve Fernet encryption)
3. **CredentialExtractor** to obtain database credentials from MWAA worker environment variables

**Scope:** ~8,750 new lines across 39 files. New components: `GlueDRFactory`, `DRFactory_3_0`, `CredentialExtractor`, `MwaaRestApiClient`, 3 Glue scripts, CDK Glue resource provisioning in both stacks, and 115 new unit tests.

**Test Results:** 345 existing tests pass (no regressions). 115 new tests pass. 1 CDK stack test (`test_glue_resources.py`) fails due to Docker bundling requirement — expected in CI-less environments.

**Overall Assessment:** The architectural approach is sound and well-motivated. The code is well-documented with comprehensive docstrings. However, there are several issues ranging from security concerns to code quality that should be addressed before merging.

---

## Findings by Severity

### CRITICAL

#### C1: SQL Injection Risk in Glue Scripts — Table Names Not Parameterized

**Files:** `assets/glue_scripts/mwaa_metadb_export.py`, `mwaa_metadb_import.py`, `mwaa_metadb_cleanup.py`

Table names are interpolated directly into SQL strings via f-strings:

```python
# mwaa_metadb_cleanup.py line ~130
delete_sql = f"DELETE FROM {table_name}"

# mwaa_metadb_export.py line ~100
f"(SELECT {select_clause} FROM {table_name}{where_str}) AS {table_name}_export"

# mwaa_metadb_cleanup.py line ~65
f"WHERE table_name = '{table_name}' LIMIT 1"
```

While table names originate from the factory's hardcoded table definitions (not user input), this is still a defense-in-depth concern. If the table definitions are ever extended to accept user-configurable table names, this becomes exploitable. **Recommendation:** Add a whitelist validation function that checks table names against `^[a-z_]+$` before use in SQL.

#### C2: Database Credentials Exposed in Glue Connection Properties

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py` (lines ~380, ~490, ~600)

The `create_glue_connection` task stores database credentials (username, password) in plaintext in the Glue connection's `ConnectionProperties`. These credentials are visible to anyone with `glue:GetConnection` permissions.

```python
"ConnectionProperties": {
    "JDBC_CONNECTION_URL": credentials["jdbc_url"],
    "USERNAME": credentials["username"],
    "PASSWORD": credentials["password"],
},
```

**Recommendation:** Consider using AWS Secrets Manager to store the credentials and reference the secret ARN in the Glue connection, or ensure the Glue connection is cleaned up after use.

---

### HIGH

#### H1: Overly Broad IAM Permissions — `resources: ["*"]`

**Files:** `lib/stacks/mwaa_primary_stack.py` (lines ~670-700), `lib/stacks/mwaa_secondary_stack.py` (lines ~790-820)

Multiple IAM policy statements use `resources: ["*"]`:

```python
# Glue permissions on MWAA role
iam.PolicyStatement(
    actions=["glue:CreateJob", "glue:GetJob", "glue:StartJobRun", ...],
    resources=["*"],
)

# MWAA and EC2 permissions
iam.PolicyStatement(
    actions=["mwaa:GetEnvironment", "mwaa:CreateWebLoginToken", "ec2:DescribeSubnets", ...],
    resources=["*"],
)

# Glue role EC2 permissions
iam.PolicyStatement(
    actions=["ec2:CreateNetworkInterface", "ec2:DeleteNetworkInterface", ...],
    resources=["*"],
)
```

**Recommendation:** Scope Glue permissions to `arn:aws:glue:{region}:{account}:*`. Scope MWAA permissions to the specific environment ARN. EC2 networking permissions legitimately need `*` but should be documented as such.

#### H2: REST API Client Creates New Session Per API Call

**File:** `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py`

Every method (`list_variables`, `create_variable`, `delete_variable`, etc.) calls `self._get_session()` which:
1. Calls `CreateWebLoginToken` API
2. Makes an HTTP login request
3. Creates a new `requests.Session`

During a REPLACE restore with many variables/connections, this means N×2 API calls just for authentication. For example, restoring 50 variables + 50 connections = 200+ web login token requests.

**Recommendation:** Cache the session with a TTL (web login tokens are valid for ~60 seconds). Add a `_session` attribute that's reused within a reasonable window.

#### H3: Massive Code Duplication — `create_glue_connection` Duplicated 3 Times

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`

The `create_glue_connection` task function is copy-pasted identically in `create_backup_dag()`, `create_restore_dag()`, and `create_cleanup_dag()` (~30 lines each, 90 lines total). Similarly, `extract_credentials` is duplicated 3 times, and `notify_success_to_sfn`/`notify_failure_to_sfn` are duplicated in restore and cleanup DAGs.

**Recommendation:** Extract these into shared module-level functions or class methods. The `@task` decorator can wrap a method call.

#### H4: No Pagination in REST API Variable/Connection Listing

**File:** `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py` (lines ~130, ~200)

`list_variables()` and `list_connections()` make a single GET request without pagination parameters:

```python
url = f"{session.base_url}/variables"
response = self._request_with_retry("GET", url, session)
```

The Airflow REST API paginates results (default limit: 100). Environments with >100 variables or connections will have incomplete backups.

**Recommendation:** Implement pagination using `offset` and `limit` query parameters, iterating until all records are retrieved.

---

### MEDIUM

#### M1: Glue Export Writes Spark Directory, Not Single CSV File

**File:** `assets/glue_scripts/mwaa_metadb_export.py` (line ~150)

```python
output_path = f"{s3_output_path}/{table_name}.csv.gz"
df.coalesce(1).write.mode("overwrite").option("compression", "gzip").csv(output_path)
```

Spark's `.csv()` writer creates a **directory** at `{table_name}.csv.gz/` containing `part-00000-*.csv.gz` plus `_SUCCESS` marker. The import script reads from this path as a directory, which works, but the naming is misleading (`.csv.gz` suffix on a directory). The existing 2.x backup format writes a single `{table_name}.csv` file. This format incompatibility means 3.x backups cannot be restored by 2.x and vice versa.

**Recommendation:** Document this format difference explicitly. Consider renaming the output path to `{table_name}/` to avoid confusion.

#### M2: `_insert_with_conflict_handling` Uses Invalid Python Import

**File:** `assets/glue_scripts/mwaa_metadb_import.py` (line ~140)

```python
import java.sql  # noqa: F401 — available in Glue/Spark JVM
```

This is not valid Python. In the Glue/Spark JVM environment, Java classes are accessed via `spark.sparkContext._gateway.jvm`, not via Python imports. This line will raise `ModuleNotFoundError` if executed. The code works because the JVM classes are accessed via the gateway later, but this import is dead code that would fail.

**Recommendation:** Remove the `import java.sql` line.

#### M3: Missing `GLUE_ROLE_ARN` Airflow Variable Setup

**Files:** `lib/stacks/mwaa_primary_stack.py`, `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`

The `GlueDRFactory.get_glue_role_name()` reads `Variable.get("GLUE_ROLE_ARN")`, but the CDK stack's `setup_variables_airflow_cli` method does not set this variable. The Glue role is created by CDK but its ARN is never propagated to the Airflow variable.

```python
def get_glue_role_name(self) -> str:
    return Variable.get("GLUE_ROLE_ARN")  # This variable is never set!
```

**Impact:** The backup/restore/cleanup DAGs will fail at runtime with `Variable not found` error.

**Recommendation:** Add a `set GLUE_ROLE_ARN` command to the Airflow CLI setup in the primary stack, similar to how `DR_BACKUP_BUCKET` is set. This is a **blocker for deployment**.

#### M4: Glue Connection Not Cleaned Up After Use

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`

The `create_glue_connection` task creates a Glue JDBC connection with database credentials but never deletes it. Over time, stale connections accumulate. More importantly, if credentials rotate, the cached connection becomes invalid but is reused (the "already exists" check returns early).

**Recommendation:** Either delete the connection after each DAG run, or update the connection properties if it already exists (instead of reusing blindly).

#### M5: `GlueJobOperator` `iam_role_name` Expects Role Name, Not ARN

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`

```python
iam_role_name=factory.get_glue_role_name(),
```

The `GlueJobOperator`'s `iam_role_name` parameter expects a **role name** (e.g., `my-glue-role`), not an ARN. But `get_glue_role_name()` returns `Variable.get("GLUE_ROLE_ARN")` which is expected to be an ARN. The naming is confusing and may cause runtime failures depending on what value is stored.

**Recommendation:** Clarify whether the variable should contain a role name or ARN, and rename accordingly.

#### M6: No Glue VPC Endpoint Provisioning

**Files:** `lib/stacks/mwaa_primary_stack.py`, `lib/stacks/mwaa_secondary_stack.py`

The Glue jobs run in the MWAA VPC and need to access S3 and Glue APIs. If the VPC has no internet gateway (private subnets), the Glue jobs will fail without VPC endpoints for S3 and Glue. The stack creates a StepFunctions VPC endpoint but not Glue or S3 endpoints.

**Recommendation:** Add VPC endpoint provisioning for Glue and S3 (or document this as a prerequisite).

---

### LOW

#### L1: `_detect_date_field` Heuristic May Be Inaccurate

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py` (line ~1100)

The date field detection checks for common column names but may match incorrectly. For example, `task_instance` has both `start_date` and `queued_dttm` — the method returns the first match (`start_date`), which may not be the best choice for age-based filtering.

#### L2: `setup_glue_resources` Duplicated Between Primary and Secondary Stacks

**Files:** `lib/stacks/mwaa_primary_stack.py`, `lib/stacks/mwaa_secondary_stack.py`

The `setup_glue_resources` method is nearly identical in both stacks (~100 lines each). The only difference is the primary stack also deploys Glue scripts to S3.

**Recommendation:** Extract shared Glue resource provisioning into `MwaaBaseStack` or a CDK construct.

#### L3: `backup_metadata.py` Version Routing Uses `startswith("3.")` — Fragile

**Files:** `assets/dags/mwaa_dr/backup_metadata.py`, `restore_metadata.py`, `cleanup_metadata.py`

```python
elif airflow_version.startswith("3."):
    from mwaa_dr.v_3_0.dr_factory import DRFactory_3_0
```

This routes ALL 3.x versions to `DRFactory_3_0`. When Airflow 3.1 ships with schema changes, this will silently use the wrong factory. The 2.x versions use explicit version checks (e.g., `startswith("2.11")`).

**Recommendation:** Use `startswith("3.0")` for now, and add a fallback/warning for unknown 3.x versions.

#### L4: Missing `__init__.py` in `tests/unit/glue_scripts/`

**File:** `tests/unit/glue_scripts/__init__.py` contains only a comment. This is fine but inconsistent — other test `__init__.py` files are empty.

#### L5: `export_filter` on `slot_pool` Uses String Comparison

**File:** `assets/dags/mwaa_dr/v_3_0/dr_factory.py` (line ~90)

```python
export_filter="pool != 'default_pool'"
```

This filter is defined on the `BaseTable` but is not used by the Glue export path (Glue scripts build their own queries). The `export_filter` and `export_mappings` attributes on `BaseTable` are only used by the ORM-based 2.x path. For 3.x, the Glue scripts handle filtering differently.

**Recommendation:** Document that `export_filter`/`export_mappings` are not used in the Glue path, or wire them through.

---

### INFO

#### I1: Well-Structured Dependency Model

The `DRFactory_3_0.setup_tables()` method correctly models the Airflow 3.0 schema with proper dependency ordering. New tables (`dag_version`, `dag_code`, `asset`, `asset_event`, `backfill`, `backfill_dag_run`, `dag_run_note`, `task_instance_note`, `task_instance_history`) are correctly placed in the dependency graph. Removed tables (`serialized_dag`, `sla_miss`, `rendered_task_instance_fields`) are properly excluded.

#### I2: Good Backward Compatibility

All 2.x behavior is completely unchanged. The `startswith("3.")` guard in config, stacks, and DAG routing ensures zero impact on existing deployments. The 345 existing tests pass without modification.

#### I3: Comprehensive Test Coverage

115 new tests cover:
- `DRFactory_3_0` table setup and dependencies (19 tests)
- `GlueDRFactory` backup/restore/cleanup DAG creation (51 tests)
- `CredentialExtractor` all 3 strategies + edge cases (14 tests)
- `MwaaRestApiClient` all CRUD operations + retry logic (21 tests)
- Version routing for 3.x (4 tests)
- Glue script property tests (2 tests)
- CDK Glue resource tests (4 tests — 1 fails due to Docker)

#### I4: Binary Column Handling

The hex-encoding strategy for binary columns (`executor_config`, `conf`, `value`, `dag_run_conf`) is well-implemented. Export encodes as `\x` + hex, import decodes back. This preserves binary data through CSV serialization.

---

## Summary Table

| ID | Severity | Finding | Blocker? |
|----|----------|---------|----------|
| C1 | CRITICAL | SQL injection risk in Glue scripts | No (mitigated by hardcoded table names) |
| C2 | CRITICAL | DB credentials in plaintext in Glue connection | No (operational risk) |
| H1 | HIGH | Overly broad IAM permissions (`*`) | No |
| H2 | HIGH | REST API creates new session per call | No (performance) |
| H3 | HIGH | 90+ lines of duplicated code across 3 DAGs | No (quality) |
| H4 | HIGH | No pagination in REST API listing | Yes (>100 vars/conns) |
| M3 | MEDIUM | `GLUE_ROLE_ARN` Airflow variable never set by CDK | **Yes — runtime blocker** |
| M4 | MEDIUM | Glue connection never cleaned up | No |
| M5 | MEDIUM | Role name vs ARN confusion | Potential blocker |
| M6 | MEDIUM | No Glue/S3 VPC endpoints provisioned | Potential blocker (private VPCs) |

**Deployment Blockers:** M3 (`GLUE_ROLE_ARN` not set) must be resolved before the 3.x path can work at runtime. H4 (pagination) is a data loss risk for environments with >100 variables/connections.

---

## Deployment Test Findings (2026-04-28)

### DT1: CRITICAL — MWAA 3.x CLI Endpoint Redirect Breaks CDK Deployment

**File:** `lib/functions/airflow_cli_client.py`

**Discovered during deployment:** The MWAA 3.x webserver redirects `POST /aws_mwaa/cli` to `/aws_mwaa/cli/` (trailing slash) with a `307 Temporary Redirect`. The `http.client.HTTPSConnection` used in the Lambda function does not follow redirects, causing the CLI custom resource to fail with an empty response body, which `ast.literal_eval("")` cannot parse.

**Fix applied:** Changed the URL from `/aws_mwaa/cli` to `/aws_mwaa/cli/` (with trailing slash). This is backward-compatible with Airflow 2.x which accepts both forms.

**Impact:** Without this fix, `cdk deploy` fails on the primary stack with `ROLLBACK_COMPLETE` — the Airflow variables (`DR_BACKUP_BUCKET`, `DR_BACKUP_SCHEDULE`, `DR_SNS_TOPIC_ARN`) are never set, and the `backup_metadata` DAG is never unpaused.

### DT2: INFO — MWAA 3.0.2 Not Available, Using 3.0.6

The PR targets MWAA version `3.0.2`, but the actual available MWAA version is `3.0.6`. Added `3.0.6` to `SUPPORTED_MWAA_VERSIONS` in `config.py`. The code's `startswith("3.")` routing works correctly with 3.0.6.

### DT3: CRITICAL — `DummyOperator` Import Breaks DAG Parsing on Airflow 3.x

**File:** `assets/dags/mwaa_dr/framework/factory/base_dr_factory.py` (line 25)

`from airflow.operators.dummy import DummyOperator` fails on Airflow 3.x — the module was removed. Since `GlueDRFactory` extends `BaseDRFactory`, the entire import chain fails and **no DAGs load at all**.

**Fix applied:** Wrapped in try/except to fall back to `airflow.operators.empty.EmptyOperator`. Same for `PythonOperator` (`airflow.operators.python_operator` → `airflow.operators.python`).

### DT4: CONFIRMED — Finding M3 (GLUE_ROLE_ARN Not Set) Is a Runtime Blocker

The `GLUE_ROLE_ARN` Airflow variable is read at DAG parse time by `GlueDRFactory.__init__` → `get_glue_role_name()`. Without it, DAGs fail to parse with `AirflowRuntimeError: VARIABLE_NOT_FOUND: {'key': 'GLUE_ROLE_ARN'}`. Had to manually set the variable via CLI. The CDK stack must be updated to set this variable alongside `DR_BACKUP_BUCKET` etc.

---

## Fixes Required for PR Contribution

These are the concrete code changes needed before this PR can be merged. Each fix references the finding ID above.

### Fix 1: CLI Trailing Slash (DT1)
**File:** `lib/functions/airflow_cli_client.py`
**Change:** Line ~137, change `/aws_mwaa/cli` → `/aws_mwaa/cli/`
**Status:** ✅ Fixed locally, needs to be committed to PR branch.

### Fix 2: DummyOperator / PythonOperator Import (DT3)
**File:** `assets/dags/mwaa_dr/framework/factory/base_dr_factory.py`
**Change:** Lines 25-26, wrap imports in try/except:
```python
try:
    from airflow.operators.dummy import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator
try:
    from airflow.operators.python_operator import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator
```
**Status:** ✅ Fixed locally, needs to be committed to PR branch.

### Fix 3: Set GLUE_ROLE_ARN via CDK Airflow CLI (M3/DT4)
**File:** `lib/stacks/mwaa_primary_stack.py` → `setup_variables_airflow_cli()`
**Change:** Add a `variables set GLUE_ROLE_ARN {glue_role.role_name}` command to the create/update lists, and a corresponding delete command. The value should be the **role name** (not ARN) since `GlueJobOperator.iam_role_name` expects a name.
**Note:** Also need to set this on the secondary environment for restore/cleanup DAGs. Consider adding it to the secondary stack's execution role setup or via a secondary Airflow CLI construct.
**Status:** ❌ Not yet fixed.

### Fix 4: Add 3.0.6 to SUPPORTED_MWAA_VERSIONS (DT2)
**File:** `config.py`
**Change:** Add `"3.0.6"` to `SUPPORTED_MWAA_VERSIONS` list.
**Status:** ✅ Fixed locally, needs to be committed to PR branch.

### Fix 5: REST API Pagination (H4)
**File:** `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py`
**Change:** `list_variables()` and `list_connections()` need to paginate using `?offset=N&limit=100` until all records are retrieved. Current implementation only fetches the first page (default 100 items).
**Status:** ❌ Not yet fixed.

### Fix 6: REST API Authentication Broken on Airflow 3.x (NEW — DT5)
**File:** `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py`
**Severity:** CRITICAL — Complete blocker for variable/connection backup and restore.
**Problem:** The `MwaaRestApiClient` uses `CreateWebLoginToken` + cookie-based session auth (`session.get('/aws_mwaa/login', params={'token': ...})`). On Airflow 3.x (MWAA 3.0.6), the login endpoint returns a React SPA HTML page and does NOT set session cookies. All subsequent REST API calls return `401 Not authenticated`.
**Root cause:** Airflow 3.x switched from Flask-AppBuilder cookie auth to FastAPI JWT-based auth. The `/aws_mwaa/login` endpoint no longer establishes a server-side session.
**Correct approach:** Use the MWAA `InvokeRestApi` AWS API (`aws mwaa invoke-rest-api --name ENV --path "/variables" --method GET`). This uses AWS IAM credentials directly and works on both 2.x and 3.x. Alternatively, use `CreateWebServerAccessToken` (not `CreateWebLoginToken`) which returns a bearer token for the REST API.
**Impact:** Without this fix, `backup_variables_via_api()`, `backup_connections_via_api()`, `restore_variables_via_api()`, and `restore_connections_via_api()` in `GlueDRFactory` will all fail silently or with 401 errors. Variables and connections will NOT be backed up or restored during DR.
**Verified:** `aws mwaa invoke-rest-api` works correctly and returns all 10 variables and 3 connections.
**Fix approach:** Replace `MwaaRestApiClient` internals to use `boto3.client('mwaa').invoke_rest_api()` instead of `requests.Session` + cookie auth. This also eliminates the session-per-call performance issue (H2).
**Status:** ❌ Not yet fixed. This is the highest priority fix.

---

## End-to-End Test Findings (2026-04-28 afternoon)

### Test Environment
- Primary: mwaa-dr-primary (eu-west-1), MWAA 3.0.6, webserver `6fb44e2d-5ff4-46b9-be2b-18bb2bcde9f6.c10.airflow.eu-west-1.on.aws`
- Secondary: mwaa-dr-secondary (eu-west-2), MWAA 3.0.6, webserver `25fb32d4-f6c3-4015-bbbf-ff8f823e9359.c1.airflow.eu-west-2.on.aws`
- DR Type: WARM_STANDBY
- Test data: 3 sample DAGs (sample_etl_pipeline, sample_data_quality, sample_ml_training), 10 variables, 3 connections

### DT6: CRITICAL — `MWAA_ENV_NAME` Environment Variable Does Not Exist on MWAA Workers

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py` — multiple locations in `create_glue_connection()` and `get_mwaa_rest_api_client()`
**Problem:** The code uses `os.environ.get("MWAA_ENV_NAME", "")` to get the MWAA environment name. This variable does NOT exist on MWAA 3.0.6 workers. The result is an empty string, which causes:
1. `create_glue_connection`: Glue connection name becomes `"_conn"` (empty prefix), and `glue_client.create_connection()` fails with `ParamValidationError: Invalid length for parameter Name, value: 0`
2. `get_mwaa_rest_api_client`: REST API client gets empty env name, so all API calls fail

**Evidence:** `backup_metadata` DAG ran 5+ times (hourly schedule + manual trigger), ALL failed. Task `extract_credentials` succeeds, `create_glue_connection` fails every time.
**Error:** `ParamValidationError: Invalid length for parameter Name, value: 0, valid min length 1`

**Fix approach:** The MWAA environment name should be passed as an Airflow variable (e.g., `DR_MWAA_ENV_NAME`) set by the CDK stack, similar to `DR_BACKUP_BUCKET`. Alternatively, extract it from `DAGS_S3_PATH` or the MWAA metadata. The CDK stack should set this variable in `setup_variables_airflow_cli()`.

**Status:** ❌ Not yet fixed. Blocks ALL Glue-based operations (backup, restore, cleanup).

### DT7: CRITICAL — `AWS_REGION` Environment Variable May Not Exist on MWAA Workers

**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py` — `create_glue_connection()` and `get_mwaa_rest_api_client()`
**Problem:** The code uses `os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", ""))`. On MWAA workers, the standard env var is `AWS_DEFAULT_REGION`, not `AWS_REGION`. If neither is set, the region defaults to empty string, causing boto3 client creation to fail.
**Fix approach:** Use `boto3.session.Session().region_name` as a more reliable fallback, or pass region as an Airflow variable.
**Status:** ❌ Not yet fixed.

### DT8: INFO — `sample_etl_pipeline` DAG Stuck in Queued State

One of the three sample DAGs (`sample_etl_pipeline`) remained in `queued` state for 5+ hours while the other two completed successfully. This appears to be an MWAA scheduler issue, not related to the PR. The DAG uses `EmptyOperator` only, so there's no resource contention. May be related to MWAA 3.0.6 scheduler behavior with new environments.

### Backup Test Results Summary

| Component | Status | Notes |
|-----------|--------|-------|
| `extract_credentials` task | ✅ PASS | Successfully extracts DB creds from `DB_SECRETS` env var |
| `create_glue_connection` task | ❌ FAIL | `MWAA_ENV_NAME` not set → empty connection name (DT6) |
| `glue_export` task | ❌ SKIP | Upstream failed |
| `backup_variables_via_api` task | ❌ FAIL | REST API auth broken on AF 3.x (DT5) |
| `backup_connections_via_api` task | ❌ FAIL | REST API auth broken on AF 3.x (DT5) |
| S3 cross-region replication (DAGs) | ✅ PASS | Sample DAGs replicated to secondary bucket |
| S3 cross-region replication (backup) | ❌ N/A | No backup data generated due to failures |
| Secondary DAGs paused | ✅ PASS | DAGs on secondary remain paused as expected |

### Blockers Preventing Full DR Test

The following bugs must be fixed before the backup→restore→failover flow can be tested end-to-end:

1. **DT5** (REST API auth) — Variables and connections cannot be backed up or restored
2. **DT6** (MWAA_ENV_NAME) — Glue connection cannot be created, so no metadata export/import
3. **DT4/M3** (GLUE_ROLE_ARN) — Must be set via CDK, not manually

Once these are fixed, the test sequence would be:
1. Trigger `backup_metadata` on primary → verify CSV files in backup S3 bucket
2. Verify S3 CRR replicates backup to secondary region bucket
3. Set `MWAA_SIMULATE_DR=YES` and redeploy → triggers StepFunctions workflow
4. StepFunctions runs: health check fails → disable schedule → cleanup_metadata → restore_metadata
5. Verify secondary environment has: restored DAG runs, variables, connections, active DAGs

### Secondary Region Verification

Verified that the secondary environment is healthy and DAGs are deployed but paused:

| Check | Status | Details |
|-------|--------|---------|
| S3 DAG replication | ✅ PASS | All sample DAGs + mwaa_dr framework replicated to secondary bucket |
| Secondary DAGs paused | ✅ PASS | sample_etl_pipeline, sample_data_quality, sample_ml_training all paused |
| Secondary variables | ✅ Expected | 0 variables (no backup succeeded yet) |
| Secondary connections | ✅ Expected | 0 connections (no backup succeeded yet) |
| DR DAGs on secondary | ⚠️ Import errors | `GLUE_ROLE_ARN` variable missing on secondary too — had to set manually |
| Secondary MWAA health | ✅ PASS | Environment AVAILABLE, version 3.0.6 |

### DT9: MEDIUM — Secondary Environment Also Needs GLUE_ROLE_ARN Variable

The secondary MWAA environment also needs the `GLUE_ROLE_ARN` Airflow variable for the `restore_metadata` and `cleanup_metadata` DAGs to parse. The CDK secondary stack does NOT set any Airflow variables (it has no `AirflowCli` construct). This means:
- `restore_metadata` DAG won't load → StepFunctions restore workflow will fail
- `cleanup_metadata` DAG won't load → StepFunctions cleanup workflow will fail

**Fix approach:** Add an `AirflowCli` construct to the secondary stack that sets `GLUE_ROLE_ARN` (and potentially `DR_BACKUP_BUCKET` for the secondary backup bucket). Or, refactor `GlueDRFactory` to read `GLUE_ROLE_ARN` at runtime (inside the task) rather than at DAG parse time.

### Fix 7: Set GLUE_ROLE_ARN on Secondary Environment (DT9)
**File:** `lib/stacks/mwaa_secondary_stack.py`
**Change:** Add `AirflowCli` construct or equivalent to set `GLUE_ROLE_ARN` variable on the secondary MWAA environment.
**Status:** ❌ Not yet fixed. Manually set via CLI for testing.

### Fix 8: MWAA_ENV_NAME Not Available on Workers (DT6)
**File:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`
**Change:** Replace all `os.environ.get("MWAA_ENV_NAME", "")` with reading from an Airflow variable (e.g., `Variable.get("DR_MWAA_ENV_NAME")`). Add this variable to the CDK `AirflowCli` setup in both primary and secondary stacks.
**Status:** ❌ Not yet fixed. Blocks ALL Glue operations.

### Fix 9: REST API Client Must Use InvokeRestApi (DT5)
**File:** `assets/dags/mwaa_dr/framework/mwaa_rest_api_client.py`
**Change:** Replace the `CreateWebLoginToken` + cookie-based session approach with `boto3.client('mwaa').invoke_rest_api()`. This:
- Works on both Airflow 2.x and 3.x
- Uses IAM credentials (no web tokens needed)
- Eliminates session-per-call overhead (H2)
- Has built-in pagination support
Example: `client.invoke_rest_api(Name=env_name, Path='/variables', Method='GET')`
**Status:** ❌ Not yet fixed. Blocks variable/connection backup and restore.

---

## Overall Assessment

### What Works
- CDK stack deployment (after DT1/DT3 fixes)
- VPC networking, S3 buckets, cross-region replication for DAGs
- MWAA 3.0.6 environment creation and health
- Credential extraction from MWAA worker env vars (`DB_SECRETS`)
- DAG version routing (`startswith("3.")`) correctly selects `DRFactory_3_0`
- Airflow 3.0 table schema definition (20 tables with correct dependencies)
- StepFunctions health check workflow deployed and scheduled
- Sample DAGs run successfully on primary (2 of 3)

### What Doesn't Work (Blocks DR)
1. **Glue connection creation** — `MWAA_ENV_NAME` env var doesn't exist (DT6)
2. **REST API auth** — Cookie-based auth broken on Airflow 3.x (DT5)
3. **GLUE_ROLE_ARN** — Not set by CDK on either environment (DT4/DT9)

### Priority Fix Order
1. **Fix 8** (DT6) — MWAA_ENV_NAME → unblocks Glue connection creation
2. **Fix 9** (DT5) — REST API auth → unblocks variable/connection backup/restore
3. **Fix 3** (DT4) + **Fix 7** (DT9) — GLUE_ROLE_ARN on both envs → unblocks DAG parsing
4. **Fix 1** (DT1) — CLI trailing slash → already fixed locally
5. **Fix 2** (DT3) — DummyOperator import → already fixed locally
6. **Fix 4** (DT2) — Add 3.0.6 to supported versions → already fixed locally
7. **Fix 5** (H4) — REST API pagination → nice-to-have
8. **Fix 6** (DT5) — Superseded by Fix 9


---

## Additional Deployment Findings (2026-04-28 evening session)

### ~~DT10~~ RETRACTED — Glue VPC Connectivity Works Fine

**Original claim was wrong.** The Glue connection `mwaa-dr-primary_conn` works correctly — verified via Glue Visual Builder data preview. The actual issues were:

1. **`extract_jdbc_conf` returns `url` without database name** — the `url` key strips the `/AirflowMetadata` path. Fix: use `fullUrl` key instead.
2. **`encode(jsonb, unknown)` SQL error** — `dag_run.conf`, `xcom.value`, `backfill.dag_run_conf` are `jsonb` in AF 3.x, not `bytea`. The `encode(...,'hex')` function only works on `bytea`. Fix: use `::text` cast for jsonb columns.
3. **`column "dag_id" does not exist`** — `task_instance_note` schema differs from what `DRFactory_3_0` defines. Fix: use `SELECT *` instead of explicit column lists.
4. **`s3:DeleteObject` missing** — Spark's `write.mode("overwrite")` needs delete permission. Fix: add to Glue role.

All four issues fixed. Backup now exports 516 rows across 16 tables successfully.

### Additional Fixes Applied During This Session

| Fix | File | Change |
|-----|------|--------|
| Glue version | `glue_dr_factory.py` | Added `GlueVersion: 4.0`, `WorkerType: G.1X`, `NumberOfWorkers: 2` to all GlueJobOperator calls |
| Glue connections | `glue_dr_factory.py` | Added `Connections: {Connections: [conn_name]}` to all GlueJobOperator `create_job_kwargs` |
| Script location | `glue_dr_factory.py` | Fixed `get_script_location()` to derive bucket from MWAA environment's `SourceBucketArn` instead of non-existent `DAGS_S3_PATH` env var |
| Glue role perms | Both stacks | Added `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`, `glue:GetConnection` to Glue IAM role |
| MWAA role perms | Both stacks | Added `airflow:GetEnvironment`, `airflow:InvokeRestApi`, `iam:GetRole` to MWAA execution role |
| CLI response size | `airflow_cli_function.py` | Truncated response data to avoid CloudFormation 4096 byte limit |
| Diagnostic logging | `mwaa_metadb_export.py` | Added table listing diagnostic and error logging in `table_exists` |

### What Works After All Fixes

| Component | Status |
|-----------|--------|
| CDK deployment (both stacks) | ✅ |
| Airflow variable setup via CLI (GLUE_ROLE_ARN, DR_MWAA_ENV_NAME) | ✅ |
| DAG parsing on Airflow 3.x | ✅ |
| `extract_credentials` task | ✅ |
| `create_glue_connection` task | ✅ |
| `backup_variables_via_api` (via InvokeRestApi) | ✅ |
| `backup_connections_via_api` (via InvokeRestApi) | ✅ |
| Glue job creation and execution | ✅ |
| Glue job JDBC connectivity to MWAA metadata DB | ❌ BLOCKED (DT10) |
| S3 cross-region replication (DAGs) | ✅ |
| StepFunctions health check workflow | ✅ |

### Conclusion

The PR's approach of using AWS Glue for metadata operations works after the fixes applied in this session. The remaining issue is the StepFunctions task token callback from the DAG — the `notify_success_to_sfn` task needs to be adapted for Airflow 3.x's `dag_run.conf` access pattern and the `trigger_dag` CLI output format changes.

### DR Failover Test Results

| Step | Status | Notes |
|------|--------|-------|
| Health check detects UNHEALTHY | ✅ | `simulate_dr=YES` correctly forces failure |
| Disable EventBridge schedule | ✅ | Schedule disabled to prevent duplicate runs |
| Trigger cleanup_metadata DAG | ✅ | DAG triggered and Glue cleanup job runs |
| Glue cleanup job | ✅ | Successfully deletes metadata rows on secondary |
| StepFunctions task token callback | ❌ | DAG's `notify_success_to_sfn` task doesn't send callback → SFN times out |
| Trigger restore_metadata DAG | ❌ | Never reached (blocked by cleanup timeout) |

### Remaining Issues for Next Session

1. **SFN task token callback** — The `notify_success_to_sfn`/`notify_failure_to_sfn` tasks in `GlueDRFactory` use `dag_run.get_task_instances()` which doesn't exist in AF 3.x (`AttributeError: 'DagRun' object has no attribute 'get_task_instances'`). Need to use AF 3.x compatible API.
2. **CLI trigger_dag output format** — AF 3.x `dags trigger -o json` doesn't include `external_trigger` field. Fixed locally but needs the `expected_result` check to be more robust.
3. **CLI unpause_dag output format** — AF 3.x outputs table format, not `paused: False`. Fixed locally.
4. **Column schema mismatches** — `task_instance_note` and potentially other tables have different columns in AF 3.0.6 vs what `DRFactory_3_0` defines. Fixed by using `SELECT *` in export.
5. **jsonb vs bytea columns** — `conf`, `value`, `dag_run_conf` are `jsonb` in AF 3.x, not `bytea`. Fixed by using `::text` cast instead of `encode()`.

---

## Security Finding (2026-04-29)

### DT11: HIGH — Database Credentials Exposed via XCom and Logs

**Files:** `assets/dags/mwaa_dr/framework/factory/glue_dr_factory.py`, `assets/glue_scripts/mwaa_metadb_export.py`

**Problem:** The `extract_credentials` `@task` returned a dict containing the plaintext database password, JDBC URL, and username. Because TaskFlow `@task` functions store their return values in XCom, these credentials were:
1. Stored in the Airflow metadata database (XCom table) in plaintext
2. Visible in the Airflow UI under XCom entries
3. Logged by the TaskFlow executor when serializing task results

Additionally, `mwaa_metadb_export.py` logged the full JDBC URL and username via `logger.info("JDBC URL: '%s', user: '%s'", jdbc_url, ...)`.

**Fix applied:**
- Merged `extract_credentials` + `create_glue_connection` into a single `setup_glue_connection` `@task` that extracts credentials, creates/updates the Glue connection, and returns ONLY the connection name string. Credentials never leave the task boundary and never enter XCom.
- Removed JDBC URL/username logging from the export Glue script.
- Added `_get_vpc_requirements()` helper to eliminate code duplication.
- Glue connection is now updated on reuse (handles credential rotation instead of silently reusing stale credentials).

**Status:** ✅ Fixed and committed.

---

## Latest Fix Summary (2026-04-29 evening)

### All Fixes Applied to `pr-52-fixes` Branch (3 commits)

| Fix | Category | Description |
|-----|----------|-------------|
| DT1 | CLI compat | Trailing slash on `/aws_mwaa/cli/` |
| DT2 | Version | Added 3.0.6 to supported versions |
| DT3 | Import compat | DummyOperator/PythonOperator try/except fallback |
| DT4/DT9 | CDK | GLUE_ROLE_ARN + DR_MWAA_ENV_NAME set via AirflowCli on both stacks |
| DT5 | REST API | Rewrote MwaaRestApiClient to use InvokeRestApi with pagination |
| DT6 | Env var | MWAA_ENV_NAME fallback to Airflow variable |
| DT11 | **Security** | Credentials no longer exposed via XCom or logs |
| Glue version | Glue config | GlueVersion 4.0, WorkerType G.1X, Connections attached |
| Script location | Glue config | Derive bucket from MWAA environment SourceBucketArn |
| fullUrl | Glue JDBC | Use `fullUrl` from `extract_jdbc_conf` (includes database name) |
| jsonb columns | Schema | `::text` cast for conf/value/dag_run_conf (not `encode()`) |
| SELECT * | Schema | Export uses `SELECT *` for schema resilience |
| CSV headers | Import/Export | Export writes headers, import reads headers + casts to target schema |
| stringtype | Import | `stringtype=unspecified` for PostgreSQL UUID column compatibility |
| s3:DeleteObject | IAM | Added to Glue role for Spark overwrite mode |
| ec2:Describe* | IAM | Added DescribeSubnets/SecurityGroups to Glue role |
| glue:GetConnection | IAM | Added to Glue role for extract_jdbc_conf |
| iam:GetRole | IAM | Added to MWAA execution role for GlueJobOperator |
| airflow:* prefix | IAM | Use `airflow:` prefix (not `mwaa:`) for MWAA IAM actions |
| InvokeRestApi | IAM + Lambda | Added permission + role ARN resource for DAG triggering |
| CLI response | CDK | Truncated response to avoid CloudFormation 4096 byte limit |
| unpause_dag | CLI compat | Handle AF 3.x table output + "No paused DAGs" message |
| trigger_dag | CLI compat | AF 3.x: use `--logical-date`, explicit run_id |
| DAG trigger | Lambda | AF 3.x: use InvokeRestApi instead of CLI (runs persist properly) |
| get_task_instances | AF 3.x compat | Removed from GlueDRFactory callbacks, try/except in BaseDRFactory |
| import java.sql | Dead code | Removed invalid Python import in Glue import fallback |
| Cleanup protection | Metadata | dag_run/task_instance protected for DR DAGs during cleanup |
| Cleanup skip | Metadata | dag_version/dag_code/active_dag skipped during cleanup |
| Import pre-cleanup | Metadata | dag_version/dag_code deleted before import to avoid FK conflicts |
| DR_BACKUP_BUCKET | CDK | Set on secondary environment via AirflowCli |

### Current DR Flow Status

| Step | Status |
|------|--------|
| Backup (all 5 tasks) | ✅ Fully working |
| S3 cross-region replication | ✅ Working |
| Health check → UNHEALTHY detection | ✅ Working |
| Disable EventBridge schedule | ✅ Working |
| Trigger cleanup_metadata DAG | ✅ Working (via InvokeRestApi) |
| Glue cleanup job | ✅ Succeeds |
| Cleanup SFN task token callback | ✅ Working (after PROTECTED_TABLES fix) |
| Cool-off wait | ✅ Working |
| Trigger restore_metadata DAG | ✅ Working (via InvokeRestApi) |
| Glue restore job | 🔄 Testing (latest fixes: CSV headers, schema casting, UUID compat, pre-import cleanup) |
| Restore SFN task token callback | 🔄 Pending (depends on restore job success) |
| Variables/connections restore via REST API | ✅ Working |
| End-to-end DR flow SUCCEEDED | 🔄 Pending final validation |

---

## TODO — Resume Point (2026-04-30)

### Overnight DR Test Running

A DR simulation test was kicked off at ~21:05 UTC on 2026-04-29. The EventBridge schedule is ENABLED with `MWAA_SIMULATE_DR=YES`. The SFN workflow should have triggered within 5 minutes and run the full flow: health check → disable schedule → cleanup → cool off (30s) → restore → success callback.

**Latest fix applied:** Pre-import cleanup now deletes `dag_run` (non-DR) before `dag_version` to prevent FK constraint violations caused by the scheduler recreating `dag_version` records during the cool-off period.

### Check Results

```bash
# 1. Check the SFN execution result
SM_ARN="arn:aws:states:eu-west-2:515232103838:stateMachine:mwaa306statemachineDB584156-ILSeX4nw8QjO"
aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --region eu-west-2 --max-results 3 --query "executions[*].{Status:status,Start:startDate,Stop:stopDate}"

# 2. If SUCCEEDED — verify restored data on secondary:
aws mwaa invoke-rest-api --name mwaa-dr-secondary --region eu-west-2 --path "/variables" --method GET
aws mwaa invoke-rest-api --name mwaa-dr-secondary --region eu-west-2 --path "/connections" --method GET
aws mwaa invoke-rest-api --name mwaa-dr-secondary --region eu-west-2 --path "/dags" --method GET

# 3. If FAILED — check what step failed:
EXEC_ARN=$(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --region eu-west-2 --max-results 1 --query "executions[0].executionArn" --output text)
aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --region eu-west-2 --query "{Status:status,Error:error,Cause:cause}"

# 4. Check Glue jobs:
aws glue get-job-runs --job-name cleanup_metadata_cleanup --region eu-west-2 --query "JobRuns[:1].{State:JobRunState}"
aws glue get-job-runs --job-name restore_metadata_import --region eu-west-2 --query "JobRuns[:1].{State:JobRunState,Error:ErrorMessage}"

# 5. Check restore DAG run task instances:
aws mwaa invoke-rest-api --name mwaa-dr-secondary --region eu-west-2 --path "/dags/restore_metadata/dagRuns?order_by=-start_date&limit=1" --method GET
```

### If DR Test SUCCEEDED — Next Steps

1. Verify secondary has the primary's variables (DB_HOST, API_KEY, ENVIRONMENT, etc.)
2. Verify secondary has the primary's connections (test_postgres, test_http_api, test_s3)
3. Verify secondary has restored DAG runs from primary
4. Update findings.md with final "End-to-end DR flow SUCCEEDED" status
5. Commit final state to `pr-52-fixes` branch
6. Disable `MWAA_SIMULATE_DR` in `.env` and redeploy to stop DR simulation
7. Consider cleaning up AWS resources (MWAA environments, VPCs, NAT gateways cost money)

### If DR Test FAILED — Debug Steps

1. Check which step failed (cleanup callback? restore Glue job? restore callback?)
2. The most likely remaining issue is FK constraint on `dag_run` → `dag_version` if the scheduler recreated records between pre-import cleanup and the actual import
3. If that's the case, the fix is to run the pre-import cleanup and import within a single transaction, or disable the scheduler temporarily during restore

### Branch State

- **Branch:** `pr-52-fixes` (4 commits ahead of `pr-52`)
- **All code changes are committed** — nothing uncommitted
- **`.env` has `MWAA_SIMULATE_DR=YES`** — remember to set back to `NO` after testing
- **EventBridge schedule:** was ENABLED at test start, the DR flow disables it as part of the workflow

### AWS Resources Running (cost reminder)

- 2x MWAA 3.0.6 environments (mw1.small) — eu-west-1 + eu-west-2
- 2x NAT Gateways — eu-west-1 + eu-west-2
- 2x VPCs with subnets
- S3 buckets (DAGs + backup in each region)
- Glue jobs (pay per run, not idle)

---

## Important Discoveries for Final Code Review (2026-04-30)

### Airflow 3.x Behavioral Changes Discovered During Testing

These are undocumented or poorly documented AF 3.x behaviors we hit during testing that affect the DR solution design:

1. **CLI `dags trigger` creates ephemeral runs** — DAG runs triggered via CLI without `--logical-date` get `logical_date=null` and are not properly tracked by the AF 3.x scheduler. Downstream `@task` functions never execute. Fix: use `InvokeRestApi` for triggering (or CLI with explicit `--logical-date`).

2. **`dags unpause` output format changed** — AF 2.x outputs `paused: False`, AF 3.x outputs a table `dag_id | is_paused\n... | False`. Also returns `No paused DAGs were found` if already unpaused. The `airflow_cli_client.py` unpause check needs all three patterns.

3. **`dags trigger -o json` output changed** — AF 2.x includes `"external_trigger": "True"`, AF 3.x doesn't have this field. Uses `"run_type": "manual"` instead.

4. **`/aws_mwaa/cli` endpoint redirects** — MWAA 3.x returns 307 redirect from `/aws_mwaa/cli` to `/aws_mwaa/cli/` (trailing slash). `http.client.HTTPSConnection` doesn't follow redirects.

5. **`CreateWebLoginToken` + cookie auth broken** — AF 3.x switched from Flask-AppBuilder to FastAPI. The login endpoint returns a React SPA, no session cookies. Must use `InvokeRestApi` AWS API instead.

6. **`DagRun.get_task_instances()` removed** — AF 3.x `DagRun` in task context is a protocol object, not the ORM model. No `get_task_instances()` method.

7. **`MWAA_ENV_NAME` env var doesn't exist** — Not a standard MWAA worker environment variable. Must use Airflow variable instead.

8. **`DAGS_S3_PATH` env var doesn't exist** — Not available on MWAA 3.x workers. Must derive bucket from `mwaa.get_environment()`.

9. **Cleanup deleting `dag_version`/`dag_code` kills running DAGs** — The scheduler loses track of DAGs mid-execution. These tables must be preserved during cleanup and only cleaned during the import step.

10. **Cleanup deleting `dag_run` kills the cleanup DAG itself** — The cleanup DAG's own run gets deleted, preventing downstream callback tasks from executing. Must protect DR DAG runs.

### Schema Differences: AF 3.x vs What PR #52 Defines

The `DRFactory_3_0` hardcodes column lists that don't match the actual MWAA 3.0.6 schema:
- `task_instance_note`: has different columns than defined (missing `dag_id`)
- `dag_run`: has extra columns (`context_carrier`, `span_status`, `created_dag_version_id`, `bundle_version`, `scheduled_by_job_id`) not in the factory definition
- `xcom.value` is `jsonb` not `bytea` — `encode()` fails, needs `::text` cast
- `dag_run.conf` is `jsonb` not `bytea` — same issue
- `backfill.dag_run_conf` is `jsonb` not `bytea` — same issue

**Resolution:** Export uses `SELECT *` (ignores column definitions), import reads CSV with headers and casts to target table schema. Column definitions in `DRFactory_3_0` are now only used for dependency ordering, not for actual SQL.

### Glue Job Configuration Gaps in Original PR

The original PR's `GlueJobOperator` calls were missing:
- `GlueVersion` — defaulted to Python 2 which is unsupported
- `WorkerType` and `NumberOfWorkers` — required for Glue 4.0
- `Connections` in `create_job_kwargs` — without this, the Glue job doesn't use the VPC connection and can't reach the database
- `extract_jdbc_conf` returns `url` (without database name) not `fullUrl` (with database name)

### IAM Permission Gaps in Original PR

The original PR was missing these permissions:
- `airflow:GetEnvironment` (note: IAM prefix is `airflow:` not `mwaa:`)
- `airflow:InvokeRestApi` + role ARN resource
- `iam:GetRole` on MWAA execution role (needed by GlueJobOperator)
- `glue:GetConnection` on Glue role (needed by `extract_jdbc_conf`)
- `glue:UpdateConnection` on MWAA execution role
- `ec2:DescribeSubnets` and `ec2:DescribeSecurityGroups` on Glue role
- `s3:DeleteObject` on Glue role (needed by Spark overwrite mode)

### Code Quality Notes for PR Review

1. **`glue_dr_factory.py` is 1000+ lines** — the 3 DAG creation methods have significant duplication. The `setup_glue_connection` task is now defined once per DAG method but the code is identical. Consider extracting to a shared function.

2. **`_get_vpc_requirements` makes 2 API calls** (GetEnvironment + DescribeSubnets) every time `setup_glue_connection` runs. Could cache or pass as parameters.

3. **`airflow_cli_client.py` version checks** use `int(sem_ver[0]) >= 3` in multiple places. Should be a property or method.

4. **`airflow_dag_trigger_function.py`** has two code paths (CLI for 2.x, InvokeRestApi for 3.x). The 2.x path is untested with these changes — need to verify backward compatibility.

5. **Unit tests need updating** — the 115 tests from the PR test the original code. Many will fail with our changes (merged tasks, removed `extract_credentials`, new `setup_glue_connection`, rewritten `MwaaRestApiClient`, etc.).

6. **`mwaa_metadb_cleanup.py` PROTECTED_TABLES** hardcodes DR DAG names (`cleanup_metadata`, `restore_metadata`, `backup_metadata`). These should come from configuration, not be hardcoded.

7. **`mwaa_metadb_import.py` `_pre_import_cleanup`** uses direct JDBC (not Spark) which is a different pattern from the rest of the script. Works but inconsistent.

8. **No integration test** for the full DR flow. The unit tests mock everything. Consider adding a test that runs against the MWAA local runner.
