---
inclusion: auto
---

# MWAA Disaster Recovery — Project Overview

This is an AWS CDK (Python) project that provides automated disaster recovery for Amazon Managed Workflows for Apache Airflow (MWAA). It supports two DR strategies: **Backup & Restore** and **Warm Standby**, deploying multi-region infrastructure via two CDK stacks (primary and secondary).

## Key Facts

- Language: Python 3.7+
- IaC: AWS CDK v2 (Python)
- Formatter: Black
- Linter: pre-commit (autoflake, pyupgrade, black)
- Test framework: pytest + coverage + moto (for AWS mocking)
- Published as PyPI package `mwaa_dr` (version in `VERSION` file)
- Supported MWAA versions: 2.4.3, 2.5.1, 2.6.3, 2.7.2, 2.8.1, 2.9.2, 2.10.1, 2.10.3, 2.11.0

## Repository Layout

```
app.py                  — CDK app entrypoint
config.py               — Config class reading env vars / .env file
lib/stacks/             — CDK stacks (primary, secondary, base)
lib/dr_constructs/      — Custom CDK constructs (AirflowCli)
lib/functions/           — Lambda function handlers
assets/dags/mwaa_dr/    — Airflow DAG framework (backup, restore, cleanup)
  framework/factory/    — DR factory pattern (base + version-specific)
  framework/model/      — Table models (base_table, connection, variable, active_dag)
  v_2_4/ .. v_2_11/     — Version-specific DR factory implementations
tests/unit/             — Unit tests mirroring source structure
tests/integration/      — Integration tests with MWAA local runner
```

## Configuration

All stack parameters are environment variables loaded via `python-dotenv` from a `.env` file at the project root. See `config.py` for the full list of `REQUIRED_CONFIGS` and `DEFAULT_CONFIGS`. Key variables include `DR_TYPE`, `MWAA_VERSION`, `PRIMARY_REGION`, `SECONDARY_REGION`, and VPC/subnet/SG IDs for both regions.

## Architecture

- **Primary Stack** (`MwaaPrimaryStack`): Creates backup S3 bucket, sets up cross-region replication for DAGs and backup buckets, deploys the `mwaa_dr` DAG framework, configures Airflow variables via CLI custom resource, and runs a one-time S3 batch replication job.
- **Secondary Stack** (`MwaaSecondaryStack`): Creates backup S3 bucket, deploys a StepFunctions state machine for health-check and recovery, with an EventBridge schedule. For Backup & Restore, it also creates a new MWAA environment on failover. For Warm Standby, it restores metadata into the existing standby environment.
