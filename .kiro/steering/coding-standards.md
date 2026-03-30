---
inclusion: auto
---

# Coding Standards

## Python Style

- Format all Python code with **Black** (line length default 88).
- Use `autoflake` to remove unused imports/variables.
- Run `pre-commit run --all-files` before committing.
- Follow PEP 8 naming: `snake_case` for functions/variables, `PascalCase` for classes.
- Type hints are encouraged but not enforced everywhere (the codebase uses them selectively).

## CDK Conventions

- All CDK construct IDs use `conf.get_name("descriptive-id")` which prefixes with `STACK_NAME_PREFIX`.
- Lambda functions use Python 3.11 runtime with bundled dependencies from `lib/functions/requirements.txt`.
- S3 buckets use `S3_MANAGED` encryption, `BLOCK_ALL` public access, versioning enabled, and `DESTROY` removal policy.
- IAM policies follow least-privilege; use specific resource ARNs where possible.
- Stack dependencies are explicit: `primary_stack.add_dependency(secondary_stack)`.

## Airflow DAG Framework

- The `mwaa_dr` package under `assets/dags/` is the Airflow-side framework.
- Each MWAA version has its own `v_2_X/dr_factory.py` extending `BaseDRFactory`.
- Table models extend `BaseTable` and use a `DependencyModel` for restore ordering.
- The `>>` and `<<` operators on `BaseTable` define restore dependencies (not Airflow task dependencies directly).
- Backup uses streaming CSV via `smart_open` to S3; restore uses PostgreSQL `COPY` command.
- DAG names: `backup_metadata`, `restore_metadata`, `cleanup_metadata`.

## Lambda Functions

- All Lambda handlers live in `lib/functions/` and follow the pattern `def handler(event, context)`.
- The `airflow_cli_function.py` is a CloudFormation custom resource handler (`on_event` → `on_create`/`on_update`/`on_delete`).
- Lambda code is bundled with `pip install -r requirements.txt` via CDK `BundlingOptions`.

## Testing

- Unit tests live in `tests/unit/` mirroring the source structure.
- Use `pytest` with `pytest-mock` and `moto` for AWS service mocking.
- Run tests: `./build.sh unit` or `coverage run -m pytest tests/unit && coverage report -m`.
- Test paths are configured in `pyproject.toml` under `[tool.pytest.ini_options]`.
- Coverage config is in `pyproject.toml` under `[tool.coverage.run]`.

## Build & Deploy

- Build/test: `./build.sh unit`
- Lint: `./build.sh lint`
- Clean: `./build.sh clean`
- Deploy: `cdk deploy --all` (requires `.env` configured)
- CDK entrypoint: `app.py` (configured in `cdk.json` as `python3 app.py`)
