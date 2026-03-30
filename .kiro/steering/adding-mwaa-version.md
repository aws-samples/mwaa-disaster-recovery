---
inclusion: manual
---

# Adding Support for a New MWAA Version

When Amazon releases a new MWAA version, follow these steps to add support:

## 1. Update Supported Versions

In `config.py`, add the new version string to the `SUPPORTED_MWAA_VERSIONS` list.

## 2. Create Version-Specific DR Factory

Create a new directory `assets/dags/mwaa_dr/v_2_XX/` with:
- `__init__.py`
- `dr_factory.py` — Extend `BaseDRFactory` (or copy from the closest existing version).

The factory's `setup_tables()` method defines which metadata tables to back up and their restore dependencies. Check the Airflow changelog for any schema changes (new tables, renamed columns, removed tables) between versions.

## 3. Update the Default DAG Factory

In `assets/dags/mwaa_dr/framework/factory/default_dag_factory.py`, add the new version mapping so the framework selects the correct factory at runtime.

## 4. Update Build Script

In `build.sh`, add the new version to the `versions` array and define its semver variable.

## 5. Add Unit Tests

Create `tests/unit/mwaa_dr/v_2_XX/` with tests for the new factory, verifying table setup and dependency ordering.

## 6. Add Integration Test Data

If needed, create `tests/integration/data/v_2_XX/` with sample backup data for integration testing.

## 7. Update Documentation

- Update the MWAA version badges in `README.md`.
- Update `CHANGELOG.md` with the new version support.
- Bump the version in `VERSION` file.
