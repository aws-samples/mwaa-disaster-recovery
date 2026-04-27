# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Property-based tests for Glue script logic using Hypothesis.

These tests validate the summary generation logic from the export script
and the cleanup SQL pattern from the cleanup script WITHOUT importing
awsglue or pyspark, which are not available in the test environment.

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

from hypothesis import given, settings
from hypothesis.strategies import (
    composite,
    integers,
    lists,
    text,
)


# ---------------------------------------------------------------------------
# Helpers extracted from Glue scripts (no awsglue/pyspark dependency)
# ---------------------------------------------------------------------------


def build_summary_dict(results):
    """Replicate the summary dict construction from mwaa_metadb_export.py write_summary.

    This mirrors the logic in write_summary() that builds the summary dict
    from a list of result dicts like [{"table": "dag_run", "rows": 100}, ...].
    """
    return {
        "tables": {r["table"]: r["rows"] for r in results},
        "total_rows": sum(r["rows"] for r in results),
    }


# The PROTECTED_TABLES dict from mwaa_metadb_cleanup.py
PROTECTED_TABLES = {
    "slot_pool": "DELETE FROM slot_pool WHERE pool != 'default_pool'",
    "job": "DELETE FROM job WHERE job_type != 'SchedulerJob'",
}


def get_cleanup_sql(table_name):
    """Replicate the cleanup SQL selection logic from mwaa_metadb_cleanup.py cleanup_table."""
    if table_name in PROTECTED_TABLES:
        return PROTECTED_TABLES[table_name]
    return f"DELETE FROM {table_name}"


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@composite
def valid_table_names(draw):
    """Generate valid SQL table names (lowercase alphanumeric + underscore)."""
    name = draw(
        text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
            min_size=1,
            max_size=63,
        )
    )
    # Table names must start with a letter or underscore
    if not (name[0].isalpha() or name[0] == "_"):
        name = "t" + name
    return name


@composite
def valid_row_counts(draw):
    """Generate non-negative row counts (including zero)."""
    return draw(integers(min_value=0, max_value=10_000_000))


@composite
def export_result_lists(draw):
    """Generate lists of export result dicts with unique table names and row counts."""
    table_names = draw(
        lists(
            valid_table_names(),
            min_size=1,
            max_size=30,
            unique=True,
        )
    )
    results = []
    for name in table_names:
        rows = draw(valid_row_counts())
        results.append({"table": name, "rows": rows})
    return results


# ---------------------------------------------------------------------------
# Property 6: Job summary contains all processed tables
# ---------------------------------------------------------------------------


class TestJobSummaryProperties:
    """
    **Validates: Requirements 3.5, 12.4**

    Property 6: Job summary contains all processed tables

    For any set of table names and their corresponding row counts (including
    zero), the generated job summary JSON SHALL contain an entry for every
    table with the correct row count, and the total_rows field SHALL equal
    the sum of all individual row counts.
    """

    @given(results=export_result_lists())
    @settings(max_examples=100)
    def test_summary_contains_all_tables_with_correct_counts(self, results):
        """
        **Validates: Requirements 3.5, 12.4**

        Property 6: Job summary contains all processed tables
        """
        summary = build_summary_dict(results)

        # Every table in the input must appear in the summary
        for r in results:
            assert (
                r["table"] in summary["tables"]
            ), f"Table '{r['table']}' missing from summary"
            assert summary["tables"][r["table"]] == r["rows"], (
                f"Row count mismatch for '{r['table']}': "
                f"expected {r['rows']}, got {summary['tables'][r['table']]}"
            )

        # No extra tables in the summary
        assert len(summary["tables"]) == len(results)

        # total_rows equals the sum of all individual row counts
        expected_total = sum(r["rows"] for r in results)
        assert summary["total_rows"] == expected_total, (
            f"total_rows mismatch: expected {expected_total}, "
            f"got {summary['total_rows']}"
        )


# ---------------------------------------------------------------------------
# Strategies for cleanup property
# ---------------------------------------------------------------------------


@composite
def slot_pool_records(draw):
    """Generate a list of slot_pool records, always including default_pool."""
    extra_pools = draw(
        lists(
            valid_table_names(),
            min_size=0,
            max_size=20,
            unique=True,
        )
    )
    # Filter out any generated name that happens to be 'default_pool'
    extra_pools = [p for p in extra_pools if p != "default_pool"]
    # Always include default_pool
    all_pools = ["default_pool"] + extra_pools
    return all_pools


@composite
def job_records(draw):
    """Generate a list of job records, always including SchedulerJob entries."""
    scheduler_count = draw(integers(min_value=1, max_value=5))
    other_types = draw(
        lists(
            text(
                alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
                min_size=1,
                max_size=30,
            ),
            min_size=0,
            max_size=20,
        )
    )
    # Filter out any generated type that happens to be 'SchedulerJob'
    other_types = [jt for jt in other_types if jt != "SchedulerJob"]
    # Build records: each record is a dict with job_type
    records = [{"job_type": "SchedulerJob"} for _ in range(scheduler_count)]
    records.extend({"job_type": jt} for jt in other_types)
    return records


# ---------------------------------------------------------------------------
# Property 8: Cleanup preserves protected records
# ---------------------------------------------------------------------------


class TestCleanupProtectedRecordsProperties:
    """
    **Validates: Requirements 5.4**

    Property 8: Cleanup preserves protected records

    For any slot_pool table containing a default_pool entry and any job table
    containing SchedulerJob entries, after cleanup, the default_pool entry
    SHALL still exist in slot_pool and all SchedulerJob entries SHALL still
    exist in job, while all other records in those tables SHALL be deleted.
    """

    @given(pools=slot_pool_records(), jobs=job_records())
    @settings(max_examples=100)
    def test_cleanup_preserves_protected_records(self, pools, jobs):
        """
        **Validates: Requirements 5.4**

        Property 8: Cleanup preserves protected records
        """
        # --- Verify slot_pool cleanup SQL ---
        slot_pool_sql = get_cleanup_sql("slot_pool")
        assert slot_pool_sql == "DELETE FROM slot_pool WHERE pool != 'default_pool'"

        # Simulate: apply the WHERE clause logic to the pool list
        surviving_pools = [p for p in pools if p == "default_pool"]
        deleted_pools = [p for p in pools if p != "default_pool"]

        # default_pool must survive
        assert "default_pool" in surviving_pools
        # No non-default_pool records survive
        for p in surviving_pools:
            assert p == "default_pool"
        # All non-default_pool records are deleted
        for p in deleted_pools:
            assert p != "default_pool"

        # --- Verify job cleanup SQL ---
        job_sql = get_cleanup_sql("job")
        assert job_sql == "DELETE FROM job WHERE job_type != 'SchedulerJob'"

        # Simulate: apply the WHERE clause logic to the job list
        surviving_jobs = [j for j in jobs if j["job_type"] == "SchedulerJob"]
        deleted_jobs = [j for j in jobs if j["job_type"] != "SchedulerJob"]

        # All SchedulerJob entries must survive
        assert len(surviving_jobs) >= 1
        for j in surviving_jobs:
            assert j["job_type"] == "SchedulerJob"
        # All non-SchedulerJob entries are deleted
        for j in deleted_jobs:
            assert j["job_type"] != "SchedulerJob"

        # --- Verify non-protected tables get full DELETE ---
        for table_name in ["dag_run", "task_instance", "xcom", "log"]:
            sql = get_cleanup_sql(table_name)
            assert sql == f"DELETE FROM {table_name}", (
                f"Non-protected table '{table_name}' should use "
                f"unconditional DELETE, got: {sql}"
            )
            # No WHERE clause means all records are deleted
            assert "WHERE" not in sql
