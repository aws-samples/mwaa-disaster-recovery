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

import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Tables with protected rows that must be preserved during cleanup
PROTECTED_TABLES = {
    "slot_pool": "DELETE FROM slot_pool WHERE pool != 'default_pool'",
    "job": "DELETE FROM job WHERE job_type != 'SchedulerJob'",
}


def get_jdbc_connection(glue_context, connection_name):
    """Extract JDBC connection details from a Glue connection.

    Args:
        glue_context: The GlueContext instance.
        connection_name: Name of the Glue connection.

    Returns:
        tuple: (jdbc_url, user, password) for direct JDBC operations.
    """
    conn = glue_context.extract_jdbc_conf(connection_name)
    return conn["url"], conn["user"], conn["password"]


def get_direct_connection(spark, jdbc_url, user, password):
    """Create a direct JDBC connection via the Spark JVM gateway.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        user: Database username.
        password: Database password.

    Returns:
        A java.sql.Connection object.
    """
    sc = spark.sparkContext
    gateway = sc._gateway
    driver_class = gateway.jvm.Class.forName("org.postgresql.Driver")  # noqa: F841
    connection = gateway.jvm.java.sql.DriverManager.getConnection(
        jdbc_url, user, password
    )
    return connection


def table_exists(connection, table_name):
    """Check whether a table exists in the database.

    Args:
        connection: A java.sql.Connection object.
        table_name: Name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    stmt = None
    rs = None
    try:
        query = (
            "SELECT 1 FROM information_schema.tables "
            f"WHERE table_name = '{table_name}' LIMIT 1"
        )
        stmt = connection.createStatement()
        rs = stmt.executeQuery(query)
        return rs.next()
    except Exception:
        return False
    finally:
        if rs:
            rs.close()
        if stmt:
            stmt.close()


def cleanup_table(connection, table_name):
    """Delete records from a single metadata table.

    For tables with protected rows (``slot_pool`` and ``job``), uses a
    targeted DELETE with a WHERE clause to preserve protected records.
    For all other tables, deletes all rows.

    Args:
        connection: A java.sql.Connection object.
        table_name: Name of the table to clean up.

    Returns:
        dict: Result with table name and rows deleted, or None if skipped.
    """
    # Skip variable and connection tables (handled via REST API)
    if table_name in ("variable", "connection"):
        logger.info("Skipping table '%s' (handled via REST API).", table_name)
        return None

    if not table_exists(connection, table_name):
        logger.warning("Table '%s' does not exist in database, skipping.", table_name)
        return None

    logger.info("Cleaning up table '%s'...", table_name)

    # Use protected DELETE for tables with rows that must be preserved
    if table_name in PROTECTED_TABLES:
        delete_sql = PROTECTED_TABLES[table_name]
    else:
        delete_sql = f"DELETE FROM {table_name}"

    stmt = None
    try:
        stmt = connection.createStatement()
        rows_deleted = stmt.executeUpdate(delete_sql)
        connection.commit()
        logger.info("Deleted %d rows from table '%s'.", rows_deleted, table_name)
        return {"table": table_name, "rows_deleted": rows_deleted}
    except Exception as e:
        logger.error("Failed to clean up table '%s': %s", table_name, e)
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        if stmt:
            stmt.close()


def cleanup_tables_by_level(
    spark, jdbc_url, user, password, table_defs, dependency_order
):
    """Delete records from tables in reverse dependency order.

    Tables at the same dependency level have no relationships between them and
    can be cleaned up concurrently. Levels are processed in descending order
    (child tables before parent tables) to avoid foreign key constraint
    violations.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        user: Database username.
        password: Database password.
        table_defs: List of table definition dicts.
        dependency_order: List of lists of table names (topological levels).

    Returns:
        list[dict]: List of cleanup results with table names and rows deleted.
    """
    table_names_in_defs = {td["table"] for td in table_defs}
    results = []

    # Process levels in reverse order (highest level first = child tables first)
    for level_tables in reversed(dependency_order):
        level_results = []

        # Tables at the same level can run in parallel
        with ThreadPoolExecutor(max_workers=min(len(level_tables), 4)) as executor:
            futures = {}
            for table_name in level_tables:
                if table_name not in table_names_in_defs:
                    continue

                def _cleanup(tbl_name):
                    conn = get_direct_connection(spark, jdbc_url, user, password)
                    try:
                        conn.setAutoCommit(False)
                        return cleanup_table(conn, tbl_name)
                    finally:
                        conn.close()

                future = executor.submit(_cleanup, table_name)
                futures[future] = table_name

            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        level_results.append(result)
                except Exception as e:
                    logger.error("Failed to clean up table '%s': %s", table_name, e)
                    raise

        results.extend(level_results)

    return results


def main():
    """Entry point for the Glue cleanup job."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "GLUE_CONNECTION_NAME",
            "CLEANUP_TABLES",
            "TABLE_DEPENDENCY_ORDER",
        ],
    )

    connection_name = args["GLUE_CONNECTION_NAME"]
    table_defs = json.loads(args["CLEANUP_TABLES"])
    dependency_order = json.loads(args["TABLE_DEPENDENCY_ORDER"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    logger.info("Starting metadata cleanup job '%s'.", args["JOB_NAME"])
    logger.info("Connection: %s", connection_name)
    logger.info("Tables to clean up: %d", len(table_defs))

    jdbc_url, user, password = get_jdbc_connection(glue_context, connection_name)

    results = cleanup_tables_by_level(
        spark,
        jdbc_url,
        user,
        password,
        table_defs,
        dependency_order,
    )

    total_deleted = sum(r["rows_deleted"] for r in results)
    logger.info(
        "Cleanup job complete. Cleaned %d tables, deleted %d total rows.",
        len(results),
        total_deleted,
    )


if __name__ == "__main__":
    main()
