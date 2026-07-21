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
from datetime import datetime, timedelta, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_jdbc_url(glue_context, connection_name):
    """Extract the JDBC URL from a Glue connection.

    Args:
        glue_context: The GlueContext instance.
        connection_name: Name of the Glue connection.

    Returns:
        tuple: (jdbc_url, connection_properties) for JDBC reads.
    """
    conn = glue_context.extract_jdbc_conf(connection_name)
    jdbc_url = conn.get("fullUrl") or conn.get("url", "")
    return jdbc_url, {
        "user": conn["user"],
        "password": conn["password"],
        "driver": "org.postgresql.Driver",
    }


def table_exists(spark, jdbc_url, conn_props, table_name):
    """Check whether a table exists in the database.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        table_name: Name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        query = (
            f"(SELECT 1 FROM information_schema.tables "
            f"WHERE table_name = '{table_name}' LIMIT 1) AS check_tbl"
        )
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=conn_props)
        return df.count() > 0
    except Exception as e:
        logger.warning("table_exists check failed for '%s': %s", table_name, e)
        return False


def hex_encode_binary_columns(df, binary_columns):
    """Hex-encode binary columns in a DataFrame.

    Converts binary column values to their hex string representation prefixed
    with ``\\x`` so they can be safely stored in CSV format.

    Args:
        df: The Spark DataFrame.
        binary_columns: List of column names containing binary data.

    Returns:
        DataFrame: The DataFrame with binary columns hex-encoded as strings.
    """
    for col_name in binary_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    F.col(col_name).isNotNull(),
                    F.concat(F.lit("\\x"), F.hex(F.col(col_name))),
                ).otherwise(F.lit(None).cast(StringType())),
            )
    return df


def build_jdbc_query(table_def, max_age_days):
    """Build a JDBC query string for a table, applying date filtering if applicable.

    Always uses SELECT * to avoid column mismatch issues across Airflow versions.
    Binary column encoding is handled post-read in Spark.

    Args:
        table_def: Dict with table definition (table, date_field, columns, binary_columns).
        max_age_days: Maximum age in days for date-based filtering. 0 means no filter.

    Returns:
        str: A JDBC-compatible table expression (subquery alias).
    """
    table_name = table_def["table"]
    date_field = table_def.get("date_field")

    where_clauses = []

    # Date-based filtering
    if date_field and max_age_days and int(max_age_days) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(max_age_days))
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        where_clauses.append(f"{date_field} >= '{cutoff_str}'")

    where_str = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    return f"(SELECT * FROM {table_name}{where_str}) AS {table_name}_export"


def export_table(spark, jdbc_url, conn_props, table_def, s3_output_path, max_age_days):
    """Export a single table to a gzip-compressed CSV file on S3.

    Uses Spark's native JDBC reader with ``fetchsize=1000`` and writes directly
    to S3 as gzip-compressed CSV. The output is coalesced to a single partition
    to produce one ``.csv.gz`` file per table.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        table_def: Dict with table definition.
        s3_output_path: Base S3 path for output files.
        max_age_days: Maximum age in days for date-based filtering.

    Returns:
        dict: Result with table name and row count, or None if skipped.
    """
    table_name = table_def["table"]

    # Skip variable and connection tables (handled via REST API)
    if table_name in ("variable", "connection"):
        logger.info("Skipping table '%s' (handled via REST API).", table_name)
        return None

    # Check if table exists
    if not table_exists(spark, jdbc_url, conn_props, table_name):
        logger.warning("Table '%s' does not exist in database, skipping.", table_name)
        return None

    logger.info("Exporting table '%s'...", table_name)

    query = build_jdbc_query(table_def, max_age_days)

    # Read from database via JDBC with fetchsize for memory control
    read_props = dict(conn_props)
    read_props["fetchsize"] = "1000"

    df = spark.read.jdbc(url=jdbc_url, table=query, properties=read_props)

    # Hex-encode binary columns (for columns not already handled in SQL)
    # The SQL query already handles hex-encoding, so we skip double-encoding
    row_count = df.count()

    if row_count == 0:
        logger.info("Table '%s' has 0 rows, writing empty file.", table_name)

    # Write to S3 as gzip-compressed CSV (single partition = single file)
    output_path = f"{s3_output_path}/{table_name}.csv.gz"

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("delimiter", "|")
        .option("compression", "gzip")
        .csv(output_path)
    )

    logger.info(
        "Exported %d rows from table '%s' to %s.", row_count, table_name, output_path
    )
    return {"table": table_name, "rows": row_count}


def export_tables_by_level(
    spark,
    jdbc_url,
    conn_props,
    table_defs,
    dependency_order,
    s3_output_path,
    max_age_days,
):
    """Export tables in reverse dependency order, parallelizing within each level.

    Tables at the same dependency level have no relationships between them and
    can be exported concurrently. Levels are processed in descending order
    (child tables before parent tables) for snapshot consistency.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        table_defs: List of table definition dicts.
        dependency_order: List of lists of table names (topological levels).
        s3_output_path: Base S3 path for output files.
        max_age_days: Maximum age in days for date-based filtering.

    Returns:
        list[dict]: List of export results with table names and row counts.
    """
    # Build lookup from table name to table definition
    table_lookup = {td["table"]: td for td in table_defs}

    results = []

    # Process levels in reverse order (highest level first = child tables first)
    for level_tables in reversed(dependency_order):
        level_results = []

        # Tables at the same level can run in parallel
        with ThreadPoolExecutor(max_workers=min(len(level_tables), 4)) as executor:
            futures = {}
            for table_name in level_tables:
                if table_name not in table_lookup:
                    continue
                table_def = table_lookup[table_name]
                future = executor.submit(
                    export_table,
                    spark,
                    jdbc_url,
                    conn_props,
                    table_def,
                    s3_output_path,
                    max_age_days,
                )
                futures[future] = table_name

            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        level_results.append(result)
                except Exception as e:
                    logger.error("Failed to export table '%s': %s", table_name, e)
                    raise

        results.extend(level_results)

    return results


def write_summary(spark, s3_output_path, results):
    """Write an export summary JSON file to S3.

    The summary contains the export timestamp, individual table results,
    and the total row count across all tables.

    Args:
        spark: The SparkSession (used for Hadoop filesystem access).
        s3_output_path: Base S3 path for the summary file.
        results: List of export result dicts.
    """
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tables": {r["table"]: r["rows"] for r in results},
        "total_rows": sum(r["rows"] for r in results),
    }

    summary_path = f"{s3_output_path}/export_summary.json"
    summary_json = json.dumps(summary, indent=2)

    # Use Spark's Hadoop filesystem to write the summary
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(s3_output_path), hadoop_conf
    )
    output_stream = fs.create(
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(summary_path), True
    )
    output_stream.write(summary_json.encode("utf-8"))
    output_stream.close()

    logger.info("Export summary written to %s", summary_path)
    logger.info("Summary: %s", summary_json)


def main():
    """Entry point for the Glue export job."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_OUTPUT_PATH",
            "EXPORT_TABLES",
            "GLUE_CONNECTION_NAME",
            "MAX_AGE_IN_DAYS",
            "TABLE_DEPENDENCY_ORDER",
        ],
    )

    s3_output_path = args["S3_OUTPUT_PATH"]
    table_defs = json.loads(args["EXPORT_TABLES"])
    connection_name = args["GLUE_CONNECTION_NAME"]
    max_age_days = args["MAX_AGE_IN_DAYS"]
    dependency_order = json.loads(args["TABLE_DEPENDENCY_ORDER"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    logger.info("Starting metadata export job '%s'.", args["JOB_NAME"])
    logger.info("Output path: %s", s3_output_path)
    logger.info("Connection: %s", connection_name)
    logger.info("Max age (days): %s", max_age_days)
    logger.info("Tables to export: %d", len(table_defs))

    jdbc_url, conn_props = get_jdbc_url(glue_context, connection_name)

    # Diagnostic: list all tables in the database
    try:
        all_tables_query = "(SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name) AS all_tables"
        all_tables_df = spark.read.jdbc(
            url=jdbc_url, table=all_tables_query, properties=conn_props
        )
        logger.info("Database tables found:")
        for row in all_tables_df.collect():
            logger.info("  %s.%s", row["table_schema"], row["table_name"])
    except Exception as e:
        logger.error("Failed to list database tables: %s", e)

    results = export_tables_by_level(
        spark,
        jdbc_url,
        conn_props,
        table_defs,
        dependency_order,
        s3_output_path,
        max_age_days,
    )

    write_summary(spark, s3_output_path, results)

    logger.info("Export job complete. Exported %d tables.", len(results))


if __name__ == "__main__":
    main()
