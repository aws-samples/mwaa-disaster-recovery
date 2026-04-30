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
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_jdbc_url(glue_context, connection_name):
    """Extract the JDBC URL from a Glue connection.

    Args:
        glue_context: The GlueContext instance.
        connection_name: Name of the Glue connection.

    Returns:
        tuple: (jdbc_url, connection_properties) for JDBC operations.
    """
    conn = glue_context.extract_jdbc_conf(connection_name)
    jdbc_url = conn.get("fullUrl") or conn.get("url", "")
    return jdbc_url, {
        "user": conn["user"],
        "password": conn["password"],
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }


def backup_file_exists(spark, s3_path):
    """Check whether a backup file exists at the given S3 path.

    Spark writes CSV output as a directory containing part files. This checks
    whether the directory exists and contains data.

    Args:
        spark: The SparkSession.
        s3_path: S3 path to check.

    Returns:
        bool: True if the backup file/directory exists, False otherwise.
    """
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(s3_path), hadoop_conf
        )
        return fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path))
    except Exception:
        return False


def hex_decode_binary_columns(df, binary_columns):
    """Decode hex-encoded binary columns back to binary.

    Reverses the hex-encoding applied during export. Values prefixed with
    ``\\x`` have the prefix stripped before decoding.

    Args:
        df: The Spark DataFrame.
        binary_columns: List of column names containing hex-encoded binary data.

    Returns:
        DataFrame: The DataFrame with binary columns decoded to binary type.
    """
    for col_name in binary_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    F.col(col_name).isNotNull(),
                    F.unbase16(
                        F.when(
                            F.col(col_name).startswith("\\x"),
                            F.expr(f"substring({col_name}, 3)"),
                        ).otherwise(F.col(col_name))
                    ),
                ).otherwise(F.lit(None).cast(BinaryType())),
            )
    return df


def import_table_jdbc(spark, jdbc_url, conn_props, df, table_name):
    """Write a DataFrame to PostgreSQL via COPY FROM STDIN.

    Uses PostgreSQL's native COPY command for type-safe bulk loading,
    matching the approach used in the 2.x DR solution.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        df: The DataFrame to write.
        table_name: Target table name.

    Returns:
        tuple: (rows_imported, rows_skipped) counts.
    """
    total_rows = df.count()
    if total_rows == 0:
        logger.info("Table '%s' has 0 rows to import.", table_name)
        return 0, 0

    # Collect data and convert to pipe-delimited CSV (no header)
    rows = df.collect()
    columns = df.columns
    csv_lines = []
    for row in rows:
        vals = []
        for v in row:
            if v is None:
                vals.append("")
            else:
                vals.append(str(v).replace("|", "\\|"))
        csv_lines.append("|".join(vals))
    csv_data = "\n".join(csv_lines).encode("utf-8")

    col_list = ", ".join([f'"{c}"' for c in columns])
    copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER '|', NULL '')"

    sc = spark.sparkContext
    gateway = sc._gateway
    gateway.jvm.Class.forName("org.postgresql.Driver")
    connection = gateway.jvm.java.sql.DriverManager.getConnection(
        jdbc_url, conn_props.get("user", ""), conn_props.get("password", "")
    )

    rows_imported = 0
    rows_skipped = 0
    try:
        pg_conn = gateway.jvm.Class.forName(
            "org.postgresql.jdbc.PgConnection"
        ).cast(connection)
        copy_mgr = gateway.jvm.org.postgresql.copy.CopyManager(pg_conn)
        input_stream = gateway.jvm.java.io.ByteArrayInputStream(
            gateway.new_array(gateway.jvm.byte, *list(csv_data))
        )
        rows_imported = copy_mgr.copyIn(copy_sql, input_stream)
    except Exception as e:
        error_msg = str(e)
        if "duplicate key" in error_msg.lower() or "unique" in error_msg.lower() or "violates" in error_msg.lower():
            logger.warning(
                "COPY failed for table '%s' with constraint violation, "
                "falling back to row-by-row insert.",
                table_name,
            )
            try:
                connection.rollback()
            except Exception:
                pass
            rows_imported, rows_skipped = _insert_with_conflict_handling(
                spark, jdbc_url, conn_props, df, table_name
            )
        else:
            raise
    finally:
        connection.close()

    logger.info(
        "Table '%s': imported %d rows, skipped %d rows.",
        table_name,
        rows_imported,
        rows_skipped,
    )
    return rows_imported, rows_skipped


def _insert_with_conflict_handling(spark, jdbc_url, conn_props, df, table_name):
    """Insert rows one-by-one using INSERT ... ON CONFLICT DO NOTHING.

    This fallback is used when a batch insert fails due to duplicate key
    conflicts. Each row is inserted individually so that non-conflicting
    rows are preserved.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        df: The DataFrame to insert.
        table_name: Target table name.

    Returns:
        tuple: (rows_imported, rows_skipped) counts.
    """
    rows = df.collect()
    columns = df.columns

    rows_imported = 0
    rows_skipped = 0

    # Build parameterized INSERT ... ON CONFLICT DO NOTHING
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {table_name} ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    # Use a direct JDBC connection for row-by-row inserts
    sc = spark.sparkContext
    gateway = sc._gateway
    driver_class = gateway.jvm.Class.forName("org.postgresql.Driver")  # noqa: F841

    connection = gateway.jvm.java.sql.DriverManager.getConnection(
        jdbc_url, conn_props.get("user", ""), conn_props.get("password", "")
    )

    try:
        connection.setAutoCommit(False)
        stmt = connection.prepareStatement(insert_sql)

        for row in rows:
            try:
                for i, val in enumerate(row):
                    if val is None:
                        stmt.setNull(i + 1, gateway.jvm.java.sql.Types.NULL)
                    elif isinstance(val, (bytes, bytearray)):
                        stmt.setBytes(i + 1, val)
                    else:
                        stmt.setString(i + 1, str(val))

                result = stmt.executeUpdate()
                if result > 0:
                    rows_imported += 1
                else:
                    rows_skipped += 1
            except Exception as e:
                error_msg = str(e)
                if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
                    rows_skipped += 1
                else:
                    logger.error("Error inserting row into '%s': %s", table_name, e)
                    rows_skipped += 1

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        stmt.close()
        connection.close()

    return rows_imported, rows_skipped


def import_table(spark, jdbc_url, conn_props, table_def, s3_input_path):
    """Import a single table from a gzip-compressed CSV backup on S3.

    Reads the CSV file, decodes hex-encoded binary columns, and writes
    records to PostgreSQL via JDBC.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        table_def: Dict with table definition.
        s3_input_path: Base S3 path for input files.

    Returns:
        dict: Result with table name, rows imported, and rows skipped, or None if skipped.
    """
    table_name = table_def["table"]

    # Skip variable and connection tables (handled via REST API)
    if table_name in ("variable", "connection"):
        logger.info("Skipping table '%s' (handled via REST API).", table_name)
        return None

    # Check if backup file exists
    backup_path = f"{s3_input_path}/{table_name}.csv.gz"
    if not backup_file_exists(spark, backup_path):
        logger.warning(
            "Backup file not found for table '%s' at %s, skipping.",
            table_name,
            backup_path,
        )
        return None

    logger.info("Importing table '%s' from %s...", table_name, backup_path)

    columns = table_def.get("columns", [])
    binary_columns = table_def.get("binary_columns", [])

    # Read CSV from S3
    df = spark.read.option("header", "true").option("delimiter", "|").csv(backup_path)

    # Cast columns to match the target table schema
    try:
        target_schema_query = f"(SELECT * FROM {table_name} WHERE 1=0) AS schema_tbl"
        target_df = spark.read.jdbc(url=jdbc_url, table=target_schema_query, properties=conn_props)
        for field in target_df.schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    except Exception as e:
        logger.warning("Could not read target schema for '%s': %s", table_name, e)

    # Decode hex-encoded binary columns
    if binary_columns:
        df = hex_decode_binary_columns(df, binary_columns)

    # Write to database
    rows_imported, rows_skipped = import_table_jdbc(
        spark, jdbc_url, conn_props, df, table_name
    )

    return {
        "table": table_name,
        "rows_imported": rows_imported,
        "rows_skipped": rows_skipped,
    }


def import_tables_by_level(
    spark, jdbc_url, conn_props, table_defs, dependency_order, s3_input_path
):
    """Import tables in dependency order, parallelizing within each level.

    Tables at the same dependency level have no relationships between them and
    can be imported concurrently. Levels are processed in ascending order
    (parent tables before child tables) to respect foreign key constraints.

    Args:
        spark: The SparkSession.
        jdbc_url: JDBC connection URL.
        conn_props: JDBC connection properties dict.
        table_defs: List of table definition dicts.
        dependency_order: List of lists of table names (topological levels).
        s3_input_path: Base S3 path for input files.

    Returns:
        list[dict]: List of import results with table names and row counts.
    """
    table_lookup = {td["table"]: td for td in table_defs}
    results = []

    # Process levels in ascending order (level 0 first = parent tables first)
    for level_tables in dependency_order:
        level_results = []

        with ThreadPoolExecutor(max_workers=min(len(level_tables), 4)) as executor:
            futures = {}
            for table_name in level_tables:
                if table_name not in table_lookup:
                    continue
                table_def = table_lookup[table_name]
                future = executor.submit(
                    import_table,
                    spark,
                    jdbc_url,
                    conn_props,
                    table_def,
                    s3_input_path,
                )
                futures[future] = table_name

            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        level_results.append(result)
                except Exception as e:
                    logger.error("Failed to import table '%s': %s", table_name, e)
                    raise

        results.extend(level_results)

    return results


def write_summary(spark, s3_input_path, results):
    """Write an import summary JSON file to S3.

    The summary contains the import timestamp, individual table results
    (rows imported and skipped), and totals.

    Args:
        spark: The SparkSession (used for Hadoop filesystem access).
        s3_input_path: Base S3 path for the summary file.
        results: List of import result dicts.
    """
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tables": {
            r["table"]: {
                "rows_imported": r["rows_imported"],
                "rows_skipped": r["rows_skipped"],
            }
            for r in results
        },
        "total_rows_imported": sum(r["rows_imported"] for r in results),
        "total_rows_skipped": sum(r["rows_skipped"] for r in results),
    }

    summary_path = f"{s3_input_path}/import_summary.json"
    summary_json = json.dumps(summary, indent=2)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(s3_input_path), hadoop_conf
    )
    output_stream = fs.create(
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(summary_path), True
    )
    output_stream.write(summary_json.encode("utf-8"))
    output_stream.close()

    logger.info("Import summary written to %s", summary_path)
    logger.info("Summary: %s", summary_json)


def _pre_import_cleanup(spark, jdbc_url, conn_props, s3_input_path):
    """Clean and re-import dag_version/dag_code in a single transaction.

    Uses PostgreSQL COPY command (same as the 2.x DR solution) for type-safe
    bulk loading. DELETE + COPY in one transaction prevents scheduler race.
    """
    import boto3
    import gzip
    from io import ByteArrayInputStream  # noqa: will use JVM version

    sc = spark.sparkContext
    gateway = sc._gateway
    gateway.jvm.Class.forName("org.postgresql.Driver")
    connection = gateway.jvm.java.sql.DriverManager.getConnection(
        jdbc_url, conn_props.get("user", ""), conn_props.get("password", "")
    )
    try:
        # Cast to PgConnection for COPY API
        pg_conn = gateway.jvm.Class.forName(
            "org.postgresql.jdbc.PgConnection"
        ).cast(connection)
        connection.setAutoCommit(False)
        stmt = connection.createStatement()

        # Delete in FK-safe order
        for sql in [
            "DELETE FROM dag_code",
            "DELETE FROM dag_run WHERE dag_id NOT IN ('cleanup_metadata', 'restore_metadata', 'backup_metadata')",
            "DELETE FROM dag_version",
        ]:
            rows = stmt.executeUpdate(sql)
            logger.info("Pre-import: %s → %d rows.", sql[:50], rows)

        # COPY dag_version from backup CSV in the SAME transaction
        _copy_table_from_s3(spark, pg_conn, gateway, "dag_version", s3_input_path)
        # COPY dag_code from backup CSV
        _copy_table_from_s3(spark, pg_conn, gateway, "dag_code", s3_input_path)

        connection.commit()
        logger.info("Pre-import: dag_version and dag_code restored in single transaction.")
    except Exception as e:
        logger.error("Pre-import cleanup failed: %s", e)
        try:
            connection.rollback()
        except Exception:
            pass
    finally:
        connection.close()


def _copy_table_from_s3(spark, pg_conn, gateway, table_name, s3_input_path):
    """Load a table from S3 CSV using PostgreSQL COPY command."""
    import boto3
    import gzip

    backup_path = f"{s3_input_path}/{table_name}.csv.gz"

    # Read the CSV from S3 via Spark's Hadoop filesystem
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(s3_input_path), hadoop_conf
    )

    # Find the actual part file inside the .csv.gz directory
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(backup_path)
    if not fs.exists(path):
        logger.warning("Backup not found for '%s' at %s, skipping.", table_name, backup_path)
        return

    # Spark writes CSV as a directory with part files - find them
    csv_data = b""
    if fs.isDirectory(path):
        file_statuses = fs.listStatus(path)
        for i in range(file_statuses.length):
            fname = file_statuses[i].getPath().getName()
            if fname.startswith("part-") and fname.endswith(".csv.gz"):
                stream = fs.open(file_statuses[i].getPath())
                gz_bytes = bytearray()
                while True:
                    b = stream.read()
                    if b == -1:
                        break
                    gz_bytes.append(b & 0xFF)
                stream.close()
                csv_data += gzip.decompress(bytes(gz_bytes))
    else:
        stream = fs.open(path)
        gz_bytes = bytearray()
        while True:
            b = stream.read()
            if b == -1:
                break
            gz_bytes.append(b & 0xFF)
        stream.close()
        csv_data = gzip.decompress(bytes(gz_bytes))

    if not csv_data:
        logger.info("No data for table '%s', skipping COPY.", table_name)
        return

    # Skip header line (first line)
    lines = csv_data.split(b"\n", 1)
    if len(lines) > 1:
        csv_data = lines[1]

    # Use PostgreSQL COPY FROM STDIN
    copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER '|')"
    copy_mgr = gateway.jvm.org.postgresql.copy.CopyManager(pg_conn)
    input_stream = gateway.jvm.java.io.ByteArrayInputStream(
        gateway.new_array(gateway.jvm.byte, *list(csv_data))
    )
    rows = copy_mgr.copyIn(copy_sql, input_stream)
    logger.info("Pre-import COPY: loaded %d rows into '%s'.", rows, table_name)


def main():
    """Entry point for the Glue import job."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_INPUT_PATH",
            "IMPORT_TABLES",
            "GLUE_CONNECTION_NAME",
            "TABLE_DEPENDENCY_ORDER",
        ],
    )

    s3_input_path = args["S3_INPUT_PATH"]
    table_defs = json.loads(args["IMPORT_TABLES"])
    connection_name = args["GLUE_CONNECTION_NAME"]
    dependency_order = json.loads(args["TABLE_DEPENDENCY_ORDER"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    logger.info("Starting metadata import job '%s'.", args["JOB_NAME"])
    logger.info("Input path: %s", s3_input_path)
    logger.info("Connection: %s", connection_name)
    logger.info("Tables to import: %d", len(table_defs))

    jdbc_url, conn_props = get_jdbc_url(glue_context, connection_name)

    # Pre-import: clean dag_version and dag_code so primary's records can be imported
    # These are skipped during cleanup (to keep DAGs alive) but must be replaced during restore
    _pre_import_cleanup(spark, jdbc_url, conn_props, s3_input_path)

    results = import_tables_by_level(
        spark,
        jdbc_url,
        conn_props,
        table_defs,
        dependency_order,
        s3_input_path,
    )

    _post_import_restore_constraints(spark, jdbc_url, conn_props)

    write_summary(spark, s3_input_path, results)

    logger.info("Import job complete. Imported %d tables.", len(results))


if __name__ == "__main__":
    main()
