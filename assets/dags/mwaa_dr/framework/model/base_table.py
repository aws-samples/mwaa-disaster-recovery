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

import csv
import os
from io import StringIO
from copy import deepcopy

from airflow import settings
from mwaa_dr.framework.model.dependency_model import DependencyModel

S3 = "S3"


class BaseTable:
    """
    Base class for all tables that need to be backed up and restored. Provides
    dependency modeling, backup, and restore functionalities to all tables that
    inherit from this class.

    Args:
        name (str): The name of the table.
        model (DependencyModel): The dependency model for the table.
        columns (list[str], optional): A list of column names for the table. Defaults to None.
        export_mappings (dict[str, str], optional): A dictionary mapping column names to their export names. Defaults to None.
        export_filter (str, optional): A filter condition for exporting data from the table. Defaults to None.
        storage_type (str, optional): The storage type for the backup (e.g., S3 or local). Defaults to None.
        path_prefix (str, optional): The path prefix for the backup file. Defaults to None.
        batch_size (int, optional): The batch size for fetching data from the table. Defaults to 5000.

    Attributes:
        model (DependencyModel): The dependency model for the table.
        name (str): The name of the table.
        columns (list[str]): A list of column names for the table.
        export_mappings (dict[str, str]): A dictionary mapping column names to their export names.
        export_filter (str): A filter condition for exporting data from the table.
        storage_type (str): The storage type for the backup (e.g., S3 or local).
        path_prefix (str): The path prefix for the backup file.
        batch_size (int): The batch size for fetching data from the table.
    """

    model: DependencyModel
    name: str
    columns: list[str]
    export_mappings: dict[str, str]
    export_filter: str
    storage_type: str
    path_prefix: str
    batch_size: int

    def __init__(
        self,
        name: str,
        model: DependencyModel,
        columns: list[str] = None,
        export_mappings: dict[str, str] = None,
        export_filter: str = None,
        storage_type: str = None,
        path_prefix: str = None,
        batch_size=5000,
    ):
        self.name = name
        self.model = model
        self.columns = columns or []
        self.export_mappings = export_mappings or {}
        self.export_filter = export_filter or ""
        self.storage_type = storage_type or S3
        self.path_prefix = path_prefix
        self.batch_size = batch_size

        model.add(self)

    @staticmethod
    def bucket(context=None) -> str:
        """
        Returns the bucket name for storing backups.

        If the context is provided and contains a 'dag_run' key with a 'bucket' value, it returns that value.
        Otherwise, it retrieves the bucket name from the 'DR_BACKUP_BUCKET' Airflow variable.
        If the variable is not set, it returns '--dummy-bucket--'.

        Args:
            context (dict, optional): The context dictionary containing the 'dag_run' information.

        Returns:
            str: The bucket name for storing backups.
        """

        if context:
            dag_run = context.get("dag_run")
            if "bucket" in dag_run.conf:
                return dag_run.conf["bucket"]

        from airflow.models import Variable

        return Variable.get("DR_BACKUP_BUCKET", default_var="--dummy-bucket--")

    def __str__(self):
        """
        Returns a string representation of the BaseTable object.

        Returns:
            str: A string in the format 'Table(table_name)'.
        """
        return f"Table({self.name})"

    def __repr__(self):
        """
        Returns a string representation of the BaseTable object.

        Returns:
            str: A string in the format 'Table(table_name)'.
        """
        return self.__str__()

    def __copy__(self):
        """
        Returns a shallow copy of the BaseTable object.

        Returns:
            BaseTable: A shallow copy of the object.
        """
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        """
        Returns a deep copy of the BaseTable object.

        Args:
            memo (dict): A dictionary to store the copied objects.

        Returns:
            BaseTable: A deep copy of the object.
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result

    def __eq__(self, other):
        """
        Checks if two BaseTable objects are equal by comparing their attributes.

        Args:
            other (BaseTable): The other BaseTable object to compare with.

        Returns:
            bool: True if the names of the two objects are equal, False otherwise.
        """
        if self is other:
            return True

        if self.__class__ is not other.__class__:
            return False

        return (
            self.name == other.name
            and self.columns == other.columns
            and self.export_mappings == other.export_mappings
            and self.export_filter == other.export_filter
            and self.storage_type == other.storage_type
            and self.path_prefix == other.path_prefix
            and self.batch_size == other.batch_size
        )

    def __hash__(self):
        """
        Returns a hash value for the BaseTable object based on its name attribute.

        Returns:
            int: A hash value for the object.
        """
        return hash(self.name)

    def __rshift__(self, others):
        """
        Adds a dependency from this table to the specified other table(s).

        Args:
            others (BaseTable or list[BaseTable]): The table(s) that depend on this table.

        Returns:
            others (BaseTable or list[BaseTable]): The table(s) that depend on this table.
        """
        self.model.add_dependency(self, others)
        return others

    def __lshift__(self, others):
        """
        Adds a dependency from the specified other table(s) to this table.

        Args:
            others (BaseTable or list[BaseTable]): The table(s) that this table depends on.

        Returns:
            others (BaseTable or list[BaseTable]): The table(s) that this table depends on.
        """
        if isinstance(others, list):
            for other in others:
                self.model.add_dependency(other, self)
        else:
            self.model.add_dependency(others, self)

        return others

    def get_name(self) -> str:
        """
        Returns the name of the table.

        Returns:
            str: The name of the table.
        """
        return self.name

    def all_columns(self) -> str:
        """
        Returns a comma-separated string of column names for the table.

        If the `columns` attribute is empty, it returns '*' to select all columns.
        If `export_mappings` is provided, it uses the mapped column names in the output.

        Returns:
            str: A comma-separated string of column names for the table.
        """
        if not self.columns:
            return "*"

        columns_text = ""
        for index in range(len(self.columns)):
            column = self.columns[index]
            if column in self.export_mappings:
                column = self.export_mappings[column]

            if index < len(self.columns) - 1:
                columns_text = f"{columns_text}{column}, "
            else:
                columns_text = f"{columns_text}{column}"

        return columns_text

    def backup(self, **context):
        """
        Backs up the table data to a CSV file in the specified storage location.

        Args:
            **context: Additional context parameters, including the 'dag_run' information.

        The backup file is stored in the following locations:
        - S3: s3://<bucket_name>/<path_prefix>/<table_name>.csv
        - Local: <AIRFLOW_HOME>/dags/<path_prefix>/<table_name>.csv

        The backup process streams the table data in batches of `batch_size` rows to the backup file.
        """
        import smart_open

        store = None

        if self.storage_type == S3:

            s3_file_uri = (
                f"s3://{self.bucket(context)}/{self.path_prefix}/{self.name}.csv"
            )
            print(f"Streaming to S3 file: {s3_file_uri} ...")
            store = smart_open.open(s3_file_uri, "wb")
        else:
            AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

            local_file_uri = f"{AIRFLOW_HOME}/dags/{self.path_prefix}/{self.name}.csv"
            print(f"Streaming to local file: {local_file_uri} ...")
            store = open(local_file_uri, "wb")

        filter_str = "" if not self.export_filter else f" WHERE {self.export_filter}"
        sql = f"SELECT {self.all_columns()} FROM {self.name}{filter_str}"
        print(f"Export SQL for {self.name}: {sql}")

        try:
            with settings.Session() as session:
                result = session.execute(sql)
                chunk = result.fetchmany(self.batch_size)
                while chunk:
                    buffer = StringIO("")
                    writer = csv.writer(buffer)
                    writer.writerows(chunk)
                    store.write(buffer.getvalue().encode("utf8"))
                    chunk = result.fetchmany(self.batch_size)
        finally:
            store.close()

    def restore(self, **context):
        """
        Restores the table data from the backup CSV file.

        Args:
            **context: Additional context parameters, including the 'dag_run' information.

        The restore process reads the backup file and copies the data into the table using the `COPY` command.
        If the `columns` attribute is provided, it specifies the column names in the `COPY` command.
        """
        backup_file = self.read(context)

        restore_sql = ""
        if self.columns:
            restore_sql = f'COPY {self.name} ({", ".join(self.columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
        else:
            restore_sql = f"COPY {self.name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
        print(f"Restore SQL: {restore_sql}")

        conn = settings.engine.raw_connection()
        try:
            with open(backup_file, encoding="utf-8") as backup:
                cursor = conn.cursor()
                cursor.copy_expert(restore_sql, backup)
                conn.commit()
        finally:
            conn.close()

    def write(self, body: str, context=None):
        """
        Writes the provided string content to a CSV file in the specified storage location.

        Args:
            body (str): The string content to be written to the file.
            context (dict, optional): The context dictionary containing the 'dag_run' information.

        The file is stored in the following locations:
        - S3: s3://<bucket_name>/<path_prefix>/<table_name>.csv
        - Local: <AIRFLOW_HOME>/dags/<path_prefix>/<table_name>.csv
        """
        if self.storage_type == S3:
            self.write_to_s3(body, self.name, context)
        else:
            self.write_to_local(body, self.name)

    def write_to_s3(self, body: str, file_name, context):
        import boto3

        s3_client = boto3.client("s3")
        key = f"{self.path_prefix}/{file_name}.csv"
        s3_client.put_object(Bucket=self.bucket(context), Key=key, Body=body)

    def write_to_local(self, body: str, file_name):
        AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
        local_file_uri = f"{AIRFLOW_HOME}/dags/{self.path_prefix}/{file_name}.csv"

        print(f"Writing to local file: {local_file_uri} ...")
        with open(local_file_uri, "w") as local_file:
            local_file.write(body)
            local_file.close()

    def read(self, context=None) -> str:
        """
        Reads the content of the backup CSV file and returns its local file path.

        Args:
            context (dict, optional): The context dictionary containing the 'dag_run' information.

        Returns:
            str: The local file path of the backup CSV file.

        If the backup file is stored in S3, it is first downloaded to a temporary local file before returning the path.
        If the backup file does not exist, it returns None.
        """
        if self.storage_type == S3:
            return self.read_from_s3(context)
        else:
            return self.read_from_local()

    def read_from_s3(self, context) -> str:
        """
        Reads the content of the backup CSV file from S3 and returns its local file path.

        Args:
            context (dict, optional): The context dictionary containing the 'dag_run' information.

        Returns:
            str: The local file path of the backup CSV file.
        """
        import boto3
        import botocore

        resource = boto3.resource("s3")
        bucket = resource.Bucket(self.bucket(context))
        backup_file = f"/tmp/{self.name}.csv"
        s3_file_key = f"{self.path_prefix}/{self.name}.csv"

        try:
            print(f"Downloading file {s3_file_key} ...")
            bucket.download_file(s3_file_key, backup_file)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            else:
                raise
        else:
            return backup_file

    def read_from_local(self) -> str:
        """
        Reads the content of the backup CSV file from local storage and returns its local file path.

        Returns:
            str: The local file path of the backup CSV file.
        """
        AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
        local_file_uri = f"{AIRFLOW_HOME}/dags/{self.path_prefix}/{self.name}.csv"
        return local_file_uri
