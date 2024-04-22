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

from airflow import settings
from mwaa_dr.framework.model.dependency_model import DependencyModel

S3 = "S3"


class BaseTable:
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
        if context:
            dag_run = context.get("dag_run")
            if "bucket" in dag_run.conf:
                return dag_run.conf["bucket"]

        from airflow.models import Variable

        try:
            return Variable.get("DR_BACKUP_BUCKET")
        except:
            return "Dummy-Bucket"

    def __str__(self):
        return f"Table({self.name})"

    def __repr__(self):
        return self.__str__()

    def __rshift__(self, others):
        self.model.add_dependency(self, others)
        return others

    def __lshift__(self, others):
        if isinstance(others, list):
            for other in others:
                self.model.add_dependency(other, self)
        else:
            self.model.add_dependency(others, self)

        return others

    def get_name(self) -> str:
        return self.name

    def all_columns(self) -> str:
        if len(self.columns) == 0:
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

        filter = "" if not self.export_filter else f" WHERE {self.export_filter}"
        sql = f"SELECT {self.all_columns()} FROM {self.name}{filter}"
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
        backup_file = self.read(context)

        restore_sql = ""
        if len(self.columns) > 0:
            restore_sql = f'COPY {self.name} ({", ".join(self.columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
        else:
            restore_sql = f"COPY {self.name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
        print(f"Restore SQL: {restore_sql}")

        conn = settings.engine.raw_connection()
        try:
            with open(backup_file) as backup:
                cursor = conn.cursor()
                cursor.copy_expert(restore_sql, backup)
                conn.commit()
        finally:
            conn.close()

    def write(self, body: str, context=None):
        if self.storage_type == S3:
            self.write_to_s3(body, self.name, context)
        else:
            self.write_to_local(body, self.name)

    def write_to_s3(self, body: str, file_name, context):
        from airflow.hooks.S3_hook import S3Hook

        s3_hook = S3Hook()
        s3_client = s3_hook.get_conn()

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
        if self.storage_type == S3:
            return self.read_from_s3(context)
        else:
            return self.read_from_local()

    def read_from_s3(self, context) -> str:
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
        AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
        local_file_uri = f"{AIRFLOW_HOME}/dags/{self.path_prefix}/{self.name}.csv"
        return local_file_uri
