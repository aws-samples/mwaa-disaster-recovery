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
from io import StringIO

from airflow import settings
from airflow.models import Connection
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class ConnectionTable(BaseTable):
    """
    A class representing the connection table in Apache Airflow.

    This class inherits from the BaseTable class and is responsible for backing up
    and restoring the connection table in Apache Airflow. Only the connections that do
    not exist in the restored environment will be restored by this class.
    The connection table stores connection information for various data sources,
    such as databases, APIs, and file systems.

    Args:
        model (DependencyModel): The dependency model object.
        storage_type (str, optional): The type of storage to use for backup/restore. Defaults to S3.
        path_prefix (str, optional): The path prefix for the backup/restore location.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.

    Attributes:
        columns (list): A list of column names in the connection table.
    """

    def __init__(
        self,
        model: DependencyModel,
        storage_type: str = None,
        path_prefix: str = None,
        batch_size=5000,
    ):
        super().__init__(
            name="connection",
            model=model,
            storage_type=storage_type,
            path_prefix=path_prefix,
            batch_size=batch_size,
        )

        self.columns = [
            "conn_id",
            "conn_type",
            "description",
            "extra",
            "host",
            "login",
            "password",
            "port",
            "schema",
        ]

    def backup(self, **context):
        """
        Backup the connection table to a specified location.

        This method retrieves all connections from the Apache Airflow database and
        writes them to a CSV file in the specified backup location.

        Args:
            **context: Additional Airflow task context parameters.
        """
        with settings.Session() as session:
            buffer = StringIO("")
            query = session.query(Connection)
            connections = query.all()

            if connections:
                keys = self.columns
                writer = csv.DictWriter(buffer, keys)
                for connection in connections:
                    writer.writerow(
                        {
                            keys[0]: connection.conn_id,
                            keys[1]: connection.conn_type,
                            keys[2]: connection.description,
                            keys[3]: connection.get_extra(),
                            keys[4]: connection.host,
                            keys[5]: connection.login,
                            keys[6]: connection.get_password(),
                            keys[7]: connection.port,
                            keys[8]: connection.schema,
                        }
                    )
            self.write(buffer.getvalue(), context)

    def restore(self, **context):
        """
        Restore the connection table from a specified backup location.

        This method reads the connections from a CSV file in the specified backup
        location and inserts or updates them in the Apache Airflow database.

        Note that only connections that do not already exist in the database will be
        restored by this method.

        Args:
            **context: Additional Airflow task context parameters.
        """
        csv_file = self.read(context)

        try:
            with settings.Session() as session:
                reader = csv.reader(csv_file)
                new_connections = []

                for connection in reader:
                    existing_connections = (
                        session.query(Connection)
                        .filter(Connection.conn_id == connection[0])
                        .all()
                    )
                    if not existing_connections:
                        port = None
                        if connection[7]:
                            port = int(connection[7])

                        connection = Connection(
                            conn_id=connection[0],
                            conn_type=connection[1],
                            description=connection[2],
                            extra=connection[3],
                            host=connection[4],
                            login=connection[5],
                            password=connection[6],
                            port=port,
                            schema=connection[8],
                        )
                        print(connection)
                        new_connections.append(connection)

                if new_connections:
                    session.add_all(new_connections)
                    session.commit()
        finally:
            csv_file.close()
