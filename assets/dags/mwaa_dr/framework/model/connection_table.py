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
        session = settings.Session()
        query = session.query(Connection)
        connections = query.all()

        buffer = StringIO("")
        if len(connections) > 0:
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
        session.close()

    def restore(self, **context):
        backup_file = self.read(context)
        session = settings.Session()

        with open(backup_file) as csv_file:
            reader = csv.reader(csv_file)
            new_connections = []

            for connection in reader:
                existing_connections = (
                    session.query(Connection)
                    .filter(Connection.conn_id == connection[0])
                    .all()
                )
                if len(existing_connections) == 0:
                    port = None
                    if len(connection[7]) > 0:
                        port = int(connection[7])

                    new_connections.append(
                        Connection(
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
                    )

            if len(new_connections) > 0:
                session.add_all(new_connections)

        session.commit()
        session.close()
