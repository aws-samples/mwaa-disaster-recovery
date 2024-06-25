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

from io import StringIO

from airflow import settings
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class ActiveDagTable(BaseTable):
    """
    A class representing the active DAG table that is temporarily created in the Apache
    Airflow metadata database to unpause active dags after the completion of DR
    restore operations.

    This class inherits from the `BaseTable` class and provides methods to backup and restore
    the active DAG table. The active DAG table contains a list of DAG IDs that are currently
    active (not paused) in the Airflow environment.

    Args:
        model (DependencyModel): The dependency model object.
        storage_type (str, optional): The storage type for the backup/restore operation.
        path_prefix (str, optional): The path prefix for the backup/restore operation.
        batch_size (int, optional): The batch size for the backup/restore operation.

    Attributes:
        name (str): The name of the table, which is "active_dag".
    """

    def __init__(
        self,
        model: DependencyModel,
        storage_type: str = None,
        path_prefix: str = None,
        batch_size=5000,
    ):
        super().__init__(
            name="active_dag",
            model=model,
            storage_type=storage_type,
            path_prefix=path_prefix,
            batch_size=batch_size,
        )

    def backup(self, **context):
        """
        Backup the active DAG table to a file.

        This method creates a temporary table `active_dags` containing the DAG IDs of all active
        (not paused) DAGs, excluding the 'backup_metadata' and 'restore_metadata' DAGs. It then
        writes the contents of this table to a file.

        Args:
            **context: Additional context parameters for the backup operation.
        """
        with settings.Session() as session:
            session.execute("DROP TABLE IF EXISTS active_dags")
            session.execute(
                "CREATE TABLE active_dags AS SELECT dag_id FROM dag WHERE NOT is_paused AND is_active AND dag_id NOT IN ('backup_metadata', 'restore_metadata')"
            )
            session.commit()

            rows = session.execute("SELECT * FROM active_dags").all()
            buffer = StringIO("")
            if rows:
                for row in rows:
                    buffer.write(f"{row.dag_id}\n")

            self.write(buffer.getvalue(), context)

    def restore(self, **context):
        """
        Restore the active DAG table from a backup file.

        This method reads the contents of the backup file and inserts the DAG IDs into the
        `active_dags` table. It then updates the `dag` table to unpause the DAGs present in
        the `active_dags` table.

        Args:
            **context: Additional context parameters for the restore operation.
        """
        backup_file = self.read(context)

        try:
            with settings.Session() as session:
                session.execute(
                    "CREATE TABLE IF NOT EXISTS active_dags(dag_id VARCHAR(250))"
                )
                session.commit()

                conn = settings.engine.raw_connection()
                cursor = conn.cursor()
                cursor.copy_expert(
                    "COPY active_dags FROM STDIN WITH (FORMAT CSV, HEADER FALSE)",
                    backup_file,
                )
                conn.commit()
                conn.close()

                session.execute(
                    "UPDATE dag d SET is_paused=false FROM active_dags ad WHERE d.dag_id = ad.dag_id"
                )
                session.commit()
        finally:
            backup_file.close()
